/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.raydp

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import io.ray.api.ActorHandle
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.spark.executor.RayDPExecutor

/**
 * Tests for the actor-slot bookkeeping in [[ApplicationInfo]].
 *
 * A Ray actor keeps one logical slot across restarts while Spark assigns a new executor id to
 * every restarted generation, so slot accounting and generation accounting have to be asserted
 * independently. Handle checks cover Java object identity, not native reference counting or GC.
 * These tests drive the state machine directly, which makes the disconnect and
 * late-kill orderings deterministic instead of racy.
 */
class ApplicationInfoTest {

  private val Cores = 2
  private val MemoryMB = 512

  /**
   * Records actor shutdowns rather than calling into Ray, which would need a live runtime.
   */
  private class TestApplicationInfo(
      numExecutors: Int,
      appCores: Option[Int] = Some(Cores),
      appMemoryMB: Int = MemoryMB)
    extends ApplicationInfo(
      startTime = 0L,
      id = "app-test",
      desc = ApplicationDescription(
        name = "test",
        numExecutors = numExecutors,
        coresPerExecutor = appCores,
        memoryPerExecutorMB = appMemoryMB,
        rayActorCPU = 1.0,
        command = Command("driver-url", Map.empty, Seq.empty, Seq.empty, Seq.empty)),
      submitDate = new Date(0L),
      driver = null) {

    val exitedActors = new ArrayBuffer[String]

    override protected def exitExecutorActor(handle: ActorHandle[RayDPExecutor]): Unit = {
      exitedActors += handle.toString
    }
  }

  /**
   * A stand-in [[ActorHandle]] that only carries an identity. The production code under test
   * never calls Ray methods on it because [[TestApplicationInfo]] intercepts the shutdown.
   */
  private def handle(actorId: String): ActorHandle[RayDPExecutor] = {
    val invocationHandler = new InvocationHandler {
      override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
        method.getName match {
          case "toString" => actorId
          case "hashCode" => Integer.valueOf(System.identityHashCode(proxy))
          case "equals" => java.lang.Boolean.valueOf(proxy eq args(0))
          case other => throw new UnsupportedOperationException(s"$other unused in tests")
        }
      }
    }
    Proxy.newProxyInstance(
      classOf[ActorHandle[_]].getClassLoader,
      Array[Class[_]](classOf[ActorHandle[_]]),
      invocationHandler).asInstanceOf[ActorHandle[RayDPExecutor]]
  }

  private def newApp(numExecutors: Int = 1): TestApplicationInfo = {
    new TestApplicationInfo(numExecutors)
  }

  private def register(
      app: TestApplicationInfo,
      executorId: String,
      actorId: String): Unit = {
    app.addPendingRegisterExecutor(executorId, actorId, handle(actorId), Cores, MemoryMB)
  }

  @Test
  def existingSlotRetainsItsCreationTimeHandle(): Unit = {
    val app = newApp()
    val owner = handle("23")
    app.addPendingRegisterExecutor("23", "23", owner, Cores, MemoryMB)
    app.kill("23", shutdownActor = false)

    app.addPendingRegisterExecutor("1002", "23", handle("lookup"), Cores, MemoryMB)

    assertSame(owner, app.actorSlots("23").handle)
    assertSame(owner, app.getExecutorHandler("1002").get)
  }

  @Test
  def killReleasesTheSlotOfALiveGeneration(): Unit = {
    val app = newApp()
    register(app, executorId = "23", actorId = "23")
    assertEquals(0, app.numActorsToAdd)

    assertTrue(app.kill("23", shutdownActor = true))

    assertTrue(app.actorSlots.isEmpty)
    assertTrue(app.executors.isEmpty)
    assertEquals(Seq("23"), app.exitedActors.toSeq)
    // The slot is gone, so reconciliation has to create a replacement.
    assertEquals(1, app.numActorsToAdd)
  }

  @Test
  def disconnectKeepsTheSlotAndTombstonesTheGeneration(): Unit = {
    val app = newApp()
    register(app, executorId = "23", actorId = "23")

    assertTrue(app.kill("23", shutdownActor = false))

    // The Spark generation is retired but the Ray actor may still come back.
    assertTrue(app.executors.isEmpty)
    assertEquals(1, app.actorSlots.size)
    assertEquals(0, app.numActorsToAdd)
    assertTrue(app.exitedActors.isEmpty)
    // The retired id still resolves, so a later kill can find the slot.
    assertEquals(Some("23"), app.executorIdToActorId.get("23"))
  }

  @Test
  def killAfterDisconnectWithoutRestartReleasesTheSlot(): Unit = {
    val app = newApp()
    register(app, executorId = "23", actorId = "23")
    app.kill("23", shutdownActor = false)

    // The actor has not re-registered, so this kill must still retire the slot.
    assertTrue(app.kill("23", shutdownActor = true))

    assertTrue(app.actorSlots.isEmpty)
    assertEquals(Seq("23"), app.exitedActors.toSeq)
    assertEquals(1, app.numActorsToAdd)
  }

  @Test
  def lateKillDoesNotTerminateASupersededGeneration(): Unit = {
    val app = newApp()
    register(app, executorId = "23", actorId = "23")
    app.kill("23", shutdownActor = false)
    // Ray restarts the actor, which registers a new Spark generation on the same slot.
    register(app, executorId = "1002", actorId = "23")

    // A KillExecutors delayed past the restart refers to a generation Spark already removed.
    assertTrue(app.kill("23", shutdownActor = true))

    assertEquals(1, app.actorSlots.size)
    assertTrue(app.executors.contains("1002"))
    assertTrue(app.exitedActors.isEmpty)
    assertEquals(0, app.numActorsToAdd)
    // Only the stale tombstone is dropped.
    assertFalse(app.executorIdToActorId.contains("23"))
    assertEquals(Some("23"), app.executorIdToActorId.get("1002"))
  }

  @Test
  def killOfALiveGenerationAlsoClearsItsTombstone(): Unit = {
    val app = newApp()
    register(app, executorId = "23", actorId = "23")
    app.kill("23", shutdownActor = false)
    register(app, executorId = "1002", actorId = "23")

    assertTrue(app.kill("1002", shutdownActor = true))

    assertTrue(app.actorSlots.isEmpty)
    assertTrue(app.executors.isEmpty)
    assertFalse(app.executorIdToActorId.contains("23"))
    assertFalse(app.executorIdToActorId.contains("1002"))
    assertEquals(Seq("23"), app.exitedActors.toSeq)
    assertEquals(1, app.numActorsToAdd)
  }

  @Test
  def restartHistoryStaysBoundedToOneRetiredGeneration(): Unit = {
    val app = newApp()
    register(app, executorId = "23", actorId = "23")
    app.kill("23", shutdownActor = false)
    register(app, executorId = "1002", actorId = "23")
    app.kill("1002", shutdownActor = false)
    register(app, executorId = "1003", actorId = "23")

    // Two restarts must not accumulate two tombstones for the same actor.
    assertFalse(app.executorIdToActorId.contains("23"))
    assertEquals(Some("23"), app.executorIdToActorId.get("1002"))
    assertEquals(Some("23"), app.executorIdToActorId.get("1003"))
    assertEquals(1, app.actorSlots.size)
  }

  @Test
  def restartsDoNotInflateTheSlotCount(): Unit = {
    val app = newApp(numExecutors = 2)
    register(app, executorId = "0", actorId = "0")
    register(app, executorId = "1", actorId = "1")
    assertEquals(0, app.numActorsToAdd)

    // A restart changes the Spark generation, not the number of logical actors.
    app.kill("1", shutdownActor = false)
    register(app, executorId = "1002", actorId = "1")

    assertEquals(2, app.actorSlots.size)
    assertEquals(0, app.numActorsToAdd)
    // The restarted generation maps back to its original actor for named-actor lookup.
    assertEquals(Map("1002" -> "1"), app.getRestartedExecutors)
  }

  @Test
  def consecutiveRestartsRetainTheOriginalOwnerHandle(): Unit = {
    val app = newApp()
    val initialId = app.getNextExecutorId().toString
    register(app, initialId, initialId)
    assertTrue(app.registerExecutor(initialId))
    val owner = app.actorSlots(initialId).handle
    var previousId = initialId

    for (executorId <- Seq("1", "2")) {
      assertTrue(app.kill(previousId, shutdownActor = false))
      assertEquals(Some(executorId), app.addPendingRestartedExecutor(initialId))
      assertSame(owner, app.actorSlots(initialId).handle)
      assertSame(owner, app.getExecutorHandler(executorId).get)
      assertEquals(Some(executorId), app.actorSlots(initialId).currentExecutorId)
      assertEquals(Some(previousId), app.actorSlots(initialId).previousExecutorId)
      assertEquals(Set(executorId), app.executors.keySet.toSet)
      assertEquals(Map(executorId -> initialId), app.getRestartedExecutors)
      assertEquals(1, app.actorSlots.size)
      assertEquals(0, app.numActorsToAdd)
      assertTrue(app.registerExecutor(executorId))
      previousId = executorId
    }
    assertTrue(app.exitedActors.isEmpty)
  }

  @Test
  def removedSlotRejectsLateRestartWithoutAllocatingAnExecutorId(): Unit = {
    val app = newApp()
    val initialId = app.getNextExecutorId().toString
    register(app, initialId, initialId)
    assertTrue(app.kill(initialId, shutdownActor = false))
    assertTrue(app.kill(initialId, shutdownActor = true))
    app.updateDesiredExecutors(0)

    assertEquals(None, app.addPendingRestartedExecutor(initialId))
    assertTrue(app.actorSlots.isEmpty)
    assertTrue(app.executors.isEmpty)
    assertTrue(app.executorIdToActorId.isEmpty)
    assertEquals(Seq(initialId), app.exitedActors.toSeq)
    assertEquals(0, app.numActorsToAdd)
    assertEquals(1, app.getNextExecutorId())
  }

  @Test
  def unknownSlotRejectsRestartWithoutChangingExistingSlots(): Unit = {
    val app = newApp()
    val initialId = app.getNextExecutorId().toString
    register(app, initialId, initialId)
    val owner = app.actorSlots(initialId).handle

    assertEquals(None, app.addPendingRestartedExecutor("unknown"))
    assertSame(owner, app.actorSlots(initialId).handle)
    assertEquals(Set(initialId), app.actorSlots.keySet.toSet)
    assertEquals(Set(initialId), app.executors.keySet.toSet)
    assertEquals(Map(initialId -> initialId), app.executorIdToActorId.toMap)
    assertEquals(1, app.getNextExecutorId())
  }

  @Test
  def restartedExecutorUsesApplicationResources(): Unit = {
    val app = new TestApplicationInfo(1, Some(3), 1024)
    val initialId = app.getNextExecutorId().toString
    app.addPendingRegisterExecutor(initialId, initialId, handle(initialId), 7, 256)
    app.kill(initialId, shutdownActor = false)

    assertEquals(Some("1"), app.addPendingRestartedExecutor(initialId))
    assertEquals(3, app.executors("1").cores)
    assertEquals(1024, app.executors("1").memoryPerExecutorMB)
  }

  @Test
  def restartedExecutorUsesDefaultCoresWhenUnspecified(): Unit = {
    val app = new TestApplicationInfo(1, None, 1024)
    val initialId = app.getNextExecutorId().toString
    app.addPendingRegisterExecutor(initialId, initialId, handle(initialId), 7, 256)
    app.kill(initialId, shutdownActor = false)

    assertEquals(Some("1"), app.addPendingRestartedExecutor(initialId))
    assertEquals(1, app.executors("1").cores)
    assertEquals(1024, app.executors("1").memoryPerExecutorMB)
  }
}

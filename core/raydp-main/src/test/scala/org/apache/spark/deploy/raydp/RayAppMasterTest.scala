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

import io.ray.api.ActorHandle
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertTrue}
import org.junit.jupiter.api.Test

import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}

/**
 * Exercises restart registration through Spark RPC without starting a Ray runtime.
 * These tests check handle identity and slot bookkeeping, not native reference counting or GC.
 */
class RayAppMasterTest {
  // Keep the real slot-removal logic, but record shutdowns instead of submitting Ray actor tasks.
  private class TestApplicationInfo extends ApplicationInfo(
      0L,
      "app-test",
      ApplicationDescription("test", 1, Some(1), 512, 1.0,
        Command("", Map.empty, Seq.empty, Seq.empty, Seq.empty)),
      new Date(0L),
      null) {
    var stoppedActors = 0

    override protected def exitExecutorActor(handle: ActorHandle[RayDPExecutor]): Unit = {
      stoppedActors += 1
    }
  }

  @Test
  def consecutiveRestartsRetainTheOriginalOwnerHandle(): Unit = {
    withApp { (app, endpoint) =>
      val owner = app.actorSlots("0").handle
      var previousId = "0"
      // Each disconnect retires a Spark generation; the same Ray actor keeps slot "0".
      for (executorId <- Seq("1", "2")) {
        assertTrue(app.kill(previousId, shutdownActor = false))
        val reply = endpoint.askSync[AddPendingRestartedExecutorReply](
          RequestAddPendingRestartedExecutor("0"))

        assertEquals(Some(executorId), reply.newExecutorId)
        // Matching actor IDs is insufficient: a lookup could return another wrapper for that
        // actor. The AppMaster must keep the exact original owner handle reachable.
        assertSame(owner, app.actorSlots("0").handle)
        assertSame(owner, app.getExecutorHandler(executorId).get)
        assertEquals(Some(executorId), app.actorSlots("0").currentExecutorId)
        assertEquals(Some(previousId), app.actorSlots("0").previousExecutorId)
        assertEquals(Set(executorId), app.executors.keySet.toSet)
        assertEquals(Map(executorId -> "0"), app.getRestartedExecutors)
        assertEquals(1, app.actorSlots.size)
        assertEquals(0, app.numActorsToAdd)
        assertTrue(app.registerExecutor(executorId))
        previousId = executorId
      }
      assertEquals(0, app.stoppedActors)
    }
  }

  @Test
  def removedSlotRejectsLateRestartWithoutAllocatingAnExecutorId(): Unit = {
    withApp { (app, endpoint) =>
      // Model scale-down after a disconnect: retire the generation, then release its slot.
      assertTrue(app.kill("0", shutdownActor = false))
      assertTrue(app.kill("0", shutdownActor = true))
      app.updateDesiredExecutors(0)

      val reply = endpoint.askSync[AddPendingRestartedExecutorReply](
        RequestAddPendingRestartedExecutor("0"))

      assertEquals(None, reply.newExecutorId)
      assertTrue(app.actorSlots.isEmpty)
      assertTrue(app.executors.isEmpty)
      assertTrue(app.executorIdToActorId.isEmpty)
      assertEquals(1, app.stoppedActors)
      assertEquals(0, app.numActorsToAdd)
      // Only the initial executor consumed ID 0; a rejected request must not consume ID 1.
      assertEquals(1, app.getNextExecutorId())
    }
  }

  @Test
  def unknownSlotRejectsRestartWithoutChangingExistingSlots(): Unit = {
    withApp { (app, endpoint) =>
      val owner = app.actorSlots("0").handle
      // An unrelated actor must not acquire a slot or disturb the existing executor.
      val reply = endpoint.askSync[AddPendingRestartedExecutorReply](
        RequestAddPendingRestartedExecutor("unknown"))

      assertEquals(None, reply.newExecutorId)
      assertSame(owner, app.actorSlots("0").handle)
      assertEquals(Set("0"), app.actorSlots.keySet.toSet)
      assertEquals(Set("0"), app.executors.keySet.toSet)
      assertEquals(Map("0" -> "0"), app.executorIdToActorId.toMap)
      // Only the initial executor consumed ID 0; a rejected request must not consume ID 1.
      assertEquals(1, app.getNextExecutorId())
    }
  }

  private def withApp(test: (TestApplicationInfo, RpcEndpointRef) => Unit): Unit = {
    val master = new RayAppMaster("127.0.0.1", 0, "")
    val envField = classOf[RayAppMaster].getDeclaredField("rpcEnv")
    envField.setAccessible(true)
    val env = envField.get(master).asInstanceOf[RpcEnv]
    try {
      val endpoint = new master.RayAppMasterEndpoint(env)
      // Seed the private application state directly: normal application registration would
      // schedule real Ray actors, which is outside the scope of these RPC tests.
      val appField = endpoint.getClass.getDeclaredFields
        .find(_.getName.endsWith("appInfo")).get
      appField.setAccessible(true)
      val app = new TestApplicationInfo
      val executorId = app.getNextExecutorId().toString
      app.addPendingRegisterExecutor(executorId, executorId, ownerHandle(), 1, 512)
      assertTrue(app.registerExecutor(executorId))
      appField.set(endpoint, app)

      // Exercise the real RPC handler without starting Ray. A restart must reuse the stored
      // owner handle rather than require a named lookup or invoke an actor method.
      test(app, env.setupEndpoint("owner-handle-test", endpoint))
    } finally {
      master.stop()
      env.awaitTermination()
    }
  }

  private def ownerHandle(): ActorHandle[RayDPExecutor] = {
    // This handle is only an identity marker. Any actor invocation fails immediately, so an
    // accidental dependency on native Ray operations cannot silently pass the test.
    val handler = new InvocationHandler {
      override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
        throw new UnsupportedOperationException("Unexpected actor call: " + method.getName)
      }
    }
    Proxy.newProxyInstance(classOf[ActorHandle[_]].getClassLoader,
      Array[Class[_]](classOf[ActorHandle[_]]), handler)
      .asInstanceOf[ActorHandle[RayDPExecutor]]
  }
}

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

import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap}

import io.ray.api.ActorHandle

import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.internal.Logging
import org.apache.spark.raydp.{RayExecutorUtils, SparkOnRayConfigs}
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}


case class ExecutorDesc(
    executorId: String,
    // Ray actors keep their original actor id across restarts, while Spark assigns a new
    // executor id for each restarted executor generation.
    actorId: String,
    cores: Int,
    memoryPerExecutorMB: Int,
    resources: Map[String, ResourceInformation]) {
  var registered: Boolean = false
  var address: Option[RpcAddress] = None
}

private[spark] class ExecutorActorSlot(
    // Retain the creation-time handle until slot release. Named lookups do not keep actors
    // alive in Ray versions with weak lookup semantics (ray-project/ray#45699).
    val handle: ActorHandle[RayDPExecutor],
    var currentExecutorId: Option[String],
    var previousExecutorId: Option[String])

private[spark] class ApplicationInfo(
    val startTime: Long,
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val driver: RpcEndpointRef)
  extends Logging {

  var state: ApplicationState.Value = _
  var executors: HashMap[String, ExecutorDesc] = _
  var addressToExecutorId: HashMap[RpcAddress, String] = _
  // Resolves both current executor ids and the previous-generation tombstone to an actor slot.
  var executorIdToActorId: HashMap[String, String] = _
  var actorSlots: HashMap[String, ExecutorActorSlot] = _
  var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  var coresGranted: Int = _
  var endTime: Long = _
  private var nextExecutorId: Int = _
  // Desired executor target comes from the Spark driver. Actor handles track the Ray actor
  // slots AppMaster still owns, independent of transient Spark executor generations.
  private var desiredExecutors: Int = _

  init()

  private def init(): Unit = {
    state = ApplicationState.WAITING
    executors = new HashMap[String, ExecutorDesc]
    addressToExecutorId = new HashMap[RpcAddress, String]
    executorIdToActorId = new HashMap[String, String]
    actorSlots = new HashMap[String, ExecutorActorSlot]
    endTime = -1L
    nextExecutorId = 0
    desiredExecutors = desc.numExecutors
    removedExecutors = new ArrayBuffer[ExecutorDesc]
  }

  def addPendingRegisterExecutor(
      executorId: String,
      actorId: String,
      handler: ActorHandle[RayDPExecutor],
      cores: Int,
      memoryInMB: Int): Unit = {
    // Adding a pending executor also declares that its Ray actor slot is still active.
    // The supplied handle is used only when creating the slot; existing slots retain theirs.
    // For restarted executors, actorId points back to the original named Ray actor.
    val slot = actorSlots.getOrElseUpdate(
      actorId, new ExecutorActorSlot(handler, None, None))
    slot.currentExecutorId = Some(executorId)
    val desc = ExecutorDesc(executorId, actorId, cores, memoryInMB, null)
    executors(executorId) = desc
    // Keep the previous generation mapping until the next disconnect or explicit actor shutdown.
    executorIdToActorId(executorId) = actorId
  }

  // A restart reuses the actor slot and allocates a new Spark executor ID.
  def addPendingRestartedExecutor(actorId: String): Option[String] = {
    actorSlots.get(actorId) match {
      case Some(slot) =>
        val executorId = getNextExecutorId().toString
        val cores = desc.coresPerExecutor.getOrElse(
          SparkOnRayConfigs.DEFAULT_SPARK_CORES_PER_EXECUTOR)
        addPendingRegisterExecutor(
          executorId, actorId, slot.handle, cores, desc.memoryPerExecutorMB)
        Some(executorId)
      case None =>
        logWarning(
          s"Rejecting executor restart for actor $actorId in application $id: " +
            "no active actor slot (unknown or removed).")
        None
    }
  }

  def updateDesiredExecutors(numExecutors: Int): Unit = {
    desiredExecutors = math.max(0, numExecutors)
  }

  def numActorsToAdd: Int = {
    math.max(0, desiredExecutors - actorSlots.size)
  }

  // Compatibility view for ObjectStoreWriter: map restarted Spark executor ids back to
  // the original Ray actor ids used in named actor lookup.
  def getRestartedExecutors: Map[String, String] = {
    executors.collect {
      case (executorId, desc) if desc.actorId != executorId =>
        executorId -> desc.actorId
    }.toMap
  }

  def registerExecutor(executorId: String): Boolean = {
    if (executors.contains(executorId)) {
      if (executors(executorId).registered) {
        logWarning(s"Try to register executor: ${executorId} twice")
        false
      } else {
        executors(executorId).registered = true
        true
      }
    } else {
      logWarning(s"Try to register executor: ${executorId} which is not existed")
      false
    }
  }

  def markExecutorStarted(executorId: String, address: RpcAddress): Unit = {
    executors.get(executorId).foreach { exec =>
      exec.address = Some(address)
      addressToExecutorId(address) = executorId
    }
  }

  def kill(address: RpcAddress, shutdownActor: Boolean): Boolean = {
    addressToExecutorId.get(address).exists(kill(_, shutdownActor))
  }

  def kill(executorId: String, shutdownActor: Boolean): Boolean = {
    val actorIdOpt = executorIdToActorId.get(executorId)

    actorIdOpt.foreach { actorId =>
      if (shutdownActor) {
        // One pass over the slot decides whether it survives this kill.
        actorSlots.updateWith(actorId) {
          // A tombstone whose actor has already re-registered refers to a generation Spark itself
          // removed on disconnect. Spark tracks the newer generation as a separate executor, so a
          // late kill for the retired id must not terminate it: drop only the tombstone and keep
          // the slot so Spark can kill that generation through its own executor id.
          case Some(slot) if slot.currentExecutorId.exists(_ != executorId) =>
            executorIdToActorId.remove(executorId)
            if (slot.previousExecutorId.contains(executorId)) {
              slot.previousExecutorId = None
            }
            Some(slot)
          // Otherwise no live generation remains, so retire every generation mapped to the actor
          // and release the slot.
          case slotOpt =>
            val executorIds = slotOpt.map { slot =>
              Set(executorId) ++ slot.currentExecutorId ++ slot.previousExecutorId
            }.getOrElse(Set(executorId))
            executorIds.foreach { id =>
              removeExecutorGeneration(id)
              executorIdToActorId.remove(id)
            }
            // Only explicit Spark/AppMaster shutdown releases the Ray actor slot. A disconnect
            // caused by actor failure keeps the slot so Ray can restart it and register a new
            // executor id.
            // Previously we used to exitExecutor for all scenarios, but it will cause
            // the following issue when a executor is down because of OOM issue:
            // - Executor E1 dies at T0 lets say because of OOm
            // - We try to kill it by firing stop call on E1 actor
            // - Since the actor is not available, the stop task fails for E1
            // - In the mean while, ray brings up the lost executor E1
            // - The failed task (stop task) gets retried as there are task retries configured.
            // - The stop task gets fired on the new executor which got recovered
            // - The Recovered executor exits with status as user intended exit.
            slotOpt.foreach(slot => exitExecutorActor(slot.handle))
            None
        }
      } else {
        removeExecutorGeneration(executorId)
        actorSlots.get(actorId).foreach { slot =>
          if (slot.currentExecutorId.contains(executorId)) {
            // Replace the older tombstone so restart history stays bounded to one generation.
            slot.previousExecutorId.foreach(executorIdToActorId.remove)
            slot.currentExecutorId = None
            slot.previousExecutorId = Some(executorId)
          }
        }
        executorIdToActorId(executorId) = actorId
      }
    }
    actorIdOpt.isDefined
  }

  private def removeExecutorGeneration(executorId: String): Unit = {
    executors.remove(executorId).foreach { exec =>
      removedExecutors += exec
      coresGranted -= exec.cores
      exec.address.foreach(addressToExecutorId.remove)
    }
  }

  // Visible for testing. Shutting an executor down needs a live Ray actor handle, which unit
  // tests cannot construct, so the Ray call is isolated behind this method.
  protected def exitExecutorActor(handle: ActorHandle[RayDPExecutor]): Unit = {
    RayExecutorUtils.exitExecutor(handle)
  }

  def getExecutorHandler(
      executorId: String): Option[ActorHandle[RayDPExecutor]] = {
    executorIdToActorId.get(executorId).flatMap(actorSlots.get).map(_.handle)
  }

  def getNextExecutorId(): Int = {
    val previous = nextExecutorId
    nextExecutorId += 1
    previous
  }

  private var _retryCount: Int = 0

  def retryCount: Int = _retryCount

  def incrementRetryCount(): Int = {
    _retryCount += 1
    _retryCount
  }

  def resetRetryCount(): Unit = _retryCount = 0

  def markFinished(endState: ApplicationState.Value): Unit = {
    state = endState
    endTime = System.currentTimeMillis()
  }

  def isFinished: Boolean = {
    state != ApplicationState.WAITING && state != ApplicationState.RUNNING
  }

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
}

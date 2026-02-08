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

package org.apache.spark

import org.apache.spark.executor.{CoarseGrainedExecutorBackend, RayCoarseGrainedExecutorBackend, RayDPExecutorBackendFactory, TaskMetrics}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv

import java.net.URL
import java.util.concurrent.atomic.AtomicLong

object Spark411Helper {
  private val nextTaskAttemptId = new AtomicLong(1000000L)
  def getExecutorBackendFactory(): RayDPExecutorBackendFactory = {
    new RayDPExecutorBackendFactory {
      override def createExecutorBackend(
          rpcEnv: RpcEnv,
          driverUrl: String,
          executorId: String,
          bindAddress: String,
          hostname: String,
          cores: Int,
          userClassPath: Seq[URL],
          env: SparkEnv,
          resourcesFileOpt: Option[String],
          resourceProfile: ResourceProfile): CoarseGrainedExecutorBackend = {
        new RayCoarseGrainedExecutorBackend(
          rpcEnv,
          driverUrl,
          executorId,
          bindAddress,
          hostname,
          cores,
          userClassPath,
          env,
          resourcesFileOpt,
          resourceProfile)
      }
    }
  }

  def setTaskContext(ctx: TaskContext): Unit = TaskContext.setTaskContext(ctx)

  def unsetTaskContext(): Unit = TaskContext.unset()

  def getDummyTaskContext(partitionId: Int, env: SparkEnv): TaskContext = {
    val taskAttemptId = nextTaskAttemptId.getAndIncrement()
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = partitionId,
      taskAttemptId = taskAttemptId,
      attemptNumber = 0,
      numPartitions = 0,
      taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskAttemptId),
      localProperties = new java.util.Properties,
      metricsSystem = env.metricsSystem,
      taskMetrics = TaskMetrics.empty,
      cpus = 0,
      resources = Map.empty
    )
  }
}

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

package org.apache.spark.sql.raydp

import com.intel.raydp.shims.SparkShimLoader
import io.ray.api.{ActorHandle, Ray}
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.{RayDPException, SparkContext, SparkEnv}
import org.apache.spark.deploy.raydp._
import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.raydp.RayExecutorUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel}

object ObjectStoreWriter {
  var driverAgent: RayDPDriverAgent = _
  var driverAgentUrl: String = _

  def connectToRay(): Unit = {
    if (!Ray.isInitialized) {
      Ray.init()
      // restore log level to WARN since it's inside Spark driver
      SparkContext.getOrCreate().setLogLevel("WARN")
      driverAgent = new RayDPDriverAgent()
      driverAgentUrl = driverAgent.getDriverAgentEndpointUrl
    }
  }

  def toArrowSchema(df: DataFrame): Schema = {
    val conf = df.queryExecution.sparkSession.sessionState.conf
    val timeZoneId = conf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE)
    val largeVarTypes = conf.arrowUseLargeVarTypes
    SparkShimLoader.getSparkShims.toArrowSchema(df.schema, timeZoneId, largeVarTypes)
  }

  /**
   * Prepare a Spark ArrowBatch RDD for recoverable conversion and return metadata needed by
   * Python to build reconstructable Ray Dataset blocks via Ray tasks.
   *
   * This method:
   * - persists and materializes the ArrowBatch RDD in Spark (so partitions can be re-fetched)
   * - computes per-partition executor locations (Spark executor IDs)
   *
   * It does NOT push any data to Ray.
   */
  def prepareRecoverableRDD(
      df: DataFrame,
      storageLevel: StorageLevel): RecoverableRDDInfo = {
    if (!Ray.isInitialized) {
      throw new RayDPException(
        "Not yet connected to Ray! Please set fault_tolerant_mode=True when starting RayDP.")
    }

    val rdd = SparkShimLoader.getSparkShims.toArrowBatchRdd(df)
    rdd.persist(storageLevel)
    rdd.count()

    var executorIds = df.sparkSession.sparkContext.getExecutorIds.toArray
    val numExecutors = executorIds.length
    val appMasterHandle = Ray.getActor(RayAppMaster.ACTOR_NAME)
                             .get.asInstanceOf[ActorHandle[RayAppMaster]]
    val restartedExecutors = RayAppMasterUtils.getRestartedExecutors(appMasterHandle)
    if (!restartedExecutors.isEmpty) {
      for (i <- 0 until numExecutors) {
        if (restartedExecutors.containsKey(executorIds(i))) {
          val oldId = restartedExecutors.get(executorIds(i))
          executorIds(i) = oldId
        }
      }
    }

    val schemaJson = ObjectStoreWriter.toArrowSchema(df).toJson
    val numPartitions = rdd.getNumPartitions

    val handles = executorIds.map { id =>
      Ray.getActor("raydp-executor-" + id)
         .get
         .asInstanceOf[ActorHandle[RayDPExecutor]]
    }
    val locations = RayExecutorUtils.getBlockLocations(handles(0), rdd.id, numPartitions)

    RecoverableRDDInfo(rdd.id, numPartitions, schemaJson, driverAgentUrl, locations)
  }

  /**
   * Streaming variant: starts materialization in a background thread and returns
   * a handle that Python can poll for completed partitions.  This lets Ray fetch
   * tasks overlap with Spark partition computation instead of blocking on rdd.count().
   */
  def startStreamingRecoverableRDD(
      df: DataFrame,
      storageLevel: StorageLevel): StreamingRecoverableRDD = {
    if (!Ray.isInitialized) {
      throw new RayDPException(
        "Not yet connected to Ray! Please set fault_tolerant_mode=True when starting RayDP.")
    }

    val rdd = SparkShimLoader.getSparkShims.toArrowBatchRdd(df)
    rdd.persist(storageLevel)

    val appMasterHandle = Ray.getActor(RayAppMaster.ACTOR_NAME)
                             .get.asInstanceOf[ActorHandle[RayAppMaster]]
    val restartedExecutors = RayAppMasterUtils.getRestartedExecutors(appMasterHandle)

    val schemaJson = ObjectStoreWriter.toArrowSchema(df).toJson
    val numPartitions = rdd.getNumPartitions

    val handle = new StreamingRecoverableRDD(
      rdd.id, numPartitions, schemaJson, driverAgentUrl,
      restartedExecutors, SparkEnv.get)

    // Start materialization in background — partitions become visible via getReadyPartitions()
    val thread = new Thread("raydp-materialize-" + rdd.id) {
      setDaemon(true)
      override def run(): Unit = {
        try {
          rdd.count()
        } catch {
          case e: Throwable => handle.setError(e)
        } finally {
          handle.setComplete()
        }
      }
    }
    thread.start()

    handle
  }

}

case class RecoverableRDDInfo(
    rddId: Int,
    numPartitions: Int,
    schemaJson: String,
    driverAgentUrl: String,
    locations: Array[String])

object RecoverableRDDInfo {
  // Empty constructor for reflection / Java interop (some tools expect it).
  def empty: RecoverableRDDInfo = RecoverableRDDInfo(0, 0, "", "", Array.empty[String])
}

/**
 * Handle returned by [[ObjectStoreWriter.startStreamingRecoverableRDD]].
 * Python polls [[getReadyPartitions]] to discover which partitions have been
 * materialized in Spark's BlockManager, then immediately submits Ray fetch
 * tasks for those partitions — overlapping Spark computation with Ray transfer.
 */
class StreamingRecoverableRDD(
    val rddId: Int,
    val numPartitions: Int,
    val schemaJson: String,
    val driverAgentUrl: String,
    private val restartedExecutors: java.util.Map[String, String],
    private val env: SparkEnv) {

  @volatile private var _error: Throwable = _
  @volatile private var _complete: Boolean = false

  private val blockIds: Array[BlockId] = (0 until numPartitions).map(i =>
    BlockId.apply("rdd_" + rddId + "_" + i)
  ).toArray

  def setError(e: Throwable): Unit = { _error = e }
  def setComplete(): Unit = { _complete = true }

  def isComplete: Boolean = _complete
  def getError: String = if (_error != null) _error.getMessage else null

  /**
   * Returns an Array[String] of length numPartitions.
   * For materialized partitions: the (mapped) executor ID suitable for Ray actor lookup.
   * For not-yet-ready partitions: null.
   */
  def getReadyPartitions(): Array[String] = {
    val locations = BlockManager.blockIdsToLocations(blockIds, env)
    val result = new Array[String](numPartitions)
    for ((key, value) <- locations if value.nonEmpty) {
      val partitionId = key.name.substring(key.name.lastIndexOf('_') + 1).toInt
      var executorId = value(0).substring(value(0).lastIndexOf('_') + 1)
      if (restartedExecutors.containsKey(executorId)) {
        executorId = restartedExecutors.get(executorId)
      }
      result(partitionId) = executorId
    }
    result
  }
}


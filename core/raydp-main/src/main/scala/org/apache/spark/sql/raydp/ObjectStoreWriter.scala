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
import io.ray.api.{ActorHandle, ObjectRef, Ray}
import io.ray.runtime.AbstractRayRuntime
import java.util.{List, UUID}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.function.{Function => JFunction}
import org.apache.arrow.vector.types.pojo.Schema
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.{RayDPException, SparkContext}
import org.apache.spark.deploy.raydp._
import org.apache.spark.executor.RayDPExecutor
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.raydp.{RayDPUtils, RayExecutorUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

class ObjectStoreWriter(@transient val df: DataFrame) extends Serializable {

  val uuid: UUID = ObjectStoreWriter.dfToId.getOrElseUpdate(df, UUID.randomUUID())

  /**
   * For test.
   */
  def getRandomRef(): List[Array[Byte]] = {

    df.queryExecution.toRdd.mapPartitions { _ =>
      Iterator(ObjectRefHolder.getRandom(uuid))
    }.collect().toSeq.asJava
  }

  def clean(): Unit = {
    ObjectStoreWriter.dfToId.remove(df)
    ObjectRefHolder.removeQueue(uuid)
  }

}

object ObjectStoreWriter {
  val dfToId = new mutable.HashMap[DataFrame, UUID]()
  private val recoverableRDDs = new ConcurrentHashMap[Integer, RDD[_]]()
  var driverAgent: RayDPDriverAgent = _
  var driverAgentUrl: String = _
  var address: Array[Byte] = null

  def connectToRay(): Unit = {
    if (!Ray.isInitialized) {
      Ray.init()
      // restore log level to WARN since it's inside Spark driver
      SparkContext.getOrCreate().setLogLevel("WARN")
      driverAgent = new RayDPDriverAgent()
      driverAgentUrl = driverAgent.getDriverAgentEndpointUrl
    }
  }

  private def parseMemoryBytes(value: String): Double = {
    if (value == null || value.isEmpty) {
      0.0
    } else {
      // Spark parser supports both plain numbers (bytes) and strings like "100M", "2g".
      JavaUtils.byteStringAsBytes(value).toDouble
    }
  }

  def getAddress(): Array[Byte] = {
    if (address == null) {
      val objectRef = Ray.put(1)
      val objectRefImpl = RayDPUtils.convert(objectRef)
      val objectId = objectRefImpl.getId
      val runtime = Ray.internal.asInstanceOf[AbstractRayRuntime]
      address = runtime.getObjectStore.getOwnershipInfo(objectId)
    }
    address
  }

  def toArrowSchema(df: DataFrame): Schema = {
    val conf = df.queryExecution.sparkSession.sessionState.conf
    val timeZoneId = conf.getConf(SQLConf.SESSION_LOCAL_TIMEZONE)
    SparkShimLoader.getSparkShims.toArrowSchema(df.schema, timeZoneId)
  }

  @deprecated
  def fromSparkRDD(df: DataFrame, storageLevel: StorageLevel): Array[Array[Byte]] = {
    if (!Ray.isInitialized) {
      throw new RayDPException(
        "Not yet connected to Ray! Please set fault_tolerant_mode=True when starting RayDP.")
    }
    val uuid = dfToId.getOrElseUpdate(df, UUID.randomUUID())
    val queue = ObjectRefHolder.getQueue(uuid)
    val rdd = df.toArrowBatchRdd
    rdd.persist(storageLevel)
    rdd.count()
    var executorIds = df.sqlContext.sparkContext.getExecutorIds.toArray
    val numExecutors = executorIds.length
    val appMasterHandle = Ray.getActor(RayAppMaster.ACTOR_NAME)
                             .get.asInstanceOf[ActorHandle[RayAppMaster]]
    val restartedExecutors = RayAppMasterUtils.getRestartedExecutors(appMasterHandle)
    // Check if there is any restarted executors
    if (!restartedExecutors.isEmpty) {
      // If present, need to use the old id to find ray actors
      for (i <- 0 until numExecutors) {
        if (restartedExecutors.containsKey(executorIds(i))) {
          val oldId = restartedExecutors.get(executorIds(i))
          executorIds(i) = oldId
        }
      }
    }
    val schema = ObjectStoreWriter.toArrowSchema(df).toJson
    val numPartitions = rdd.getNumPartitions
    val results = new Array[Array[Byte]](numPartitions)
    val refs = new Array[ObjectRef[Array[Byte]]](numPartitions)
    val handles = executorIds.map {id =>
      Ray.getActor("raydp-executor-" + id)
         .get
         .asInstanceOf[ActorHandle[RayDPExecutor]]
    }
    val handlesMap = (executorIds zip handles).toMap
    val locations = RayExecutorUtils.getBlockLocations(
        handles(0), rdd.id, numPartitions)
    for (i <- 0 until numPartitions) {
      // TODO use getPreferredLocs, but we don't have a host ip to actor table now
      refs(i) = RayExecutorUtils.getRDDPartition(
          handlesMap(locations(i)), rdd.id, i, schema, driverAgentUrl)
      queue.add(refs(i))
    }
    for (i <- 0 until numPartitions) {
      results(i) = RayDPUtils.convert(refs(i)).getId.getBytes
    }
    results
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

    val rdd = df.toArrowBatchRdd
    rdd.persist(storageLevel)
    rdd.count()
    // Keep a strong reference so Spark's ContextCleaner does not GC the cached blocks
    // before Ray tasks fetch them.
    recoverableRDDs.put(rdd.id, rdd)

    var executorIds = df.sqlContext.sparkContext.getExecutorIds.toArray
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
   * Release the strong reference to a recoverable RDD and unpersist it.
   * Call this after all Ray tasks have finished fetching the cached blocks,
   * or when the dataset is no longer needed.
   */
  def releaseRecoverableRDD(rddId: Int): Unit = {
    val rdd = recoverableRDDs.remove(rddId)
    if (rdd != null) {
      rdd.unpersist()
    }
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

object ObjectRefHolder {
  type Queue = ConcurrentLinkedQueue[ObjectRef[Array[Byte]]]
  private val dfToQueue = new ConcurrentHashMap[UUID, Queue]()

  def getQueue(df: UUID): Queue = {
    dfToQueue.computeIfAbsent(df, new JFunction[UUID, Queue] {
      override def apply(v1: UUID): Queue = {
        new Queue()
      }
    })
  }

  @inline
  def checkQueueExists(df: UUID): Queue = {
    val queue = dfToQueue.get(df)
    if (queue == null) {
      throw new RuntimeException("The DataFrame does not exist")
    }
    queue
  }

  def getQueueSize(df: UUID): Int = {
    val queue = checkQueueExists(df)
    queue.size()
  }

  def getRandom(df: UUID): Array[Byte] = {
    val queue = checkQueueExists(df)
    val ref = RayDPUtils.convert(queue.peek())
    ref.get()
  }

  def removeQueue(df: UUID): Unit = {
    dfToQueue.remove(df)
  }

  def clean(): Unit = {
    dfToQueue.clear()
  }
}

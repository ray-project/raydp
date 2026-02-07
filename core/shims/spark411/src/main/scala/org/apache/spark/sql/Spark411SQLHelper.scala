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

package org.apache.spark.sql

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}

object Spark411SQLHelper {
  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
  }

  def toArrowBatchRdd(df: DataFrame): org.apache.spark.rdd.RDD[Array[Byte]] = {
    val conf = df.sparkSession.asInstanceOf[ClassicSparkSession].sessionState.conf
    val timeZoneId = conf.sessionLocalTimeZone
    val maxRecordsPerBatch = conf.getConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH)
    val schema = df.schema
    df.queryExecution.toRdd.mapPartitions(iter => {
      val context = TaskContext.get()
      ArrowConverters.toBatchIterator(
        iter,
        schema,
        maxRecordsPerBatch,
        timeZoneId,
        true, // errorOnDuplicatedFieldNames
        false, // largeVarTypes
        context)
    })
  }

  /**
   * Converts a JavaRDD of Arrow batches (serialized as byte arrays) to a DataFrame.
   * This is the reverse operation of toArrowBatchRdd.
   *
   * @param rdd     JavaRDD containing Arrow batches serialized as byte arrays
   * @param schema  JSON string representation of the StructType schema
   * @param session SparkSession to use for DataFrame creation
   * @return DataFrame reconstructed from the Arrow batches
   */
  def toDataFrame(rdd: JavaRDD[Array[Byte]], schema: String, session: SparkSession): DataFrame = {
    val structType = DataType.fromJson(schema).asInstanceOf[StructType]
    val classicSession = session.asInstanceOf[ClassicSparkSession]
    
    // Capture timezone on driver side - cannot access sessionState on executors
    val timeZoneId = classicSession.sessionState.conf.sessionLocalTimeZone

    // Create an RDD of InternalRow by deserializing Arrow batches per partition
    val rowRdd = rdd.rdd.flatMap { arrowBatch =>
      ArrowConverters.fromBatchIterator(
        Iterator(arrowBatch),
        structType,
        timeZoneId,  // Use captured value, not sessionState
        true,  // errorOnDuplicatedFieldNames
        false, // largeVarTypes
        TaskContext.get()
      )
    }

    classicSession.internalCreateDataFrame(rowRdd, structType)
  }
}

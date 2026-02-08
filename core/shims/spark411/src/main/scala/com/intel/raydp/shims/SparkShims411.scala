package com.intel.raydp.shims

import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.{Spark411Helper, SparkEnv, TaskContext}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.executor.RayDPExecutorBackendFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Spark411SQLHelper, SparkSession}

class SparkShims411 extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimDescriptor(4, 1, 1)

  override def toDataFrame(rdd: JavaRDD[Array[Byte]], schema: String, session: SparkSession): DataFrame = {
    Spark411SQLHelper.toDataFrame(rdd, schema, session)
  }

  override def getExecutorBackendFactory(): RayDPExecutorBackendFactory = {
    Spark411Helper.getExecutorBackendFactory
  }

  override def getDummyTaskContext(partitionId: Int, env: SparkEnv): TaskContext = {
    Spark411Helper.getDummyTaskContext(partitionId, env)
  }

  override def toArrowSchema(schema: StructType, timeZoneId: String, largeVarTypes: Boolean = false): Schema = {
    Spark411SQLHelper.toArrowSchema(schema, timeZoneId, largeVarTypes)
  }

  override def toArrowBatchRdd(df: DataFrame): RDD[Array[Byte]] = {
    Spark411SQLHelper.toArrowBatchRdd(df)
  }
}

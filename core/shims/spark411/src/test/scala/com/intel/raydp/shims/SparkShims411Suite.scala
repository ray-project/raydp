package com.intel.raydp.shims

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, ZoneId}

import scala.reflect.classTag

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.{Spark411Helper, SparkEnv, TaskContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

class SparkShims411Suite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkShims411Suite")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    super.afterAll()
  }

  test("shim descriptor returns 4.1.1") {
    val shim = new SparkShims411()
    val descriptor = shim.getShimDescriptor
    assert(descriptor.isInstanceOf[SparkShimDescriptor])
    val sparkDescriptor = descriptor.asInstanceOf[SparkShimDescriptor]
    assert(sparkDescriptor.major === 4)
    assert(sparkDescriptor.minor === 1)
    assert(sparkDescriptor.patch === 1)
    assert(sparkDescriptor.toString === "4.1.1")
  }

  test("provider matches 4.1 versions") {
    val provider = new spark411.SparkShimProvider()
    assert(provider.matches("4.1.1"))
    assert(provider.matches("4.1.0"))
    assert(!provider.matches("3.5.0"))
    assert(!provider.matches("4.0.0"))
  }

  test("provider creates SparkShims411 instance") {
    val provider = new spark411.SparkShimProvider()
    val shim = provider.createShim
    assert(shim.isInstanceOf[SparkShims411])
  }

  test("SPI service loading works") {
    SparkShimLoader.setSparkShimProviderClass(
      "com.intel.raydp.shims.spark411.SparkShimProvider")
    val shim = SparkShimLoader.getSparkShims
    assert(shim.isInstanceOf[SparkShims411])
    assert(shim.getShimDescriptor.toString === "4.1.1")
  }

  test("toArrowSchema produces valid Arrow schema") {
    val shim = new SparkShims411()
    val sparkSchema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("value", DoubleType)

    val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone
    val arrowSchema = shim.toArrowSchema(sparkSchema, timeZoneId)

    assert(arrowSchema.getFields.size() === 3)
    assert(arrowSchema.getFields.get(0).getName === "id")
    assert(arrowSchema.getFields.get(1).getName === "name")
    assert(arrowSchema.getFields.get(2).getName === "value")
  }

  test("Arrow round-trip: DataFrame to ArrowBatch to DataFrame") {
    val shim = new SparkShims411()

    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("value", DoubleType)
    val rows = java.util.Arrays.asList(
      Row(1, "alice", 10.0),
      Row(2, "bob", 20.0),
      Row(3, "carol", 30.0))
    val original = spark.createDataFrame(rows, schema)

    val arrowRdd = shim.toArrowBatchRdd(original)
    val schemaJson = original.schema.json

    val restored = shim.toDataFrame(
      arrowRdd.toJavaRDD(), schemaJson, spark)

    val originalRows = original.collect().sortBy(_.getInt(0))
    val restoredRows = restored.collect().sortBy(_.getInt(0))

    assert(originalRows.length === restoredRows.length)
    originalRows.zip(restoredRows).foreach { case (orig, rest) =>
      assert(orig.getInt(0) === rest.getInt(0))
      assert(orig.getString(1) === rest.getString(1))
      assert(orig.getDouble(2) === rest.getDouble(2))
    }
  }

  test("toArrowSchema maps Spark types to correct Arrow types") {
    val shim = new SparkShims411()
    val sparkSchema = new StructType()
      .add("bool", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("str", StringType)
      .add("bin", BinaryType)

    val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone
    val arrowSchema = shim.toArrowSchema(sparkSchema, timeZoneId)
    val fields = arrowSchema.getFields

    assert(fields.get(0).getType.isInstanceOf[ArrowType.Bool])
    assert(fields.get(1).getType === new ArrowType.Int(8, true))
    assert(fields.get(2).getType === new ArrowType.Int(16, true))
    assert(fields.get(3).getType === new ArrowType.Int(32, true))
    assert(fields.get(4).getType === new ArrowType.Int(64, true))
    assert(fields.get(5).getType === new ArrowType.FloatingPoint(
      org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE))
    assert(fields.get(6).getType === new ArrowType.FloatingPoint(
      org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))
    assert(fields.get(7).getType.isInstanceOf[ArrowType.Utf8])
    assert(fields.get(8).getType.isInstanceOf[ArrowType.Binary])
  }

  test("Arrow round-trip preserves null values") {
    val shim = new SparkShims411()

    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("value", DoubleType, nullable = true)
    val rows = java.util.Arrays.asList(
      Row(1, "alice", 10.0),
      Row(2, null, null),
      Row(3, "carol", 30.0))
    val original = spark.createDataFrame(rows, schema)

    val arrowRdd = shim.toArrowBatchRdd(original)
    val schemaJson = original.schema.json
    val restored = shim.toDataFrame(arrowRdd.toJavaRDD(), schemaJson, spark)

    val restoredRows = restored.collect().sortBy(_.getInt(0))
    assert(restoredRows.length === 3)
    assert(restoredRows(0).getString(1) === "alice")
    assert(restoredRows(1).isNullAt(1))
    assert(restoredRows(1).isNullAt(2))
    assert(restoredRows(2).getDouble(2) === 30.0)
  }

  test("Arrow round-trip with multiple batches") {
    val shim = new SparkShims411()

    // Force small batches: 2 records per batch
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "2")
    try {
      val schema = new StructType()
        .add("id", IntegerType)
        .add("label", StringType)
      val rows = (1 to 10).map(i => Row(i, s"row_$i"))
      val original = spark.createDataFrame(
        java.util.Arrays.asList(rows: _*), schema)
        .repartition(1) // single partition so multiple batches are in one partition

      val arrowRdd = shim.toArrowBatchRdd(original)
      val schemaJson = original.schema.json
      val restored = shim.toDataFrame(arrowRdd.toJavaRDD(), schemaJson, spark)

      val restoredRows = restored.collect().sortBy(_.getInt(0))
      assert(restoredRows.length === 10)
      (1 to 10).foreach { i =>
        assert(restoredRows(i - 1).getInt(0) === i)
        assert(restoredRows(i - 1).getString(1) === s"row_$i")
      }
    } finally {
      spark.conf.unset("spark.sql.execution.arrow.maxRecordsPerBatch")
    }
  }

  test("Arrow round-trip preserves Timestamp and Date values") {
    val shim = new SparkShims411()

    val ts1 = Timestamp.valueOf("2025-01-15 10:30:00")
    val ts2 = Timestamp.valueOf("2025-06-30 23:59:59")
    val d1 = Date.valueOf("2025-01-15")
    val d2 = Date.valueOf("2025-06-30")

    val schema = new StructType()
      .add("id", IntegerType)
      .add("ts", TimestampType)
      .add("dt", DateType)
    val rows = java.util.Arrays.asList(
      Row(1, ts1, d1),
      Row(2, ts2, d2))
    val original = spark.createDataFrame(rows, schema)

    val arrowRdd = shim.toArrowBatchRdd(original)
    val schemaJson = original.schema.json
    val restored = shim.toDataFrame(arrowRdd.toJavaRDD(), schemaJson, spark)

    val restoredRows = restored.collect().sortBy(_.getInt(0))
    assert(restoredRows.length === 2)
    assert(restoredRows(0).getTimestamp(1) === ts1)
    assert(restoredRows(0).getDate(2) === d1)
    assert(restoredRows(1).getTimestamp(1) === ts2)
    assert(restoredRows(1).getDate(2) === d2)
  }

  test("Arrow round-trip preserves Decimal values") {
    val shim = new SparkShims411()

    val schema = new StructType()
      .add("id", IntegerType)
      .add("price", DecimalType(18, 6))
      .add("quantity", DecimalType(10, 0))
    val rows = java.util.Arrays.asList(
      Row(1, new java.math.BigDecimal("12345.678900"), new java.math.BigDecimal("100")),
      Row(2, new java.math.BigDecimal("0.000001"), new java.math.BigDecimal("0")),
      Row(3, new java.math.BigDecimal("-9999.999999"), new java.math.BigDecimal("999")))
    val original = spark.createDataFrame(rows, schema)

    val arrowRdd = shim.toArrowBatchRdd(original)
    val schemaJson = original.schema.json
    val restored = shim.toDataFrame(arrowRdd.toJavaRDD(), schemaJson, spark)

    val restoredRows = restored.collect().sortBy(_.getInt(0))
    assert(restoredRows.length === 3)
    assert(restoredRows(0).getDecimal(1).compareTo(
      new java.math.BigDecimal("12345.678900")) === 0)
    assert(restoredRows(1).getDecimal(1).compareTo(
      new java.math.BigDecimal("0.000001")) === 0)
    assert(restoredRows(2).getDecimal(1).compareTo(
      new java.math.BigDecimal("-9999.999999")) === 0)
    assert(restoredRows(2).getDecimal(2).compareTo(
      new java.math.BigDecimal("999")) === 0)
  }

  test("Arrow round-trip with empty DataFrame") {
    val shim = new SparkShims411()

    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
    val rows = java.util.Arrays.asList[Row]()
    val original = spark.createDataFrame(rows, schema)

    val arrowRdd = shim.toArrowBatchRdd(original)
    val schemaJson = original.schema.json
    val restored = shim.toDataFrame(arrowRdd.toJavaRDD(), schemaJson, spark)

    assert(restored.collect().length === 0)
    assert(restored.schema === original.schema)
  }

  test("getDummyTaskContext returns valid TaskContext") {
    val shim = new SparkShims411()
    val env = SparkEnv.get

    val ctx = shim.getDummyTaskContext(42, env)

    assert(ctx.isInstanceOf[TaskContext])
    assert(ctx.partitionId() === 42)
    assert(ctx.stageId() === 0)
    assert(ctx.attemptNumber() === 0)
  }

  test("getDummyTaskContext generates unique taskAttemptIds") {
    val shim = new SparkShims411()
    val env = SparkEnv.get

    val ids = (0 until 100).map { i =>
      shim.getDummyTaskContext(i, env).taskAttemptId()
    }

    assert(ids.distinct.size === 100, "all taskAttemptIds must be unique")
    assert(ids.forall(_ >= 1000000L), "taskAttemptIds must start at 1000000+")
  }

  test("BlockManager.get NPEs without registerTask (regression)") {
    // This documents the exact NPE bug we fixed: Spark 4.1's BlockInfoManager
    // requires tasks to be registered before they can acquire read locks.
    // Setting a TaskContext with a specific taskAttemptId WITHOUT calling
    // registerTask causes lockForReading to NPE on:
    //   readLocksByTask.get(taskAttemptId).add(blockId)
    // Note: if NO TaskContext were set, BlockInfoManager would use the
    // pre-registered NON_TASK_WRITER (-1024) and no NPE would occur.
    val env = SparkEnv.get
    val blockManager = env.blockManager

    // Cache a small RDD to have a block to read
    val rdd = spark.sparkContext.parallelize(Seq(Array[Byte](1, 2, 3)), 1)
    rdd.persist(StorageLevel.MEMORY_ONLY)
    rdd.count() // force materialization
    val blockId = RDDBlockId(rdd.id, 0)

    // Without registerTask: set TaskContext but do NOT register → NPE
    val ctx = Spark411Helper.getDummyTaskContext(0, env)
    Spark411Helper.setTaskContext(ctx)
    try {
      val ex = intercept[NullPointerException] {
        blockManager.get(blockId)(classTag[Array[Byte]])
      }
      assert(ex != null)
    } finally {
      Spark411Helper.unsetTaskContext()
    }

    rdd.unpersist(blocking = true)
  }

  test("BlockManager.get succeeds with registerTask and cleanup") {
    // Full lifecycle test mirroring RayDPExecutor.getRDDPartition:
    // register task → read block → release locks → unset context
    val env = SparkEnv.get
    val blockManager = env.blockManager

    val rdd = spark.sparkContext.parallelize(Seq(Array[Byte](10, 20, 30)), 1)
    rdd.persist(StorageLevel.MEMORY_ONLY)
    rdd.count()
    val blockId = RDDBlockId(rdd.id, 0)

    val ctx = Spark411Helper.getDummyTaskContext(0, env)
    Spark411Helper.setTaskContext(ctx)
    val taskAttemptId = ctx.taskAttemptId()
    blockManager.registerTask(taskAttemptId)
    try {
      val result = blockManager.get(blockId)(classTag[Array[Byte]])
      assert(result.isDefined, "block should be readable after registerTask")
    } finally {
      blockManager.releaseAllLocksForTask(taskAttemptId)
      Spark411Helper.unsetTaskContext()
    }

    // Verify lock was released: a second task can also read the same block
    val ctx2 = Spark411Helper.getDummyTaskContext(0, env)
    Spark411Helper.setTaskContext(ctx2)
    val taskAttemptId2 = ctx2.taskAttemptId()
    blockManager.registerTask(taskAttemptId2)
    try {
      val result2 = blockManager.get(blockId)(classTag[Array[Byte]])
      assert(result2.isDefined, "block should be readable by a second task after cleanup")
    } finally {
      blockManager.releaseAllLocksForTask(taskAttemptId2)
      Spark411Helper.unsetTaskContext()
    }

    rdd.unpersist(blocking = true)
  }
}

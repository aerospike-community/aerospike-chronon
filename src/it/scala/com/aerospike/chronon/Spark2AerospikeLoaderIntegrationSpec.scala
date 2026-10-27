package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants, PartitionSpec}
import ai.chronon.online.KVStore.GetRequest
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Try}

/** Integration coverage for the Spark bulk-load path (tasks 5.3, 5.5, 5.5a). */
class Spark2AerospikeLoaderIntegrationSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private val daily = PartitionSpec(format = "yyyy-MM-dd", spanMillis = 86400000L)
  private val ds = "2026-10-13"
  private val timeout = 30.seconds
  private val table = "test_upload.my_gb_upload"
  private val dataset = "MY_GB_BATCH"

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("Spark2AerospikeLoaderIntegrationSpec")
    .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private def createUploadTable(): Unit = {
    import spark.implicits._
    spark.sql(s"CREATE DATABASE IF NOT EXISTS test_upload")
    Seq(
      ("user1", "ir-bytes-1", ds),
      ("user2", "ir-bytes-2", ds),
      (ApiConstants.GroupByServingInfoKey, "serving-info-bytes", ds)
    ).toDF("key_str", "value_str", "ds")
      .selectExpr("CAST(key_str AS BINARY) AS key_bytes",
                  "CAST(value_str AS BINARY) AS value_bytes",
                  "ds")
      .write
      .mode("overwrite")
      .saveAsTable(table)
  }

  private def loaderArgs(table: String = table, endDs: String = ds): Array[String] = Array(
    "--table-name", table,
    "--end-ds", endDs,
    "--aerospike-hosts", aerospikeHost,
    "--namespace", aerospikeNamespace,
    "--ttl", "86400",
    "--max-lookback-days", "1"
  )

  // 5.3 — load lands rows + serving-info, idempotent on rerun
  "Spark2AerospikeLoader.main" should "load an _upload table into GROUPBY_BATCH and be idempotent" in {
    createUploadTable()
    Spark2AerospikeLoader.main(loaderArgs())
    spark.sparkContext.isStopped shouldBe false // main must not stop a session it didn't create
    Spark2AerospikeLoader.main(loaderArgs()) // rerun must not duplicate versions
    spark.sparkContext.isStopped shouldBe false

    val store = newStore()
    val resp = Await.result(store.multiGet(Seq(GetRequest("user1".getBytes(UTF_8), dataset))), timeout).head
    resp.values shouldBe a[Success[_]]
    val values = resp.values.get
    values should have size 1
    new String(values.head.bytes, UTF_8) shouldBe "ir-bytes-1"
    values.head.millis shouldBe daily.epochMillis(ds) + daily.spanMillis

    store.getString(ApiConstants.GroupByServingInfoKey, dataset, timeout.toMillis)
      .get shouldBe "serving-info-bytes"
  }

  // 5.5 — batch-ts invariant through the store
  it should "store ts = batchEndTsMillis so the fetcher millis >= batchEndTsMillis filter selects it" in {
    createUploadTable()
    Spark2AerospikeLoader.main(loaderArgs())
    spark.sparkContext.isStopped shouldBe false

    val store = newStore()
    val batchEndTs = daily.epochMillis(ds) + daily.spanMillis
    val resp = Await.result(
      store.multiGet(Seq(GetRequest("user1".getBytes(UTF_8), dataset, Some(batchEndTs)))), timeout).head
    val values = resp.values.get
    values should not be empty
    values.maxBy(_.millis).millis shouldBe batchEndTs
  }

  // 5.5a — bulkPut delegation
  "AerospikeKvStore.bulkPut" should "delegate to the loader and land rows" in {
    createUploadTable()
    val store = newStore()
    store.bulkPut(table, dataset, ds)

    val resp = Await.result(store.multiGet(Seq(GetRequest("user2".getBytes(UTF_8), dataset))), timeout).head
    resp.values.get.map(v => new String(v.bytes, UTF_8)) should contain("ir-bytes-2")
  }

  // comment 8 — bulkPut applies the loader pool sizing and bulk timeouts
  it should "pass the forLoader-derived config (loader pool and bulk timeouts) to the upload" in {
    val conf = testUserConf +
      ("aerospike.loader.maxConnsPerNode" -> "40") +
      ("aerospike.loader.minConnsPerNode" -> "2")
    val store = newStore(conf)
    var captured: AerospikeConfig = null
    store.bulkLoadConfigSink = (c: AerospikeConfig) => captured = c
    // the table need not exist: the sink captures the config before upload runs
    Try(store.bulkPut("missing_table", dataset, ds))
    captured should not be null
    captured.maxConnsPerNode shouldBe 40
    captured.minConnsPerNode shouldBe 2
    captured.socketTimeoutMillis shouldBe AerospikeConfig.LoaderSocketTimeoutMs
    captured.writeTotalTimeoutMillis shouldBe AerospikeConfig.LoaderWriteTotalTimeoutMs
    captured.readTotalTimeoutMillis shouldBe AerospikeConfig.LoaderReadTotalTimeoutMs
    captured.maxRetriesWrite shouldBe AerospikeConfig.LoaderMaxRetriesWrite
    captured.maxRetriesRead shouldBe AerospikeConfig.LoaderMaxRetriesRead
    captured.maxCommandsInProcess shouldBe None
  }

  // 13.1 — conf-driven partition spec: yyyyMMdd / dt
  it should "load a table partitioned by dt in yyyyMMdd with ts at the end of the partition day" in {
    import spark.implicits._
    val custom = PartitionSpec(format = "yyyyMMdd", spanMillis = 86400000L)
    val customTable = "test_upload.my_gb_dt_upload"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS test_upload")
    Seq(
      ("user1", "ir-bytes-1", "20261013"),
      (ApiConstants.GroupByServingInfoKey, "serving-info-bytes", "20261013")
    ).toDF("key_str", "value_str", "dt")
      .selectExpr("CAST(key_str AS BINARY) AS key_bytes",
                  "CAST(value_str AS BINARY) AS value_bytes",
                  "dt")
      .write
      .mode("overwrite")
      .saveAsTable(customTable)

    spark.conf.set("spark.chronon.partition.column", "dt")
    spark.conf.set("spark.chronon.partition.format", "yyyyMMdd")
    try {
      Spark2AerospikeLoader.main(loaderArgs(table = customTable, endDs = "20261013"))
    } finally {
      spark.conf.set("spark.chronon.partition.column", "ds")
      spark.conf.set("spark.chronon.partition.format", "yyyy-MM-dd")
    }

    val store = newStore()
    val resp = Await.result(store.multiGet(Seq(GetRequest("user1".getBytes(UTF_8), dataset))), timeout).head
    val values = resp.values.get
    values should have size 1
    new String(values.head.bytes, UTF_8) shouldBe "ir-bytes-1"
    values.head.millis shouldBe custom.epochMillis("20261013") + custom.spanMillis // 2026-10-14T00:00:00Z
  }
}

package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8

class AerospikeDataModelSpec extends AnyFlatSpec with Matchers {

  import AerospikeDataModel._

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)

  "getTableType" should "map *_BATCH datasets to the shared batch table" in {
    getTableType("MY_GB_BATCH") shouldBe BatchTable
    getTableType("CHRONON_STATS_BATCH") shouldBe BatchTable
  }

  it should "map *_STREAMING datasets to the shared streaming table" in {
    getTableType("MY_GB_STREAMING") shouldBe StreamingTable
  }

  it should "map any other dataset to a metadata table named by the dataset" in {
    getTableType("ZIPLINE_METADATA") shouldBe MetadataTable("ZIPLINE_METADATA")
    getTableType("ConfByKeyEndPointName") shouldBe MetadataTable("ConfByKeyEndPointName")
  }

  "retentionFor" should "keep N versions for group-by batch datasets" in {
    val config = AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts" -> "h", "aerospike.namespace" -> "n", "aerospike.batch.retainVersions" -> "4"))
    retentionFor("MY_GB_BATCH", bytes("k"), config) shouldBe KeepVersions(4)
  }

  it should "never trim the stats timeseries keys in the three stats datasets" in {
    val config = AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts" -> "h", "aerospike.namespace" -> "n"))
    for (ds <- Seq(ApiConstants.StatsBatchDataset, ApiConstants.LogStatsBatchDataset,
      ApiConstants.ConsistencyMetricsDataset)) {
      getTableType(ds) shouldBe BatchTable
      retentionFor(ds, bytes("some-join"), config) shouldBe NoTrim
      isStatsTimeseries(ds, bytes("some-join")) shouldBe true
      isLatestOnly(ds, bytes("some-join")) shouldBe false
    }
  }

  it should "keep versions and read latest-only for stats schema keys" in {
    val config = AerospikeConfig.fromUserConf(Map(
      "aerospike.hosts" -> "h", "aerospike.namespace" -> "n", "aerospike.batch.retainVersions" -> "4"))
    for (ds <- Seq(ApiConstants.StatsBatchDataset, ApiConstants.LogStatsBatchDataset,
      ApiConstants.ConsistencyMetricsDataset);
         suffix <- Seq(ApiConstants.TimedKvRDDKeySchemaKey, ApiConstants.TimedKvRDDValueSchemaKey)) {
      val key = bytes(s"team/join.v1$suffix")
      retentionFor(ds, key, config) shouldBe KeepVersions(4)
      isStatsSchemaKey(key) shouldBe true
      isStatsTimeseries(ds, key) shouldBe false
      isLatestOnly(ds, key) shouldBe true
    }
    isStatsSchemaKey(bytes("team/join.v1")) shouldBe false
    isStatsSchemaKey(bytes("__keySchemaExtra")) shouldBe false
  }

  it should "keep versions for serving-info inside a batch dataset and for other metadata" in {
    val config = AerospikeConfig.fromUserConf(Map("aerospike.hosts" -> "h", "aerospike.namespace" -> "n"))
    retentionFor("MY_GB_BATCH", bytes(ApiConstants.GroupByServingInfoKey), config) shouldBe KeepVersions(2)
    retentionFor("ZIPLINE_METADATA", bytes("conf"), config) shouldBe KeepVersions(2)
    retentionFor(ApiConstants.GroupByServingInfoKey, bytes("x"), config) shouldBe KeepVersions(2)
    isLatestOnly("MY_GB_BATCH", bytes(ApiConstants.GroupByServingInfoKey)) shouldBe true
    isLatestOnly("ZIPLINE_METADATA", bytes("conf")) shouldBe true
  }

  it should "never trim streaming datasets" in {
    val config = AerospikeConfig.fromUserConf(Map("aerospike.hosts" -> "h", "aerospike.namespace" -> "n"))
    retentionFor("MY_GB_STREAMING", bytes("k"), config) shouldBe NoTrim
    isLatestOnly("MY_GB_STREAMING", bytes("k")) shouldBe false
  }

  "statsFirstDayKey" should "build DATASET#keyBytes#first" in {
    statsFirstDayKey("CHRONON_STATS_BATCH", bytes("team/join.v1")) shouldBe
      bytes("CHRONON_STATS_BATCH#team/join.v1#first")
    statsFirstDayKey("CHRONON_STATS_BATCH", bytes("k"), Some("ns1:")) shouldBe
      bytes("ns1:CHRONON_STATS_BATCH#k#first")
  }

  it should "preserve binary key bytes verbatim (no UTF-8 round-trip)" in {
    val k = Array[Byte](0xFF.toByte, 0x01)
    statsFirstDayKey("CHRONON_STATS_BATCH", k) shouldBe
      ("CHRONON_STATS_BATCH#".getBytes(UTF_8) ++ k ++ "#first".getBytes(UTF_8))
  }

  it should "no longer provide statsSegmentKey" in {
    AerospikeDataModel.getClass.getDeclaredMethods.map(_.getName) should not contain "statsSegmentKey"
  }

  "stats day keys" should "use the streaming record key layout" in {
    streamingRecordKeyFromDay("CHRONON_STATS_BATCH", bytes("team/join.v1"), 86400000L) shouldBe
      bytes("CHRONON_STATS_BATCH#team/join.v1#86400000#0")
    streamingRecordKeyFromDay("CHRONON_STATS_BATCH", bytes("team/join.v1"), 86400000L, seg = 3) shouldBe
      bytes("CHRONON_STATS_BATCH#team/join.v1#86400000#3")
  }

  "statsDays" should "enumerate from max(dayTs(afterTs), firstDay) through today" in {
    val today = dayTs(System.currentTimeMillis())
    val first = today - 10 * MillisPerDay
    statsDays(Some(today - 3 * MillisPerDay + 1234L), first, today) shouldBe
      (today - 3 * MillisPerDay to today by MillisPerDay)
    statsDays(Some(0L), first, today) shouldBe (first to today by MillisPerDay)
    statsDays(Some(today - 20 * MillisPerDay), first, today) shouldBe (first to today by MillisPerDay)
    statsDays(None, first, today) shouldBe (first to today by MillisPerDay)
    statsDays(Some(today + MillisPerDay), first, today) shouldBe empty
    statsDays(None, today, today) shouldBe Seq(today)
  }

  "physicalSetName" should "return GROUPBY_BATCH / GROUPBY_STREAMING for groupBy datasets" in {
    physicalSetName("MY_GB_BATCH") shouldBe "GROUPBY_BATCH"
    physicalSetName("MY_GB_STREAMING") shouldBe "GROUPBY_STREAMING"
  }

  it should "return the dataset name itself for metadata datasets" in {
    physicalSetName("ZIPLINE_METADATA") shouldBe "ZIPLINE_METADATA"
  }

  "recordKey" should "build DATASET#keyBytes as raw bytes" in {
    recordKey("MY_GB_BATCH", bytes("user|123")) shouldBe bytes("MY_GB_BATCH#user|123")
  }

  it should "prepend the configured key prefix" in {
    recordKey("MY_GB_BATCH", bytes("k"), Some("ns1:")) shouldBe bytes("ns1:MY_GB_BATCH#k")
  }

  it should "not collide distinct binary keyBytes (no UTF-8 String round-trip)" in {
    // 0xFF is not valid UTF-8; both of these decode to the same replacement-char
    // String, so a String-based key would alias two entities to one record
    val k1 = Array[Byte](0xFF.toByte, 0x01)
    val k2 = Array[Byte](0xFF.toByte, 0x02)
    recordKey("MY_GB_BATCH", k1) should not be recordKey("MY_GB_BATCH", k2)
    recordKey("MY_GB_BATCH", k1) shouldBe ("MY_GB_BATCH#".getBytes(UTF_8) ++ Array[Byte](0xFF.toByte, 0x01))
  }

  "dayTs" should "floor a timestamp to start-of-day millis" in {
    dayTs(86400000L + 1234L) shouldBe 86400000L
    dayTs(0L) shouldBe 0L
    dayTs(-1L) shouldBe -86400000L
    dayTs(-86400000L) shouldBe -86400000L
  }

  it should "not overflow at the extremes" in {
    dayTs(2 * MillisPerDay) shouldBe 2 * MillisPerDay
    dayTs(-2 * MillisPerDay - 1) shouldBe -3 * MillisPerDay
    val maxDay = dayTs(Long.MaxValue)
    maxDay should be <= Long.MaxValue
    maxDay % MillisPerDay shouldBe 0L
    dayTs(Long.MaxValue - 1) shouldBe maxDay
    dayTs(Long.MinValue) shouldBe Long.MinValue
    dayTs(Long.MinValue + 1) shouldBe Long.MinValue
  }

  "streamingRecordKey" should "append the day bucket derived from tsMillis" in {
    val ts = 86400000L + 999L // second day
    streamingRecordKey("MY_GB_STREAMING", bytes("k"), ts) shouldBe bytes("MY_GB_STREAMING#k#86400000#0")
  }

  it should "prepend the configured key prefix" in {
    streamingRecordKey("MY_GB_STREAMING", bytes("k"), 1L, Some("ns1:")) shouldBe bytes("ns1:MY_GB_STREAMING#k#0#0")
  }

  "streaming keys" should "carry a segment marker on every key, with the root at #0" in {
    streamingRecordKey("MY_GB_STREAMING", bytes("k"), 5L) shouldBe bytes("MY_GB_STREAMING#k#0#0")
    streamingRecordKey("MY_GB_STREAMING", bytes("k"), 5L, seg = 3) shouldBe bytes("MY_GB_STREAMING#k#0#3")
    streamingRecordKeyFromDay("MY_GB_STREAMING", bytes("k"), 86400000L, seg = 12) shouldBe
      bytes("MY_GB_STREAMING#k#86400000#12")
  }

  it should "leave batch and metadata keys unchanged" in {
    recordKey("MY_GB_BATCH", bytes("k")) shouldBe bytes("MY_GB_BATCH#k")
    recordKey("ZIPLINE_METADATA", bytes("k")) shouldBe bytes("ZIPLINE_METADATA#k")
  }

  "streamingRecordKeyFromDay" should "use the supplied day bucket directly" in {
    streamingRecordKeyFromDay("MY_GB_STREAMING", bytes("k"), 86400000L) shouldBe
      bytes("MY_GB_STREAMING#k#86400000#0")
    streamingRecordKeyFromDay("MY_GB_STREAMING", bytes("k"), 86400000L, Some("ns1:")) shouldBe
      bytes("ns1:MY_GB_STREAMING#k#86400000#0")
  }
}

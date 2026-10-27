package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import com.aerospike.client.Info
import com.aerospike.client.policy.WritePolicy
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.aerospike.chronon.AerospikeDataModel.{MillisPerDay, dayTs}

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class AerospikeKvStoreSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)
  private val timeout = 30.seconds

  private def put(dataset: String, key: String, value: String, ts: Long): PutRequest =
    PutRequest(bytes(key), bytes(value), dataset, Some(ts))

  private def get(dataset: String, key: String, afterTs: Option[Long] = None): GetRequest =
    GetRequest(bytes(key), dataset, afterTs)

  // 3.1 — round trip
  "multiPut/multiGet" should "round-trip a TimedValue" in {
    val store = newStore()
    val ds = "ROUNDTRIP_GB_BATCH"
    Await.result(store.multiPut(Seq(put(ds, "k1", "v1", 100L))), timeout)
    val responses = Await.result(store.multiGet(Seq(get(ds, "k1"))), timeout)
    responses.head.values shouldBe a[Success[_]]
    val values = responses.head.values.get
    values should have size 1
    new String(values.head.bytes, UTF_8) shouldBe "v1"
    values.head.millis shouldBe 100L
  }

  // 3.3 — version filter
  it should "return only versions with ts >= afterTsMillis, in ts order" in {
    val store = newStore()
    val ds = "FILTER_GB_BATCH"
    val writes = Seq(put(ds, "k", "v100", 100L), put(ds, "k", "v200", 200L), put(ds, "k", "v300", 300L))
    Await.result(store.multiPut(writes), timeout)
    val values = Await.result(store.multiGet(Seq(get(ds, "k", Some(200L)))), timeout).head.values.get
    values.map(_.millis) shouldBe Seq(200L, 300L)
    values.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("v200", "v300")
  }

  // 3.4 — LWW same-ts rewrite
  it should "overwrite same-ts entries (last-write-wins, idempotent)" in {
    val store = newStore()
    val ds = "LWW_GB_BATCH"
    Await.result(store.multiPut(Seq(put(ds, "k", "v1", 100L))), timeout)
    Await.result(store.multiPut(Seq(put(ds, "k", "v2", 100L))), timeout)
    val values = Await.result(store.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
    values should have size 1
    new String(values.head.bytes, UTF_8) shouldBe "v2"
  }

  // spec scenario — two groupBys share GROUPBY_BATCH under equal keyBytes
  it should "keep same-keyBytes records from different datasets disjoint" in {
    val store = newStore()
    Await.result(store.multiPut(Seq(
      put("GB_ONE_BATCH", "shared-key", "v-one", 100L),
      put("GB_TWO_BATCH", "shared-key", "v-two", 100L))), timeout)
    val resps = Await.result(store.multiGet(Seq(
      get("GB_ONE_BATCH", "shared-key"), get("GB_TWO_BATCH", "shared-key"))), timeout)
    new String(resps(0).values.get.head.bytes, UTF_8) shouldBe "v-one"
    new String(resps(1).values.get.head.bytes, UTF_8) shouldBe "v-two"
  }

  // 3.6 — day-spanning streaming read
  it should "merge tiles across day-bucketed streaming records" in {
    val store = newStore()
    val ds = "SPAN_GB_STREAMING"
    // Use timestamps relative to now so the configurable lookback window includes them.
    val now = System.currentTimeMillis()
    val day1 = dayTs(now - MillisPerDay)
    val day2 = dayTs(now)
    Await.result(store.multiPut(Seq(
      put(ds, "k", "tileA", day1 + 100L),
      put(ds, "k", "tileB", day2 + 200L),
      put(ds, "k", "tileC", day2 + 300L))), timeout)
    // afterTsMillis inside day 1 -> tiles from both day records, ts-ordered
    val values = Await.result(store.multiGet(Seq(get(ds, "k", Some(day1 + 50L)))), timeout).head.values.get
    values.map(_.millis) shouldBe Seq(day1 + 100L, day2 + 200L, day2 + 300L)
    values.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("tileA", "tileB", "tileC")
  }

  it should "return the full lookback window for streaming reads without afterTsMillis" in {
    val store = newStore()
    val ds = "NOAFTER_GB_STREAMING"
    val now = System.currentTimeMillis()
    val day1 = dayTs(now - MillisPerDay)
    val day2 = dayTs(now)
    Await.result(store.multiPut(Seq(
      put(ds, "k", "tileA", day1 + 100L),
      put(ds, "k", "tileB", day2 + 200L))), timeout)
    val values = Await.result(store.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
    values.map(_.millis) shouldBe Seq(day1 + 100L, day2 + 200L)
  }

  it should "keep a cold key to one root record per day and one BatchRead per day" in {
    val store = newStore()
    val ds = "COLD_GB_STREAMING"
    val now = System.currentTimeMillis()
    val day1 = dayTs(now - MillisPerDay)
    val day2 = dayTs(now)
    Await.result(store.multiPut(Seq(
      put(ds, "k", "a", day1 + 100L), put(ds, "k", "b", day1 + 200L), put(ds, "k", "c", day2 + 100L))), timeout)

    val client = AerospikeClientHolder.clientFor(store.config)
    def record(day: Long, seg: Int) = client.get(null, new com.aerospike.client.Key(store.config.namespace,
      AerospikeDataModel.physicalSetName(ds), AerospikeDataModel.streamingRecordKeyFromDay(ds, bytes("k"), day, None, seg)))
    Seq(day1, day2).foreach { day =>
      val root = record(day, 0)
      root should not be null
      root.getLong("tail") shouldBe 0L
      record(day, 1) shouldBe null
    }

    val reads = store.buildBatchReads(get(ds, "k", Some(day1)))
    reads.map(_._1.key.userKey.toString).distinct should have size 2
    reads should have size 2
    reads.foreach(_._1.ops should have size 2)
    Await.result(store.multiGet(Seq(get(ds, "k", Some(day1)))), timeout).head.values.get.map(_.millis) shouldBe
      Seq(day1 + 100L, day1 + 200L, day2 + 100L)
  }

  it should "answer duplicate requests in one batch independently" in {
    val store = newStore()
    val ds = "DUP_GB_STREAMING"
    val day = dayTs(System.currentTimeMillis())
    Await.result(store.multiPut(Seq(put(ds, "k", "t", day + 100L))), timeout)
    val req = get(ds, "k")
    val resps = Await.result(store.multiGet(Seq(req, req, get(ds, "other"))), timeout)
    resps should have size 3
    resps(0).values.get.map(_.millis) shouldBe Seq(day + 100L)
    resps(1).values.get.map(_.millis) shouldBe Seq(day + 100L)
    resps(2).values shouldBe Success(Seq.empty)
  }

  // 3.8 — partial failure + absent key contract
  it should "return false only at the rejected index and never fail the Future" in {
    val store = newStore()
    val ds = "PARTIAL_GB_BATCH"
    // a request the server must reject: over the rig's 1 MiB max-record-size (code 13)
    val tooBig = new Array[Byte](2 * 1024 * 1024)
    val results = Await.result(store.multiPut(Seq(
      put(ds, "good", "v", 100L),
      PutRequest(bytes("bad"), tooBig, ds, Some(100L)),
      put(ds, "good2", "v", 100L))), timeout)
    results shouldBe Seq(true, false, true)
  }

  it should "reject invalid puts client-side without a cluster call" in {
    val store = newStore()
    val ds = "REJECT_GB_BATCH"
    val badDs = "Y" * 70 // over-long physical set name
    val events = metricEvents(store) {
      val results = Await.result(store.multiPut(Seq(
        put(ds, "good", "v", 100L),
        PutRequest(null, bytes("v"), ds, Some(100L)),      // null key
        PutRequest(bytes("k"), null, ds, Some(100L)),      // null value
        put(badDs, "k", "v", 100L),                        // over-long set name
        put(ds, "good2", "v", 100L))), timeout)
      results shouldBe Seq(true, false, false, false, true)
    }
    countSum(events, ds, "write.rejected") shouldBe 2
    countSum(events, badDs, "write.rejected") shouldBe 1
    events.count(e => e.name.startsWith("write.exceptions")) shouldBe 0 // no server error was ever produced
  }

  it should "return Success(Seq.empty) for a missing record" in {
    val store = newStore()
    val resp = Await.result(store.multiGet(Seq(get("MISSING_GB_BATCH", "nope"))), timeout).head
    resp.values shouldBe Success(Seq.empty)
  }

  it should "surface per-request Failures in GetResponse.values, never a failed Future" in {
    val store = newStore()
    // >63-byte set name is rejected client-side (datasetNameError) before any cluster call,
    // so this exercises the per-request Failure contract, not a server error path
    val badDs = "X" * 80
    val resps = Await.result(
      store.multiGet(Seq(get("OK_GB_BATCH", "k"), get(badDs, "k2"))), timeout)
    resps should have size 2
    resps(0).values shouldBe Success(Seq.empty)
    resps(1).values shouldBe a[Failure[_]]
    resps(1).request.dataset shouldBe badDs
  }

  // 3.9a — concurrency / shared client
  it should "complete 100 concurrent multiGets through one shared client" in {
    val store1 = newStore()
    val store2 = newStore()
    val before = AerospikeClientHolder.constructionCount.get()
    val futures = (1 to 100).map(i => store1.multiGet(Seq(get("CONC_GB_BATCH", s"k$i"))))
    val all = Await.result(scala.concurrent.Future.sequence(futures), timeout)
    all should have size 100
    AerospikeClientHolder.constructionCount.get() - before should be <= 1
    store1 should not be null
    store2 should not be null
  }

  // pool exhaustion (NO_MORE_CONNECTIONS, -7) must be absorbed by client-side queueing
  it should "queue a burst larger than the connection pool instead of failing writes" in {
    val store = newStore(testUserConf + (AerospikeConfig.MaxConnsPerNodeKey -> "8", AerospikeConfig.MinConnsPerNodeKey -> "0"))
    val ds = "BURST_GB_BATCH"
    val burst = (1 to 2000).map(i => put(ds, s"k$i", "v", 100L))
    val results = Await.result(store.multiPut(burst), timeout)
    results.count(!_) shouldBe 0
    val resp = Await.result(store.multiGet(Seq(get(ds, "k1"), get(ds, "k2000"))), timeout)
    resp.map(_.values.get.size) shouldBe Seq(1, 1)
  }

  // 3.10 — TTL exception for group_by_serving_info
  it should "keep group_by_serving_info alive while normal batch rows expire" in {
    // enabled once a short-TTL namespace config is wired; requires nsup-period > 0
    val store = newStore(testUserConf +
      (AerospikeConfig.TtlSecondsKey -> "5") +
      (AerospikeConfig.StreamingMaxLookbackDaysKey -> "0"))
    val ds = "TTL_GB_BATCH"
    Await.result(store.multiPut(Seq(
      put(ds, "k", "v", 100L),
      put(ds, ApiConstants.GroupByServingInfoKey, "serving-info", 100L))), timeout)

    // poll for expiry instead of a fixed long sleep
    val deadline = System.currentTimeMillis() + 20000
    var expired = false
    while (System.currentTimeMillis() < deadline && !expired) {
      val values = Await.result(store.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
      expired = values.isEmpty
      if (!expired) Thread.sleep(500)
    }
    expired shouldBe true

    store.getString(ApiConstants.GroupByServingInfoKey, ds, timeout.toMillis) shouldBe a[Success[_]]
  }

  // comment 1 — bounded version history
  private def versions(store: AerospikeKvStore, ds: String, key: String): Seq[Long] =
    Await.result(store.multiGet(Seq(get(ds, key, Some(0L)))), timeout).head.values.get.map(_.millis).toSeq

  "version retention" should "keep retainVersions versions and the latest across consecutive uploads" in {
    val store = newStore()
    val ds = "RETAIN_GB_BATCH"
    Seq(100L, 200L, 300L).foreach(ts => Await.result(store.multiPut(Seq(put(ds, "k", s"v$ts", ts))), timeout))
    versions(store, ds, "k") shouldBe Seq(200L, 300L)
    val latest = Await.result(store.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
    latest.map(_.millis) shouldBe Seq(300L)
    new String(latest.head.bytes, UTF_8) shouldBe "v300"
  }

  it should "honour aerospike.batch.retainVersions" in {
    val store = newStore(testUserConf + (AerospikeConfig.RetainVersionsKey -> "4"))
    val ds = "RETAIN4_GB_BATCH"
    (1L to 6L).foreach(ts => Await.result(store.multiPut(Seq(put(ds, "k", "v", ts * 100))), timeout))
    versions(store, ds, "k") shouldBe Seq(300L, 400L, 500L, 600L)
  }

  it should "not let a late older-ts write evict newer versions" in {
    val store = newStore()
    val ds = "LATE_GB_BATCH"
    Seq(200L, 300L, 50L).foreach(ts => Await.result(store.multiPut(Seq(put(ds, "k", s"v$ts", ts))), timeout))
    versions(store, ds, "k") shouldBe Seq(200L, 300L)
  }

  it should "keep repeated metadata and serving-info writes bounded" in {
    val store = newStore()
    (1L to 10L).foreach { ts =>
      Await.result(store.multiPut(Seq(
        put("ZIPLINE_METADATA", "conf", s"m$ts", ts),
        put("INFO_GB_BATCH", ApiConstants.GroupByServingInfoKey, s"i$ts", ts))), timeout)
    }
    versions(store, "ZIPLINE_METADATA", "conf") shouldBe Seq(9L, 10L)
    versions(store, "INFO_GB_BATCH", ApiConstants.GroupByServingInfoKey) shouldBe Seq(9L, 10L)
    new String(Await.result(store.multiGet(Seq(get("ZIPLINE_METADATA", "conf"))), timeout)
      .head.values.get.head.bytes, UTF_8) shouldBe "m10"
  }

  it should "read latest-only datasets as exactly the newest version, even from a bloated record" in {
    val big = newStore(testUserConf + (AerospikeConfig.RetainVersionsKey -> "10"))
    val ds = "BLOAT_GB_BATCH"
    (1L to 8L).foreach(ts => Await.result(big.multiPut(Seq(put(ds, "k", s"v$ts", ts))), timeout))
    versions(big, ds, "k") should have size 8
    val latest = Await.result(big.multiGet(Seq(get(ds, "k"))), timeout).head.values.get
    latest.map(_.millis) shouldBe Seq(8L)
  }

  // comment 2, section 4 — chained segment reads
  private def streamingStore(entries: Int): AerospikeKvStore =
    newStore(testUserConf + (AerospikeConfig.SegmentMaxEntriesKey -> entries.toString))

  private def streamKey(store: AerospikeKvStore, ds: String, keyBytes: Array[Byte], day: Long, seg: Int) =
    new com.aerospike.client.Key(store.config.namespace, AerospikeDataModel.physicalSetName(ds),
      AerospikeDataModel.streamingRecordKeyFromDay(ds, keyBytes, day, None, seg))

  "chained segment reads" should "do a second round only when a day's tail is positive and merge all segments" in {
    val store = streamingStore(5)
    val ds = "CHAIN_GB_STREAMING"
    val day = dayTs(System.currentTimeMillis())
    val cold = s"cold-${System.nanoTime()}"
    val hot = s"hot-${System.nanoTime()}"

    Await.result(store.multiPut(Seq(put(ds, cold, "c0", day + 1), put(ds, cold, "c1", day + 2))), timeout)
    val beforeCold = store.secondRoundCalls.get()
    val coldValues = Await.result(store.multiGet(Seq(get(ds, cold))), timeout).head.values.get
    coldValues.map(_.millis) shouldBe Seq(day + 1, day + 2)
    store.secondRoundCalls.get() shouldBe beforeCold

    Await.result(store.multiPut((0 until 12).map(i => put(ds, hot, s"v$i", day + 100 + i))), timeout)
    val beforeHot = store.secondRoundCalls.get()
    val values = Await.result(store.multiGet(Seq(get(ds, hot))), timeout).head.values.get
    store.secondRoundCalls.get() - beforeHot shouldBe 1
    values.map(_.millis) shouldBe (0 until 12).map(day + 100 + _)
    values.map(_.millis).distinct.size shouldBe 12

    val filtered = Await.result(store.multiGet(Seq(get(ds, hot, Some(day + 105)))), timeout).head.values.get
    filtered.map(_.millis) shouldBe (5 until 12).map(day + 100 + _)
  }

  it should "keep the day root alive past the segment TTL so later segments stay readable" in {
    val store = newStore(testUserConf +
      (AerospikeConfig.TtlSecondsKey -> "6") +
      (AerospikeConfig.StreamingMaxLookbackDaysKey -> "0") +
      (AerospikeConfig.SegmentMaxEntriesKey -> "2"))
    val ds = "TTLCHAIN_GB_STREAMING"
    val day = dayTs(System.currentTimeMillis())
    val name = s"ttl-${System.nanoTime()}"
    val keyBytes = bytes(name)
    val client = AerospikeClientHolder.clientFor(store.config)

    // segs 0 and 1 fill and expire; the advance has already stretched the root's expiration
    (0 until 4).foreach(i => Await.result(store.multiPut(Seq(put(ds, name, s"v$i", day + i))), timeout))
    val deadline = System.currentTimeMillis() + 40000
    var segOne = client.get(null, streamKey(store, ds, keyBytes, day, 1))
    while (segOne != null && System.currentTimeMillis() < deadline) {
      Thread.sleep(500)
      segOne = client.get(null, streamKey(store, ds, keyBytes, day, 1))
    }
    segOne shouldBe null

    // replayed late events land in fresh segments; their TTL restarts at write time
    Await.result(store.multiPut(Seq(put(ds, name, "v4", day + 4), put(ds, name, "v5", day + 5))), timeout)
    val root = client.get(null, streamKey(store, ds, keyBytes, day, 0))
    root should not be null
    root.getLong("tail") shouldBe 3L
    Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get.map(_.millis) shouldBe
      Seq(day + 0, day + 1, day + 4, day + 5)
  }

  it should "skip an expired earlier segment and still return later segments" in {
    val store = streamingStore(2)
    val ds = "SKIPPED_GB_STREAMING"
    val day = dayTs(System.currentTimeMillis())
    val name = s"skip-${System.nanoTime()}"
    val keyBytes = bytes(name)
    val client = AerospikeClientHolder.clientFor(store.config)

    (0 until 6).foreach(i => Await.result(store.multiPut(Seq(put(ds, name, s"v$i", day + i))), timeout))
    client.delete(null, streamKey(store, ds, keyBytes, day, 1)) shouldBe true

    val resp = Await.result(store.multiGet(Seq(get(ds, name))), timeout).head
    resp.values shouldBe a[Success[_]]
    resp.values.get.map(_.millis) shouldBe Seq(day + 0, day + 1, day + 4, day + 5)
  }

  // comment 1 revision — permanent stats timeseries as day buckets with overflow segments
  private def statsStore(targetKiB: Int): AerospikeKvStore =
    newStore(testUserConf + (AerospikeConfig.StatsSegmentTargetKiBKey -> targetKiB.toString))

  private val KiBEntry: String = "x" * 1024

  private def statsDayKey(store: AerospikeKvStore, ds: String, keyBytes: Array[Byte], day: Long, seg: Int) =
    new com.aerospike.client.Key(store.config.namespace, AerospikeDataModel.physicalSetName(ds),
      AerospikeDataModel.streamingRecordKeyFromDay(ds, keyBytes, day, None, seg))

  private def statsTailOf(store: AerospikeKvStore, ds: String, keyBytes: Array[Byte], day: Long): Long =
    AerospikeClientHolder.clientFor(store.config).get(null, statsDayKey(store, ds, keyBytes, day, 0)).getLong("tail")

  private def statsMarker(store: AerospikeKvStore, ds: String, keyBytes: Array[Byte]) =
    AerospikeClientHolder.clientFor(store.config).get(null, new com.aerospike.client.Key(
      store.config.namespace, AerospikeDataModel.physicalSetName(ds),
      AerospikeDataModel.statsFirstDayKey(ds, keyBytes, None)))

  private def statsPlainKey(store: AerospikeKvStore, ds: String, keyBytes: Array[Byte]) =
    new com.aerospike.client.Key(store.config.namespace, AerospikeDataModel.physicalSetName(ds),
      AerospikeDataModel.recordKey(ds, keyBytes, None))

  "stats timeseries" should "roll over within a day past the byte target and read every version back sorted and unique" in {
    val store = statsStore(4) // 4 KiB target, ~1 KiB entries -> 3 entries per segment
    val ds = ApiConstants.StatsBatchDataset
    val day = dayTs(System.currentTimeMillis())
    val name = s"stats-${System.nanoTime()}"
    val results = (0 until 10).map(i =>
      Await.result(store.multiPut(Seq(put(ds, name, KiBEntry, day + i * 3600000L))), timeout).head)
    results.count(!_) shouldBe 0
    statsTailOf(store, ds, bytes(name), day) should be >= 3L
    // the plain legacy key is never written
    AerospikeClientHolder.clientFor(store.config).get(null, statsPlainKey(store, ds, bytes(name))) shouldBe null

    val before = store.secondRoundCalls.get()
    val values = Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get
    store.secondRoundCalls.get() - before shouldBe 1
    values.map(_.millis) shouldBe (0 until 10).map(day + _ * 3600000L)
    values.map(_.millis).distinct.size shouldBe 10
  }

  it should "read only the requested days and return only ts >= afterTsMillis" in {
    val store = statsStore(512)
    val ds = ApiConstants.StatsBatchDataset
    val now = System.currentTimeMillis()
    val name = s"range-${System.nanoTime()}"
    // one entry per day, days now-9d .. now, at +1h inside each day
    (0 until 10).foreach(k =>
      Await.result(store.multiPut(Seq(put(ds, name, s"v$k", dayTs(now - (9 - k) * MillisPerDay) + 3600000L))), timeout))

    val afterTs = dayTs(now - 2 * MillisPerDay) + 1800000L
    val before = store.statsRootReads.get()
    val values = Await.result(store.multiGet(Seq(get(ds, name, Some(afterTs)))), timeout).head.values.get
    store.statsRootReads.get() - before shouldBe 3 // days now-2d, now-1d, now
    values.map(_.millis) shouldBe Seq(
      dayTs(now - 2 * MillisPerDay) + 3600000L,
      dayTs(now - MillisPerDay) + 3600000L,
      dayTs(now) + 3600000L)
  }

  it should "clamp afterTsMillis = 0 to the first-day marker" in {
    val store = statsStore(512)
    val ds = ApiConstants.StatsBatchDataset
    val now = System.currentTimeMillis()
    val name = s"clamp-${System.nanoTime()}"
    (0 until 5).foreach(k =>
      Await.result(store.multiPut(Seq(put(ds, name, s"v$k", dayTs(now - (4 - k) * MillisPerDay) + 3600000L))), timeout))

    val before = store.statsRootReads.get()
    val values = Await.result(store.multiGet(Seq(get(ds, name, Some(0L)))), timeout).head.values.get
    store.statsRootReads.get() - before shouldBe 5 // days first..today, not ~20000 since 1970
    values.map(_.millis) shouldBe (0 until 5).map(k => dayTs(now - (4 - k) * MillisPerDay) + 3600000L)
  }

  it should "read the full history across several days in ts order" in {
    val store = statsStore(512)
    val ds = ApiConstants.StatsBatchDataset
    val now = System.currentTimeMillis()
    val name = s"hist-${System.nanoTime()}"
    val tss = Seq(
      dayTs(now - 3 * MillisPerDay) + 3600000L, dayTs(now - 3 * MillisPerDay) + 7200000L,
      dayTs(now - MillisPerDay) + 3600000L, dayTs(now - MillisPerDay) + 7200000L,
      dayTs(now - MillisPerDay) + 10800000L,
      dayTs(now) + 3600000L, dayTs(now) + 7200000L)
    tss.foreach(ts => Await.result(store.multiPut(Seq(put(ds, name, s"v$ts", ts))), timeout))
    Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get.map(_.millis) shouldBe tss
  }

  it should "lower the first-day marker on an older-day write and not rewrite it once ensured" in {
    val store = statsStore(512)
    val ds = ApiConstants.StatsBatchDataset
    val now = System.currentTimeMillis()
    val name = s"marker-${System.nanoTime()}"
    val keyBytes = bytes(name)
    val before = store.statsMarkerWrites.get()

    Seq(5L, 4L, 3L).foreach(d =>
      Await.result(store.multiPut(Seq(put(ds, name, "v", dayTs(now - d * MillisPerDay) + 3600000L))), timeout))
    statsMarker(store, ds, keyBytes).getLong("first") shouldBe dayTs(now - 5 * MillisPerDay)
    store.statsMarkerWrites.get() - before shouldBe 1

    // a late write for an earlier day lowers the marker and issues one more write
    Await.result(store.multiPut(Seq(put(ds, name, "old", dayTs(now - 8 * MillisPerDay) + 3600000L))), timeout)
    statsMarker(store, ds, keyBytes).getLong("first") shouldBe dayTs(now - 8 * MillisPerDay)
    store.statsMarkerWrites.get() - before shouldBe 2

    // writes for later days do not re-issue the marker write
    Seq(2L, 0L).foreach(d =>
      Await.result(store.multiPut(Seq(put(ds, name, "v", dayTs(now - d * MillisPerDay) + 3600000L))), timeout))
    statsMarker(store, ds, keyBytes).getLong("first") shouldBe dayTs(now - 8 * MillisPerDay)
    store.statsMarkerWrites.get() - before shouldBe 2
  }

  it should "read a legacy plain-key record and merge it with day buckets" in {
    val store = statsStore(4)
    val ds = ApiConstants.StatsBatchDataset
    val now = System.currentTimeMillis()
    val name = s"legacy-${System.nanoTime()}"
    val keyBytes = bytes(name)
    val client = AerospikeClientHolder.clientFor(store.config)
    val legacyTs0 = dayTs(now - 3 * MillisPerDay) + 3600000L
    val legacyTs1 = dayTs(now - 2 * MillisPerDay) + 3600000L

    // legacy shape: bin "v" only at the plain record key, as the old layout wrote
    val policy = new com.aerospike.client.cdt.MapPolicy(
      com.aerospike.client.cdt.MapOrder.KEY_ORDERED, com.aerospike.client.cdt.MapWriteFlags.DEFAULT)
    client.operate(null, statsPlainKey(store, ds, keyBytes),
      com.aerospike.client.cdt.MapOperation.put(policy, "v", com.aerospike.client.Value.get(legacyTs0), com.aerospike.client.Value.get(bytes("old0"))),
      com.aerospike.client.cdt.MapOperation.put(policy, "v", com.aerospike.client.Value.get(legacyTs1), com.aerospike.client.Value.get(bytes("old1"))))

    // no marker yet: the read returns just the legacy entries
    Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get.map(_.millis) shouldBe
      Seq(legacyTs0, legacyTs1)

    val newTs = dayTs(now) + 3600000L
    Await.result(store.multiPut(Seq(put(ds, name, KiBEntry, newTs))), timeout) shouldBe Seq(true)
    Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get.map(_.millis) shouldBe
      Seq(legacyTs0, legacyTs1, newTs)
  }

  it should "write stats records with never-expire TTL on day roots, segments, the marker and schema keys" in {
    val store = statsStore(1) // 1 KiB target, 1 KiB entries -> each entry advances the tail
    val ds = ApiConstants.StatsBatchDataset
    val day = dayTs(System.currentTimeMillis())
    val name = s"ttl-${System.nanoTime()}"
    val schema = name + ApiConstants.TimedKvRDDKeySchemaKey
    val client = AerospikeClientHolder.clientFor(store.config)

    (0 until 4).foreach(i => Await.result(store.multiPut(Seq(put(ds, name, KiBEntry, day + i * 3600000L))), timeout))
    Await.result(store.multiPut(Seq(put(ds, schema, "schema", day))), timeout)

    client.get(null, statsDayKey(store, ds, bytes(name), day, 0), "tail").expiration shouldBe 0
    client.get(null, statsDayKey(store, ds, bytes(name), day, 1)).expiration shouldBe 0
    statsMarker(store, ds, bytes(name)).expiration shouldBe 0
    client.get(null, statsPlainKey(store, ds, bytes(schema))).expiration shouldBe 0
  }

  it should "keep retainVersions versions on schema keys and read the newest only" in {
    val store = statsStore(4)
    val ds = ApiConstants.StatsBatchDataset
    val schema = s"schema-${System.nanoTime()}${ApiConstants.TimedKvRDDKeySchemaKey}"
    (1L to 5L).foreach(ts => Await.result(store.multiPut(Seq(put(ds, schema, s"s$ts", ts * 100))), timeout))

    val all = Await.result(store.multiGet(Seq(get(ds, schema, Some(0L)))), timeout).head.values.get
    all.map(_.millis) shouldBe Seq(400L, 500L)
    val latest = Await.result(store.multiGet(Seq(get(ds, schema))), timeout).head.values.get
    latest.map(_.millis) shouldBe Seq(500L)
    new String(latest.head.bytes, UTF_8) shouldBe "s5"
    AerospikeClientHolder.clientFor(store.config)
      .get(null, statsPlainKey(store, ds, bytes(schema))).bins.containsKey("tail") shouldBe false
  }

  it should "land a late older-ts write in its own day bucket and read everything in ts order" in {
    val store = statsStore(512)
    val ds = ApiConstants.StatsBatchDataset
    val now = System.currentTimeMillis()
    val today = dayTs(now)
    val name = s"late-${System.nanoTime()}"
    (0 until 5).foreach(i => Await.result(store.multiPut(Seq(put(ds, name, s"v$i", today + i * 3600000L))), timeout))

    val oldTs = dayTs(now - 2 * MillisPerDay) + 3600000L
    Await.result(store.multiPut(Seq(put(ds, name, "old", oldTs))), timeout) shouldBe Seq(true)
    AerospikeClientHolder.clientFor(store.config)
      .get(null, statsDayKey(store, ds, bytes(name), dayTs(now - 2 * MillisPerDay), 0)).getMap("v")
      .keySet() should contain(oldTs)
    Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get.map(_.millis) shouldBe
      (Seq(oldTs) ++ (0 until 5).map(today + _ * 3600000L)).sorted
  }

  it should "give an entry larger than the target its own segment within the day" in {
    val store = statsStore(512)
    val ds = ApiConstants.StatsBatchDataset
    val day = dayTs(System.currentTimeMillis())
    val name = s"big-${System.nanoTime()}"
    val results = (0 until 3).map(i =>
      Await.result(store.multiPut(Seq(PutRequest(bytes(name), new Array[Byte](600 * 1024), ds, Some(day + i * 3600000L)))), timeout).head)
    results shouldBe Seq(true, true, true)
    statsTailOf(store, ds, bytes(name), day) shouldBe 3L
    Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get.map(_.millis) shouldBe
      (0 until 3).map(day + _ * 3600000L)
  }

  it should "use the RECORD_TOO_BIG backstop when the entry limit exceeds the record limit" in {
    val store = statsStore(4096) // limit = 10 entries, but ~400 KiB values cap a record at 2
    val ds = ApiConstants.StatsBatchDataset
    val day = dayTs(System.currentTimeMillis())
    val name = s"backstop-${System.nanoTime()}"
    val results = (0 until 5).map(i =>
      Await.result(store.multiPut(Seq(PutRequest(bytes(name), new Array[Byte](400 * 1024), ds, Some(day + i * 3600000L)))), timeout).head)
    results shouldBe Seq(true, true, true, true, true)
    statsTailOf(store, ds, bytes(name), day) shouldBe 2L
    Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get.map(_.millis) shouldBe
      (0 until 5).map(day + _ * 3600000L)
  }

  it should "never trim the timeseries (all writes remain readable past retainVersions)" in {
    val store = statsStore(512)
    val ds = ApiConstants.StatsBatchDataset
    val hour = (System.currentTimeMillis() / 3600000L) * 3600000L
    val name = s"notrim-${System.nanoTime()}"
    (0 until 300).foreach(i => Await.result(store.multiPut(Seq(put(ds, name, "v", hour - i * 3600000L))), timeout))
    val values = Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get
    values.map(_.millis) shouldBe (0 until 300).map(hour - _ * 3600000L).sorted
  }

  // 3.12 — push mode contract guard (passes via trait default)
  "multiPutWithNotification" should "throw UnsupportedOperationException naming the impl class" in {
    val store = newStore()
    val ex = intercept[UnsupportedOperationException] {
      store.multiPutWithNotification(Seq.empty, "topic")
    }
    ex.getMessage should include(classOf[AerospikeKvStore].getName)
  }

  // 3.12a — per-dataset metrics contexts
  // Upstream Metrics.Context has no dataset tag; per spec the dataset is carried
  // in the metric-name prefix via Context.withSuffix (see design.md).
  "metrics" should "produce per-dataset Metrics.Contexts" in {
    val store = newStore()
    store.metricsContextFor("GB_ONE_BATCH").suffix should include("GB_ONE_BATCH")
    store.metricsContextFor("GB_TWO_BATCH").suffix should include("GB_TWO_BATCH")
    store.metricsContextFor("GB_ONE_BATCH") should not be store.metricsContextFor("GB_TWO_BATCH")
  }

  // comment 3, section 6 — chunked batch reads and total per-request assembly
  private def chunkedStore(chunkSize: Int): AerospikeKvStore =
    newStore(testUserConf + (AerospikeConfig.ReadBatchChunkSizeKey -> chunkSize.toString))

  private def futureAfterTs: Long = System.currentTimeMillis() + MillisPerDay

  "multiGet assembly" should "return Success(Seq.empty) for a future-afterTs streaming request without affecting other requests" in {
    val store = newStore()
    val streamDs = "FUTURE_GB_STREAMING"
    val batchDs = "FUTURE_GB_BATCH"
    Await.result(store.multiPut(Seq(put(batchDs, "k", "v", 100L))), timeout)

    // the empty day range expands to zero reads, not a phantom plain-key read
    store.buildBatchReads(get(streamDs, "k", Some(futureAfterTs))) shouldBe empty

    val resps = Await.result(store.multiGet(Seq(
      get(streamDs, "k", Some(futureAfterTs)),
      get(batchDs, "k"))), timeout)
    resps should have size 2
    resps(0).request.dataset shouldBe streamDs
    resps(0).values shouldBe Success(Seq.empty)
    resps(1).values.get.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("v")
  }

  it should "keep an invalid request's Failure when every valid request expands to zero reads" in {
    val store = newStore()
    // MetadataTable: the set name is the dataset name itself, so > 63 bytes is invalid
    val badDs = "X" * 70
    val resps = Await.result(store.multiGet(Seq(
      get(badDs, "k"),
      get("EMPTY_GB_STREAMING", "k", Some(futureAfterTs)))), timeout)
    resps should have size 2
    resps(0).request.dataset shouldBe badDs
    resps(0).values shouldBe a[Failure[_]]
    resps(0).values.failed.get shouldBe an[IllegalArgumentException]
    resps(1).values shouldBe Success(Seq.empty)
  }

  it should "align every response to its request across mixes of invalid, zero-read, batch and streaming" in {
    val store = newStore()
    val batchDs = "MIX_GB_BATCH"
    val streamDs = "MIX_GB_STREAMING"
    val day = dayTs(System.currentTimeMillis())
    Await.result(store.multiPut(Seq(
      put(batchDs, "k", "batch-v", 100L),
      put(streamDs, "k", "stream-v", day + 100L))), timeout)

    sealed trait Kind
    case object Invalid extends Kind
    case object ZeroRead extends Kind
    case object Batch extends Kind
    case object Streaming extends Kind
    val kinds = Seq(Invalid, ZeroRead, Batch, Streaming)
    val badDs = "Y" * 70

    def reqOf(kind: Kind): GetRequest = kind match {
      case Invalid   => get(badDs, "k")
      case ZeroRead  => get("MIXEMPTY_GB_STREAMING", "k", Some(futureAfterTs))
      case Batch     => get(batchDs, "k")
      case Streaming => get(streamDs, "k")
    }
    def outcomeOk(kind: Kind, resp: GetResponse): Boolean = kind match {
      case Invalid   => resp.values.isFailure && resp.values.failed.get.isInstanceOf[IllegalArgumentException]
      case ZeroRead  => resp.values == Success(Seq.empty)
      case Batch     => resp.values.toOption.exists(_.map(v => new String(v.bytes, UTF_8)) == Seq("batch-v"))
      case Streaming => resp.values.toOption.exists(_.map(v => new String(v.bytes, UTF_8)) == Seq("stream-v"))
    }

    for {
      a <- kinds
      b <- kinds
      c <- kinds
    } {
      val reqs = Seq(reqOf(a), reqOf(b), reqOf(c))
      val resps = Await.result(store.multiGet(reqs), timeout)
      resps should have size 3
      resps.zip(reqs).zip(Seq(a, b, c)).foreach { case ((resp, req), kind) =>
        withClue(s"request ${reqs.indexOf(req)} of ($a, $b, $c): ") {
          resp.request shouldBe theSameInstanceAs(req)
          outcomeOk(kind, resp) shouldBe true
        }
      }
    }
  }

  "batch chunking" should "split a fetch larger than the chunk size and keep request order" in {
    val store = chunkedStore(3)
    val ds = "CHUNK_GB_BATCH"
    val keys = (1 to 10).map(i => s"key$i")
    Await.result(store.multiPut(keys.map(k => put(ds, k, s"v-$k", 100L))), timeout)
    val before = store.batchCalls.get()
    val resps = Await.result(store.multiGet(keys.map(k => get(ds, k))), timeout)
    resps.map(_.values.get.head.bytes).map(b => new String(b, UTF_8)) shouldBe keys.map(k => s"v-$k")
    store.batchCalls.get() - before shouldBe 4 // ceil(10/3)
  }

  it should "issue exactly one batch for a fetch at or below the chunk size" in {
    val store = chunkedStore(3)
    val ds = "ONECHUNK_GB_BATCH"
    val keys = (1 to 3).map(i => s"key$i")
    Await.result(store.multiPut(keys.map(k => put(ds, k, "v", 100L))), timeout)
    val before = store.batchCalls.get()
    val resps = Await.result(store.multiGet(keys.map(k => get(ds, k))), timeout)
    resps.map(_.values.get.size) shouldBe Seq(1, 1, 1)
    store.batchCalls.get() - before shouldBe 1
  }

  private def recordKeyBytesOf(br: com.aerospike.client.BatchRead): Array[Byte] =
    br.key.userKey.getObject.asInstanceOf[Array[Byte]]

  private def failChunksContaining(store: AerospikeKvStore, target: Array[Byte])(body: => Unit): Unit = {
    val real = store.batchExecutor
    store.batchExecutor = { (reads, policy) =>
      if (reads.exists(r => java.util.Arrays.equals(recordKeyBytesOf(r), target)))
        scala.concurrent.Future.failed(new com.aerospike.client.AerospikeException(1, "injected chunk failure"))
      else real(reads, policy)
    }
    try body
    finally store.batchExecutor = real
  }

  it should "fail only the requests whose reads are in a failed chunk" in {
    val store = chunkedStore(2)
    val ds = "PARTCHUNK_GB_BATCH"
    val keys = (1 to 4).map(i => s"key$i")
    Await.result(store.multiPut(keys.map(k => put(ds, k, s"v-$k", 100L))), timeout)

    // chunks of 2 in request order: [key1,key2] succeeds, [key3,key4] fails
    failChunksContaining(store, AerospikeDataModel.recordKey(ds, bytes("key3"), None)) {
      val resps = Await.result(store.multiGet(keys.map(k => get(ds, k))), timeout)
      resps(0).values.get.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("v-key1")
      resps(1).values.get.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("v-key2")
      resps(2).values shouldBe a[Failure[_]]
      resps(3).values shouldBe a[Failure[_]]
    }
  }

  it should "fail a streaming request whose day reads span a failed chunk while other requests succeed" in {
    val store = chunkedStore(1) // every read is its own chunk
    val streamDs = "SPANCHUNK_GB_STREAMING"
    val batchDs = "SPANCHUNK_GB_BATCH"
    val now = System.currentTimeMillis()
    val day1 = dayTs(now - MillisPerDay)
    val day2 = dayTs(now)
    Await.result(store.multiPut(Seq(
      put(streamDs, "k", "s1", day1 + 100L),
      put(streamDs, "k", "s2", day2 + 100L),
      put(batchDs, "k", "bv", 100L))), timeout)

    // the streaming request expands to 2 day reads (chunks [day1], [day2]); failing today's
    // chunk must not fail the batch request in the same call
    failChunksContaining(store, AerospikeDataModel.streamingRecordKeyFromDay(streamDs, bytes("k"), day2, None, 0)) {
      val resps = Await.result(store.multiGet(Seq(get(streamDs, "k"), get(batchDs, "k"))), timeout)
      resps(0).values shouldBe a[Failure[_]]
      resps(1).values.get.map(v => new String(v.bytes, UTF_8)) shouldBe Seq("bv")
    }
  }

  it should "chunk segment reads the same way as root reads" in {
    val store = newStore(testUserConf +
      (AerospikeConfig.ReadBatchChunkSizeKey -> "2") +
      (AerospikeConfig.SegmentMaxEntriesKey -> "1"))
    val ds = "SEGCHUNK_GB_STREAMING"
    val day = dayTs(System.currentTimeMillis())
    val name = s"seg-${System.nanoTime()}"
    // maxEntries 1: each write advances the tail -> 5 segments under the root
    (0 until 6).foreach(i => Await.result(store.multiPut(Seq(put(ds, name, s"v$i", day + i))), timeout))

    val before = store.batchCalls.get()
    val values = Await.result(store.multiGet(Seq(get(ds, name))), timeout).head.values.get
    values.map(_.millis) shouldBe (0 until 6).map(day + _)
    // roots round: 2 day roots -> 1 chunk; segment round: 5 segment reads -> 3 chunks of 2
    store.batchCalls.get() - before shouldBe 4
  }

  // comment 6, section 7 — metrics environment and counter conventions
  private def metricEvents(store: AerospikeKvStore)(body: => Unit): Seq[AerospikeKvStore.MetricEvent] = {
    val buf = scala.collection.mutable.ListBuffer.empty[AerospikeKvStore.MetricEvent]
    store.metricsSink = Some(e => buf.synchronized { buf += e })
    try body
    finally store.metricsSink = None
    buf.toSeq
  }

  private def countSum(events: Seq[AerospikeKvStore.MetricEvent], dataset: String, name: String): Long =
    events.filter(e => e.count && e.name == name && e.suffix.contains(dataset)).map(_.value).sum

  private def distSamples(events: Seq[AerospikeKvStore.MetricEvent], dataset: String, name: String): Int =
    events.count(e => !e.count && e.name == name && e.suffix.contains(dataset))

  "store metrics" should "emit every event under environment kv_store with aerospike and the dataset in the prefix" in {
    val store = newStore()
    val datasets = Seq("ENV_GB_BATCH", "ENV_META")
    val events = metricEvents(store) {
      Await.result(store.multiPut(datasets.map(ds => put(ds, "k", "v", 100L))), timeout)
      Await.result(store.multiGet(datasets.map(ds => get(ds, "k"))), timeout)
    }
    events should not be empty
    events.foreach { e =>
      e.environment shouldBe "kv_store"
      e.suffix should startWith("aerospike.")
      datasets.exists(ds => e.suffix.contains(ds)) shouldBe true
      (e.environment + e.suffix) should not include "group_by.upload"
    }
  }

  it should "count read RequestCount per request, ResponseCount per success and FetchExceptions per failure" in {
    val store = chunkedStore(1)
    val ds = "MCOUNT_GB_BATCH"
    (1 to 3).foreach(i => Await.result(store.multiPut(Seq(put(ds, s"k$i", s"v$i", 100L))), timeout))

    // chunk size 1: the chunk holding k2 fails, so exactly one request fails
    val first = metricEvents(store) {
      failChunksContaining(store, AerospikeDataModel.recordKey(ds, bytes("k2"), None)) {
        Await.result(store.multiGet((1 to 3).map(i => get(ds, s"k$i"))), timeout)
      }
    }
    countSum(first, ds, "request.count") shouldBe 3
    countSum(first, ds, "response.count") shouldBe 2
    countSum(first, ds, "fetch.exception_count") shouldBe 1

    // an invalid request counts as received and failed, with no cluster call
    val badDs = "Z" * 70
    val before = store.batchCalls.get()
    val second = metricEvents(store) {
      Await.result(store.multiGet(Seq(get(badDs, "k"))), timeout)
    }
    store.batchCalls.get() shouldBe before
    countSum(second, badDs, "request.count") shouldBe 1
    countSum(second, badDs, "response.count") shouldBe 0
    countSum(second, badDs, "fetch.exception_count") shouldBe 1
  }

  it should "count write RequestCount per request, ResponseCount per success and split rejected from server errors" in {
    val store = newStore()
    val ds = "WCOUNT_GB_BATCH"
    val tooBig = new Array[Byte](2 * 1024 * 1024) // over the rig's 1 MiB max-record-size

    val first = metricEvents(store) {
      Await.result(store.multiPut(Seq(
        put(ds, "k1", "v", 100L),
        PutRequest(bytes("k2"), tooBig, ds, Some(100L)),
        put(ds, "k3", "v", 100L))), timeout)
    }
    countSum(first, ds, "request.count") shouldBe 3
    countSum(first, ds, "response.count") shouldBe 2
    countSum(first, ds, "write.exceptions") shouldBe 1
    countSum(first, ds, "write.exceptions.record_too_big") shouldBe 1
    countSum(first, ds, "write.rejected") shouldBe 0

    // client-side rejections are failures but NOT server errors
    val badDs = "W" * 70
    val second = metricEvents(store) {
      Await.result(store.multiPut(Seq(
        put(ds, "k4", "v", 100L),
        PutRequest(null, bytes("v"), ds, Some(100L)),
        PutRequest(bytes("k5"), null, ds, Some(100L)),
        put(badDs, "k", "v", 100L),
        put(ds, "k6", "v", 100L))), timeout)
    }
    countSum(second, ds, "request.count") shouldBe 4
    countSum(second, badDs, "request.count") shouldBe 1
    countSum(second, ds, "response.count") shouldBe 2
    countSum(second, ds, "write.rejected") shouldBe 2
    countSum(second, badDs, "write.rejected") shouldBe 1
    countSum(second, ds, "write.exceptions") shouldBe 0
    countSum(second, badDs, "write.exceptions") shouldBe 0
    second.count(e => e.name.startsWith("write.exceptions.")) shouldBe 0
  }

  it should "record read latency once per dataset per multiGet call" in {
    val store = newStore()
    val ds1 = "LAT1_GB_BATCH"
    val ds2 = "LAT2_GB_BATCH"
    Await.result(store.multiPut(Seq(put(ds1, "k1", "v", 100L), put(ds2, "k", "v", 100L))), timeout)

    val first = metricEvents(store) {
      Await.result(store.multiGet((1 to 20).map(i => get(ds1, s"k$i"))), timeout)
    }
    distSamples(first, ds1, "latency.millis") shouldBe 1

    val second = metricEvents(store) {
      Await.result(store.multiGet(Seq(get(ds1, "k1"), get(ds2, "k"))), timeout)
    }
    distSamples(second, ds1, "latency.millis") shouldBe 1
    distSamples(second, ds2, "latency.millis") shouldBe 1
  }

  it should "emit chunk.latency.millis once per chunk per dataset" in {
    val store = chunkedStore(3)
    val ds = "CHUNKLAT_GB_BATCH"
    Await.result(store.multiPut((1 to 10).map(i => put(ds, s"k$i", "v", 100L))), timeout)
    val events = metricEvents(store) {
      Await.result(store.multiGet((1 to 10).map(i => get(ds, s"k$i"))), timeout)
    }
    distSamples(events, ds, "chunk.latency.millis") shouldBe 4 // ceil(10/3)
  }

  // comment 7, section 8 — timeouts and retries
  "the tail advance policy" should "never retry (compare-and-set must not be re-issued)" in {
    val store = newStore()
    store.tailAdvancePolicy(3, -1).maxRetries shouldBe 0
  }

  "a stalled node" should "retry reads and writes within their per-class totals" in {
    val baseline = awaitStableConnections()
    // dedicated read total gives this store its own cached client and policies
    val conf = testUserConf + (AerospikeConfig.ReadTotalTimeoutKey -> "1001")
    val store = newStore(conf)
    val ds = "STALL_GB_BATCH"
    val container = "aerospike-chronon-test"

    // warm the client so the pause hits an established pool
    Await.result(store.multiPut(Seq(put(ds, "warm", "v", 100L))), timeout)
    Await.result(store.multiGet(Seq(get(ds, "warm"))), timeout)

    def iterationsOf(e: Throwable): Int = {
      val m = "Error 9,(\\d+)".r.findFirstMatchIn(e.getMessage)
      m.map(_.group(1).toInt).getOrElse(-1)
    }

    var readIterations = -1
    var readElapsedMs = -1L
    var writeIterations = -1
    var writeElapsedMs = -1L
    var writeInDoubt = false
    var writeResult = true

    sys.process.Process(s"docker pause $container").!
    try {
      val readStart = System.nanoTime()
      val resp = Await.result(store.multiGet(Seq(get(ds, "warm"))), timeout)
      readElapsedMs = (System.nanoTime() - readStart) / 1000000
      resp.head.values shouldBe a[Failure[_]]
      readIterations = iterationsOf(resp.head.values.failed.get)

      // the store does not expose the write exception, so the write detail is
      // captured through the same client and write template it uses
      val client = AerospikeClientHolder.clientFor(store.config)
      val policy = new WritePolicy(AerospikeClientHolder.policyTemplates(store.config).write)
      val key = new com.aerospike.client.Key(store.config.namespace,
        com.aerospike.chronon.AerospikeDataModel.physicalSetName(ds), "stall-write")
      val promise = scala.concurrent.Promise[Throwable]()
      val writeStart = System.nanoTime()
      client.put(null, new com.aerospike.client.listener.WriteListener {
        override def onSuccess(k: com.aerospike.client.Key): Unit = promise.success(null)
        override def onFailure(e: com.aerospike.client.AerospikeException): Unit = promise.success(e)
      }, policy, key, new com.aerospike.client.Bin("v", java.util.Collections.singletonMap(1L, bytes("v"))))
      val writeEx = Await.result(promise.future, timeout)
      writeElapsedMs = (System.nanoTime() - writeStart) / 1000000
      writeEx shouldBe a[com.aerospike.client.AerospikeException]
      writeIterations = iterationsOf(writeEx)
      writeInDoubt = writeEx.asInstanceOf[com.aerospike.client.AerospikeException].getInDoubt

      writeResult = Await.result(store.multiPut(Seq(put(ds, "k", "v", 100L))), timeout).head
    } finally {
      sys.process.Process(s"docker unpause $container").!
      // drop the dedicated client so its paused pool cannot churn into later
      // tests, then wait until the server's connection count is back at the
      // pre-test baseline and stable — the footprint tests depend on a quiet rig
      AerospikeClientHolder.evict(store.config)
      awaitStableConnections(Some(baseline))
    }

    info(s"stalled node: read ${readElapsedMs}ms/$readIterations iterations; " +
      s"write ${writeElapsedMs}ms/$writeIterations iterations inDoubt=$writeInDoubt result=$writeResult")
    readIterations should be > 1
    readElapsedMs should be <= store.config.readTotalTimeoutMillis + 500L
    writeResult shouldBe false
    writeIterations should be > 1
    writeInDoubt shouldBe true
    writeElapsedMs should be <= store.config.writeTotalTimeoutMillis + 500L
  }

  // comment 8, section 9 — connection footprint
  private def clientConnections(): Int = {
    val node = AerospikeClientHolder.clientFor(newStore().config).getNodes.head
    Info.request(null, node, "statistics")
      .split(";")
      .collectFirst { case kv if kv.startsWith("client_connections=") => kv.stripPrefix("client_connections=").toInt }
      .getOrElse(fail("client_connections not found in statistics"))
  }

  /** Polls `client_connections` until it reads the same value (and matches
    * `target` when given) for 5 consecutive 200 ms readings, or fails after
    * 20 s. Server connections churn for a while after a pause/unpause or a
    * client close, so every base reading is taken through this. */
  private def awaitStableConnections(target: Option[Int] = None): Int = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    val readings = new scala.collection.mutable.ListBuffer[Int]()
    var stable = 0
    var last = -1
    while (System.nanoTime() < deadline && stable < 5) {
      Thread.sleep(200)
      val now = clientConnections()
      readings += now
      if (now == last && target.forall(_ == now)) stable += 1 else stable = 0
      last = now
    }
    if (stable < 5)
      fail(s"client_connections did not settle${target.fold("")(t => s" at $t")}: " +
        s"last readings ${readings.takeRight(10).mkString(",")}")
    last
  }

  private def connectionDelta(build: => Any): Int = {
    val base = awaitStableConnections()
    build // builds a new client (unique cache key)
    // the async floor fills in the background; wait for it to settle
    val deadline = System.nanoTime() + 5.seconds.toNanos
    var delta = clientConnections() - base
    var prev = -1
    while (delta != prev && System.nanoTime() < deadline) {
      prev = delta
      Thread.sleep(200)
      delta = clientConnections() - base
    }
    delta
  }

  "connection footprint" should "pre-open only the async floor plus the tend connection" in {
    val conf = testUserConf + (AerospikeConfig.ReadTotalTimeoutKey -> "1201") // unique cache key -> new client
    val floor = AerospikeConfig.fromUserConf(conf).minConnsPerNode
    val delta = connectionDelta(newStore(conf))
    info(s"idle connections for floor $floor: delta=$delta")
    delta should be >= floor + 1 // floor + tend
    delta should be <= floor + 3
    delta should not be 2 * floor + 1 // the old mirrored sync pool
  }

  it should "pre-open nothing but the tend connection with floor 0" in {
    val conf = testUserConf +
      (AerospikeConfig.ReadTotalTimeoutKey -> "1202") +
      (AerospikeConfig.MinConnsPerNodeKey  -> "0")
    val delta = connectionDelta(newStore(conf))
    info(s"idle connections for floor 0: delta=$delta")
    delta should be >= 0
    delta should be <= 4 // tend + the build-time namespace info request; was 33 before async-only pools
  }

  // comment 11 — a failed client build must close its event loops
  it should "leave no event-loop threads behind when the seed is unreachable" in {
    def loopThreads: Int =
      Thread.getAllStackTraces.keySet.asScala.count(_.getName.startsWith("aerospike-nio"))
    val base = loopThreads
    // a closed local port as seed; a short connect timeout fails the build fast
    val socket = new java.net.ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    (1 to 4).foreach { i =>
      val conf = testUserConf ++ Map(
        AerospikeConfig.PortKey             -> port.toString,
        AerospikeConfig.ConnectTimeoutKey   -> "300",
        AerospikeConfig.ReadTotalTimeoutKey -> (1300 + i).toString) // unique cache key per attempt
      intercept[Exception] { newStore(conf) }
    }
    val deadline = System.nanoTime() + 10.seconds.toNanos
    while (loopThreads > base && System.nanoTime() < deadline) Thread.sleep(200)
    info(s"event-loop threads after 4 failed builds: ${loopThreads - base}")
    loopThreads shouldBe base
  }

  // comment 18, finding 2 — namespace settings resolve per (client, namespace)
  "namespace settings" should "resolve on demand so a second namespace on the same client is checked" in {
    newStore() // builds the shared client and resolves namespace 'test'
    val realSource = AerospikeClientHolder.namespaceInfoSource
    AerospikeClientHolder.namespaceInfoSource = { (client, ns) =>
      if (ns == "other") "nsup-period=0;allow-ttl-without-nsup=false;default-ttl=0"
      else realSource(client, ns)
    }
    try {
      val otherConf = testUserConf + (AerospikeConfig.NamespaceKey -> "other")
      val ex = intercept[IllegalStateException] { newStore(otherConf) }
      ex.getMessage should include("nsup-period")
      ex.getMessage should include("allow-ttl-without-nsup")
    } finally AerospikeClientHolder.namespaceInfoSource = realSource
  }

  it should "not cache a failed namespace resolution — a later store resolves again" in {
    val realSource = AerospikeClientHolder.namespaceInfoSource
    var reply = "ERROR:4:unrecognized command"
    val conf = testUserConf + (AerospikeConfig.NamespaceKey -> "nother")
    AerospikeClientHolder.namespaceInfoSource = { (_, _) => reply }
    try {
      newStore(conf) // resolution failed: the check is skipped with a WARN
      reply = "nsup-period=0;allow-ttl-without-nsup=false;default-ttl=0"
      val ex = intercept[IllegalStateException] { newStore(conf) } // retried, now enforced
      ex.getMessage should include("nsup-period")
    } finally AerospikeClientHolder.namespaceInfoSource = realSource
  }

  // comment 18, finding 3 — one clock read per multiGet keeps chains and segments aligned
  "multiGet" should "read the clock once per call so segment reads match the root days" in {
    // streaming writes only need the store's pool; segMax 2 forces segment 1 per day
    val store = newStore(testUserConf ++ Map(
      AerospikeConfig.TtlSecondsKey               -> "0",
      AerospikeConfig.SegmentMaxEntriesKey        -> "2",
      AerospikeConfig.StreamingMaxLookbackDaysKey -> "10"))
    val ds = "CLOCK_GB_STREAMING"
    val key = "clocky"
    val now = System.currentTimeMillis()
    val days = (0 to 2).map(i => dayTs(now) - i * MillisPerDay)
    // 4 entries/day with segMax 2 -> ~2 in the root, ~2 in segment 1
    val writes = days.flatMap(d => (1 to 4).map(i => put(ds, key, s"v$d-$i", d + i)))
    Await.result(store.multiPut(writes), timeout)

    // a clock that advances a full day on every call: a second read (the old
    // code called System.currentTimeMillis() again in segmentReadsFor) shifts
    // the chains by one day, so segment keys miss every entry — the old code
    // cannot compile this test at all because the seam does not exist there
    var t = now
    store.clock = { () => t += MillisPerDay; t }
    try {
      val values = Await.result(store.multiGet(Seq(get(ds, key))), timeout).head.values.get
      values.map(_.millis) should contain theSameElementsAs writes.flatMap(_.tsMillis)
    } finally store.clock = { () => System.currentTimeMillis() }
  }

  // key prefix — three stores on one namespace/set share one client and see only their own keys
  "key prefix" should "isolate identical logical keys between prefixed and unprefixed stores" in {
    val base = testUserConf + (AerospikeConfig.SegmentMaxEntriesKey -> "1") // every streaming write rolls a segment
    val storeA = newStore(base + (AerospikeConfig.KeyPrefixKey -> "a:"))
    val storeB = newStore(base + (AerospikeConfig.KeyPrefixKey -> "b:"))
    val store0 = newStore(base)
    val client = AerospikeClientHolder.clientFor(storeA.config)
    // the prefix is not part of the client cache key: one client serves all three
    client shouldBe theSameInstanceAs(AerospikeClientHolder.clientFor(storeB.config))
    client shouldBe theSameInstanceAs(AerospikeClientHolder.clientFor(store0.config))

    val batchDs = "PREFIX_GB_BATCH"
    val streamDs = "PREFIX_GB_STREAMING"
    val statsDs = ApiConstants.StatsBatchDataset
    val day = dayTs(System.currentTimeMillis())
    val schemaKey = s"prefix-${System.nanoTime()}${ApiConstants.TimedKvRDDKeySchemaKey}"
    val stores = Seq(storeA -> "av", storeB -> "bv", store0 -> "v")

    stores.foreach { case (store, v) =>
      Await.result(store.multiPut(Seq(put(batchDs, "k", v, 100L))), timeout) shouldBe Seq(true)
      (0 until 3).foreach(i =>
        Await.result(store.multiPut(Seq(put(streamDs, "k", s"$v-s$i", day + i))), timeout) shouldBe Seq(true))
      Await.result(store.multiPut(Seq(put(statsDs, "j", s"$v-stats", day + 1))), timeout) shouldBe Seq(true)
      Await.result(store.multiPut(Seq(put(statsDs, schemaKey, s"$v-schema", 100L))), timeout) shouldBe Seq(true)
    }

    stores.foreach { case (store, v) =>
      Await.result(store.multiGet(Seq(get(batchDs, "k"))), timeout)
        .head.values.get.map(t => new String(t.bytes, UTF_8)) shouldBe Seq(v)
      Await.result(store.multiGet(Seq(get(streamDs, "k"))), timeout)
        .head.values.get.map(t => new String(t.bytes, UTF_8)) shouldBe (0 until 3).map(i => s"$v-s$i")
      Await.result(store.multiGet(Seq(get(statsDs, "j"))), timeout)
        .head.values.get.map(t => new String(t.bytes, UTF_8)) shouldBe Seq(s"$v-stats")
      Await.result(store.multiGet(Seq(get(statsDs, schemaKey))), timeout)
        .head.values.get.map(t => new String(t.bytes, UTF_8)) shouldBe Seq(s"$v-schema")
    }

    // the physical records exist under prefixed keys, disjoint from the unprefixed ones
    def record(ns: String, set: String, keyBytes: Array[Byte]) =
      client.get(null, new com.aerospike.client.Key(ns, set, keyBytes))
    val ns = storeA.config.namespace
    Seq("a:", "b:").foreach { p =>
      val prefix = Some(p)
      record(ns, "GROUPBY_BATCH", AerospikeDataModel.recordKey(batchDs, bytes("k"), prefix)) should not be null
      (0 to 2).foreach { seg =>
        record(ns, "GROUPBY_STREAMING",
          AerospikeDataModel.streamingRecordKeyFromDay(streamDs, bytes("k"), day, prefix, seg)) should not be null
      }
      record(ns, "GROUPBY_BATCH", AerospikeDataModel.statsFirstDayKey(statsDs, bytes("j"), prefix)) should not be null
      record(ns, "GROUPBY_BATCH", AerospikeDataModel.recordKey(statsDs, bytes(schemaKey), prefix)) should not be null
    }
    def soleValue(r: com.aerospike.client.Record): String =
      new String(r.getMap("v").values().iterator().next().asInstanceOf[Array[Byte]], UTF_8)
    val plainBatch = record(ns, "GROUPBY_BATCH", AerospikeDataModel.recordKey(batchDs, bytes("k"), None))
    plainBatch should not be null
    soleValue(plainBatch) shouldBe "v"
    soleValue(record(ns, "GROUPBY_BATCH", AerospikeDataModel.recordKey(batchDs, bytes("k"), Some("a:")))) shouldBe "av"
    soleValue(record(ns, "GROUPBY_BATCH", AerospikeDataModel.recordKey(batchDs, bytes("k"), Some("b:")))) shouldBe "bv"
    (0 to 2).foreach { seg =>
      record(ns, "GROUPBY_STREAMING",
        AerospikeDataModel.streamingRecordKeyFromDay(streamDs, bytes("k"), day, None, seg)) should not be null
    }
    record(ns, "GROUPBY_BATCH", AerospikeDataModel.statsFirstDayKey(statsDs, bytes("j"), None)) should not be null
    record(ns, "GROUPBY_BATCH", AerospikeDataModel.recordKey(statsDs, bytes(schemaKey), None)) should not be null
  }
}

package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants}
import ai.chronon.online.KVStore.GetRequest
import ai.chronon.online.Metrics
import com.aerospike.client.{BatchRead, Record, ResultCode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import java.util.AbstractMap.SimpleEntry
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class AerospikeKvStoreReadFailureSpec extends AnyFlatSpec with Matchers {

  import AerospikeDataModel.{dayTs, MillisPerDay}

  private val timeout = 10.seconds
  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)

  private def testConfig(extra: (String, String)*): AerospikeConfig =
    AerospikeConfig.fromUserConf(Map(
      AerospikeConfig.HostsKey     -> "localhost",
      AerospikeConfig.NamespaceKey -> "test") ++ extra)

  private def newStore(extra: (String, String)*): AerospikeKvStore =
    new AerospikeKvStore(testConfig(extra: _*), None,
      () => throw new IllegalStateException("no client"))

  private def get(ds: String, key: String, afterTs: Option[Long] = None): GetRequest =
    GetRequest(bytes(key), ds, afterTs)

  private def keyBytes(br: BatchRead): Array[Byte] =
    br.key.userKey.getObject.asInstanceOf[Array[Byte]]

  private def keyString(br: BatchRead): String = new String(keyBytes(br), UTF_8)

  private def entries(tss: Long*): java.util.List[Object] =
    tss.map(ts => new SimpleEntry[Object, Object](Long.box(ts), bytes("v")): Object).asJava

  private def recordWith(bins: (String, Object)*): Record =
    new Record(Map(bins: _*).asJava.asInstanceOf[java.util.Map[String, Object]], 0, 0)

  /** Installs a `batchExecutor` that answers every read from `answers`:
    * key-byte-substring -> either a record or an error code. Unmatched reads
    * get an empty record; `failChunks` substrings turn the whole chunk's
    * future into a failure. Returns the list of key strings the stub saw. */
  private def stubReads(store: AerospikeKvStore,
                        answers: (String, Either[Int, Record])*)
                        (failChunks: String*): scala.collection.mutable.ListBuffer[String] = {
    val seen = scala.collection.mutable.ListBuffer.empty[String]
    // patterns starting with '#' match key suffixes ("#first", "#1"); others match anywhere
    def matches(ks: String, pattern: String): Boolean =
      if (pattern.startsWith("#")) ks.endsWith(pattern) else ks.contains(pattern)
    store.batchExecutor = { (reads, _) =>
      reads.foreach(r => seen += keyString(r))
      if (failChunks.exists(f => reads.exists(r => matches(keyString(r), f))))
        Future.failed(new com.aerospike.client.AerospikeException(ResultCode.TIMEOUT, "injected chunk failure"))
      else {
        reads.foreach { r =>
          val ks = keyString(r)
          answers.collectFirst { case (s, outcome) if matches(ks, s) => outcome } match {
            case Some(Right(rec)) => r.record = rec; r.resultCode = 0
            case Some(Left(code)) => r.resultCode = code; r.inDoubt = false
            case None             => r.record = recordWith("v" -> entries()); r.resultCode = 0
          }
        }
        Future.successful(reads.toList)
      }
    }
    seen
  }

  private def metricEvents(store: AerospikeKvStore)(body: => Unit): Seq[AerospikeKvStore.MetricEvent] = {
    val buf = scala.collection.mutable.ListBuffer.empty[AerospikeKvStore.MetricEvent]
    store.metricsSink = Some(e => buf.synchronized { buf += e })
    try body
    finally store.metricsSink = None
    buf.toSeq
  }

  private def countSum(events: Seq[AerospikeKvStore.MetricEvent], dataset: String, name: String): Long =
    events.filter(e => e.count && e.name == name && e.suffix.contains(dataset)).map(_.value).sum

  "multiGet" should "fail a null-keyBytes request without a cluster call while others succeed" in {
    val store = newStore()
    val ds = "RF_GB_BATCH"
    val seen = stubReads(store, "RF_GB_BATCH" -> Right(recordWith("v" -> entries(7L))))()
    val events = metricEvents(store) {
      val resps = Await.result(store.multiGet(Seq(
        GetRequest(null, ds, None),
        get(ds, "good"))), timeout)
      resps should have size 2
      resps(0).values shouldBe a[Failure[_]]
      resps(0).values.failed.get shouldBe an[IllegalArgumentException]
      resps(0).values.failed.get.getMessage should include("keyBytes is null")
      resps(1).values shouldBe a[Success[_]]
      resps(1).values.get.map(_.millis) shouldBe Seq(7L)
    }
    countSum(events, ds, Metrics.Name.FetchExceptions) shouldBe 1
    seen should have size 1
    seen.head should include("good")
  }

  "stats round 0" should "fail only the stats request when the marker read errors" in {
    val store = newStore()
    val statsDs = ApiConstants.StatsBatchDataset
    val batchDs = "RF_GB_BATCH"
    stubReads(store,
      "#first" -> Left(ResultCode.TIMEOUT),
      batchDs   -> Right(recordWith("v" -> entries(11L))))()
    val resps = Await.result(store.multiGet(Seq(get(statsDs, "j"), get(batchDs, "k"))), timeout)
    resps(0).values shouldBe a[Failure[_]]
    resps(0).values.failed.get shouldBe a[com.aerospike.client.AerospikeException]
    resps(1).values.get.map(_.millis) shouldBe Seq(11L)
  }

  it should "fail only the stats request when the legacy plain-key read errors" in {
    val store = newStore()
    val statsDs = ApiConstants.StatsBatchDataset
    val batchDs = "RF_GB_BATCH"
    stubReads(store,
      s"$statsDs#j" -> Left(ResultCode.TIMEOUT),
      "#first"      -> Right(recordWith("first" -> Long.box(0L))),
      batchDs       -> Right(recordWith("v" -> entries(12L))))()
    val resps = Await.result(store.multiGet(Seq(get(statsDs, "j"), get(batchDs, "k"))), timeout)
    resps(0).values shouldBe a[Failure[_]]
    resps(0).values.failed.get shouldBe a[com.aerospike.client.AerospikeException]
    resps(1).values.get.map(_.millis) shouldBe Seq(12L)
  }

  it should "fail only the stats request when its round-0 chunk fails" in {
    val store = newStore(AerospikeConfig.ReadBatchChunkSizeKey -> "1")
    val statsDs = ApiConstants.StatsBatchDataset
    val batchDs = "RF_GB_BATCH"
    stubReads(store,
      batchDs -> Right(recordWith("v" -> entries(13L))))(statsDs)
    val resps = Await.result(store.multiGet(Seq(get(statsDs, "j"), get(batchDs, "k"))), timeout)
    resps(0).values shouldBe a[Failure[_]]
    resps(0).values.failed.get shouldBe a[com.aerospike.client.AerospikeException]
    resps(1).values.get.map(_.millis) shouldBe Seq(13L)
  }

  "the segment round" should "fail only the request whose segment read errors" in {
    val store = newStore(AerospikeConfig.StreamingMaxLookbackDaysKey -> "0")
    val ds = "RF_GB_STREAMING"
    // root reads (ending "#0") get an empty map and tail 2; segments 1 and 2
    // error; a second key stays cold and succeeds
    stubReads(store,
      "#1"          -> Left(ResultCode.TIMEOUT),
      s"${ds}#k#"  -> Right(recordWith("v" -> entries(), "tail" -> Long.box(2L))),
      s"${ds}#k2#" -> Right(recordWith("v" -> entries(5L), "tail" -> Long.box(0L))))()
    val resps = Await.result(store.multiGet(Seq(get(ds, "k"), get(ds, "k2"))), timeout)
    resps(0).values shouldBe a[Failure[_]]
    resps(0).values.failed.get shouldBe a[com.aerospike.client.AerospikeException]
    resps(1).values.get.map(_.millis) shouldBe Seq(5L)
  }

  it should "fail only the request whose segment chunk fails" in {
    val store = newStore(AerospikeConfig.StreamingMaxLookbackDaysKey -> "0")
    val ds = "RF_GB_STREAMING"
    stubReads(store,
      s"${ds}#k#"  -> Right(recordWith("v" -> entries(6L), "tail" -> Long.box(1L))),
      s"${ds}#k2#" -> Right(recordWith("v" -> entries(7L), "tail" -> Long.box(0L))))("#1")
    val resps = Await.result(store.multiGet(Seq(get(ds, "k"), get(ds, "k2"))), timeout)
    resps(0).values shouldBe a[Failure[_]]
    resps(1).values.get.map(_.millis) shouldBe Seq(7L)
  }

  it should "fail a stats timeseries whose day segment read errors" in {
    val store = newStore()
    val statsDs = ApiConstants.StatsBatchDataset
    val day = dayTs(System.currentTimeMillis())
    stubReads(store,
      "#first" -> Right(recordWith("first" -> Long.box(day))),
      "#0"     -> Right(recordWith("v" -> entries(), "tail" -> Long.box(1L))),
      "#1"     -> Left(ResultCode.TIMEOUT))()
    val resps = Await.result(store.multiGet(Seq(get(statsDs, "j"))), timeout)
    resps.head.values shouldBe a[Failure[_]]
    resps.head.values.failed.get shouldBe a[com.aerospike.client.AerospikeException]
  }

  "a malformed record" should "fail only the request whose v bin cannot be decoded" in {
    val store = newStore()
    val ds = "RF_GB_BATCH"
    stubReads(store,
      s"$ds#bad"  -> Right(recordWith("v" -> "not-a-list")),
      s"$ds#good" -> Right(recordWith("v" -> entries(9L))))()
    val resps = Await.result(store.multiGet(Seq(get(ds, "bad"), get(ds, "good"))), timeout)
    resps(0).values shouldBe a[Failure[_]]
    // Record.getList casts the bin to java.util.List: a non-list bin is a
    // ClassCastException, isolated to that request by the per-request Try
    resps(0).values.failed.get shouldBe a[ClassCastException]
    resps(1).values.get.map(_.millis) shouldBe Seq(9L)
  }

  "streaming day windows" should "shift with the clock across a day boundary" in {
    val store = newStore(
      AerospikeConfig.TtlSecondsKey -> "86400",
      AerospikeConfig.StreamingMaxLookbackDaysKey -> "1")
    val ds = "RF_GB_STREAMING"

    def daysReadAt(epochMillis: Long): Seq[Long] = {
      store.clock = () => epochMillis
      try {
        store.buildBatchReads(get(ds, "k")).map { case (br, _) =>
          val parts = keyString(br).split('#')
          parts(parts.length - 2).toLong
        }
      } finally store.clock = () => System.currentTimeMillis()
    }

    val d13 = java.time.LocalDate.of(2026, 10, 13).atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    daysReadAt(java.time.Instant.parse("2026-10-13T23:59:59.999Z").toEpochMilli) shouldBe Seq(d13 - MillisPerDay, d13)
    daysReadAt(java.time.Instant.parse("2026-10-14T00:00:00.000Z").toEpochMilli) shouldBe Seq(d13, d13 + MillisPerDay)
  }

  "extreme afterTsMillis" should "read the full window at Long.MinValue and nothing at Long.MaxValue" in {
    val now = System.currentTimeMillis()
    val store = newStore()
    store.clock = () => now
    val ds = "RF_GB_STREAMING"

    val window = store.buildBatchReads(get(ds, "k")).map { case (br, _) => keyString(br) }
    window should not be empty
    store.buildBatchReads(get(ds, "k", Some(Long.MinValue))).map { case (br, _) => keyString(br) } shouldBe window
    store.buildBatchReads(get(ds, "k", Some(Long.MaxValue))) shouldBe empty

    val batchDs = "RF_GB_BATCH"
    store.buildBatchReads(get(batchDs, "k", Some(Long.MinValue))) should have size 1
    store.buildBatchReads(get(batchDs, "k", Some(Long.MaxValue))) should have size 1

    val first = dayTs(now) - 5 * MillisPerDay
    AerospikeDataModel.statsDays(Some(Long.MinValue), first, dayTs(now)).head shouldBe first
  }

  "batch read policy" should "use the stats template for stats datasets and the serving template otherwise" in {
    val store = newStore(
      "aerospike.timeout.read.total.millis" -> "1500",
      "aerospike.timeout.stats.read.total.millis" -> "7000")
    val statsDs = ApiConstants.StatsBatchDataset
    val seen = scala.collection.mutable.ListBuffer.empty[(Set[String], Int)]
    store.batchExecutor = { (reads, policy) =>
      seen.synchronized { seen += ((reads.map(keyString).toSet, policy.totalTimeout)) }
      reads.foreach { r => r.record = recordWith("v" -> entries(1L)); r.resultCode = 0 }
      Future.successful(reads.toList)
    }
    val resps = Await.result(store.multiGet(Seq(
      get("POL_GB_BATCH", "plain"),
      get(statsDs, "join"),
      get("POL_GB_BATCH", "plain2"),
      get(statsDs, "join2"))), timeout)
    resps should have size 4
    resps.foreach(_.values shouldBe a[Success[_]])
    val statsCalls = seen.filter(_._1.exists(_.contains("join")))
    val plainCalls = seen.filter(_._1.exists(_.contains("plain")))
    statsCalls should not be empty
    plainCalls should not be empty
    statsCalls.foreach(_._2 shouldBe 7000)
    plainCalls.foreach(_._2 shouldBe 1500)
    seen.foreach { case (keys, _) => (keys.exists(_.contains("join")) && keys.exists(_.contains("plain"))) shouldBe false }
  }
}

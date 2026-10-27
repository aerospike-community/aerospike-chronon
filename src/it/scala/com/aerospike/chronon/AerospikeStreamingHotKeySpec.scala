package com.aerospike.chronon

import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import com.aerospike.chronon.AerospikeDataModel.{dayTs, physicalSetName, streamingRecordKey}
import com.aerospike.client.cdt.{MapOperation, MapOrder, MapPolicy, MapWriteFlags}
import com.aerospike.client.policy.WritePolicy
import com.aerospike.client.{AerospikeException, Info, Key, ResultCode, Value}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/** Opt-in probe (set `AEROSPIKE_PROBE=1`): fills one entity's streaming day
  * record with distinct-ts events until Aerospike rejects the write, for several
  * payload sizes. Answers: how many events fit in one day record, what the
  * failure looks like to the caller, and whether earlier data stays readable.
  * Prints a summary table and writes it to `target/hot-key-probe-results.txt`. */
class AerospikeStreamingHotKeySpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private case class Result(payloadBytes: Int,
                            events: Int,
                            softLimit: Int,
                            segments: Int,
                            maxSegmentEntries: Int)

  private val dataset      = "HOTKEY_GB_STREAMING"
  private val payloadSizes = Seq(64, 128, 256, 512, 1024)
  private val maxEvents    = 200000
  private val timeout      = 60.seconds
  private val results      = ArrayBuffer.empty[Result]
  private var limits       = Map.empty[String, String]
  private var concurrencyRows = Seq.empty[String]

  private val probeEnabled = sys.env.contains("AEROSPIKE_PROBE")

  private def enabled(): Unit =
    assume(probeEnabled, "set AEROSPIKE_PROBE=1 to run the hot-key record-size probe")

  private def payload(size: Int, i: Int): Array[Byte] = Array.fill[Byte](size)((i % 251).toByte)

  private def median(xs: Seq[Long]): Long = xs.sorted.apply(xs.size / 2)

  private def timed[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val out   = f
    (out, (System.nanoTime() - start) / 1000000)
  }

  "namespace config" should "be recorded so the limits under test are not assumed" in {
    enabled()
    val client = AerospikeClientHolder.clientFor(newStore().config)
    val raw    = Info.request(null, client.getNodes.head, s"namespace/$aerospikeNamespace")
    limits = raw.split(";").flatMap(_.split("=", 2) match {
      case Array(k, v) => Some(k -> v)
      case _           => None
    }).toMap
    val interesting = limits.filterKeys(k => k.contains("block-size") || k.contains("record-size") || k.contains("storage-engine") || k == "default-ttl")
    info(s"effective limits: ${interesting.toSeq.sorted.map { case (k, v) => s"$k=$v" }.mkString(", ")}")
  }

  // events that filled one day record before segmenting (measured by this probe on aerospike-server:8.1.2.5)
  private val oldCeiling = Map(64 -> 13796, 128 -> 7489, 256 -> 3897, 512 -> 1997, 1024 -> 1011)
  private val targetKiB  = 64

  payloadSizes.foreach { size =>
    val softLimit = math.max(1, targetKiB * 1024 / (size + 13))
    val events    = oldCeiling(size) * 2

    "a hot streaming day" should s"roll over at $softLimit entries for $size-byte events and write twice the old ceiling ($events) without RECORD_TOO_BIG" in {
      enabled()
      val store  = newStore(testUserConf + (AerospikeConfig.SegmentMaxEntriesKey -> softLimit.toString))
      val config = store.config
      val client = AerospikeClientHolder.clientFor(config)
      val entity = s"hot-$size-${System.nanoTime()}".getBytes(UTF_8)
      val baseTs = dayTs(System.currentTimeMillis())

      val failed = (0 until events).count(i =>
        !Await.result(store.multiPut(Seq(PutRequest(entity, payload(size, i), dataset, Some(baseTs + i)))), timeout).head)
      failed shouldBe 0

      def segKey(seg: Int) = new Key(config.namespace, physicalSetName(dataset), streamingRecordKey(dataset, entity, baseTs, config.keyPrefix, seg))
      val tail  = client.get(null, segKey(0)).getLong("tail").toInt
      val sizes = (0 to tail).map(seg => client.operate(null, segKey(seg), MapOperation.size("v")).getLong("v").toInt)
      sizes.sum shouldBe events
      sizes.foreach(_ should be <= softLimit)
      results += Result(size, events, softLimit, tail + 1, sizes.max)
    }
  }

  "concurrent writes to one hot record" should "be counted separately from record-size failures" in {
    enabled()
    val store   = newStore()
    val entity  = s"busy-${System.nanoTime()}".getBytes(UTF_8)
    val baseTs  = dayTs(System.currentTimeMillis())
    val rounds  = Seq(10, 25, 50, 100, 250).map { n =>
      val requests = (0 until n).map(i => PutRequest(entity, payload(64, i), dataset, Some(baseTs + n * 1000L + i)))
      val outcome  = Await.result(store.multiPut(requests), timeout)
      val retried  = requests.zip(outcome).collect { case (r, false) => r }.map(r => Await.result(store.multiPut(Seq(r)), timeout).head)
      (n, outcome.count(_ == false), retried.count(identity))
    }
    concurrencyRows = rounds.map { case (n, failures, recovered) => f"$n%12d $failures%10d $recovered%18d" }
    rounds.foreach { case (n, failures, recovered) => recovered shouldBe failures }
  }

  "the probe" should "report the summary table" in {
    enabled()
    val blockSize = limits.get("max-record-size").orElse(limits.get("write-block-size")).map(_.toLong)
    val header    = f"${"payload(B)"}%10s ${"events"}%8s ${"softLimit"}%10s ${"segments"}%9s ${"max entries"}%12s ${"max KiB (est)"}%14s"
    val rows = results.sortBy(_.payloadBytes).map { r =>
      f"${r.payloadBytes}%10d ${r.events}%8d ${r.softLimit}%10d ${r.segments}%9d ${r.maxSegmentEntries}%12d ${r.maxSegmentEntries.toLong * (r.payloadBytes + 13) / 1024}%14d"
    }
    val concurrencyHeader = f"${"concurrent puts"}%12s ${"failures"}%10s ${"succeeded on retry"}%18s"
    val table = (Seq(
      s"record size limit=${blockSize.map(_.toString).getOrElse("unknown")}  (${limits.filterKeys(k => k.contains("block-size") || k.contains("record-size")).mkString(", ")})",
      header) ++ rows ++ Seq("", "same-record concurrency (64-byte events, one fresh record per row)", concurrencyHeader) ++ concurrencyRows).mkString("\n")
    println("\n==== hot-key record-size probe ====\n" + table + "\n===================================")
    val out = new PrintWriter(new File("target/hot-key-probe-results.txt"))
    try out.println(table) finally out.close()
    results.size shouldBe payloadSizes.size
  }
}

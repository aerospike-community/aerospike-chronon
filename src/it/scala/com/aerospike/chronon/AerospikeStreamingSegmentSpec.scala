package com.aerospike.chronon

import ai.chronon.online.KVStore.PutRequest
import com.aerospike.chronon.AerospikeDataModel.{dayTs, physicalSetName, streamingRecordKeyFromDay}
import com.aerospike.client.cdt.{MapOperation, MapOrder, MapPolicy, MapWriteFlags}
import com.aerospike.client.{Key, Value}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class AerospikeStreamingSegmentSpec extends AnyFlatSpec with Matchers with AerospikeDockerSpec {

  private val timeout = 60.seconds
  private val ds = "SEG_GB_STREAMING"
  private val day = dayTs(System.currentTimeMillis())

  private def key(name: String) = name.getBytes(UTF_8)
  private def put(name: String, ts: Long, size: Int = 8) = PutRequest(key(name), new Array[Byte](size), ds, Some(ts))

  private def storeWith(entries: Int, segments: Int = 1024) = newStore(testUserConf +
    (AerospikeConfig.SegmentMaxEntriesKey -> entries.toString) + (AerospikeConfig.MaxSegmentsPerDayKey -> segments.toString))

  private def recordKey(store: AerospikeKvStore, name: String, seg: Int) =
    new Key(store.config.namespace, physicalSetName(ds), streamingRecordKeyFromDay(ds, key(name), day, None, seg))

  private def tailOf(store: AerospikeKvStore, name: String): Long =
    AerospikeClientHolder.clientFor(store.config).get(null, recordKey(store, name, 0)).getLong("tail")

  /** Raw entry timestamps of every segment from 0 through the root's tail. */
  private def segments(store: AerospikeKvStore, name: String): Seq[Seq[Long]] = {
    val client = AerospikeClientHolder.clientFor(store.config)
    (0 to tailOf(store, name).toInt).map { seg =>
      Option(client.get(null, recordKey(store, name, seg))).map(_.getMap("v").keySet.asScala.map(_.asInstanceOf[Long]).toSeq.sorted).getOrElse(Seq.empty)
    }
  }

  "the map put result" should "be the new map size, readable from the operate response" in {
    val store = newStore()
    val client = AerospikeClientHolder.clientFor(store.config)
    val k = recordKey(store, s"size-${System.nanoTime()}", 7)
    val policy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT)
    client.operate(null, k, MapOperation.put(policy, "v", Value.get(1L), Value.get(Array[Byte](1)))).getLong("v") shouldBe 1L
    client.operate(null, k, MapOperation.put(policy, "v", Value.get(2L), Value.get(Array[Byte](1)))).getLong("v") shouldBe 2L
    client.operate(null, k, MapOperation.put(policy, "v", Value.get(2L), Value.get(Array[Byte](2)))).getLong("v") shouldBe 2L
  }

  "streaming rollover" should "roll a hot key over with no false results and no segment above the soft limit" in {
    val store = storeWith(20)
    val name = s"hot-${System.nanoTime()}"
    val results = (0 until 100).map(i => Await.result(store.multiPut(Seq(put(name, day + i))), timeout).head)
    results.count(!_) shouldBe 0
    val segs = segments(store, name)
    segs.size should be >= 5
    segs.foreach(_.size should be <= 20)
    segs.flatten.sorted shouldBe (0 until 100).map(day + _)
  }

  it should "store each ts once across segments under concurrent writers crossing the limit" in {
    val store = storeWith(20)
    val name = s"concurrent-${System.nanoTime()}"
    val pool = Executors.newFixedThreadPool(16)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    try {
      val futures = (0 until 16).map(t => Future {
        (0 until 50).map(i => Await.result(store.multiPut(Seq(put(name, day + t * 50 + i))), timeout).head)
      })
      val results = Await.result(Future.sequence(futures), 120.seconds).flatten
      results.count(!_) shouldBe 0
    } finally pool.shutdown()
    val segs = segments(store, name)
    segs.foreach(_.size should be <= 20 + 2 * 16)
    segs.flatten.toSet shouldBe (0 until 800).map(day + _).toSet
    segs.flatten.size should be <= 800 + 16
  }

  it should "redirect a stale writer to the current tail instead of writing into the full root" in {
    val a = storeWith(10)
    val name = s"stale-${System.nanoTime()}"
    (0 until 25).foreach(i => Await.result(a.multiPut(Seq(put(name, day + i))), timeout).head shouldBe true)
    val before = segments(a, name)
    before.head.size shouldBe 10

    val b = storeWith(10) // fresh hint cache: starts at segment 0
    Await.result(b.multiPut(Seq(put(name, day + 1000))), timeout) shouldBe Seq(true)
    val after = segments(b, name)
    after.head shouldBe before.head
    after.flatten should contain(day + 1000)
  }

  it should "report a lost tail advance as a race and follow the winner" in {
    val store = storeWith(1000)
    val name = s"race-${System.nanoTime()}"
    Await.result(store.multiPut(Seq(put(name, day))), timeout) shouldBe Seq(true)
    val req = put(name, day)
    val chain = store.Chain(day, stats = false)
    Await.result(store.advanceTail(req, chain, 0), timeout) shouldBe 1
    tailOf(store, name) shouldBe 1L
    Await.result(store.advanceTail(req, chain, 0), timeout) shouldBe 1 // stale expected: filtered out, winner followed
    tailOf(store, name) shouldBe 1L
    Await.result(store.advanceTail(req, chain, 1), timeout) shouldBe 2
  }

  it should "trigger the RECORD_TOO_BIG backstop for oversized payloads and still succeed" in {
    val store = storeWith(1000)
    val name = s"big-${System.nanoTime()}"
    val results = (0 until 3).map(i => Await.result(store.multiPut(Seq(put(name, day + i, 600 * 1024))), timeout).head)
    results shouldBe Seq(true, true, true)
    tailOf(store, name) shouldBe 2L
    segments(store, name).map(_.size) shouldBe Seq(1, 1, 1)
  }

  it should "return false and count record_too_big once maxSegmentsPerDay is reached" in {
    val store = storeWith(1000, segments = 2)
    val name = s"cap-${System.nanoTime()}"
    val results = (0 until 3).map(i => Await.result(store.multiPut(Seq(put(name, day + i, 600 * 1024))), timeout).head)
    results shouldBe Seq(true, true, false)
    tailOf(store, name) shouldBe 1L
  }
}

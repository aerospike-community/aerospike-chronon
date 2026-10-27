package com.aerospike.chronon

import com.aerospike.client.Record
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.AbstractMap.SimpleEntry
import scala.collection.JavaConverters._

class AerospikeKvStoreExtractSpec extends AnyFlatSpec with Matchers {

  private def record(entries: (Long, Array[Byte])*): Record = {
    val list = entries.map { case (ts, v) => new SimpleEntry[Object, Object](Long.box(ts), v): Object }.asJava
    new Record(Map[String, Object]("v" -> list).asJava, 1, 0)
  }

  "extractTimedValues" should "decode the single-entry KEY_VALUE shape of a getByRank read" in {
    val values = AerospikeKvStore.extractTimedValues(record(42L -> Array[Byte](1, 2)))
    values.map(_.millis) shouldBe Seq(42L)
    values.head.bytes shouldBe Array[Byte](1, 2)
  }

  it should "decode multi-entry lists and tolerate null or empty records" in {
    AerospikeKvStore.extractTimedValues(record(1L -> Array[Byte](1), 2L -> Array[Byte](2))).map(_.millis) shouldBe Seq(1L, 2L)
    AerospikeKvStore.extractTimedValues(null) shouldBe empty
    AerospikeKvStore.extractTimedValues(record()) shouldBe empty
  }

  private def entry(k: Object, v: Object): Object = new SimpleEntry[Object, Object](k, v)

  private def rawRecord(items: Object*): Record =
    new Record(Map[String, Object]("v" -> items.asJava).asJava, 1, 0)

  it should "skip malformed entries and keep the good ones in order" in {
    val values = AerospikeKvStore.extractTimedValues(rawRecord(
      entry(Long.box(1L), Array[Byte](1)),
      entry("not-a-number", Array[Byte](2)),          // key not numeric -> skipped
      Long.box(99L),                                   // not a Map.Entry -> skipped
      entry(Long.box(3L), "not-bytes"),                // value not bytes -> skipped
      entry(Long.box(4L), Array[Byte](4))))
    values.map(_.millis) shouldBe Seq(1L, 4L)
    values.map(_.bytes.head) shouldBe Seq(1.toByte, 4.toByte)
  }

  it should "fall back to parsing a numeric-string key" in {
    val values = AerospikeKvStore.extractTimedValues(rawRecord(entry("123", Array[Byte](7))))
    values.map(_.millis) shouldBe Seq(123L)
    values.head.bytes shouldBe Array[Byte](7)
  }

  it should "decode Value-typed keys and values" in {
    val values = AerospikeKvStore.extractTimedValues(rawRecord(
      entry(com.aerospike.client.Value.get(55L), Array[Byte](8)),
      entry(Long.box(66L), com.aerospike.client.Value.get(Array[Byte](9)))))
    values.map(_.millis) shouldBe Seq(55L, 66L)
    values.map(_.bytes.head) shouldBe Seq(8.toByte, 9.toByte)
  }

  private def tv(ts: Long, b: Byte): ai.chronon.online.KVStore.TimedValue =
    ai.chronon.online.KVStore.TimedValue(Array[Byte](b), ts)

  "mergeByTs" should "merge disjoint chunks in ts order" in {
    val merged = AerospikeKvStore.mergeByTs(Seq(Seq(tv(1, 1), tv(3, 3)), Seq(tv(2, 2), tv(4, 4))))
    merged.map(_.millis) shouldBe Seq(1L, 2L, 3L, 4L)
    merged.map(_.bytes.head) shouldBe Seq(1.toByte, 2.toByte, 3.toByte, 4.toByte)
  }

  it should "merge overlapping ranges and sort the output by ts" in {
    val merged = AerospikeKvStore.mergeByTs(Seq(Seq(tv(5, 5), tv(7, 7)), Seq(tv(1, 1), tv(6, 6)), Seq(tv(9, 9))))
    merged.map(_.millis) shouldBe Seq(1L, 5L, 6L, 7L, 9L)
  }

  it should "let the later chunk win on duplicate ts" in {
    val merged = AerospikeKvStore.mergeByTs(Seq(Seq(tv(1, 1), tv(2, 2)), Seq(tv(2, 9), tv(3, 3))))
    merged.map(_.millis) shouldBe Seq(1L, 2L, 3L)
    merged.map(_.bytes.head) shouldBe Seq(1.toByte, 9.toByte, 3.toByte)
  }

  it should "return empty on empty input" in {
    AerospikeKvStore.mergeByTs(Nil) shouldBe empty
    AerospikeKvStore.mergeByTs(Seq(Seq.empty, Seq.empty)) shouldBe empty
  }

  "statsEntryLimit" should "derive the per-segment entry cap from the byte target and value size" in {
    AerospikeKvStore.statsEntryLimit(512, 1024) shouldBe 505       // 524288 / 1037
    AerospikeKvStore.statsEntryLimit(512, 600 * 1024) shouldBe 1   // value larger than the target
    AerospikeKvStore.statsEntryLimit(4, 1024) shouldBe 3
    AerospikeKvStore.statsEntryLimit(512, 0) shouldBe 40329        // 524288 / 13
    AerospikeKvStore.statsEntryLimit(512, Int.MaxValue) shouldBe 1
    AerospikeKvStore.statsEntryLimit(1, 50) shouldBe 16            // 1024 / 63
  }

  "chunkReads" should "split into ordered chunks of at most the chunk size" in {
    AerospikeKvStore.chunkReads(Seq.empty[Int], 3) shouldBe empty
    AerospikeKvStore.chunkReads(Seq(1, 2, 3), 3).map(_.toSeq) shouldBe Seq(Seq(1, 2, 3))
    AerospikeKvStore.chunkReads(1 to 4, 3).map(_.toSeq) shouldBe Seq(Seq(1, 2, 3), Seq(4))
    AerospikeKvStore.chunkReads(1 to 5, 1).map(_.toSeq) shouldBe Seq(Seq(1), Seq(2), Seq(3), Seq(4), Seq(5))
    AerospikeKvStore.chunkReads(1 to 7, 3).flatten.toSeq shouldBe (1 to 7)
  }
}

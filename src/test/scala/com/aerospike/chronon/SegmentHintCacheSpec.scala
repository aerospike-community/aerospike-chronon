package com.aerospike.chronon

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

class SegmentHintCacheSpec extends AnyFlatSpec with Matchers {

  private def k(s: String): Array[Byte] = s.getBytes("UTF-8")

  "SegmentHintCache" should "default to segment 0 and distinguish dataset, key and day" in {
    val c = new SegmentHintCache(10)
    c.get("D", k("a"), 1L) shouldBe 0
    c.advanceTo("D", k("a"), 1L, 3)
    c.get("D", k("a"), 1L) shouldBe 3
    c.get("D", k("a"), 2L) shouldBe 0
    c.get("D", k("b"), 1L) shouldBe 0
    c.get("E", k("a"), 1L) shouldBe 0
  }

  it should "match keys by content, not by array identity" in {
    val c = new SegmentHintCache(10)
    c.advanceTo("D", k("same"), 1L, 2)
    c.get("D", k("same"), 1L) shouldBe 2
  }

  it should "only move forward" in {
    val c = new SegmentHintCache(10)
    c.advanceTo("D", k("a"), 1L, 5) shouldBe 5
    c.advanceTo("D", k("a"), 1L, 2) shouldBe 5
    c.get("D", k("a"), 1L) shouldBe 5
  }

  it should "evict the least recently used entry beyond capacity" in {
    val c = new SegmentHintCache(2)
    c.advanceTo("D", k("a"), 1L, 1)
    c.advanceTo("D", k("b"), 1L, 1)
    c.get("D", k("a"), 1L) shouldBe 1 // touch a so b is the eldest
    c.advanceTo("D", k("c"), 1L, 1)
    c.size shouldBe 2
    c.get("D", k("b"), 1L) shouldBe 0
    c.get("D", k("a"), 1L) shouldBe 1
  }

  it should "stay consistent under concurrent updates" in {
    val c = new SegmentHintCache(1000)
    val pool = Executors.newFixedThreadPool(8)
    val done = new CountDownLatch(8)
    (0 until 8).foreach { t =>
      pool.submit(new Runnable { def run(): Unit = {
        (0 until 2000).foreach(i => c.advanceTo("D", k("hot"), 1L, (i * 8 + t) % 5000))
        done.countDown()
      } })
    }
    done.await(30, TimeUnit.SECONDS) shouldBe true
    pool.shutdown()
    c.get("D", k("hot"), 1L) shouldBe 4999
  }

  it should "clone the key bytes on insert so later caller mutation does not corrupt the entry" in {
    val c = new SegmentHintCache(10)
    val key = k("mutable")
    c.advanceTo("D", key, 1L, 3)
    key(0) = 'X'.toByte
    c.get("D", k("mutable"), 1L) shouldBe 3
    c.get("D", key, 1L) shouldBe 0 // the mutated array is a different key now
  }

  "FirstDayCache" should "clone the key bytes on ensure so later caller mutation does not corrupt the entry" in {
    val c = new FirstDayCache(10)
    val key = k("mutable")
    c.ensure("D", key, 100L)
    key(0) = 'X'.toByte
    c.get("D", k("mutable")) shouldBe Some(100L)
    c.get("D", key) shouldBe None
  }

  it should "return None for an unknown key" in {
    new FirstDayCache(10).get("D", k("a")) shouldBe None
  }

  it should "return and keep the minimum ensured day" in {
    val c = new FirstDayCache(10)
    c.ensure("D", k("a"), 100L) shouldBe 100L
    c.ensure("D", k("a"), 200L) shouldBe 100L // a later day does not raise it
    c.ensure("D", k("a"), 50L) shouldBe 50L  // an earlier day lowers it
    c.get("D", k("a")) shouldBe Some(50L)
  }

  it should "distinguish datasets and match keys by content" in {
    val c = new FirstDayCache(10)
    c.ensure("D", k("a"), 100L)
    c.ensure("E", k("a"), 200L)
    c.get("D", k("a")) shouldBe Some(100L)
    c.get("D", "a".getBytes("UTF-8").clone()) shouldBe Some(100L)
    c.get("E", k("a")) shouldBe Some(200L)
    c.get("D", k("b")) shouldBe None
  }

  it should "evict the least recently used entry beyond capacity" in {
    val c = new FirstDayCache(2)
    c.ensure("D", k("a"), 10L)
    c.ensure("D", k("b"), 20L)
    c.get("D", k("a")) shouldBe Some(10L) // touch a so b is the eldest
    c.ensure("D", k("c"), 30L)
    c.size shouldBe 2
    c.get("D", k("b")) shouldBe None
    c.get("D", k("a")) shouldBe Some(10L)
  }

  it should "converge to the global minimum under concurrent ensures" in {
    val c = new FirstDayCache(1000)
    val threads = 16
    val pool = Executors.newFixedThreadPool(threads)
    val done = new CountDownLatch(threads)
    (0 until threads).foreach { t =>
      pool.submit(new Runnable { def run(): Unit = {
        (0 until 500).foreach(i => c.ensure("D", k("hot"), (t * 500 + i) * 86400000L))
        done.countDown()
      } })
    }
    done.await(30, TimeUnit.SECONDS) shouldBe true
    pool.shutdown()
    c.get("D", k("hot")) shouldBe Some(0L)
  }
}

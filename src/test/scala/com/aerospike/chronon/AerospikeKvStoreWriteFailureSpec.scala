package com.aerospike.chronon

import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.Metrics
import com.aerospike.client.{AerospikeException, Key, Operation, Record, ResultCode}
import com.aerospike.client.policy.WritePolicy
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class AerospikeKvStoreWriteFailureSpec extends AnyFlatSpec with Matchers {

  import AerospikeKvStoreWriteFailureSpec._
  import AerospikeDataModel.dayTs

  private val timeout = 10.seconds
  private def bytes(s: String): Array[Byte] = s.getBytes(UTF_8)

  private def testConfig(extra: (String, String)*): AerospikeConfig =
    AerospikeConfig.fromUserConf(Map(
      AerospikeConfig.HostsKey     -> "localhost",
      AerospikeConfig.NamespaceKey -> "test") ++ extra)

  private def newStore(extra: (String, String)*): (AerospikeKvStore, ScriptedOperate) = {
    val store = new AerospikeKvStore(testConfig(extra: _*), None,
      () => throw new IllegalStateException("no client"))
    val op = new ScriptedOperate
    store.operateExecutor = op
    (store, op)
  }

  private def put(ds: String, key: String, ts: Long): PutRequest =
    PutRequest(bytes(key), bytes("v"), ds, Some(ts))

  private def metricEvents(store: AerospikeKvStore)(body: => Unit): Seq[AerospikeKvStore.MetricEvent] = {
    val buf = scala.collection.mutable.ListBuffer.empty[AerospikeKvStore.MetricEvent]
    store.metricsSink = Some(e => buf.synchronized { buf += e })
    try body
    finally store.metricsSink = None
    buf.toSeq
  }

  private def countSum(events: Seq[AerospikeKvStore.MetricEvent], dataset: String, name: String): Long =
    events.filter(e => e.count && e.name == name && e.suffix.contains(dataset)).map(_.value).sum

  "multiPut" should "redirect a filtered-out segment write up to MaxWriteAttempts" in {
    val (store, op) = newStore()
    val ds = "WF_GB_STREAMING"
    op.always(SegmentPut, Fail(ResultCode.FILTERED_OUT))
    val events = metricEvents(store) {
      val results = Await.result(store.multiPut(Seq(put(ds, "k", dayTs(System.currentTimeMillis()) + 1))), timeout)
      results shouldBe Seq(false)
    }
    op.callsOf(SegmentPut) should have size AerospikeKvStore.MaxWriteAttempts.toLong
    op.callsOf(SegmentPut).map(_.seg) shouldBe Seq(0, 0, 0, 0, 0)
    op.callsOf(ReadTail) should have size 4
    countSum(events, ds, AerospikeKvStore.SegmentRedirectsMetric) shouldBe 4
    countSum(events, ds, "write.exceptions") shouldBe 1
    countSum(events, ds, s"write.exceptions.${AerospikeKvStore.resultName(ResultCode.FILTERED_OUT)}") shouldBe 1
    countSum(events, ds, Metrics.Name.ResponseCount) shouldBe 0
  }

  it should "walk the segment chain forward on RECORD_TOO_BIG until attempts run out" in {
    val (store, op) = newStore()
    val ds = "WF_GB_STREAMING"
    op.always(SegmentPut, Fail(ResultCode.RECORD_TOO_BIG))
    val events = metricEvents(store) {
      val results = Await.result(store.multiPut(Seq(put(ds, "k", dayTs(System.currentTimeMillis()) + 1))), timeout)
      results shouldBe Seq(false)
    }
    op.callsOf(SegmentPut).map(_.seg) shouldBe Seq(0, 1, 2, 3, 4)
    op.calls.asScala.map(_.kind).toSeq shouldBe Seq(
      SegmentPut, ReadTail, TailAdvance,
      SegmentPut, ReadTail, TailAdvance,
      SegmentPut, ReadTail, TailAdvance,
      SegmentPut, ReadTail, TailAdvance,
      SegmentPut)
    countSum(events, ds, AerospikeKvStore.SegmentBackstopMetric) shouldBe 4
    countSum(events, ds, "write.exceptions") shouldBe 1
    countSum(events, ds, s"write.exceptions.${AerospikeKvStore.resultName(ResultCode.RECORD_TOO_BIG)}") shouldBe 1
  }

  it should "retry from the root when readTail itself fails after a filtered-out write" in {
    val (store, op) = newStore()
    val ds = "WF_GB_STREAMING"
    op.first(SegmentPut, 1, Fail(ResultCode.FILTERED_OUT))
    op.always(ReadTail, Fail(ResultCode.TIMEOUT))
    val results = Await.result(store.multiPut(Seq(put(ds, "k", dayTs(System.currentTimeMillis()) + 1))), timeout)
    results shouldBe Seq(true)
    op.calls.asScala.map(_.kind).toSeq shouldBe Seq(SegmentPut, ReadTail, SegmentPut)
    op.callsOf(SegmentPut).map(_.seg) shouldBe Seq(0, 0)
  }

  it should "succeed without advancing the hint when the tail advance fails non-race" in {
    val (store, op) = newStore(AerospikeConfig.SegmentMaxEntriesKey -> "1")
    val ds = "WF_GB_STREAMING"
    op.always(SegmentPut, Rec("v" -> 1L))
    op.always(TailAdvance, Fail(ResultCode.TIMEOUT))
    val ts = dayTs(System.currentTimeMillis()) + 1
    val events = metricEvents(store) {
      Await.result(store.multiPut(Seq(put(ds, "k", ts))), timeout) shouldBe Seq(true)
      Await.result(store.multiPut(Seq(put(ds, "k", ts + 1))), timeout) shouldBe Seq(true)
    }
    countSum(events, ds, AerospikeKvStore.SegmentAdvancesMetric) shouldBe 0
    countSum(events, ds, AerospikeKvStore.SegmentLostRacesMetric) shouldBe 0
    op.callsOf(SegmentPut).map(_.seg) shouldBe Seq(0, 0)
    op.callsOf(TailAdvance) should have size 2
  }

  it should "follow the winning writer when the tail advance loses the race" in {
    val (store, op) = newStore(AerospikeConfig.SegmentMaxEntriesKey -> "1")
    val ds = "WF_GB_STREAMING"
    op.always(SegmentPut, Rec("v" -> 1L))
    op.first(TailAdvance, 1, Fail(ResultCode.FILTERED_OUT))
    op.always(ReadTail, Rec("tail" -> 3L))
    op.tail = 2 // the scripted readTail returns 3, so the next real advance yields 3
    val ts = dayTs(System.currentTimeMillis()) + 1
    val events = metricEvents(store) {
      Await.result(store.multiPut(Seq(put(ds, "k", ts))), timeout) shouldBe Seq(true)
      Await.result(store.multiPut(Seq(put(ds, "k", ts + 1))), timeout) shouldBe Seq(true)
    }
    countSum(events, ds, AerospikeKvStore.SegmentLostRacesMetric) shouldBe 1
    op.callsOf(SegmentPut).map(_.seg) shouldBe Seq(0, 3)
  }

  it should "fail a stats write on marker failure without touching segments, and retry the marker next time" in {
    val (store, op) = newStore()
    val ds = ai.chronon.api.Constants.StatsBatchDataset
    val ts = dayTs(System.currentTimeMillis()) + 1
    op.first(MarkerPut, 1, Fail(ResultCode.TIMEOUT))

    Await.result(store.multiPut(Seq(put(ds, "j", ts))), timeout) shouldBe Seq(false)
    op.callsOf(SegmentPut) shouldBe empty
    store.statsMarkerWrites.get() shouldBe 1

    Await.result(store.multiPut(Seq(put(ds, "j", ts + 1))), timeout) shouldBe Seq(true)
    store.statsMarkerWrites.get() shouldBe 2
    op.callsOf(MarkerPut) should have size 2
    op.callsOf(SegmentPut) should have size 1

    Await.result(store.multiPut(Seq(put(ds, "j", ts + 2))), timeout) shouldBe Seq(true)
    store.statsMarkerWrites.get() shouldBe 2
    op.callsOf(MarkerPut) should have size 2
    op.callsOf(SegmentPut) should have size 2
  }

  it should "write the stats marker as a filtered no-op that never expires" in {
    val (store, op) = newStore()
    val ds = ai.chronon.api.Constants.StatsBatchDataset
    Await.result(store.multiPut(Seq(put(ds, "j", dayTs(System.currentTimeMillis()) + 1))), timeout) shouldBe Seq(true)
    val marker = op.callsOf(MarkerPut).head
    marker.policy.failOnFilteredOut shouldBe false
    marker.policy.filterExp should not be null
    marker.policy.expiration shouldBe -1
    op.callsOf(SegmentPut) should have size 1 // the entry write proceeded after the marker "success"
  }

  it should "cap stats days at MaxStatsSegmentsPerDay segments regardless of configured maxSegmentsPerDay" in {
    val (store, op) = newStore(
      AerospikeConfig.SegmentMaxEntriesKey -> "2",
      AerospikeConfig.MaxSegmentsPerDayKey -> "2")
    val ds = ai.chronon.api.Constants.StatsBatchDataset
    op.always(SegmentPut, Fail(ResultCode.RECORD_TOO_BIG))
    op.always(ReadTail, Rec("tail" -> 1023L))
    Await.result(store.multiPut(Seq(put(ds, "j", dayTs(System.currentTimeMillis()) + 1))), timeout) shouldBe Seq(false)
    op.callsOf(SegmentPut).map(_.seg) shouldBe Seq(0, 1023)
    op.callsOf(TailAdvance) shouldBe empty
  }

  it should "bucket a ts-less write into the current UTC day" in {
    val (store, op) = newStore()
    val ds = "WF_GB_STREAMING"
    val before = dayTs(System.currentTimeMillis())
    Await.result(store.multiPut(Seq(PutRequest(bytes("k"), bytes("v"), ds, None))), timeout) shouldBe Seq(true)
    val after = dayTs(System.currentTimeMillis())
    val writtenDay = op.callsOf(SegmentPut).head.day
    Seq(before, after) should contain(writtenDay.get)
  }

  it should "turn a synchronous throw from the operate launch into a false result" in {
    val store = new AerospikeKvStore(testConfig(), None,
      () => throw new IllegalStateException("no client"))
    store.operateExecutor = { (_, _, _) => throw new IllegalStateException("launch boom") }
    val ds = "WF_GB_STREAMING"
    Await.result(store.multiPut(Seq(put(ds, "k", dayTs(System.currentTimeMillis()) + 1))), timeout) shouldBe Seq(false)
  }
}

object AerospikeKvStoreWriteFailureSpec {

  sealed trait Kind
  case object SegmentPut extends Kind
  case object TailAdvance extends Kind
  case object ReadTail extends Kind
  case object MarkerPut extends Kind
  case object Other extends Kind

  sealed trait Outcome

  /** Succeed with a record holding the given long bins (integer bins come back
    * as `java.lang.Long`, as from the server). */
  case class Rec(bins: (String, Long)*) extends Outcome

  case class Fail(resultCode: Int, inDoubt: Boolean = false) extends Outcome

  case class Call(kind: Kind, seg: Int, day: Option[Long], key: Key, policy: WritePolicy)

  /** Scripts the store's `operate` seam per call kind. Unscripted calls fall
    * back to defaults mirroring a healthy server: SegmentPut returns map size
    * 1 in `v`, TailAdvance bumps and returns `tail`, ReadTail returns `tail`,
    * MarkerPut an empty record. */
  class ScriptedOperate extends ((Key, WritePolicy, scala.collection.Seq[Operation]) => Future[Record]) {

    val calls = new java.util.concurrent.ConcurrentLinkedQueue[Call]()

    @volatile var tail: Int = 0

    private val counts = new java.util.concurrent.ConcurrentHashMap[Kind, java.util.concurrent.atomic.AtomicInteger]()
    private var scripts = Map.empty[Kind, Int => Option[Outcome]]

    def on(kind: Kind)(f: Int => Option[Outcome]): Unit = scripts += kind -> f
    def always(kind: Kind, outcome: Outcome): Unit = on(kind)(_ => Some(outcome))
    def first(kind: Kind, n: Int, outcome: Outcome): Unit = on(kind)(i => if (i <= n) Some(outcome) else None)

    def callsOf(kind: Kind): Seq[Call] = calls.asScala.toSeq.filter(_.kind == kind)

    private def keyBytes(key: Key): Array[Byte] = key.userKey.getObject.asInstanceOf[Array[Byte]]

    private def tailDaySeg(bytes: Array[Byte]): Option[(Long, Int)] = {
      val s = new String(bytes, UTF_8)
      s.lastIndexOf('#') match {
        case -1 => None
        case i =>
          val seg = s.substring(i + 1)
          s.lastIndexOf('#', i - 1) match {
            case -1 => None
            case j =>
              try Some((s.substring(j + 1, i).toLong, seg.toInt))
              catch { case _: NumberFormatException => None }
          }
      }
    }

    private def classify(key: Key, ops: scala.collection.Seq[Operation]): Kind = {
      val bins = ops.map(o => (o.`type`, o.binName))
      bins match {
        case Seq((Operation.Type.MAP_MODIFY, _)) => SegmentPut
        case Seq((Operation.Type.MAP_MODIFY, _), (Operation.Type.WRITE, "tail")) => SegmentPut
        case Seq((Operation.Type.WRITE, "tail"), (Operation.Type.READ, "tail")) => TailAdvance
        case Seq((Operation.Type.READ, "tail")) => ReadTail
        case Seq((Operation.Type.WRITE, "first")) => MarkerPut
        case _ => Other
      }
    }

    private def default(kind: Kind): Outcome = kind match {
      case SegmentPut => Rec("v" -> 1L)
      case TailAdvance => { tail += 1; Rec("tail" -> tail.toLong) }
      case ReadTail   => Rec("tail" -> tail.toLong)
      case MarkerPut  => Rec()
      case Other      => Rec()
    }

    override def apply(key: Key, policy: WritePolicy, ops: scala.collection.Seq[Operation]): Future[Record] = {
      val kind = classify(key, ops)
      val (day, seg) = tailDaySeg(keyBytes(key)).map { case (d, s) => (Some(d), s) }.getOrElse((None, -1))
      calls.add(Call(kind, seg, day, key, policy))
      val n = counts.computeIfAbsent(kind, _ => new java.util.concurrent.atomic.AtomicInteger).incrementAndGet()
      scripts.get(kind).flatMap(_(n)).getOrElse(default(kind)) match {
        case Rec(bins @ _*) =>
          val map = new java.util.HashMap[String, Object]()
          bins.foreach { case (name, v) => map.put(name, Long.box(v)) }
          Future.successful(new Record(map, 0, 0))
        case Fail(code, inDoubt) =>
          val e = new AerospikeException(code, s"scripted ${ResultCode.getResultString(code)}")
          e.setInDoubt(inDoubt)
          Future.failed(e)
      }
    }
  }
}

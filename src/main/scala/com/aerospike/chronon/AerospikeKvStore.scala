/*
 * Copyright (C) 2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.chronon

import ai.chronon.api.{Constants => ApiConstants}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import ai.chronon.online.{KVStore, Metrics}
import com.aerospike.client.{AerospikeException, BatchRead, Key, ResultCode}
import com.aerospike.client.cdt.{MapOperation, MapOrder, MapPolicy, MapReturnType, MapWriteFlags}
import com.aerospike.client.listener.{BatchListListener, RecordListener}
import com.aerospike.client.policy.{BatchPolicy, WritePolicy}
import com.aerospike.client.{Bin, Value, Operation}
import com.aerospike.client.exp.{Exp, Expression}

import java.util.Arrays
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/** `ai.chronon.online.KVStore` implementation backed by the Aerospike async
  * Java client.
  *
  * == Record layout ==
  *
  * Every record holds a single bin `"v"`: an Aerospike map ordered by key, of
  * `tsMillis -> valueBytes`. Each map entry is one version of the value.
  *
  *  - '''Writes''' use `MapOperation.put` against a KEY_ORDERED map policy;
  *    a write at an existing timestamp overwrites it (last-write-wins), so
  *    retries are idempotent.
  *  - '''Reads''' use `MapOperation.getByKeyRange` server-side, so only
  *    versions with `ts >= afterTsMillis` cross the wire.
  *  - '''Streaming''' datasets shard one entity into one record per UTC day
  *    (see [[AerospikeDataModel]]); a `multiGet` fans out into one `BatchRead`
  *    per day in the lookback window and merges the results.
  *
  * == Failure semantics ==
  *
  * `multiPut` never fails the batch for a single bad write: per-request results
  * are booleans in request order (`false` on Aerospike errors, launch
  * exceptions, or invalid requests such as null key/value bytes or an
  * over-63-byte set name). `multiGet` returns per-request
  * `GetResponse(Failure(ex))` on errors but maps `KEY_NOT_FOUND` to an empty
  * value list rather than a failure.
  *
  * == TTL ==
  *
  * Batch records expire after `config.batchTtl`, streaming records and their
  * segments after `config.streamingTtl` — each a positive number of seconds,
  * `0` = never expire (`expiration = -1`) or `namespace` = the server's
  * `default-ttl` (`expiration = 0`) — EXCEPT metadata datasets, every
  * stats key (the timeseries is permanent) and the `group_by_serving_info`
  * row in batch sets, which are always written with
  * `expiration = -1` (never expire) so a stalled batch upload degrades to
  * "stale features" rather than "every fetch throws". The client build fails
  * fast when a TTL-bearing record class would be written while the namespace
  * runs `nsup-period = 0` without `allow-ttl-without-nsup` (the server rejects
  * such writes with code 22).
  *
  * The underlying `AerospikeClient` is shared per JVM via
  * [[AerospikeClientHolder]]; constructing additional `AerospikeKvStore`
  * instances with equivalent configs does not open new connections.
  *
  * @param config parsed and validated connection/tuning settings
  * @param initialNamespaceSettings the namespace settings resolved when this
  *                                 store's client was built (`None` when the
  *                                 info lookup failed or was never run)
  * @param clientFactory builds the shared async client on first use; the
  *                      default resolves it from [[AerospikeClientHolder]]
  */
class AerospikeKvStore private[chronon] (val config: AerospikeConfig,
                                         initialNamespaceSettings: Option[AerospikeClientHolder.NamespaceSettings],
                                         clientFactory: () => com.aerospike.client.AerospikeClient) extends KVStore {

  def this(config: AerospikeConfig) =
    this(config, AerospikeClientHolder.namespaceSettingsFor(config), () => AerospikeClientHolder.clientFor(config))

  import AerospikeDataModel._
  import AerospikeKvStore.WriteFailure

  private lazy val client = clientFactory()

  private val ValueBin: String = AerospikeKvStore.ValueBin

  /** Int bin on the root streaming record (segment 0): the authoritative index of the
    * last segment of the day, `0` while the root is the only record. */
  private val TailBin: String = AerospikeKvStore.TailBin

  /** The namespace settings resolved when this store's client was built;
    * `private[chronon]` so tests can inject them without a server. */
  private[chronon] var namespaceSettings: Option[AerospikeClientHolder.NamespaceSettings] =
    initialNamespaceSettings

  // The TTL keys are not in the client cache key, so every store re-checks its
  // own config against the client+namespace settings resolved at build.
  AerospikeClientHolder.checkNamespacePolicy(config, namespaceSettings)

  /** Long bin on the stats first-day marker record: the earliest day bucket with data. */
  private val FirstDayBin: String = AerospikeKvStore.FirstDayBin

  /** Root writes are applied only while the tail has not advanced (`tail` absent or `0`);
    * a stale writer is filtered out instead of writing into a record that has rolled over. */
  private val RootWriteFilter: Expression =
    Exp.build(Exp.or(Exp.not(Exp.binExists(TailBin)), Exp.eq(Exp.intBin(TailBin), Exp.`val`(0))))

  /** Where each (dataset, entity, day) is currently being written; a hint only, the root's `tail` is authoritative. */
  private val hints = new SegmentHintCache(AerospikeKvStore.HintCacheCapacity)

  /** KEY_ORDERED so server-side `getByKeyRange` on `afterTsMillis` is efficient
    * and results come back sorted by timestamp. */
  private val KeyOrderedPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT)

  /** Lazily-created Chronon metrics context per dataset name. */
  private val metricsByDataset = TrieMap.empty[String, Metrics.Context]

  /** Per-dataset metrics context (latency distributions, success/failure counters),
    * created on first use under the `kv_store` environment with suffix
    * `aerospike.<dataset>`. */
  def metricsContextFor(dataset: String): Metrics.Context =
    metricsByDataset.getOrElseUpdate(
      dataset,
      Metrics.Context(AerospikeKvStore.MetricsEnvironment).withSuffix("aerospike").withSuffix(dataset))

  /** Test seam: when set, every metric emission is also delivered as an
    * [[AerospikeKvStore.MetricEvent]]. */
  @volatile private[chronon] var metricsSink: Option[AerospikeKvStore.MetricEvent => Unit] = None

  private def count(ctx: Metrics.Context, name: String): Unit = {
    ctx.increment(name)
    metricsSink.foreach(_(AerospikeKvStore.MetricEvent(ctx.environment, ctx.suffix, name, count = true, 1L)))
  }

  private def dist(ctx: Metrics.Context, name: String, value: Long): Unit = {
    ctx.distribution(name, value)
    metricsSink.foreach(_(AerospikeKvStore.MetricEvent(ctx.environment, ctx.suffix, name, count = false, value)))
  }

  /** No-op: Aerospike sets are created implicitly on first write. */
  override def create(dataset: String): Unit =
    logger.debug(s"create($dataset) is a no-op for Aerospike (sets are created on write)")

  /** Asynchronously writes every request; returns per-request success flags in
    * the same order as `putRequests`.
    *
    * Each request becomes one `MapOperation.put` on bin `"v"` keyed by the
    * request timestamp (or `System.currentTimeMillis()` when absent). Requests
    * with null `keyBytes`/`valueBytes` or an invalid dataset name resolve to
    * `false` without touching the cluster.
    *
    * Metrics per dataset: `Metrics.Name.WriteLatencyMillis` (batch wall time),
    * `Metrics.Name.RequestCount` (one per request received, including rejected),
    * `Metrics.Name.ResponseCount` (successes), `AerospikeKvStore.WriteRejectedMetric`
    * (`write.rejected`: client-side rejections — no cluster call was made, so they
    * are NOT server errors), `write.exceptions` and
    * `write.exceptions.<result_name>` (server failures only, see
    * `AerospikeKvStore.resultName`). The write failure ratio is
    * `1 - response.count / request.count`; the counters above split failures by cause.
    *
    * A `false` result (for example `RECORD_TOO_BIG`) is final: Chronon's upstream
    * writers do not retry it, so the event is dropped.
    */
  override def multiPut(putRequests: scala.collection.Seq[PutRequest]): Future[scala.collection.Seq[Boolean]] = {
    val startNs = System.nanoTime()
    val rejected =
      Future.successful(Some(WriteFailure(ResultCode.PARAMETER_ERROR, inDoubt = false, clientRejected = true)))
    val results = Future.sequence(putRequests.map { req =>
      if (req.keyBytes == null || req.valueBytes == null || datasetNameError(req.dataset).isDefined) rejected
      else putOne(req)
    })
    results.map { outcomes =>
      val latencyMs = (System.nanoTime() - startNs) / 1000000
      putRequests.zip(outcomes).groupBy(_._1.dataset).foreach { case (dataset, pairs) =>
        val ctx = metricsContextFor(dataset)
        dist(ctx, Metrics.Name.WriteLatencyMillis, latencyMs)
        pairs.foreach { case (_, outcome) =>
          count(ctx, Metrics.Name.RequestCount)
          outcome match {
            case None => count(ctx, Metrics.Name.ResponseCount)
            case Some(failure) if failure.clientRejected =>
              count(ctx, AerospikeKvStore.WriteRejectedMetric)
            case Some(failure) =>
              count(ctx, "write.exceptions")
              count(ctx, s"write.exceptions.${AerospikeKvStore.resultName(failure.resultCode)}")
          }
        }
        logWriteFailures(dataset, pairs.flatMap(_._2).filterNot(_.clientRejected), pairs.size)
      }
      outcomes.map(_.isEmpty)
    }
  }

  /** One summary line per dataset per `multiPut`, so a failure storm cannot
    * flood the log; groups failures by result code with counts and `inDoubt`
    * tallies. */
  private def logWriteFailures(dataset: String, failures: scala.collection.Seq[WriteFailure], total: Int): Unit =
    if (failures.nonEmpty) {
      val summary = failures.groupBy(_.resultCode).toSeq.sortBy(-_._2.size).map { case (code, fs) =>
        s"${ResultCode.getResultString(code)} (code=$code) x${fs.size}, inDoubt=${fs.count(_.inDoubt)}"
      }.mkString("; ")
      val hint =
        if (failures.exists(_.resultCode == ResultCode.RECORD_TOO_BIG))
          s" Hint: RECORD_TOO_BIG means a record reached the namespace write-block-size/max-record-size; " +
            s"lower ${AerospikeKvStore.SegmentMaxEntriesHint} so streaming days roll into more, smaller segments. " +
            "Chronon's writers do not retry a false result, so these events are dropped."
        else ""
      logger.warn(s"multiPut to $dataset: ${failures.size}/$total writes failed: $summary$hint")
    }

  /** Issues one async write. Returns `None` on success or `Some(WriteFailure)`
    * capturing the Aerospike result code and `inDoubt` flag — both async
    * listener failures and synchronous launch exceptions are converted to
    * `Some`, so the returned future never fails. Streaming requests go through
    * the segment-aware [[putStreaming]]. */
  private def putOne(req: PutRequest): Future[Option[WriteFailure]] = {
    val ts = req.tsMillis.getOrElse(System.currentTimeMillis())
    if (getTableType(req.dataset) == StreamingTable) return putStreaming(req, ts, Chain(dayTs(ts), stats = false))
    if (isStatsTimeseries(req.dataset, req.keyBytes)) return putStats(req, ts)

    val key = new Key(config.namespace, physicalSetName(req.dataset), recordKey(req.dataset, req.keyBytes, config.keyPrefix))
    val put = MapOperation.put(KeyOrderedPolicy, ValueBin, Value.get(ts), Value.get(req.valueBytes))
    val ops: Array[Operation] = retentionFor(req.dataset, req.keyBytes, config) match {
      case KeepVersions(n) => Array(put, MapOperation.removeByIndexRange(ValueBin, -n, n, MapReturnType.INVERTED))
      case NoTrim          => Array(put)
    }
    operateAsync(key, writePolicyFor(req.dataset, req.keyBytes), ops: _*).map(_ => Option.empty[WriteFailure]).recover {
      case e: Exception =>
        if (!e.isInstanceOf[AerospikeException]) logger.warn(s"Failed to launch put for ${req.dataset}", e)
        Some(toFailure(e))
    }
  }

  /** The write call behind every `operate`; injectable so tests can script
    * per-operation outcomes without a server. Mirrors [[batchExecutor]]. */
  @volatile private[chronon] var operateExecutor: (Key, WritePolicy, scala.collection.Seq[Operation]) => Future[com.aerospike.client.Record] =
    operateLaunch

  /** The real async `client.operate`; the future fails with the
    * `AerospikeException` (listener failure) or the launch exception. */
  private def operateLaunch(key: Key, policy: WritePolicy, ops: scala.collection.Seq[Operation]): Future[com.aerospike.client.Record] = {
    val promise = Promise[com.aerospike.client.Record]()
    try {
      client.operate(
        null, // round-robin event loop
        new RecordListener {
          override def onSuccess(key: Key, record: com.aerospike.client.Record): Unit = promise.success(record)
          override def onFailure(e: AerospikeException): Unit = promise.failure(e)
        },
        policy,
        key,
        ops.toSeq: _* // varargs need immutable.Seq on 2.13; a no-op on 2.12
      )
    } catch {
      case e: Exception => promise.failure(e)
    }
    promise.future
  }

  /** Runs one async `operate` through [[operateExecutor]]; a synchronous throw
    * from the launch still resolves to a failed future, never a thrown
    * exception. */
  private def operateAsync(key: Key, policy: WritePolicy, ops: Operation*): Future[com.aerospike.client.Record] =
    try operateExecutor(key, policy, ops)
    catch { case e: Exception => Future.failed(e) }

  private def toFailure(e: Throwable): WriteFailure = e match {
    case ae: AerospikeException => WriteFailure(ae.getResultCode, ae.getInDoubt)
    case _                      => WriteFailure(ResultCode.CLIENT_ERROR, inDoubt = false)
  }

  /** A write/read day chain: streaming (`stats = false`, expires with the day)
    * or a stats timeseries day bucket (`stats = true`, permanent). */
  private[chronon] final case class Chain(day: Long, stats: Boolean) {
    def hintDay: Long = day
    def maxEntriesFor(valueBytes: Int): Int =
      if (stats) AerospikeKvStore.statsEntryLimit(config.statsSegmentTargetKiB, valueBytes)
      else config.segmentMaxEntries
    def maxSegments: Int = if (stats) AerospikeKvStore.MaxStatsSegmentsPerDay else config.maxSegmentsPerDay
    def advanceExpiration: Int =
      if (stats) -1 else AerospikeKvStore.advanceExpiration(config.streamingTtl, namespaceSettings)
    def keyBytesFor(dataset: String, keyBytes: Array[Byte], seg: Int): Array[Byte] =
      streamingRecordKeyFromDay(dataset, keyBytes, day, config.keyPrefix, seg)
  }

  private def chainKey(dataset: String, keyBytes: Array[Byte], chain: Chain, seg: Int): Key =
    new Key(config.namespace, physicalSetName(dataset), chain.keyBytesFor(dataset, keyBytes, seg))

  /** Writes one streaming entry into the day's current segment.
    *
    * The hinted segment is written with the plain atomic map put; its result is
    * the new map size, and at `segmentMaxEntries` the tail advances so later
    * writers move on (the write itself already succeeded). The root (segment 0)
    * is guarded on `tail` so a stale writer is filtered out once the tail has
    * advanced, and `RECORD_TOO_BIG` is the backstop: both cases re-resolve the
    * tail from the root and retry, within [[AerospikeKvStore.MaxWriteAttempts]].
    * The put is idempotent per `(key, ts)` within a segment, so retries are safe;
    * a duplicate across segments is removed on read. */
  private def putStreaming(req: PutRequest, ts: Long, chain: Chain): Future[Option[WriteFailure]] = {
    val ctx = metricsContextFor(req.dataset)

    def attempt(n: Int): Future[Option[WriteFailure]] = {
      val seg = hints.get(req.dataset, req.keyBytes, chain.hintDay)
      writeSegment(req, ts, chain, seg).flatMap {
        case Right(size) =>
          if (size >= chain.maxEntriesFor(req.valueBytes.length)) advanceTail(req, chain, seg).map(_ => None)
          else Future.successful(None)
        case Left(failure) if n >= AerospikeKvStore.MaxWriteAttempts => Future.successful(Some(failure))
        case Left(failure) if failure.resultCode == ResultCode.FILTERED_OUT =>
          count(ctx, AerospikeKvStore.SegmentRedirectsMetric)
          readTail(req, chain).flatMap { tail =>
            hints.advanceTo(req.dataset, req.keyBytes, chain.hintDay, tail)
            attempt(n + 1)
          }
        case Left(failure) if failure.resultCode == ResultCode.RECORD_TOO_BIG =>
          count(ctx, AerospikeKvStore.SegmentBackstopMetric)
          readTail(req, chain).flatMap { tail =>
            if (tail > seg) {
              hints.advanceTo(req.dataset, req.keyBytes, chain.hintDay, tail)
              attempt(n + 1)
            } else if (seg + 1 >= chain.maxSegments) Future.successful(Some(failure))
            else advanceTail(req, chain, seg).flatMap(_ => attempt(n + 1))
          }
        case Left(failure) => Future.successful(Some(failure))
      }
    }
    attempt(1)
  }

  /** Stats timeseries write: ensure the first-day marker for the entry's day,
    * then write through the day chain. The marker write is skipped once the
    * day is ensured locally; a marker failure fails the request WITHOUT
    * attempting the entry write, so a day can never exist that a full-history
    * read cannot find. */
  private def putStats(req: PutRequest, ts: Long): Future[Option[WriteFailure]] = {
    val day = dayTs(ts)
    firstDays.get(req.dataset, req.keyBytes) match {
      case Some(ensured) if day >= ensured => putStreaming(req, ts, Chain(day, stats = true))
      case _ =>
        statsMarkerWrites.incrementAndGet()
        val key = new Key(config.namespace, physicalSetName(req.dataset), statsFirstDayKey(req.dataset, req.keyBytes, config.keyPrefix))
        val policy = writePolicyFor(req.dataset, req.keyBytes)
        policy.filterExp = Exp.build(Exp.or(
          Exp.not(Exp.binExists(FirstDayBin)), Exp.gt(Exp.intBin(FirstDayBin), Exp.`val`(day))))
        policy.failOnFilteredOut = false // a filtered write is a no-op: the marker already holds a lower day
        operateAsync(key, policy, Operation.put(new Bin(FirstDayBin, day))).flatMap { _ =>
          firstDays.ensure(req.dataset, req.keyBytes, day)
          putStreaming(req, ts, Chain(day, stats = true))
        }.recover { case e: Exception => Some(toFailure(e)) }
    }
  }

  /** Lowest stats day this store has already ensured in the marker record. */
  private val firstDays = new FirstDayCache(AerospikeKvStore.HintCacheCapacity)

  /** Marker write attempts issued by this store (tests assert the cache skips). */
  private[chronon] val statsMarkerWrites = new java.util.concurrent.atomic.AtomicLong(0)

  /** Stats day-root `BatchRead`s issued by this store (tests assert skipped days are not read). */
  private[chronon] val statsRootReads = new java.util.concurrent.atomic.AtomicLong(0)

  /** One write to `seg`: `Right(new map size)` or `Left(failure)`. Segment 0 also
    * creates `tail` = 0 and is guarded on it (`failOnFilteredOut` so a filtered-out
    * write is an error, not a silent no-op); later segments carry no filter. */
  private def writeSegment(req: PutRequest, ts: Long, chain: Chain, seg: Int): Future[Either[WriteFailure, Long]] = {
    val policy = writePolicyFor(req.dataset, req.keyBytes)
    val put = MapOperation.put(KeyOrderedPolicy, ValueBin, Value.get(ts), Value.get(req.valueBytes))
    val ops: Array[Operation] =
      if (seg == 0) {
        policy.filterExp = RootWriteFilter
        policy.failOnFilteredOut = true
        Array(put, Operation.put(new Bin(TailBin, 0L)))
      } else Array(put)
    operateAsync(chainKey(req.dataset, req.keyBytes, chain, seg), policy, ops: _*)
      .map(record => Right(record.getLong(ValueBin)): Either[WriteFailure, Long])
      .recover { case e: Exception => Left(toFailure(e)) }
  }

  /** The chain's authoritative tail from the root record; `0` when the root
    * does not exist — and, with a WARN, when the read fails, so the writer
    * retries from segment 0 instead of failing the request. The operate runs
    * under the write template (operate needs a `WritePolicy`). */
  private def readTail(req: PutRequest, chain: Chain): Future[Int] =
    operateAsync(chainKey(req.dataset, req.keyBytes, chain, 0), new WritePolicy(AerospikeClientHolder.policyTemplates(config).write), Operation.get(TailBin))
      .map(record =>
        if (record == null || record.bins == null || !record.bins.containsKey(TailBin)) 0
        else record.getInt(TailBin))
      .recover { case e: Exception =>
        logger.warn(s"readTail for dataset ${req.dataset} failed, assuming tail 0: ${e.getMessage}")
        0
      }

  /** Compare-and-set advance of the root's `tail` from `expected` to `expected + 1`.
    * The guarded `operate` also refreshes the root's expiration (streaming: to
    * `ttl + 1 day` so the root outlives its day bucket's read window; stats:
    * never-expire). Losing the race (`FILTERED_OUT`) follows the winner.
    * Returns the tail now in effect and moves the hint to it; any other failure
    * leaves the hint where it is (the next write retries the advance). No
    * advance happens past the chain's segment cap. */
  private[chronon] def advanceTail(req: PutRequest, chain: Chain, expected: Int): Future[Int] = {
    val ctx = metricsContextFor(req.dataset)
    if (expected + 1 >= chain.maxSegments) return Future.successful(expected)
    operateAsync(chainKey(req.dataset, req.keyBytes, chain, 0), tailAdvancePolicy(expected, chain.advanceExpiration), Operation.put(new Bin(TailBin, (expected + 1).toLong)), Operation.get(TailBin))
      .flatMap { record =>
        count(ctx, AerospikeKvStore.SegmentAdvancesMetric)
        Future.successful(record.getInt(TailBin))
      }
      .recoverWith {
        case e: AerospikeException if e.getResultCode == ResultCode.FILTERED_OUT =>
          count(ctx, AerospikeKvStore.SegmentLostRacesMetric)
          readTail(req, chain)
        case e: Exception =>
          logger.debug(s"Tail advance for ${req.dataset} chain ${chain.day} from $expected failed: ${e.getMessage}")
          Future.successful(expected)
      }
      .map(tail => hints.advanceTo(req.dataset, req.keyBytes, chain.hintDay, tail))
  }

  /** The compare-and-set policy for the guarded tail advance. `maxRetries = 0`
    * is required: a retry after a successful-but-unacknowledged advance would
    * re-issue the stale `expected` filter, and any future non-idempotent
    * operation must do the same. */
  private[chronon] def tailAdvancePolicy(expected: Int, expiration: Int): WritePolicy = {
    val policy = new WritePolicy(AerospikeClientHolder.policyTemplates(config).write)
    policy.filterExp = Exp.build(Exp.eq(Exp.intBin(TailBin), Exp.`val`(expected)))
    policy.failOnFilteredOut = true
    policy.maxRetries = 0
    policy.recordExistsAction = com.aerospike.client.policy.RecordExistsAction.UPDATE_ONLY
    policy.expiration = expiration
    policy
  }

  /** Validates the physical set name fits Aerospike's 63-byte set-name limit;
    * returns the error message when it doesn't. */
  private def datasetNameError(dataset: String): Option[String] = {
    val setNameBytes = physicalSetName(dataset).getBytes("UTF-8")
    if (setNameBytes.length > 63) {
      Some(s"Aerospike set name '${physicalSetName(dataset)}' exceeds 63 bytes (${setNameBytes.length})")
    } else None
  }

  /** Pre-flight validation for a `GetRequest`: null `keyBytes` or an invalid
    * dataset name produce a descriptive error used to build a `Failure`
    * response without hitting the cluster. */
  private def requestError(req: GetRequest): Option[String] = {
    if (req.keyBytes == null) Some("GetRequest.keyBytes is null")
    else datasetNameError(req.dataset)
  }

  /** Batch-reads all requests in chunks of at most `config.readBatchChunkSize`,
    * returning a `GetResponse` per request in the original order.
    *
    * Each valid request expands into zero or more `BatchRead`s (one per day
    * bucket for streaming datasets; exactly one otherwise; none for a
    * streaming request whose day range is empty), each carrying a
    * server-side map key-range filter honouring `afterTsMillis`. Invalid
    * requests get an immediate `IllegalArgumentException` failure response
    * without a cluster call. Per-record `KEY_NOT_FOUND` results become empty
    * `TimedValue` lists; a failed chunk or a per-record error becomes a
    * `Failure` only on the requests that have a read in it.
    *
    * Metrics per dataset: `Metrics.Name.LatencyMillis` (one distribution sample
    * per dataset present in the call, call wall time to assembly),
    * `Metrics.Name.RequestCount` (one per request received, including invalid),
    * `Metrics.Name.ResponseCount` (one per `Success`), `Metrics.Name.FetchExceptions`
    * (one per `Failure`, including invalid requests), `chunk.latency.millis` (one
    * sample per chunk per dataset it contains), `read.chunks` and
    * `read.chunk.failures`.
    */
  /** Test seam: the clock for day-window computations; one read per `multiGet`
    * so root chains and their segment reads always agree on "today". */
  private[chronon] var clock: () => Long = () => System.currentTimeMillis()

  override def multiGet(requests: scala.collection.Seq[GetRequest]): Future[scala.collection.Seq[GetResponse]] = {
    val startNs = System.nanoTime()
    if (requests.isEmpty) return Future.successful(Nil)
    val now = clock()

    val indexed = requests.zipWithIndex
    // per-request outcome, filled for every input index by construction
    val outcomes = scala.collection.mutable.Map.empty[Int, GetResponse]
    indexed.foreach { case (req, idx) =>
      requestError(req).foreach { msg =>
        val ctx = metricsContextFor(req.dataset)
        count(ctx, Metrics.Name.RequestCount)
        count(ctx, Metrics.Name.FetchExceptions)
        outcomes(idx) = GetResponse(req, Failure(new IllegalArgumentException(msg)))
      }
    }
    val goodIndexed = indexed.filterNot { case (_, idx) => outcomes.contains(idx) }

    val (statsIndexed, otherIndexed) = goodIndexed.partition { case (req, _) =>
      isStatsTimeseries(req.dataset, req.keyBytes)
    }

    statsRound0(statsIndexed).flatMap { plans =>
      plans.collect { case (idx, Left(e)) =>
        val ctx = metricsContextFor(requests(idx).dataset)
        count(ctx, Metrics.Name.RequestCount)
        count(ctx, Metrics.Name.FetchExceptions)
        outcomes(idx) = GetResponse(requests(idx), Failure(e))
      }
      val planByIndex = plans.collect { case (idx, Right(plan)) => idx -> plan }

      val planned: scala.collection.Seq[(Int, Seq[Chain], (BatchRead, GetRequest))] =
        otherIndexed.flatMap { case (req, idx) =>
          val (chains, reads) = plannedReads(req, None, now)
          reads.map { case (br, _) => (idx, chains, (br, req)) }
        } ++
        statsIndexed.flatMap { case (req, idx) =>
          planByIndex.get(idx).toSeq.flatMap { plan =>
            val (chains, reads) = plannedReads(req, plan.first, now)
            reads.map { case (br, _) => (idx, chains, (br, req)) }
          }
        }
      val chainsByIndex = planned.map { case (idx, chains, _) => idx -> chains }.toMap
      val taggedReads = planned.map { case (idx, _, brReq) => (brReq._1, brReq._2, idx) }

      batchGetChunked(taggedReads.map { case (br, req, _) => (br, req.dataset) }).flatMap { rootsResult =>
        val rootsByIndex: Map[Int, Seq[Try[BatchRead]]] =
          taggedReads.zip(rootsResult.results).groupBy(_._1._3).map { case (idx, tagged) =>
            idx -> tagged.map(_._2).toSeq
          }

        val segmentReads = goodIndexed.flatMap { case (req, idx) =>
          if (outcomes.contains(idx)) Nil
          else {
            val roots = rootsByIndex.getOrElse(idx, Nil)
            if (roots.exists(readFailed)) Nil
            else segmentReadsFor(req, roots.collect { case Success(br) => br }, chainsByIndex.getOrElse(idx, Nil)).map(idx -> _)
          }
        }

        val secondRoundStartNs = System.nanoTime()
        val reqByIndex = goodIndexed.map { case (req, idx) => idx -> req }.toMap
        val segmentResults: Future[Map[Int, Seq[Try[BatchRead]]]] =
          if (segmentReads.isEmpty) Future.successful(Map.empty)
          else {
            secondRoundCalls.incrementAndGet()
            batchGetChunked(segmentReads.map { case (idx, br) => (br, reqByIndex(idx).dataset) }).map { res =>
              segmentReads.map(_._1).zip(res.results).groupBy(_._1).map { case (idx, pairs) => idx -> pairs.map(_._2).toSeq }
            }
          }

        segmentResults.map { segmentsByIndex =>
          val latencyMs = (System.nanoTime() - startNs) / 1000000
          goodIndexed.foreach { case (request, idx) =>
            if (!outcomes.contains(idx)) {
              val ctx = metricsContextFor(request.dataset)
              count(ctx, Metrics.Name.RequestCount)
              outcomes(idx) = Try {
                // stats: the legacy plain-key record read in round 0 is part of the history
                val roots = planByIndex.get(idx).map(plan => Success(plan.legacy)).toSeq ++
                  rootsByIndex.getOrElse(idx, Nil)
                val segments = segmentsByIndex.getOrElse(idx, Nil)
                if (segments.nonEmpty) {
                  dist(ctx, AerospikeKvStore.SegmentsReadMetric, segments.size.toLong)
                  dist(ctx, AerospikeKvStore.SecondRoundLatencyMetric, (System.nanoTime() - secondRoundStartNs) / 1000000)
                }

                val failure: Option[Throwable] =
                  roots.collectFirst { case Failure(e) => e }
                    .orElse(roots.collect { case Success(br) if isReadError(br) => aerospikeException(br.resultCode, br.inDoubt) }.headOption)
                    .orElse(segments.collectFirst { case Failure(e) => e })
                    .orElse(segments.collect { case Success(br) if isReadError(br) => aerospikeException(br.resultCode, br.inDoubt) }.headOption)
                failure match {
                  case Some(e) =>
                    count(ctx, Metrics.Name.FetchExceptions)
                    GetResponse(request, Failure(e))
                  case None =>
                    val rootBrs = roots.collect { case Success(br) => br }
                    val segmentBrs = segments.collect { case Success(br) => br }
                    val values =
                      if (segmentBrs.isEmpty) rootBrs.flatMap(br => AerospikeKvStore.extractTimedValues(br.record)).sortBy(_.millis)
                      else AerospikeKvStore.mergeByTs((rootBrs ++ segmentBrs).map(br => AerospikeKvStore.extractTimedValues(br.record)))
                    count(ctx, Metrics.Name.ResponseCount)
                    GetResponse(request, Success(values))
                }
              }.recover { case e =>
                count(ctx, Metrics.Name.FetchExceptions)
                GetResponse(request, Failure(e))
              }.get
            }
          }

          // one latency sample per dataset present in the call
          requests.map(_.dataset).distinct.foreach { ds =>
            dist(metricsContextFor(ds), Metrics.Name.LatencyMillis, latencyMs)
          }

          requests.zipWithIndex.map { case (req, i) =>
            outcomes.getOrElse(i, {
              logger.error(s"multiGet produced no outcome for request index $i (dataset ${req.dataset}); this is a bug")
              val ctx = metricsContextFor(req.dataset)
              count(ctx, Metrics.Name.RequestCount)
              count(ctx, Metrics.Name.FetchExceptions)
              GetResponse(req, Failure(new IllegalStateException(s"multiGet produced no outcome for request index $i")))
            })
          }
        }
      }
    }
    .recover { case ex =>
        // last resort: applies only to requests with no outcome yet
        requests.zipWithIndex.map { case (req, i) =>
          outcomes.getOrElse(i, {
            val ctx = metricsContextFor(req.dataset)
            count(ctx, Metrics.Name.RequestCount)
            count(ctx, Metrics.Name.FetchExceptions)
            GetResponse(req, Failure(ex))
          })
        }
      }
  }

  /** Number of `multiGet` calls that needed a second round for chained segments. */
  private[chronon] val secondRoundCalls = new java.util.concurrent.atomic.AtomicLong(0)

  private def isReadError(br: BatchRead): Boolean =
    br.resultCode != 0 && br.resultCode != ResultCode.KEY_NOT_FOUND_ERROR

  /** True when a chunked read result is a chunk failure or carries a record-level error. */
  private def readFailed(result: Try[BatchRead]): Boolean =
    result match {
      case Failure(_)  => true
      case Success(br) => isReadError(br)
    }

  /** Number of executor invocations (chunks) this store has issued (tests assert batch counts). */
  private[chronon] val batchCalls = new java.util.concurrent.atomic.AtomicLong(0)

  /** The batch call behind every chunked read; injectable so tests can fail or
    * count individual chunks. */
  @volatile private[chronon] var batchExecutor: (scala.collection.Seq[BatchRead], BatchPolicy) => Future[List[BatchRead]] =
    batchGetAsync

  /** One async batch `client.get` of `reads`; the future fails on a listener failure or launch exception. */
  private def batchGetAsync(reads: scala.collection.Seq[BatchRead], policy: BatchPolicy): Future[List[BatchRead]] = {
    val promise = Promise[List[BatchRead]]()
    try {
      client.get(
        null,
        new BatchListListener {
          override def onSuccess(records: java.util.List[BatchRead]): Unit =
            promise.success(records.asScala.toList)
          override def onFailure(e: AerospikeException): Unit =
            promise.failure(e)
        },
        policy,
        reads.asJava
      )
    } catch {
      case e: Exception => promise.failure(e)
    }
    promise.future
  }

  /** One chunked round of batch reads: the per-read results in input order plus
    * the chunk statistics the caller needs for metrics. */
  private[chronon] case class ChunkedResult(results: Seq[Try[BatchRead]],
                                            chunks: Int,
                                            failedChunksByDataset: Map[String, Int])

  /** Splits `taggedReads` into chunks of at most `config.readBatchChunkSize`,
    * issues them concurrently through [[batchExecutor]] and returns one result
    * per read in the original order. A failed chunk yields `Failure` for the
    * reads in that chunk only. Emits `chunk.latency.millis` once per chunk per
    * dataset it contains and, per dataset present in the round,
    * `read.chunks` (the round's chunk count) and `read.chunk.failures` (one per
    * failed chunk containing a read of that dataset). */
  private[chronon] def batchGetChunked(taggedReads: scala.collection.Seq[(BatchRead, String)]): Future[ChunkedResult] = {
    val templates = AerospikeClientHolder.policyTemplates(config)
    val (statsPart, otherPart) = taggedReads.toSeq.zipWithIndex.partition {
      case ((_, ds), _) => AerospikeDataModel.isStatsDataset(ds)
    }
    val plan = AerospikeKvStore.chunkReads(otherPart, config.readBatchChunkSize).map(_ -> templates.batch) ++
      AerospikeKvStore.chunkReads(statsPart, config.readBatchChunkSize).map(_ -> templates.statsBatch)
    Future
      .sequence(plan.map { case (chunk, policy) =>
        val chunkStartNs = System.nanoTime()
        val chunkF =
          try {
            batchCalls.incrementAndGet()
            batchExecutor(chunk.map(_._1._1), policy)
          } catch {
            case e: Exception => Future.failed(e)
          }
        chunkF
          .map(brs => chunk.zip(brs).map { case (((_, ds), idx), br) => (ds, (Success(br): Try[BatchRead]), idx) })
          .recover { case e => chunk.map { case ((_, ds), idx) => (ds, (Failure(e): Try[BatchRead]), idx) } }
          .map { tagged =>
            val chunkLatencyMs = (System.nanoTime() - chunkStartNs) / 1000000
            tagged.map(_._1).distinct.foreach { ds =>
              dist(metricsContextFor(ds), AerospikeKvStore.ChunkLatencyMetric, chunkLatencyMs)
            }
            tagged
          }
      })
      .map { chunksResults =>
        taggedReads.map(_._2).distinct.foreach { ds =>
          dist(metricsContextFor(ds), AerospikeKvStore.ReadChunksMetric, chunksResults.size.toLong)
        }
        val failedByDataset = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
        chunksResults.foreach { chunkResult =>
          chunkResult.collect { case (ds, Failure(_), _) => ds }.toSet.foreach { ds: String =>
            failedByDataset(ds) = failedByDataset(ds) + 1
            count(metricsContextFor(ds), AerospikeKvStore.ReadChunkFailuresMetric)
          }
        }
        ChunkedResult(chunksResults.flatten.sortBy(_._3).map(_._2), chunksResults.size, failedByDataset.toMap)
      }
  }

  /** Result of the stats round-0 read: `first` is the first-day marker (None
    * when the marker is absent) and `legacy` is the plain-key `BatchRead`
    * (the earlier single-record layout), kept so errors and entries merge
    * with the day buckets downstream. */
  private case class StatsPlan(first: Option[Long], legacy: BatchRead)

  /** Stats timeseries round 0: one chunked batch of, per request, the marker
    * (`first` bin) and the legacy plain key (the request's `readOp`). Per
    * request yields `Right(StatsPlan)` or `Left(error)` — a failed chunk or a
    * marker read error fails that request only. */
  private def statsRound0(statsIndexed: scala.collection.Seq[(GetRequest, Int)]): Future[Map[Int, Either[Throwable, StatsPlan]]] = {
    if (statsIndexed.isEmpty) return Future.successful(Map.empty)
    val reads = statsIndexed.flatMap { case (req, _) =>
      Seq(
        (new BatchRead(
          new Key(config.namespace, physicalSetName(req.dataset), statsFirstDayKey(req.dataset, req.keyBytes, config.keyPrefix)),
          Array(Operation.get(FirstDayBin))), req.dataset),
        (new BatchRead(
          new Key(config.namespace, physicalSetName(req.dataset), recordKey(req.dataset, req.keyBytes, config.keyPrefix)),
          Array(readOp(req))), req.dataset))
    }
    batchGetChunked(reads).map { res =>
      statsIndexed.map(_._2).zip(res.results.grouped(2).toSeq).map { case (idx, pair) =>
        val plan = (pair(0), pair(1)) match {
          case (Failure(e), _) => Left(e)
          case (_, Failure(e)) => Left(e)
          case (Success(m), _) if isReadError(m) => Left(aerospikeException(m.resultCode, m.inDoubt))
          case (Success(m), Success(l)) =>
            val first =
              if (m.record == null || m.record.bins == null || !m.record.bins.containsKey(FirstDayBin)) None
              else Some(m.record.getLong(FirstDayBin).longValue())
            Right(StatsPlan(first, l))
        }
        idx -> plan
      }.toMap
    }
  }

  /** The chains a request reads, in the same order as the root `BatchRead`s
    * produced by [[buildBatchReads]]: one per day bucket for streaming, one
    * per day in `[max(dayTs(afterTs), firstDay) .. today]` for a stats
    * timeseries (none without a marker), none otherwise. */
  private def chainsFor(req: GetRequest, firstDay: Option[Long], now: Long): Seq[Chain] =
    getTableType(req.dataset) match {
      case StreamingTable => streamingDays(req, now).map(Chain(_, stats = false))
      case _ if isStatsTimeseries(req.dataset, req.keyBytes) =>
        firstDay.toSeq.flatMap { first =>
          statsDays(req.afterTsMillis, first, dayTs(now)).map(Chain(_, stats = true))
        }
      case _ => Nil
    }

  /** Segment `BatchRead`s (segments `1..tail`) for each root whose `tail` bin
    * is positive; `roots` are the request's root results and `chains` the same
    * chains those roots were read for, in order. A missing or expired segment
    * (`KEY_NOT_FOUND`) is skipped downstream. */
  private def segmentReadsFor(req: GetRequest, roots: scala.collection.Seq[BatchRead], chains: Seq[Chain]): scala.collection.Seq[BatchRead] = {
    if (chains.isEmpty) Nil
    else {
      val op = readOp(req)
      chains.zip(roots).flatMap { case (chain, root) =>
        val tail =
          if (root.record == null || root.record.bins == null || !root.record.bins.containsKey(TailBin)) 0
          else root.record.getInt(TailBin)
        (1 to tail).map { seg =>
          new BatchRead(chainKey(req.dataset, req.keyBytes, chain, seg), Array(op))
        }
      }
    }
  }

  private def readOp(req: GetRequest): Operation = req.afterTsMillis match {
    case Some(ts) => MapOperation.getByKeyRange(ValueBin, Value.get(ts), null, MapReturnType.KEY_VALUE)
    case None if isLatestOnly(req.dataset, req.keyBytes) => MapOperation.getByIndex(ValueBin, -1, MapReturnType.KEY_VALUE)
    case None => MapOperation.getByKeyRange(ValueBin, null, null, MapReturnType.KEY_VALUE)
  }

  /** Day buckets a streaming request reads, from `clampedStartDay` through
    * `now`'s day (`now` is read once per `multiGet` by the caller). */
  private def streamingDays(req: GetRequest, now: Long): Seq[Long] = {
    val endDay = dayTs(now)
    val effectiveLookbackMillis = math.min(
      AerospikeKvStore.resolvedTtlSeconds(config.streamingTtl, namespaceSettings)
        .map(_ * 1000L).getOrElse(Long.MaxValue),
      config.streamingMaxLookbackDays.toLong * MillisPerDay
    )
    val lookbackStartDay = dayTs(now - effectiveLookbackMillis)
    val clampedStartDay = req.afterTsMillis.map(ts => math.max(dayTs(ts), lookbackStartDay)).getOrElse(lookbackStartDay)
    clampedStartDay to endDay by MillisPerDay
  }

  /** Expands one `GetRequest` into its root `BatchRead`s, each tagged back to
    * the request. Batch/metadata datasets and stats schema keys yield a single
    * read; streaming datasets yield one root read per day bucket from
    * `clampedStartDay` through today, where the start is `afterTsMillis`'s day
    * (the window start when absent) clamped to `min(resolved streaming TTL,
    * streamingMaxLookbackDays)` of lookback — expired data is never read; a
    * stats timeseries yields one day root per day in
    * `[max(dayTs(afterTsMillis), firstDay) .. today]`. Chained roots also read `tail`, so
    * [[segmentReadsFor]] can fan out to segments `1..tail`. Each read carries a
    * map key-range op filtering entries with `ts >= afterTsMillis` (all
    * entries when absent, or just the newest for latest-only keys). */
  private[chronon] def buildBatchReads(req: GetRequest): Seq[(BatchRead, GetRequest)] =
    buildBatchReads(req, firstDay = None)

  /** `firstDay` is the stats marker read in round 0; without it a stats
    * timeseries yields no day roots (only the legacy plain key, already read). */
  private[chronon] def buildBatchReads(req: GetRequest, firstDay: Option[Long]): Seq[(BatchRead, GetRequest)] =
    plannedReads(req, firstDay, clock())._2

  /** The request's chains and its root `BatchRead`s, computed once so the
    * segment fan-out can reuse the identical chains ([[multiGet]] reads the
    * clock once per call). */
  private def plannedReads(req: GetRequest,
                           firstDay: Option[Long],
                           now: Long): (Seq[Chain], Seq[(BatchRead, GetRequest)]) = {
    val op = readOp(req)

    chainsFor(req, firstDay, now) match {
      case chains if chains.nonEmpty =>
        if (isStatsTimeseries(req.dataset, req.keyBytes)) statsRootReads.addAndGet(chains.size.toLong)
        chains -> chains.map { chain =>
          (new BatchRead(chainKey(req.dataset, req.keyBytes, chain, 0), Array(op, Operation.get(TailBin))), req)
        }
      // no chains: a stats timeseries without a marker, or a streaming request
      // whose day range is empty (afterTsMillis later than now) — zero reads
      case _ if isStatsTimeseries(req.dataset, req.keyBytes) || getTableType(req.dataset) == StreamingTable =>
        Nil -> Nil
      case _ =>
        val key = new Key(config.namespace, physicalSetName(req.dataset), recordKey(req.dataset, req.keyBytes, config.keyPrefix))
        Nil -> Seq((new BatchRead(key, Array(op)), req))
    }
  }

  /** Clones the cached write template and sets per-record expiration via
    * [[AerospikeKvStore.expirationFor]]. */
  private def writePolicyFor(dataset: String, keyBytes: Array[Byte]): WritePolicy = {
    val base = new WritePolicy(AerospikeClientHolder.policyTemplates(config).write)
    base.expiration = AerospikeKvStore.expirationFor(dataset, keyBytes, config)
    base
  }

  /** True when `keyBytes` is Chronon's `group_by_serving_info` key (never
    * expire; see [[AerospikeKvStore.isServingInfoKeyBytes]]). */
  private[chronon] def isServingInfoKey(keyBytes: Array[Byte]): Boolean =
    AerospikeKvStore.isServingInfoKeyBytes(keyBytes)

  /** Rebuilds an `AerospikeException` from a batch-read result code so
    * per-record failures surface with the same type callers see from the
    * client. */
  private def aerospikeException(resultCode: Int, inDoubt: Boolean): AerospikeException = {
    val msg = s"Aerospike error: ${ResultCode.getResultString(resultCode)} (code=$resultCode, inDoubt=$inDoubt)"
    new AerospikeException(resultCode, msg)
  }

  /** Loads one partition of `sourceOfflineTable` into `destinationOnlineDataSet`
    * via [[Spark2AerospikeLoader.upload]] on the ambient `SparkSession`.
    * Blocks until the load finishes; throws if any `multiPut` chunk fails. */
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().getOrCreate()
    val loaderConfig = AerospikeConfig.forLoader(config)
    bulkLoadConfigSink(loaderConfig)
    Spark2AerospikeLoader.upload(spark, sourceOfflineTable, Some(destinationOnlineDataSet), partition, loaderConfig)
  }

  /** Test seam: invoked with the [[AerospikeConfig.forLoader]]-derived config
    * before each `bulkPut` upload. */
  private[chronon] var bulkLoadConfigSink: AerospikeConfig => Unit = { _ => () }
}

object AerospikeKvStore {

  private val TailBin: String = "tail"

  private val FirstDayBin: String = "first"

  /** Bound on remembered (dataset, entity, day) segment hints per store. */
  private val HintCacheCapacity = 100000

  /** Redirect/advance/retry rounds a single streaming write may take before it reports failure. */
  private[chronon] val MaxWriteAttempts = 5

  private[chronon] val SegmentAdvancesMetric  = "streaming.segment.advances"
  private[chronon] val SegmentLostRacesMetric = "streaming.segment.lost_races"
  private[chronon] val SegmentRedirectsMetric = "streaming.segment.redirects"
  private[chronon] val SegmentBackstopMetric  = "streaming.segment.backstop"
  private[chronon] val SegmentsReadMetric       = "streaming.segments_read"
  private[chronon] val SecondRoundLatencyMetric = "streaming.second_round.latency.millis"
  private[chronon] val ReadChunksMetric         = "read.chunks"
  private[chronon] val ReadChunkFailuresMetric  = "read.chunk.failures"
  private[chronon] val ChunkLatencyMetric       = "chunk.latency.millis"
  private[chronon] val WriteRejectedMetric      = "write.rejected"

  // Chronon 0.0.115 has no Metrics.Environment.KVStore; Environment is a String
  // alias so the store defines the same string.
  private[chronon] val MetricsEnvironment = "kv_store"

  /** The write-policy expiration for one record: metadata tables, every stats
    * key (timeseries and schema) and the batch-set `group_by_serving_info` key
    * never expire (`-1`); batch records use `config.batchTtl`, streaming
    * records `config.streamingTtl` (seconds / `-1` / `0` = namespace default). */
  private[chronon] def expirationFor(dataset: String, keyBytes: Array[Byte], config: AerospikeConfig): Int = {
    import AerospikeDataModel._
    val neverExpire = getTableType(dataset) match {
      case MetadataTable(_) => true
      case BatchTable =>
        isServingInfoKeyBytes(keyBytes) || isStatsDataset(dataset)
      case StreamingTable => false
    }
    if (neverExpire) -1
    else (if (getTableType(dataset) == StreamingTable) config.streamingTtl else config.batchTtl).expiration
  }

  private def isServingInfoKeyBytes(keyBytes: Array[Byte]): Boolean =
    java.util.Arrays.equals(keyBytes, ApiConstants.GroupByServingInfoKey.getBytes(ApiConstants.UTF8))

  /** The TTL of a class resolved to seconds, or `None` for never-expire
    * (`NeverExpire`, `NamespaceDefault` on a namespace with `default-ttl` 0,
    * or an unresolvable namespace). */
  private[chronon] def resolvedTtlSeconds(
      ttl: TtlSetting,
      ns: Option[AerospikeClientHolder.NamespaceSettings]): Option[Long] = ttl match {
    case TtlSetting.Seconds(s)       => Some(s.toLong)
    case TtlSetting.NeverExpire      => None
    case TtlSetting.NamespaceDefault => ns.flatMap(_.defaultTtlSeconds).filter(_ > 0)
  }

  /** Root expiration after a tail advance: the resolved streaming TTL plus one
    * day (D12), or never-expire when the streaming TTL does not resolve to
    * positive seconds. */
  private[chronon] def advanceExpiration(
      ttl: TtlSetting,
      ns: Option[AerospikeClientHolder.NamespaceSettings]): Int =
    resolvedTtlSeconds(ttl, ns).map(s => math.min(Int.MaxValue.toLong, s + 86400L).toInt).getOrElse(-1)

  /** One metric emission captured through [[AerospikeKvStore.metricsSink]] (test seam). */
  private[chronon] final case class MetricEvent(environment: String,
                                                suffix: String,
                                                name: String,
                                                count: Boolean,
                                                value: Long)

  /** Internal cap on segments per stats day bucket; a runaway day stops here
    * and writes past the cap return `false`. */
  private[chronon] val MaxStatsSegmentsPerDay = 1024

  /** Per-write entry cap for a stats chain segment, derived from the byte
    * target so a segment stays near `targetKiB` whatever the entry size;
    * 13 is the measured per-entry map overhead. A value at or above the
    * target still gets a segment of one. */
  private[chronon] def statsEntryLimit(targetKiB: Int, valueBytes: Int): Int =
    math.max(1, ((targetKiB.toLong * 1024L) / (valueBytes.toLong + 13L)).min(Int.MaxValue.toLong).toInt)

  /** Splits `reads` into consecutive chunks of at most `size`, preserving order. */
  private[chronon] def chunkReads[T](reads: scala.collection.Seq[T], size: Int): Seq[Seq[T]] =
    reads.grouped(size).toSeq.map(_.toSeq)

  /** Merges per-record entry lists in order, deduplicating by `ts` (later lists win) and sorting by `ts`. */
  private[chronon] def mergeByTs(chunks: scala.collection.Seq[scala.collection.Seq[KVStore.TimedValue]]): Seq[KVStore.TimedValue] = {
    val merged = new java.util.TreeMap[Long, KVStore.TimedValue]()
    chunks.foreach(_.foreach(v => merged.put(v.millis, v)))
    merged.values.asScala.toSeq
  }

  private[chronon] val SegmentMaxEntriesHint = "aerospike.streaming.segmentMaxEntries"

  private val log = org.slf4j.LoggerFactory.getLogger(classOf[AerospikeKvStore])

  /** Single bin holding the `tsMillis -> valueBytes` key-ordered map. */
  private val ValueBin: String = "v"

  /** Decodes bin `"v"` map entries into `TimedValue`s. Null/missing records and
    * bins yield empty; individual malformed entries are skipped with a warning
    * rather than failing the request. */
  private[chronon] def extractTimedValues(record: com.aerospike.client.Record): Seq[KVStore.TimedValue] = {
    if (record == null) return Seq.empty
    val raw = record.getList(ValueBin)
    if (raw == null) return Seq.empty
    raw.asScala.flatMap {
      case entry: java.util.Map.Entry[_, _] =>
        try {
          val ts = entry.getKey match {
            case l: java.lang.Long => l.longValue()
            case v: Value          => v.toLong
            case other             => other.toString.toLong
          }
          val bytes = entry.getValue match {
            case b: Array[Byte] => b
            case v: Value       => v.getObject.asInstanceOf[Array[Byte]]
            case other          => other.asInstanceOf[Array[Byte]]
          }
          Some(KVStore.TimedValue(bytes, ts))
        } catch {
          case e: Exception =>
            log.warn(s"Skipping malformed map entry in bin '$ValueBin': ${e.getMessage}")
            None
        }
      case other =>
        log.warn(s"Skipping unexpected entry type in bin '$ValueBin': ${other.getClass.getName}")
        None
    }.toSeq
  }

  /** Failure detail kept internally; the public `multiPut` contract stays
    * `Seq[Boolean]`. `inDoubt` is Aerospike's may-have-applied flag — the write
    * could have succeeded despite the error (e.g. timeout), which matters
    * because retries are only safe thanks to last-write-wins idempotency. */
  private[chronon] case class WriteFailure(resultCode: Int, inDoubt: Boolean, clientRejected: Boolean = false)

  /** Metric-name-safe label, e.g. -7 -> "no_more_available_connections". The
    * metrics API has no tags, so the result is encoded in the name; cardinality
    * is bounded by `ResultCode`. Unknown codes fall back to `code_<n>`. */
  private[chronon] def resultName(code: Int): String = {
    val name = ResultCode.getResultString(code).toLowerCase.replaceAll("[^a-z0-9]+", "_").stripPrefix("_").stripSuffix("_")
    if (name.nonEmpty) name else s"code_${if (code < 0) "neg_" else ""}${math.abs(code)}"
  }

  /** Builds a store from Chronon's `userConf` map; see [[AerospikeConfig]]
    * for required and optional keys. Throws `IllegalArgumentException` on
    * missing/invalid config. */
  def apply(userConf: Map[String, String]): AerospikeKvStore =
    new AerospikeKvStore(AerospikeConfig.fromUserConf(userConf))
}

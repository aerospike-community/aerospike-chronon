# AerospikeKvStore.scala

`ai.chronon.online.KVStore` implementation backed by the Aerospike async Java
client. Each record holds a bin `"v"`: a key-ordered map of
`tsMillis -> valueBytes`. Two record shapes are chains: streaming day roots
(segment 0 of `[prefix]dataset#keyBytes#dayTs#seg`) and stats timeseries roots
(segment 0 is the legacy `dataset#keyBytes`, segments at `#<n>`); both carry an
int bin `"tail"` on the root holding the last segment index, with segments
`1..tail` holding the rest of the entries.
Source: `src/main/scala/com/aerospike/chronon/AerospikeKvStore.scala`

## Metrics

All store metrics emit on environment `kv_store`
(`AerospikeKvStore.MetricsEnvironment` — Chronon 0.0.115 has no
`Environment.KVStore`, and `Environment` is a String alias) with prefix
`aerospike.<dataset>` (`withSuffix("aerospike").withSuffix(dataset)`). This is
a separate namespace from the Fetcher's `group_by.fetch.*` metrics, which
cover the same traffic from Chronon's side.

Counter rules: `request.count` increments once per request received (invalid
and client-side rejected requests included), `response.count` once per
successful request, `fetch.exception_count` once per failed `multiGet`
request; writes split failures into `write.exceptions` +
`write.exceptions.<result_name>` (server-side) versus `write.rejected`
(client-side — no cluster call, not a server error). Failure ratio per
dataset is `1 - response.count / request.count`, with the failure counters
giving the breakdown. Distributions: `latency.millis` once per dataset per
`multiGet`, `write.latency.millis` once per dataset per `multiPut`,
`chunk.latency.millis` once per chunk per dataset it contains. Store-specific
names: `write.exceptions`, `write.exceptions.<result_name>`, `write.rejected`,
`read.chunks`, `read.chunk.failures`, `chunk.latency.millis`,
`streaming.segment.advances|lost_races|redirects|backstop`,
`streaming.segments_read`, `streaming.second_round.latency.millis`.

Every emission goes through the private `count`/`dist` helpers, which also
feed the `metricsSink` test seam (`MetricEvent`s per emission) — `batchCalls`
counts executor invocations the same way the batch counters do.

## `metricsContextFor`, `metricsSink`, `count`, `dist` (lines 105–127)

### Summary
Returns the lazily-created per-dataset Chronon metrics context; `metricsSink`
is the test seam every emission is mirrored into, via `count`/`dist`.

### Walkthrough
- **Lines 105–114 — Get-or-create:** `TrieMap.getOrElseUpdate` builds a `Metrics.Context` on environment `kv_store` with suffix `aerospike.<dataset>` on first use.
- **Lines 116–127 — Seam and helpers:** `@volatile metricsSink` (an `Option[MetricEvent => Unit]`); `count`/`dist` call `ctx.increment`/`ctx.distribution` and then, when set, the sink with a `MetricEvent(environment, suffix, name, count, value)`.

### Review
No significant issues identified.

## `create` (lines 98–99)

### Summary
No-op dataset creation (Aerospike sets are created implicitly on first write); emits no metrics.

### Walkthrough
- **Lines 98–99 — Log:** Emits a debug line documenting the no-op.

### Review
No significant issues identified.

## `multiPut` (lines ~133–184)

### Summary
Asynchronously writes every `PutRequest` as one `MapOperation.put` each and returns per-request success flags in request order, never failing the batch for a single bad write.

### Walkthrough
- **Setup:** Captures start time and pre-builds the client-rejected-write outcome (`PARAMETER_ERROR`, `clientRejected = true`).
- **Fan-out:** Sequences a future per request: invalid requests (null key/value bytes or over-63-byte set name) resolve to the shared rejection without a cluster call; valid ones go to `putOne`.
- **Metrics:** On completion, groups request/outcome pairs by dataset and emits `write.latency.millis` once, `request.count` per request received (rejected included), `response.count` per success, `write.rejected` per client-side rejection (NOT a server error), and `write.exceptions` + `write.exceptions.<result_name>` per server failure; then logs a per-dataset failure summary (server failures only).
- **Result:** Maps each outcome to a boolean (`None` = success).

### Review
No significant issues identified.

## `logWriteFailures` (lines 144–150)

### Summary
Emits one summary log line per dataset per `multiPut` grouping failures by result code with counts and `inDoubt` tallies.

### Walkthrough
- **Line 145 — Guard:** Skips logging entirely when `failures` is empty.
- **Lines 146–148 — Summary:** Groups failures by result code, sorts by descending count, and formats `NAME (code=n) xN, inDoubt=k` segments.
- **Line 149 — Log:** Warns with `failed/total` and the grouped summary.

### Review
No significant issues identified.

## `putOne` (lines 178–194)

### Summary
Issues one async write, returning `None` on success or `Some(WriteFailure)` on any error, so the returned future never fails. Streaming and stats-timeseries requests go through the segment-aware `putStreaming`/`putStats` with a `Chain(day, stats)` (`stats = false` for streaming, `stats = true` for the stats timeseries).

### Walkthrough
- **Lines ~179–182 — Setup:** Resolves the write timestamp (request `tsMillis` or `System.currentTimeMillis()`); routes streaming and stats-timeseries requests to `putStreaming` with the right `Chain`.
- **Lines ~184–185 — Key and op:** Builds the day-independent `recordKey` and a `MapOperation.put` on bin `"v"` keyed by the timestamp.
- **Lines ~186–189 — Retention trim in the same operate:** `retentionFor` picks `KeepVersions` (`removeByIndexRange(-n, n, INVERTED)` — keep the newest n) or `NoTrim` (streaming, stats timeseries and other never-trimmed keys); the trim is appended to the put's `operate` and is relative to the written ts.
- **Lines ~190–194 — Async write and errors:** `operateAsync` completes with `None` on success; listener failures and launch exceptions recover to `Some(toFailure(e))`.

### Review
No significant issues identified.

## `datasetNameError` (lines 196–201)

### Summary
Validates that the dataset's physical set name fits Aerospike's 63-byte set-name limit, returning the error message when it doesn't.

### Walkthrough
- **Line 197 — Measure:** Encodes the physical set name as UTF-8.
- **Lines 198–200 — Check:** Returns `Some(error)` when the byte length exceeds 63, else `None`.

### Review
No significant issues identified.

## `requestError` (lines 206–209)

### Summary
Pre-flight validation for a `GetRequest` returning a descriptive error for null `keyBytes` or an invalid dataset name.

### Walkthrough
- **Line 207 — Null key:** Returns `Some("GetRequest.keyBytes is null")` when `keyBytes` is null.
- **Line 208 — Name check:** Otherwise delegates to `datasetNameError`.

### Review
No significant issues identified.

## `multiGet` (lines ~433–558)

### Summary
Batch-reads all `GetRequest`s in chunks of at most `config.readBatchChunkSize` through one shared helper (`batchGetChunked`). A per-request `outcomes` map is filled for every input index by construction: invalid requests get an immediate `IllegalArgumentException` failure with no cluster call; valid requests with zero reads (a streaming request whose `afterTsMillis` is later than now, a stats request with no marker and no legacy data) get `Success(Seq.empty)`. Stats timeseries requests get an extra round 0 (`statsRound0`): the first-day marker (`first` bin) and the legacy plain key, per request. Round 1 then reads the day roots over `statsDays(afterTsMillis, firstDay, today)`; round 2 the segments `1..tail` of days with `tail > 0`; everything merges by `ts` and returns a `GetResponse` per request in original order.

### Walkthrough
- **Setup and expansion:** Empty input returns `Nil`; invalid requests land an `IllegalArgumentException` `Failure` in `outcomes` up front; stats requests are partitioned out and go through `statsRound0` first — a chunked batch of two reads per request (marker + legacy `recordKey` with `readOp`). A failed chunk or a marker read error fails that request only (`Left` → an outcome `Failure`); success yields `StatsPlan(first, legacyBatchRead)`.
- **First round and segment expansion:** Non-stats requests expand via `buildBatchReads(req)`; stats requests via `buildBatchReads(req, first)` — one `BatchRead` per day in `[max(dayTs(afterTs), first) .. today]` (`statsRootReads` counts them; empty when no marker or start > today). A streaming request whose day range is empty expands to zero reads. All reads go through `batchGetChunked`; results group per request index as `Try[BatchRead]`. Requests whose roots contain no chunk failure or read error get segment `BatchRead`s from `segmentReadsFor` (only days with `tail > 0`), themselves chunked the same way.
- **Merge and assemble:** Per request, inside a `Try` (an unexpected exception becomes that request's `Failure` + `fetch.exception_count`, never another request's): `request.count` increments for every assembled request; the stats legacy `BatchRead` is prepended to the day roots, so its entries merge with the day buckets and its errors fail the request. Chunk failures, root errors or failed/errored segment results become `Failure` (`fetch.exception_count`); otherwise values decode via `extractTimedValues` and combine with `mergeByTs` (dedupe by `ts`, later list wins, sorted) and count `response.count`. Segment-bearing requests emit `streaming.segments_read` and `streaming.second_round.latency.millis`. Invalid and stats-plan-failed requests were counted (`request.count` + `fetch.exception_count`) when classified.
- **Emission and last resort:** One `latency.millis` sample per dataset present in the call; responses come from `outcomes` by index; a missing entry logs an error and yields a defensive `IllegalStateException` failure (unreachable by construction, still counted). An exception thrown before assembly becomes `Failure(ex)` only on requests with no outcome yet (counted `request.count` + `fetch.exception_count`).

### Review
No significant issues identified.

## Chained write path (`Chain`, `putStats`, `putStreaming`, `writeSegment`, `readTail`, `advanceTail`; lines ~224–360)

### Summary
`Chain(day: Long, stats: Boolean)` is one day bucket's segment chain for both shapes: streaming (`stats = false`, limits `segmentMaxEntries`/`maxSegmentsPerDay`, advance sets expiration to the resolved streaming TTL + 1 day (never-expire when unresolved)) and the stats timeseries (`stats = true`, per-write entry limit `statsEntryLimit(statsSegmentTargetKiB, valueBytes)`, cap `MaxStatsSegmentsPerDay` = 1024, advance keeps never-expire). Both use `streamingRecordKeyFromDay` for keys. `putStats` runs a stats write: ensure the first-day marker for the entry's day (skipped once the `FirstDayCache` has ensured that day or an earlier one; counted by `statsMarkerWrites`), then the day chain via `putStreaming`. The marker write is a guarded `operate` on `[prefix]dataset#keyBytes#first` with filter `first` absent or `> day` and `failOnFilteredOut = false` — an atomic min, never a read-modify-write — and a marker failure returns the failure WITHOUT attempting the entry write. Writes go to the hinted segment — a hint only, the root's `tail` bin is authoritative. When a put grows a segment to the chain's `maxEntriesFor(valueBytes)`, the writer advances `tail` via a guarded compare-and-set on the root; `FILTERED_OUT` and `RECORD_TOO_BIG` re-resolve `tail` and retry within `MaxWriteAttempts` (5), and the chain's `maxSegments` is the hard stop returning `false`.

### Walkthrough
- **`putStreaming` (~260–290):** Loops `writeSegment` on the hinted segment (`hints.get` on `chain.hintDay`); on success at size `>= chain.maxEntriesFor(req.valueBytes.length)` calls `advanceTail`. `FILTERED_OUT` increments `streaming.segment.redirects` and retries at the re-read tail; `RECORD_TOO_BIG` increments `streaming.segment.backstop`, then jumps to a later tail or advances (stop at `chain.maxSegments`).
- **`writeSegment` (~285–300):** One `operate` containing the map `put`; on segment 0 it also writes `tail = 0` and sets `RootWriteFilter` (`tail` absent or `0`) with `failOnFilteredOut = true` — required so a filtered-out write surfaces as `FILTERED_OUT` instead of silently succeeding without writing. Returns `Right(new map size)` or `Left(WriteFailure)`.
- **`readTail` (~300–310):** Reads `tail` from the day root; `0` when the root is missing or unreadable.
- **`advanceTail` (~315–345):** `operate` on the day root with filter `tail == expected`, `failOnFilteredOut = true`, `maxRetries = 0`, `UPDATE_ONLY`, and `chain.advanceExpiration` (streaming refreshes to `ttl + 1 day`; stats stays never-expire). On `FILTERED_OUT` it counts a lost race and follows the winner's `tail`; the hint cache is moved to the tail now in effect.

### Review
No significant issues identified.

## `batchGetChunked`, `statsRound0`, `chainsFor`, `segmentReadsFor`, `readOp`, `streamingDays`, `buildBatchReads` (lines ~538–740)

### Summary
Read-path helpers: `batchGetChunked` splits tagged `(BatchRead, dataset)` reads into chunks of at most `config.readBatchChunkSize` via `chunkReads`, issues them concurrently through `batchExecutor` (the injectable batch call; `batchCalls` counts invocations) and returns a `ChunkedResult` — one `Try[BatchRead]` per read in input order plus the round's chunk count and per-dataset failed-chunk counts. Metrics: `chunk.latency.millis` once per chunk per dataset it contains (measured around the executor call), `read.chunks` once per dataset per round, `read.chunk.failures` once per failed chunk containing a read of that dataset. A failed chunk fails only the requests with a read in it. `statsRound0` issues the marker + legacy reads for stats requests and maps each to `Either[Throwable, StatsPlan]`; `chainsFor(req, firstDay)` gives the chains a request reads (one per streaming day bucket, or one per day in `[max(dayTs(afterTs), firstDay) .. today]` for a stats timeseries), in the same order as `buildBatchReads`' root reads; `segmentReadsFor` emits segments `1..tail` for each chain root with a positive `tail`.

### Walkthrough
- **`readOp`:** `getByKeyRange(ts, null)` with `afterTsMillis`, `getByIndex(-1)` for latest-only keys without `afterTsMillis` (`isLatestOnly(dataset, keyBytes)` — includes stats schema keys), else unbounded range.
- **`streamingDays`:** Day buckets from `clampedStartDay` through today, floored by `min(resolved streaming TTL, maxLookbackDays)` of lookback (resolved via the namespace `default-ttl`; never-expire/unresolvable bounds to `maxLookbackDays` only).
- **`chainsFor`/`buildBatchReads`:** Streaming -> one `BatchRead` per day root (none when the day range is empty); stats timeseries -> one `BatchRead` per day root in the `statsDays` range (counted in `statsRootReads`, empty without a marker — the plain key is read in round 0); chained roots carry `[rangeOp, get(tail)]`. Stats schema keys and other non-chained keys -> one `BatchRead` on `recordKey` (latest-only op when applicable).
- **`segmentReadsFor`:** Zips `chainsFor` with the root results; `tail > 0` chains yield `1..tail` `BatchRead`s on `chainKey` (`streamingRecordKeyFromDay` keys for both shapes) with the same range op. `KEY_NOT_FOUND` segments are skipped on merge.

### Review
No significant issues identified.

## `mergeByTs` (lines ~589–593)

### Summary
Merges per-record `TimedValue` lists in order through a `TreeMap` keyed by `ts` — deduplicating by `ts` (later chunks win) and returning entries sorted by `ts`.

### Review
No significant issues identified.

## `extractTimedValues` (lines ~605–632)

### Summary
Decodes bin `"v"` map entries into `TimedValue`s, skipping malformed entries with a warning rather than failing the request.

### Walkthrough
- **Lines 335–337 — Empty guards:** Returns empty for null records or missing `"v"` bin.
- **Lines 338–356 — Entry decode:** For each `Map.Entry`, extracts the timestamp key as `Long`/`Value`/string-parsed long and the value as bytes (handling `Array[Byte]` and `Value` wrappers), yielding a `TimedValue`; per-entry exceptions are warned and skipped.
- **Lines 357–360 — Unexpected types:** Non-entry list elements are warned and skipped.

### Review
No significant issues identified.

## `writePolicyFor` (lines ~560–574)

### Summary
Clones the cached write template and sets per-record expiration: never-expire for metadata tables, `group_by_serving_info` keys and every stats key (timeseries and schema — stats are permanent), per class via `expirationFor` (`batchTtl`/`streamingTtl` expiration: seconds / `-1` / `0`).

### Walkthrough
- **Clone:** Copies the shared write template so per-record TTL doesn't mutate the cached policy.
- **Classification:** Marks the record never-expiring when the dataset is a `MetadataTable`, or a `BatchTable` row whose key is `group_by_serving_info` or inside a stats dataset (`isStatsDataset`).
- **Expiration:** Sets `expiration` from `AerospikeKvStore.expirationFor` — `-1` for metadata/serving-info/stats, else the class TTL setting (`Seconds` n, `NeverExpire` -1, `NamespaceDefault` 0).

### Review
No significant issues identified.

## `isServingInfoKey` (lines 381–382)

### Summary
Byte-wise check whether `keyBytes` equals Chronon's `group_by_serving_info` key.

### Walkthrough
- **Lines 381–382 — Compare:** `Arrays.equals` against `GroupByServingInfoKey` UTF-8 bytes — raw comparison because the key isn't valid UTF-8 in general.

### Review
No significant issues identified.

## `aerospikeException` (lines 387–390)

### Summary
Rebuilds an `AerospikeException` from a batch-read result code so per-record failures surface with the client's own exception type.

### Walkthrough
- **Lines 388–389 — Build:** Formats a message with the result name, code, and `inDoubt`, and constructs `new AerospikeException(resultCode, msg)`.

### Review
No significant issues identified.

## `bulkPut` (lines 395–398)

### Summary
Loads one partition of an offline table into the online dataset via `Spark2AerospikeLoader.upload` on the ambient `SparkSession`, blocking until done.

### Walkthrough
- **Line 396 — Session:** Obtains (or reuses) the ambient `SparkSession` via `getOrCreate`.
- **Line 397 — Load:** Delegates to `Spark2AerospikeLoader.upload`, which throws if any `multiPut` chunk fails.

### Review
No significant issues identified.

## `resultName` (lines 412–415)

### Summary
Converts an Aerospike result code into a metric-name-safe label like `no_more_available_connections`, encoding the failure in the metric name since the API has no tags.

### Walkthrough
- **Line 413 — Normalize:** Lowercases the result string and replaces non-alphanumeric runs with underscores, stripping edge underscores.
- **Line 414 — Fallback:** Returns `code_<n>` (with `neg_` for negatives) when the normalized name is empty.

### Review
No significant issues identified.

## `apply` (lines 420–421)

### Summary
Builds an `AerospikeKvStore` from Chronon's `userConf` map.

### Walkthrough
- **Lines 420–421 — Construct:** Parses `AerospikeConfig.fromUserConf` (throwing on invalid config) and instantiates the store.

### Review
No significant issues identified.

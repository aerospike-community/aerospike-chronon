# AerospikeDataModel.scala

Physical layout of Chronon online data in Aerospike — dataset-to-set mapping
and raw-byte record key construction.
Source: `src/main/scala/com/aerospike/chronon/AerospikeDataModel.scala`

## `getTableType` (lines 67–71)

### Summary
Classifies a dataset name by suffix into `BatchTable`, `StreamingTable`, or `MetadataTable`.

### Walkthrough
- **Line 68 — Batch:** Names ending in `_BATCH` map to `BatchTable`.
- **Line 69 — Streaming:** Names ending in `_STREAMING` map to `StreamingTable`.
- **Line 70 — Metadata:** Everything else becomes a `MetadataTable` carrying the dataset name.

### Review
No significant issues identified.

## `physicalSetName` (line 77)

### Summary
Returns the Aerospike set a dataset maps to — shared for batch/streaming, per-dataset for metadata.

### Walkthrough
- **Line 77 — Set lookup:** Returns `getTableType(dataset).setName`, which is `GROUPBY_BATCH`, `GROUPBY_STREAMING`, or the dataset name itself.

### Review
No significant issues identified.

## `dayTs` (line 81)

### Summary
Floors a millisecond timestamp to start-of-day epoch millis, the streaming day-bucket granularity.

### Walkthrough
- **Line 81 — Floor:** Returns `Math.floorDiv(tsMillis, MillisPerDay) * MillisPerDay`, which floors correctly even for negative (pre-epoch) timestamps.

### Review
No significant issues identified.

## `concat` (lines 86–102)

### Summary
Byte-level key assembly shared by all key builders: `[prefix]dataset#keyBytes[#suffix]` built via a single exact-size allocation and `arraycopy`, preserving arbitrary binary keys.

### Walkthrough
- **Lines 87–89 — Segment bytes:** Encodes prefix and dataset as UTF-8 and measures the optional suffix length.
- **Line 90 — Size:** Computes the exact output length including `#` separators (a separator precedes the suffix only when present).
- **Lines 91–96 — Fill:** Allocates `out` and copies prefix, dataset, a `KeySeparator` byte, and `keyBytes` verbatim, tracking `pos`.
- **Lines 97–100 — Suffix:** When a suffix is present, appends another `KeySeparator` then the suffix bytes.
- **Line 101 — Result:** Returns the assembled key array.

### Review
No significant issues identified.

## `Retention` ADT (lines ~61–66)

### Summary
Per-write retention policy: `KeepVersions(n)` or `NoTrim`. (The `Window` variant was removed when the stats timeseries became permanent.)

### Review
No significant issues identified.

## `isStatsSchemaKey`, `isStatsTimeseries` (lines ~80–108)

### Summary
Byte-wise classification of keys inside the three stats datasets: a stats key ending in `__keySchema`/`__valueSchema` is a schema key; anything else is the join timeseries.

### Walkthrough
- **`isStatsSchemaKey`:** Suffix compare against `ApiConstants.TimedKvRDDKeySchemaKey`/`TimedKvRDDValueSchemaKey` byte arrays — raw comparison like `isServingInfoKey`, never a UTF-8 round-trip.
- **`isStatsTimeseries`:** `StatsDatasets.contains(dataset) && !isStatsSchemaKey(keyBytes)` — permanent, segmented, never trimmed.
- **`isStatsDataset`:** `StatsDatasets.contains(dataset)` — used by `writePolicyFor` for never-expire.

### Review
No significant issues identified.

## `retentionFor` (lines ~115–122)

### Summary
Classifies a write into a `Retention`: stats timeseries and streaming -> `NoTrim`; everything else (group-by batch, `group_by_serving_info`, stats schema keys, metadata) -> `KeepVersions(retainVersions)`. Independent of `getTableType`, so set names and record keys are unchanged.

### Walkthrough
- **Stats:** inside a stats dataset, schema keys get `KeepVersions`; the timeseries gets `NoTrim` (permanent chain).
- **Rest:** `StreamingTable` -> `NoTrim` (bounded by TTL and the segment layout); all other tables -> `KeepVersions(retainVersions)`.

### Review
No significant issues identified.

## `isLatestOnly` (lines ~126–128)

### Summary
`isLatestOnly(dataset, keyBytes)`: true for stats schema keys and for every non-stats non-streaming dataset — so a read without `afterTsMillis` can fetch just the top entry server-side. False for the stats timeseries (full history) and streaming.

### Review
No significant issues identified.

## `recordKey` (lines 115–116)

### Summary
Builds a batch/metadata record key as raw bytes: `[keyPrefix]dataset#keyBytes`.

### Walkthrough
- **Lines 115–116 — Delegate:** Calls `concat(keyPrefix, dataset, keyBytes)` with no suffix, keeping `keyBytes` binary-verbatim (never UTF-8 round-tripped).

### Review
No significant issues identified.

## `streamingRecordKey` (lines ~162–168)

### Summary
Builds a streaming record key `[keyPrefix]dataset#keyBytes#dayTs#seg` where the day bucket is derived from `tsMillis` and `seg` is the segment index within the day (`0` is the root record, which also carries the `tail` bin).

### Walkthrough
- **Lines ~162–168 — Delegate:** Computes `dayTs(tsMillis)` and forwards to `streamingRecordKeyFromDay` with `seg` defaulting to `0`.

### Review
No significant issues identified.

## `streamingRecordKeyFromDay` (lines ~171–179)

### Summary
Builds a streaming record key using an already-computed start-of-day bucket and a non-negative segment index; the suffix is the decimal string `<dayTs>#<seg>`.

### Walkthrough
- **Lines ~171–179 — Delegate:** Calls `concat` with `s"$dayTsValue#$seg"` UTF-8 bytes as the suffix, appending `#<dayTs>#<seg>` after `keyBytes`.

### Review
No significant issues identified.

## `statsFirstDayKey` (lines ~203–209)

### Summary
Builds the stats first-day marker key: `[keyPrefix]dataset#keyBytes#first`. One never-expire record per stats join key holds a long bin `first` = the earliest day bucket with data, so a range read can enumerate days without scanning back to epoch.

### Walkthrough
- **Delegate:** `concat` with `"first"` UTF-8 bytes as the suffix. Stats day-bucket keys themselves use `streamingRecordKeyFromDay` (`dataset#keyBytes#<dayTs>#<seg>`); data written by the earlier single-record layout stays at `recordKey` and is still read.

## `statsDays` (lines ~213–217)

### Summary
Pure day enumeration for a stats timeseries read: `max(dayTs(afterTsMillis), firstDay)` (or `firstDay` when absent) through `today`, stepping `MillisPerDay`; empty when the start is after today. `afterTsMillis = 0` is clamped to `firstDay`, never 1970.

### Review
No significant issues identified.

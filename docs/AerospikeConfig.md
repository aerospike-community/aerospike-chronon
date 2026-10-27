# AerospikeConfig.scala

Parsed and validated connection/tuning settings built from Chronon's
`userConf` map. Source: `src/main/scala/com/aerospike/chronon/AerospikeConfig.scala`

## `fromUserConf` (lines 251–400)

### Summary
Parses the `aerospike.*` keys of `userConf` into a validated `AerospikeConfig`, throwing `IllegalArgumentException` naming the offending key on any missing, malformed, or inconsistent value.

### Walkthrough
- **Lines 125–131 — Hosts:** Requires `aerospike.hosts`, splits on commas, trims, drops empties, and throws when no non-empty host remains.
- **Lines 133–144 — `parseInt` helper:** Strict int parser — present-but-non-numeric values throw `IllegalArgumentException`; absent keys fall back to the supplied default.
- **Lines 146–152 — `parsePositiveInt` helper:** Wraps `parseInt` and throws when the parsed value is `<= 0`.
- **Lines 154–165 — `parseLong` helper:** Strict long parser with the same naming-the-key failure behavior as `parseInt`.
- **Lines 167–168 — Namespace:** Requires `aerospike.namespace`, throwing when absent.
- **Lines 305–331 — TTL settings:** `parseTtl` accepts a positive number of seconds (`Seconds`), `0` (`NeverExpire`, written `expiration = -1`) or `namespace` (`NamespaceDefault`, written `expiration = 0`); negative, non-numeric and `Int.MaxValue`-overflowing values are rejected naming the key. `ttl.batch.seconds`/`ttl.streaming.seconds` fall back to `ttl.seconds` (default 5 days for both).
- **Lines 333–347 — Lookback validation:** Parses `streaming.maxLookbackDays` (non-negative), then rejects the combination when `days * 86400000` exceeds the streaming TTL — only when it is `Seconds` (never-expire is unbounded; `namespace` is validated against the resolved `default-ttl` at client build, or skipped if unresolvable).
- **Line 193 — Loader timeout:** Parses `loader.batch.timeout.seconds` as a positive int (default 60).
- **Lines 195–199 — In-flight cap:** Optionally parses `maxCommandsInProcess` and rejects values below `MinCommandsInProcess` (5, the client minimum).
- **Lines 303–312 — Pool bounds:** Parses `maxConnsPerNode` (positive, default 100) and `minConnsPerNode` (non-negative, default 16), rejecting `min > max`. Both are async-only pool settings — the sync pool keeps the client default cap with floor 0.
- **Lines 314–319 — Loader pool bounds:** Parses `loader.maxConnsPerNode` (positive, default 32) and `loader.minConnsPerNode` (non-negative, default 0), rejecting `min > max`. Not part of the client cache key.
- **Lines 321–345 — Result:** Assembles the `AerospikeConfig` with parsed port/timeouts as positive ints, retries as non-negative ints, `keyPrefix` filtered to non-empty, and the validated fields above. The case class itself `require`s `socket < read/write total` whenever that class has `maxRetries > 0` (a stalled attempt would consume the whole total and retries could never fire), throwing `IllegalArgumentException` naming the socket, total and retries keys.

### TTL keys

Each accepts a positive number of seconds, `0` (never expire,
`expiration = -1`) or `namespace` (the server's `default-ttl`,
`expiration = 0`); none is part of the client cache key:

| Key | Default | Applies to |
| --- | --- | --- |
| `aerospike.ttl.seconds` | `432000` (5 days) | Fallback for both classes |
| `aerospike.ttl.batch.seconds` | `ttl.seconds` | Batch records (not the stats datasets) |
| `aerospike.ttl.streaming.seconds` | `ttl.seconds` | Streaming records, their segments and roots |

Metadata, `group_by_serving_info` and the stats datasets are always
never-expire. A positive TTL requires `nsup-period > 0` on the namespace (or
`allow-ttl-without-nsup`, testing only) — the server rejects TTL-bearing
writes with code 22 otherwise, and the client build now fails fast on that
combination (see AerospikeClientHolder.md `checkNamespacePolicy`).

### Retention and streaming-segment keys

Parsed with `parsePositiveInt` (non-numeric or `<= 0` rejected naming the key);
all five are data-shape settings excluded from the client cache key, so stores
that differ only here share one `AerospikeClient`:

| Key | Default | Meaning |
| --- | --- | --- |
| `aerospike.batch.retainVersions` | `2` | Newest N versions kept per record for group-by batch, `group_by_serving_info`, stats schema keys and other metadata datasets |
| `aerospike.stats.segmentTargetKiB` | `512` | Byte target per stats timeseries day segment; each write derives its entry limit as `targetKiB * 1024 / (valueBytes + 13)` so a segment stays near the target (internal cap 1024 segments per day) |
| `aerospike.streaming.segmentMaxEntries` | `256` (provisional) | Soft limit of map entries in a streaming day segment; reaching it advances the day's `tail` |
| `aerospike.streaming.maxSegmentsPerDay` | `1024` | Cap on segments per entity-day; a write past the cap returns `false` |
| `aerospike.read.batchChunkSize` | `1000` | Max `BatchRead`s per `multiGet` batch call; larger reads split into concurrent chunks with per-chunk failure isolation (see README "Read chunking") |

### Timeout and retry keys

Timeouts parse with `parsePositiveInt`, retries with `parseNonNegativeInt`
(`0` allowed; negative/non-numeric rejected naming the key). All five are
baked into the shared client's policy templates, so they ARE part of the
client cache key:

| Key | Default | Meaning |
| --- | --- | --- |
| `aerospike.timeout.socket.millis` | `250` | Per-attempt socket timeout; must be below each per-class total while that class retries (validated) |
| `aerospike.timeout.read.total.millis` | `2000` | Total timeout for batch reads, retries included |
| `aerospike.timeout.stats.read.total.millis` | `10000` | Total timeout for batch reads of the stats datasets (cache-key member) |
| `aerospike.timeout.write.total.millis` | `1000` | Total timeout for writes, retries included |
| `aerospike.maxRetries.read` | `2` | Retries of a timed-out/failed batch read (`0` disables) |
| `aerospike.maxRetries.write` | `2` | Retries of a timed-out/failed write (`0` disables) |

The previous `aerospike.timeout.total.millis` key (single 30 s total) is
removed outright. See README "Timeouts and retries" for the model and the
stuck-node measurements behind the defaults. The Spark loader overrides these
with bulk-oriented defaults (socket 5000, totals 30000, write retries 2).

### Connection-pool keys

| Key | Default | Meaning |
| --- | --- | --- |
| `aerospike.maxConnsPerNode` | `100` | Async pool cap per node (cache-key member) |
| `aerospike.minConnsPerNode` | `16` | Async pool floor per node, `<= max` (cache-key member) |
| `aerospike.loader.maxConnsPerNode` | `32` | Async pool cap applied by `forLoader` to bulk loads (not in cache key) |
| `aerospike.loader.minConnsPerNode` | `0` | Async pool floor applied by `forLoader` (not in cache key) |

See README "Connection footprint" for sizing against `proto-fd-max` and the
measured idle/burst numbers.

## `forLoader` (lines 353–362)

`copy` that switches a serving config to the bulk-load profile: pool floor
and cap from the `aerospike.loader.*` keys, the loader constants
`LoaderSocketTimeoutMs` (5000) / `LoaderReadTotalTimeoutMs` (30000) /
`LoaderWriteTotalTimeoutMs` (30000) / `LoaderMaxRetriesRead` (0) /
`LoaderMaxRetriesWrite` (2), and `maxCommandsInProcess = None` so the
in-flight cap falls back to the new `maxConnsPerNode`. Used by
`AerospikeKvStore.bulkPut`; the `Spark2AerospikeLoader` CLI builds its config
from its own flags (`--max-conns-per-node` / `--min-conns-per-node`,
defaults 32 / 0) instead.

### Review
No significant issues identified.

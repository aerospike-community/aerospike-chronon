# AerospikeClientHolder.scala

One shared Aerospike client per JVM, keyed by the config fields that physically
shape the client. Source: `src/main/scala/com/aerospike/chronon/AerospikeClientHolder.scala`

## `eventLoopSizing` (lines 61–65)

### Summary
Computes (event loop count, maxCommandsInProcess per loop) that spreads a total in-flight cap across NIO event loops while respecting the client's per-loop minimum.

### Walkthrough
- **Line 62 — Load floor:** Reads `MinCommandsInProcess` (5), the client's per-loop minimum.
- **Line 63 — Loop count:** Picks `max(1, min(cpus, totalInFlight / min))` so small caps get fewer loops and never dip below the per-loop floor.
- **Line 64 — Per-loop cap:** Returns `max(min, totalInFlight / loops)`, guaranteeing each loop's cap is at least the client minimum.

### Review
No significant issues identified.

## `clientCacheKey` (lines 74–77)

### Summary
Builds the cache key covering only the config fields that affect client construction (hosts, port, pool bounds, in-flight cap, timeouts).

### Walkthrough
- **Lines 75–77 — Key assembly:** Concatenates sorted hosts, port, `maxConnsPerNode`, `minConnsPerNode`, `maxCommandsInProcess` (0 when unset), and the three timeouts into a colon-separated string, deliberately excluding namespace, TTL, key prefix, lookback, and loader timeout.

### Review
No significant issues identified.

## `buildHolder` (lines 96–125)

### Summary
Constructs an `AerospikeClient` plus its dedicated `NioEventLoops`, applies pool and timeout settings, and registers a JVM shutdown hook that closes both.

### Walkthrough
- **Lines 95–99 — Skew warning:** Logs a warning when `maxCommandsInProcess` exceeds `maxConnsPerNode`, since a single-node-skewed workload can still exhaust the pool.
- **Lines 100–101 — Loop sizing:** Computes loop count and per-loop cap from `maxCommandsInProcess` (defaulting to `maxConnsPerNode`) and available CPUs.
- **Lines 102–104 — Event loops:** Sets `EventPolicy.maxCommandsInProcess` and creates `NioEventLoops`.
- **Lines 105–107 — Build log:** INFO line per client build with the async floor, async cap, in-flight cap, event-loop count and per-loop cap.
- **Line 111 — Client policy:** Delegates to `clientPolicyFor` (below): async pool floor/cap from config, sync floor 0 (the store never calls the sync API; sync cap stays at the client default), `connectTimeoutMillis` mapped to `ClientPolicy.timeout` (the cluster-tend timeout, not per-transaction).
- **Lines 113–115 — Client construction:** Builds `Host` objects, instantiates `AerospikeClient`, and increments `constructionCount`.
- **Namespace check:** `namespaceSettingsFor(config)` returns a cached successful resolution for `(client cache key, namespace)` or runs `resolveNamespace` now — one `info namespace/<ns>` request per client+namespace via the `namespaceInfoSource` seam (`Info.request` on the first node). Only successful resolutions are cached: a failure, an `ERROR:` reply or a response with no recognised keys resolves to `None` (the check is skipped with a WARN) and is retried at the next store construction. `checkNamespacePolicy` throws `IllegalStateException` naming `nsup-period`/`allow-ttl-without-nsup` and the options when a TTL-bearing class would be written while the supervisor is off; it also validates `maxLookbackDays` against a resolved `namespace` streaming TTL. A first-build failure closes the client and loops before rethrow. A WARN also fires once per client+namespace when the streaming TTL is never-expire.
- **Lines 125–128 — Shutdown hook:** Registers a hook closing client then event loops (each in `Try`), and records it under the same cache key for `clear` removal.
- **Line 130 — Result:** Returns the `Holder` pairing client and event loops.

### Review
No significant issues identified.

## `clientFor` (lines 129–130)

### Summary
Returns the shared client for a config, lazily building it (event loops and shutdown hook included) on first use per cache key.

### Walkthrough
- **Lines 131–141 — `clientPolicyFor`:** `private[chronon]` seam extracted from `buildHolder` so unit tests can inspect the `ClientPolicy` without a server: `eventLoops` wiring, `asyncMaxConnsPerNode`/`asyncMinConnsPerNode` from config, `minConnsPerNode = 0`, `timeout = connectTimeoutMillis`.
- **Lines 129–130 — Get-or-build:** Atomically computes `buildHolder` via `ConcurrentHashMap.computeIfAbsent` on `clientCacheKey` and returns the client's `.client`.

### Review
No significant issues identified.

## `policyTemplates` (lines 134–135)

### Summary
Returns cached write/batch policy templates derived from the client's defaults for the config's cache key.

### Walkthrough
- **Lines 134–135 — Get-or-build:** Computes `buildTemplates` once per cache key via `computeIfAbsent`; callers needing per-request mutation must clone the returned template.

### Review
No significant issues identified.

## `clear` (lines 139–152)

### Summary
Closes and drops all cached clients, templates, and shutdown hooks — a test-only reset so each integration suite gets its own container.

### Walkthrough
- **Lines 140–141 — Snapshot:** Copies current holders and hooks to lists before mutating the maps.
- **Lines 142–144 — Drop:** Clears the `clients`, `templates`, and `shutdownHooks` maps.
- **Lines 145–148 — Close:** Closes each client's `AerospikeClient` and `EventLoops`, each wrapped in `Try`.
- **Lines 149–151 — Unhook:** Removes each shutdown hook from the runtime, ignoring failures.

### Review
- **Lines 140–142 — Clear/build race:** A concurrent `clientFor` between the snapshot at lines 140–141 and `clients.clear()` at line 142 can insert a `Holder` that is neither closed nor removed, leaking a client and event loops in racy usage.

## `buildTemplates` (lines ~157–176)

### Summary
Builds write and batch policy templates from the client's policy defaults, then overrides connect, socket, the per-class total timeout and per-class `maxRetries` from config.

### Walkthrough
- **Write template:** `new WritePolicy()` (the client default) plus `connectTimeout`, `socketTimeout`, `totalTimeout = writeTotalTimeoutMillis` and `maxRetries = maxRetriesWrite`.
- **Batch templates:** `new BatchPolicy()` plus `connectTimeout`, `socketTimeout`, `totalTimeout = readTotalTimeoutMillis` and `maxRetries = maxRetriesRead`; a second `statsBatch` template is identical except `totalTimeout = statsReadTotalTimeoutMillis`, used for batch reads of the stats datasets.
- **`sleepBetweenRetries`:** left at the default 0 — ignored in async mode.
- **Result:** Returns `PolicyTemplates(write, batch)`.

### Review
No significant issues identified.

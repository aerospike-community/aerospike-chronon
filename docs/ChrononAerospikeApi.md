# ChrononAerospikeApi.scala

Chronon `ai.chronon.online.Api` implementation for Aerospike — the class to
point `api.impl` at. Everything is driven by `userConf`.
Source: `src/main/scala/com/aerospike/chronon/ChrononAerospikeApi.scala`

## `genKvStore` (line 52)

### Summary
Returns the singleton `AerospikeKvStore` for this `userConf`.

### Walkthrough
- **Line 52 — Store:** Returns the `@transient lazy val kvStore`, rebuilt on first use after deserialization rather than shipping connections.

### Review
No significant issues identified.

## `externalRegistry` (lines 59–60)

### Summary
One `ExternalSourceRegistry` per Api instance, held in a `@transient lazy val`
— the same object `buildFetcher`/`buildJavaFetcher` receive, so handlers and
factories registered through `api.externalRegistry` are visible to serving.

### Walkthrough
- **Line 59 — Registry:** `@transient lazy val registry = new ExternalSourceRegistry()`; a deserialized instance gets a fresh empty registry and registrations must be redone.
- **Line 60 — Accessor:** Returns `registry` — identical on repeated calls and identical to the fetchers' registry.

### Review
No significant issues identified.

## `logConsumer` (lines 66–74)

### Summary
A `@transient lazy val Try[Consumer[LoggableResponse]]` holding the configured
consumer, `Success(null)` when `chronon.log.consumer.class` is unset, or a
cached `Failure`. Evaluated once per instance — a resolution failure after
deserialization is logged once and rethrown without repeating the lookup.
Forced in the constructor, so a bad class fails the build.

### Walkthrough
- **Lines 67–68 — Resolve:** `Try(userConf.get(key).map(instantiateLogConsumer).orNull)`.
- **Lines 69–73 — Log once:** `result.failed.foreach` logs the error with the cause at evaluation time.

### Review
No significant issues identified.

## `decoderClass` (lines 77–82)

### Summary
A `@transient lazy val Class[_]` holding the validated
`chronon.stream.decoder.class` class — resolved once (exists, is a
`StreamDecoder`, has a `(GroupByServingInfoParsed)` constructor), forced in
the constructor when the key is set; instantiation stays per `streamDecoder`
call.

### Walkthrough
- **Lines 78–80 — Key:** `IllegalArgumentException` naming the key when absent (thrown on `streamDecoder`, not construction).
- **Line 82 — Validate:** Delegates to `validateDecoderClass`.

### Review
No significant issues identified.

## `streamDecoder` (lines 91–105)

### Summary
Instantiates the (construction-validated) decoder class per call, wrapping
failures in `IllegalArgumentException` naming the class and key.

### Walkthrough
- **Lines 93–96 — Construct:** `decoderClass.getConstructor(GroupByServingInfoParsed).newInstance(gbsip)` per call.
- **Lines 97–104 — Wrap:** `IllegalArgumentException` (missing key) propagates; other `NonFatal` is wrapped naming class and key.

### Review
No significant issues identified.

## `logResponse` (lines 109–110)

### Summary
No-op unless `chronon.log.consumer.class` is configured; forwards the
`LoggableResponse` to the instance created at construction.

### Walkthrough
- **Line 110 — Forward:** `Option(logConsumer.get)` — `null` when unconfigured; a cached `Failure` is rethrown by `get` without re-resolving.

### Review
No significant issues identified.

## `instantiateLogConsumer` (lines 112–131)

### Summary
Reflectively resolves and instantiates the consumer FQN with staged errors,
each naming `chronon.log.consumer.class`, the class and the reason, cause
attached.

### Walkthrough
- **Lines 115–118 — Load:** `Class.forName`, `IllegalArgumentException` on `NonFatal`.
- **Lines 119–120 — Type:** `Consumer` assignability checked before any instantiation (a wrong type is never constructed).
- **Lines 121–125 — Constructor:** public no-argument constructor required, error names the reason.
- **Lines 126–130 — Instantiate:** constructor failures wrapped with the cause attached.

### Review
No significant issues identified.

## `validateDecoderClass` (lines 133–147)

### Summary
`Class.forName` + `StreamDecoder` assignability + a public
`(GroupByServingInfoParsed)` constructor — each failure naming
`chronon.stream.decoder.class`, the class and the reason. Never instantiates.

### Review
No significant issues identified.

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

import ai.chronon.online.{Api, ExternalSourceRegistry, GroupByServingInfoParsed, KVStore, LoggableResponse, StreamDecoder}
import org.slf4j.LoggerFactory

import java.util.function.Consumer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Chronon `ai.chronon.online.Api` implementation for Aerospike; the class to
  * point `api.impl` at. Everything is driven by `userConf` — the same map
  * Chronon passes to fetchers, the Spark loader, and streaming jobs.
  *
  * Provides the shared [[AerospikeKvStore]] plus two pluggable hooks, both
  * validated at construction so misconfiguration fails fast:
  *
  *  - `chronon.stream.decoder.class` (required for streaming) — FQN of a
  *    `StreamDecoder` with a `(GroupByServingInfoParsed)` constructor, checked
  *    to exist, be a `StreamDecoder` and have that constructor at construction;
  *    still instantiated per [[streamDecoder]] call.
  *  - `chronon.log.consumer.class` (optional) — FQN of a
  *    `java.util.function.Consumer[LoggableResponse]` with a public no-argument
  *    constructor, instantiated exactly once at construction;
  *    [[logResponse]] is a no-op without it.
  *
  * The KV store, the registry and the consumer are `@transient lazy`:
  * deserialized instances rebuild them on first use rather than shipping
  * connections or handlers. Registrations must be redone in the serving
  * process.
  */
class ChrononAerospikeApi(userConf: Map[String, String]) extends Api(userConf) {

  @transient private lazy val kvStore: KVStore = AerospikeKvStore(userConf)

  /** The singleton [[AerospikeKvStore]] for this `userConf`. */
  override def genKvStore: KVStore = kvStore

  /** The one registry for this Api instance — the same object Chronon's
    * fetchers receive through `buildFetcher`/`buildJavaFetcher`, so handlers
    * registered here are visible to serving. Not serialized; a deserialized
    * instance gets a fresh empty registry and registrations must be redone
    * (repeated `registerJoinExternalSources` is safe). */
  @transient private lazy val registry: ExternalSourceRegistry = new ExternalSourceRegistry()
  override def externalRegistry: ExternalSourceRegistry = registry

  /** The configured consumer or `Failure` — evaluated exactly once per Api
    * instance, so an unresolvable consumer after deserialization is reported
    * and rethrown without repeating the lookup. Forced at construction:
    * a bad `chronon.log.consumer.class` fails the build, not the first log. */
  @transient private lazy val logConsumer: Try[Consumer[LoggableResponse]] = {
    val result = Try(userConf.get(ChrononAerospikeApi.LogConsumerClassKey).map(instantiateLogConsumer).orNull)
    result.failed.foreach { e =>
      ChrononAerospikeApi.logger.error(
        s"Unable to resolve ${ChrononAerospikeApi.LogConsumerClassKey}: ${e.getMessage}", e)
    }
    result
  }

  /** The validated decoder class — resolved once; instantiation stays per
    * [[streamDecoder]] call. Forced at construction when the key is set. */
  @transient private lazy val decoderClass: Class[_] = {
    val className = userConf.getOrElse(
      ChrononAerospikeApi.StreamDecoderClassKey,
      throw new IllegalArgumentException(s"Missing required config key: ${ChrononAerospikeApi.StreamDecoderClassKey}")
    )
    validateDecoderClass(className)
  }

  // fail fast at construction when the optional keys name unusable classes
  logConsumer.get
  userConf.get(ChrononAerospikeApi.StreamDecoderClassKey).foreach(_ => decoderClass)

  /** Instantiates the `chronon.stream.decoder.class` impl (validated at
    * construction). Throws `IllegalArgumentException` if the key is absent. */
  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder = {
    try {
      decoderClass
        .getConstructor(classOf[GroupByServingInfoParsed])
        .newInstance(groupByServingInfoParsed)
        .asInstanceOf[StreamDecoder]
    } catch {
      case e: IllegalArgumentException => throw e
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Unable to instantiate stream decoder class '${decoderClass.getName}' from config key ${ChrononAerospikeApi.StreamDecoderClassKey}",
          e
        )
    }
  }

  /** No-op unless `chronon.log.consumer.class` is configured, in which case the
    * response is forwarded to the instance created at construction. */
  override def logResponse(resp: LoggableResponse): Unit =
    Option(logConsumer.get).foreach(_.accept(resp))

  private def instantiateLogConsumer(className: String): Consumer[LoggableResponse] = {
    val key = ChrononAerospikeApi.LogConsumerClassKey
    val cls =
      try { Class.forName(className) } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Config key '$key': class '$className' not found", e)
      }
    if (!classOf[Consumer[_]].isAssignableFrom(cls))
      throw new IllegalArgumentException(s"Config key '$key': class '$className' is not a java.util.function.Consumer")
    val ctor =
      try { cls.getDeclaredConstructor() } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Config key '$key': class '$className' has no public no-argument constructor", e)
      }
    try {
      ctor.newInstance().asInstanceOf[Consumer[LoggableResponse]]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Config key '$key': constructor of '$className' failed", e)
    }
  }

  private def validateDecoderClass(className: String): Class[_] = {
    val key = ChrononAerospikeApi.StreamDecoderClassKey
    val cls =
      try { Class.forName(className) } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Config key '$key': class '$className' not found", e)
      }
    if (!classOf[StreamDecoder].isAssignableFrom(cls))
      throw new IllegalArgumentException(s"Config key '$key': class '$className' is not a StreamDecoder")
    try { cls.getConstructor(classOf[GroupByServingInfoParsed]) } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Config key '$key': class '$className' has no public (GroupByServingInfoParsed) constructor", e)
    }
    cls
  }
}

object ChrononAerospikeApi {
  private val logger = LoggerFactory.getLogger(classOf[ChrononAerospikeApi])

  /** `userConf` key: optional `Consumer[LoggableResponse]` FQN for response logging. */
  val LogConsumerClassKey: String = "chronon.log.consumer.class"

  /** `userConf` key: `StreamDecoder` FQN (ctor takes `GroupByServingInfoParsed`);
    * required by [[ChrononAerospikeApi.streamDecoder]]. */
  val StreamDecoderClassKey: String = "chronon.stream.decoder.class"
}

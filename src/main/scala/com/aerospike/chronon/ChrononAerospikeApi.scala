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

import java.util.function.Consumer
import scala.util.control.NonFatal

class ChrononAerospikeApi(userConf: Map[String, String]) extends Api(userConf) {

  @transient private lazy val kvStore: KVStore = AerospikeKvStore(userConf)

  override def genKvStore: KVStore = kvStore

  override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): StreamDecoder = {
    val className = userConf.getOrElse(
      ChrononAerospikeApi.StreamDecoderClassKey,
      throw new IllegalArgumentException(s"Missing required config key: ${ChrononAerospikeApi.StreamDecoderClassKey}")
    )
    instantiateStreamDecoder(className, groupByServingInfoParsed)
  }

  override def externalRegistry: ExternalSourceRegistry = new ExternalSourceRegistry()

  /** Default no-op; pluggable via `chronon.log.consumer.class` in userConf. */
  override def logResponse(resp: LoggableResponse): Unit = {
    userConf.get(ChrononAerospikeApi.LogConsumerClassKey).foreach { className =>
      logConsumerFor(className).accept(resp)
    }
  }

  @transient private lazy val logConsumerFor: String => Consumer[LoggableResponse] = {
    val cache = scala.collection.concurrent.TrieMap.empty[String, Consumer[LoggableResponse]]
    className =>
      cache.getOrElseUpdate(
        className, {
          Class
            .forName(className)
            .getDeclaredConstructor()
            .newInstance()
            .asInstanceOf[Consumer[LoggableResponse]]
        }
      )
  }

  private def instantiateStreamDecoder(className: String, gbsip: GroupByServingInfoParsed): StreamDecoder = {
    try {
      Class
        .forName(className)
        .getConstructor(classOf[GroupByServingInfoParsed])
        .newInstance(gbsip)
        .asInstanceOf[StreamDecoder]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Unable to instantiate stream decoder class '$className' from config key ${ChrononAerospikeApi.StreamDecoderClassKey}",
          e
        )
    }
  }
}

object ChrononAerospikeApi {
  val LogConsumerClassKey: String = "chronon.log.consumer.class"
  val StreamDecoderClassKey: String = "chronon.stream.decoder.class"
}

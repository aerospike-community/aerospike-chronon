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
import com.aerospike.client.policy.WritePolicy
import com.aerospike.client.{Value, Operation}

import java.util.Arrays
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class AerospikeKvStore(val config: AerospikeConfig) extends KVStore {

  import AerospikeDataModel._
  import AerospikeKvStore.WriteFailure

  private val client = AerospikeClientHolder.clientFor(config)

  private val ValueBin: String = "v"

  private val KeyOrderedPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT)

  private val metricsByDataset = TrieMap.empty[String, Metrics.Context]

  /** Per-dataset metrics context (latency distributions, success/failure counters). */
  def metricsContextFor(dataset: String): Metrics.Context =
    metricsByDataset.getOrElseUpdate(dataset, Metrics.Context(Metrics.Environment.GroupByUpload).withSuffix(dataset))

  override def create(dataset: String): Unit = {
    logger.info(s"create($dataset) is a no-op for Aerospike (sets are created on write)")
    metricsContextFor(dataset).increment("create.successes")
  }

  override def multiPut(putRequests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    val startNs = System.nanoTime()
    val rejected = Future.successful(Some(WriteFailure(ResultCode.PARAMETER_ERROR, inDoubt = false)))
    val results = Future.sequence(putRequests.map { req =>
      if (req.keyBytes == null || req.valueBytes == null || datasetNameError(req.dataset).isDefined) rejected
      else putOne(req)
    })
    results.map { outcomes =>
      val latencyMs = (System.nanoTime() - startNs) / 1000000
      putRequests.zip(outcomes).groupBy(_._1.dataset).foreach { case (dataset, pairs) =>
        val ctx = metricsContextFor(dataset)
        ctx.distribution(Metrics.Name.WriteLatencyMillis, latencyMs)
        pairs.foreach { case (_, outcome) =>
          ctx.increment(Metrics.Name.ResponseCount)
          outcome match {
            case None => ctx.increment(Metrics.Name.RequestCount)
            case Some(failure) =>
              ctx.increment("write.exceptions")
              ctx.increment(s"write.exceptions.${AerospikeKvStore.resultName(failure.resultCode)}")
          }
        }
        logWriteFailures(dataset, pairs.flatMap(_._2), pairs.size)
      }
      outcomes.map(_.isEmpty)
    }
  }

  /** One line per dataset per multiPut so a failure storm cannot flood the log. */
  private def logWriteFailures(dataset: String, failures: Seq[WriteFailure], total: Int): Unit =
    if (failures.nonEmpty) {
      val summary = failures.groupBy(_.resultCode).toSeq.sortBy(-_._2.size).map { case (code, fs) =>
        s"${ResultCode.getResultString(code)} (code=$code) x${fs.size}, inDoubt=${fs.count(_.inDoubt)}"
      }.mkString("; ")
      logger.warn(s"multiPut to $dataset: ${failures.size}/$total writes failed: $summary")
    }

  private def putOne(req: PutRequest): Future[Option[WriteFailure]] = {
    val ts = req.tsMillis.getOrElse(System.currentTimeMillis())
    val tableType = getTableType(req.dataset)
    val setName = physicalSetName(req.dataset)
    val keyBytes = tableType match {
      case StreamingTable => streamingRecordKey(req.dataset, req.keyBytes, ts, config.keyPrefix)
      case _                => recordKey(req.dataset, req.keyBytes, config.keyPrefix)
    }
    val key = new Key(config.namespace, setName, keyBytes)
    val writePolicy = writePolicyFor(req.dataset, req.keyBytes)
    val op = MapOperation.put(KeyOrderedPolicy, ValueBin, Value.get(ts), Value.get(req.valueBytes))

    val promise = Promise[Option[WriteFailure]]()
    try {
      client.operate(
        null, // round-robin event loop
        new RecordListener {
          override def onSuccess(key: Key, record: com.aerospike.client.Record): Unit = promise.success(None)
          override def onFailure(e: AerospikeException): Unit =
            promise.success(Some(WriteFailure(e.getResultCode, e.getInDoubt)))
        },
        writePolicy,
        key,
        op
      )
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to launch put for ${req.dataset}", e)
        val failure = e match {
          case ae: AerospikeException => WriteFailure(ae.getResultCode, ae.getInDoubt)
          case _                      => WriteFailure(ResultCode.CLIENT_ERROR, inDoubt = false)
        }
        promise.success(Some(failure))
    }
    promise.future
  }

  private def datasetNameError(dataset: String): Option[String] = {
    val setNameBytes = physicalSetName(dataset).getBytes("UTF-8")
    if (setNameBytes.length > 63) {
      Some(s"Aerospike set name '${physicalSetName(dataset)}' exceeds 63 bytes (${setNameBytes.length})")
    } else None
  }

  private def requestError(req: GetRequest): Option[String] = {
    if (req.keyBytes == null) Some("GetRequest.keyBytes is null")
    else datasetNameError(req.dataset)
  }

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    val startNs = System.nanoTime()
    if (requests.isEmpty) return Future.successful(Nil)

    val indexed = requests.zipWithIndex
    val (badIndexed, goodIndexed) = indexed.partition { case (req, _) => requestError(req).isDefined }
    val badByIndex = badIndexed.map { case (req, idx) =>
      idx -> GetResponse(req, Failure(new IllegalArgumentException(requestError(req).get)))
    }.toMap

    if (goodIndexed.isEmpty) return Future.successful((0 until requests.size).map(badByIndex))

    val goodIndexByRequest = goodIndexed.toMap
    val goodRequests = goodIndexed.map(_._1)
    val taggedReads = goodRequests.flatMap(buildBatchReads)
    if (taggedReads.isEmpty) return Future.successful(requests.map(r => GetResponse(r, Success(Seq.empty))))

    val batchReads = taggedReads.map(_._1).asJava
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
        AerospikeClientHolder.policyTemplates(config).batch,
        batchReads
      )
    } catch {
      case e: Exception => promise.failure(e)
    }

    promise.future
      .map { results =>
        val latencyMs = (System.nanoTime() - startNs) / 1000000
        val readByRequest = taggedReads.zip(results).groupBy(_._1._2)
        val goodByIndex = readByRequest.map { case (request, taggedWithResults) =>
          val ctx = metricsContextFor(request.dataset)
          ctx.distribution(Metrics.Name.LatencyMillis, latencyMs)
          ctx.increment(Metrics.Name.ResponseCount)

          val firstError = taggedWithResults.map(_._2).find { br =>
            br.resultCode != 0 && br.resultCode != ResultCode.KEY_NOT_FOUND_ERROR
          }
          val response = firstError match {
            case Some(br) =>
              ctx.increment(Metrics.Name.FetchExceptions)
              GetResponse(request, Failure(aerospikeException(br.resultCode, br.inDoubt)))
            case None =>
              val values = taggedWithResults.flatMap { case (_, br) => extractTimedValues(br.record) }.sortBy(_.millis)
              ctx.increment(Metrics.Name.RequestCount)
              GetResponse(request, Success(values))
          }
          val idx = goodIndexByRequest.getOrElse(request,
            throw new IllegalStateException(s"Request $request not found in index"))
          idx -> response
        }.toMap

        (0 until requests.size).map(i => goodByIndex.getOrElse(i, badByIndex(i)))
      }
      .recover { case ex =>
        requests.map { req =>
          metricsContextFor(req.dataset).increment(Metrics.Name.FetchExceptions)
          GetResponse(req, Failure(ex))
        }
      }
  }

  private def buildBatchReads(req: GetRequest): Seq[(BatchRead, GetRequest)] = {
    val op = req.afterTsMillis match {
      case Some(ts) => MapOperation.getByKeyRange(ValueBin, Value.get(ts), null, MapReturnType.KEY_VALUE)
      case None     => MapOperation.getByKeyRange(ValueBin, null, null, MapReturnType.KEY_VALUE)
    }

    getTableType(req.dataset) match {
      case StreamingTable =>
        val now = System.currentTimeMillis()
        val endDay = dayTs(now)
        val requestedStartDay = req.afterTsMillis.map(dayTs).getOrElse(endDay)
        val effectiveLookbackMillis = math.min(
          config.ttlSeconds * 1000L,
          config.streamingMaxLookbackDays.toLong * MillisPerDay
        )
        val clampedStartDay = math.max(requestedStartDay, dayTs(now - effectiveLookbackMillis))
        (clampedStartDay to endDay by MillisPerDay).map { day =>
          val recordKeyBytes = streamingRecordKeyFromDay(req.dataset, req.keyBytes, day, config.keyPrefix)
          val key = new Key(config.namespace, physicalSetName(req.dataset), recordKeyBytes)
          (new BatchRead(key, Array(op)), req)
        }
      case _ =>
        val recordKeyBytes = recordKey(req.dataset, req.keyBytes, config.keyPrefix)
        val key = new Key(config.namespace, physicalSetName(req.dataset), recordKeyBytes)
        Seq((new BatchRead(key, Array(op)), req))
    }
  }

  private def extractTimedValues(record: com.aerospike.client.Record): Seq[KVStore.TimedValue] = {
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
            logger.warn(s"Skipping malformed map entry in bin '$ValueBin': ${e.getMessage}")
            None
        }
      case other =>
        logger.warn(s"Skipping unexpected entry type in bin '$ValueBin': ${other.getClass.getName}")
        None
    }.toSeq
  }

  private def writePolicyFor(dataset: String, keyBytes: Array[Byte]): WritePolicy = {
    val base = new WritePolicy(AerospikeClientHolder.policyTemplates(config).write)
    val tableType = getTableType(dataset)
    val neverExpire = tableType match {
      case MetadataTable(_) => true
      case BatchTable       => isServingInfoKey(keyBytes)
      case StreamingTable   => false
    }
    base.expiration = if (neverExpire) -1 else config.ttlSeconds.toInt
    base
  }

  private def isServingInfoKey(keyBytes: Array[Byte]): Boolean =
    Arrays.equals(keyBytes, ApiConstants.GroupByServingInfoKey.getBytes(ApiConstants.UTF8))

  private def aerospikeException(resultCode: Int, inDoubt: Boolean): AerospikeException = {
    val msg = s"Aerospike error: ${ResultCode.getResultString(resultCode)} (code=$resultCode, inDoubt=$inDoubt)"
    new AerospikeException(resultCode, msg)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder().getOrCreate()
    Spark2AerospikeLoader.upload(spark, sourceOfflineTable, Some(destinationOnlineDataSet), partition, config)
  }
}

object AerospikeKvStore {

  /** Failure detail kept internally; the public `multiPut` contract stays `Seq[Boolean]`. */
  private[chronon] case class WriteFailure(resultCode: Int, inDoubt: Boolean)

  /** Metric-name-safe label, e.g. -7 -> "no_more_available_connections". The metrics API
    * has no tags, so the result is encoded in the name; cardinality is bounded by ResultCode. */
  private[chronon] def resultName(code: Int): String = {
    val name = ResultCode.getResultString(code).toLowerCase.replaceAll("[^a-z0-9]+", "_").stripPrefix("_").stripSuffix("_")
    if (name.nonEmpty) name else s"code_${if (code < 0) "neg_" else ""}${math.abs(code)}"
  }

  def apply(userConf: Map[String, String]): AerospikeKvStore =
    new AerospikeKvStore(AerospikeConfig.fromUserConf(userConf))
}

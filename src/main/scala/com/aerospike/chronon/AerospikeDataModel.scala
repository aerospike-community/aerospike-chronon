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

object AerospikeDataModel {

  sealed trait TableType { def setName: String }
  case object BatchTable extends TableType { override val setName: String = "GROUPBY_BATCH" }
  case object StreamingTable extends TableType { override val setName: String = "GROUPBY_STREAMING" }
  final case class MetadataTable(dataset: String) extends TableType { override def setName: String = dataset }

  val KeySeparator: Byte = '#'.toByte
  val MillisPerDay: Long = 86400000L

  /** `_BATCH` -> BatchTable, `_STREAMING` -> StreamingTable, anything else -> MetadataTable(dataset). */
  def getTableType(dataset: String): TableType = {
    if (dataset.endsWith("_BATCH")) BatchTable
    else if (dataset.endsWith("_STREAMING")) StreamingTable
    else MetadataTable(dataset)
  }

  def physicalSetName(dataset: String): String = getTableType(dataset).setName

  /** Start-of-day epoch millis for a version timestamp (streaming day-bucket suffix). */
  def dayTs(tsMillis: Long): Long = tsMillis - (tsMillis % MillisPerDay)

  private def concat(prefix: Option[String], dataset: String, keyBytes: Array[Byte], suffix: Option[Array[Byte]] = None): Array[Byte] = {
    val prefixBytes = prefix.map(_.getBytes("UTF-8")).getOrElse(Array.empty)
    val datasetBytes = dataset.getBytes("UTF-8")
    val suffixLen = suffix.map(_.length).getOrElse(0)
    val totalLen = prefixBytes.length + datasetBytes.length + 1 + keyBytes.length + (if (suffixLen > 0) 1 + suffixLen else 0)
    val out = new Array[Byte](totalLen)
    var pos = 0
    System.arraycopy(prefixBytes, 0, out, pos, prefixBytes.length); pos += prefixBytes.length
    System.arraycopy(datasetBytes, 0, out, pos, datasetBytes.length); pos += datasetBytes.length
    out(pos) = KeySeparator; pos += 1
    System.arraycopy(keyBytes, 0, out, pos, keyBytes.length); pos += keyBytes.length
    suffix.foreach { s =>
      out(pos) = KeySeparator; pos += 1
      System.arraycopy(s, 0, out, pos, s.length); pos += s.length
    }
    out
  }

  /** Batch/metadata record key as raw bytes: `[keyPrefix]dataset#keyBytes`.
    * keyBytes is Avro-encoded binary and MUST be concatenated verbatim — never
    * round-tripped through a UTF-8 String (distinct binaries can decode to the
    * same replacement-char string and collide). */
  def recordKey(dataset: String, keyBytes: Array[Byte], keyPrefix: Option[String] = None): Array[Byte] =
    concat(keyPrefix, dataset, keyBytes)

  /** Streaming record key: `[keyPrefix]dataset#keyBytes#dayTs` where day is
    * derived from `tsMillis`. */
  def streamingRecordKey(dataset: String,
                         keyBytes: Array[Byte],
                         tsMillis: Long,
                         keyPrefix: Option[String] = None): Array[Byte] =
    streamingRecordKeyFromDay(dataset, keyBytes, dayTs(tsMillis), keyPrefix)

  /** Streaming record key using an already-computed start-of-day bucket. */
  def streamingRecordKeyFromDay(dataset: String,
                                keyBytes: Array[Byte],
                                dayTsValue: Long,
                                keyPrefix: Option[String] = None): Array[Byte] =
    concat(keyPrefix, dataset, keyBytes, Some(dayTsValue.toString.getBytes("UTF-8")))
}

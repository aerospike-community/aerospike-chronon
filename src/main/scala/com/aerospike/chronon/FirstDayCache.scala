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

import java.nio.ByteBuffer
import java.util.{LinkedHashMap, Map => JMap}

/** Bounded LRU map from `(dataset, entity key)` to the lowest stats day bucket
  * this store has already ensured in the first-day marker record. It only ever
  * decreases: once a day is ensured, writes on that or a later day skip the
  * marker write. It is only a shortcut — the marker record is authoritative,
  * so an evicted or stale entry costs one extra guarded write, never correctness.
  *
  * @param capacity maximum number of entries kept; the least recently used is evicted
  */
final class FirstDayCache(capacity: Int) {
  require(capacity > 0, s"capacity must be positive, got $capacity")

  private final case class EnsuredKey(dataset: String, key: ByteBuffer)

  private val map = new LinkedHashMap[EnsuredKey, java.lang.Long](16, 0.75f, true) {
    override def removeEldestEntry(eldest: JMap.Entry[EnsuredKey, java.lang.Long]): Boolean = size() > capacity
  }

  private def ensuredKey(dataset: String, keyBytes: Array[Byte]) =
    EnsuredKey(dataset, ByteBuffer.wrap(keyBytes))

  /** The lowest day already ensured for this key, or None when unknown. */
  def get(dataset: String, keyBytes: Array[Byte]): Option[Long] =
    map.synchronized(Option(map.get(ensuredKey(dataset, keyBytes))).map(_.longValue()))

  /** Records `day` unless an earlier day is already ensured; returns the ensured day.
    * The key bytes are cloned so the stored entry cannot alias the caller's array. */
  def ensure(dataset: String, keyBytes: Array[Byte], day: Long): Long = {
    val ek = ensuredKey(dataset, keyBytes.clone())
    map.synchronized {
      val next = math.min(Option(map.get(ek)).map(_.longValue()).getOrElse(Long.MaxValue), day)
      map.put(ek, next)
      next
    }
  }

  def size: Int = map.synchronized(map.size())
}

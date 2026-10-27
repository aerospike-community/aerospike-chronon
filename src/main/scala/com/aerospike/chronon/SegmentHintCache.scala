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

/** Bounded LRU map from `(dataset, entity key, day)` to the streaming segment
  * writers should currently use, `0` when unknown. It is only a hint: the root
  * record's `tail` bin is authoritative, so a stale or evicted entry costs one
  * redirect, never correctness. Values only move forward.
  *
  * @param capacity maximum number of entries kept; the least recently used is evicted
  */
final class SegmentHintCache(capacity: Int) {
  require(capacity > 0, s"capacity must be positive, got $capacity")

  private final case class HintKey(dataset: String, key: ByteBuffer, day: Long)

  private val map = new LinkedHashMap[HintKey, Integer](16, 0.75f, true) {
    override def removeEldestEntry(eldest: JMap.Entry[HintKey, Integer]): Boolean = size() > capacity
  }

  private def hintKey(dataset: String, keyBytes: Array[Byte], day: Long) =
    HintKey(dataset, ByteBuffer.wrap(keyBytes), day)

  /** The segment to write to for this entity and day (`0` if there is no hint). */
  def get(dataset: String, keyBytes: Array[Byte], day: Long): Int = {
    val hk = hintKey(dataset, keyBytes, day)
    map.synchronized(Option(map.get(hk)).map(_.intValue).getOrElse(0))
  }

  /** Records `segment` unless a later segment is already known; returns the resulting hint.
    * The key bytes are cloned so the stored entry cannot alias the caller's array. */
  def advanceTo(dataset: String, keyBytes: Array[Byte], day: Long, segment: Int): Int = {
    val hk = hintKey(dataset, keyBytes.clone(), day)
    map.synchronized {
      val current = Option(map.get(hk)).map(_.intValue).getOrElse(0)
      val next = math.max(current, segment)
      map.put(hk, next)
      next
    }
  }

  def size: Int = map.synchronized(map.size())
}

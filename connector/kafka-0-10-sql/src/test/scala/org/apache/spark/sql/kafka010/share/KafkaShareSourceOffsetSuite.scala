/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kafka010.share

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for KafkaShareSourceOffset and related classes.
 */
class KafkaShareSourceOffsetSuite extends SparkFunSuite {

  test("RecordKey - basic creation") {
    val key = RecordKey("test-topic", 0, 100L)

    assert(key.topic === "test-topic")
    assert(key.partition === 0)
    assert(key.offset === 100L)
  }

  test("RecordKey - from TopicPartition") {
    val tp = new TopicPartition("test-topic", 2)
    val key = RecordKey(tp, 200L)

    assert(key.topic === "test-topic")
    assert(key.partition === 2)
    assert(key.offset === 200L)
    assert(key.topicPartition === tp)
  }

  test("RecordKey - toString and fromString round-trip") {
    val original = RecordKey("my-topic", 5, 12345L)
    val str = original.toString
    val parsed = RecordKey.fromString(str)

    assert(parsed === original)
  }

  test("AcquiredRecordRange - basic creation") {
    val offsets = Set(100L, 105L, 110L)
    val range = AcquiredRecordRange(offsets, 1000L, 31000L, 1)

    assert(range.offsets === offsets)
    assert(range.size === 3)
    assert(range.minOffset === 100L)
    assert(range.maxOffset === 110L)
    assert(range.acquiredAt === 1000L)
    assert(range.lockExpiresAt === 31000L)
  }

  test("AcquiredRecordRange - contains offset") {
    val range = AcquiredRecordRange(Set(100L, 105L, 110L), 1000L, 31000L, 1)

    assert(range.contains(100L))
    assert(range.contains(105L))
    assert(!range.contains(101L))
    assert(!range.contains(99L))
  }

  test("AcquiredRecordRange - isLockExpired") {
    val range = AcquiredRecordRange(Set(100L), 1000L, 31000L, 1)

    assert(!range.isLockExpired(1000L))
    assert(!range.isLockExpired(30999L))
    assert(range.isLockExpired(31000L))
    assert(range.isLockExpired(32000L))
  }

  test("AcquiredRecordRange - withOffset adds offset") {
    val range = AcquiredRecordRange(Set(100L), 1000L, 31000L, 1)
    val updated = range.withOffset(200L)

    assert(updated.offsets === Set(100L, 200L))
    assert(range.offsets === Set(100L)) // Original unchanged
  }

  test("AcquiredRecordRange - withoutOffset removes offset") {
    val range = AcquiredRecordRange(Set(100L, 200L), 1000L, 31000L, 1)
    val updated = range.withoutOffset(100L)

    assert(updated.offsets === Set(200L))
    assert(range.offsets === Set(100L, 200L)) // Original unchanged
  }

  test("AcquiredRecordRange - empty creation") {
    val range = AcquiredRecordRange.empty(1000L, 30000L)

    assert(range.offsets.isEmpty)
    assert(range.size === 0)
    assert(range.acquiredAt === 1000L)
    assert(range.lockExpiresAt === 31000L)
  }

  test("AcquiredRecordRange - fromRange creation") {
    val range = AcquiredRecordRange.fromRange(100L, 104L, 1000L, 30000L)

    assert(range.offsets === Set(100L, 101L, 102L, 103L, 104L))
    assert(range.size === 5)
  }

  test("KafkaShareSourceOffset - empty offset") {
    val offset = KafkaShareSourceOffset.empty("test-group")

    assert(offset.shareGroupId === "test-group")
    assert(offset.batchId === 0L)
    assert(offset.acquiredRecords.isEmpty)
    assert(!offset.hasRecords)
    assert(offset.totalRecords === 0)
  }

  test("KafkaShareSourceOffset - forBatch creation") {
    val offset = KafkaShareSourceOffset.forBatch("test-group", 5L)

    assert(offset.shareGroupId === "test-group")
    assert(offset.batchId === 5L)
    assert(offset.acquiredRecords.isEmpty)
  }

  test("KafkaShareSourceOffset - JSON serialization round-trip") {
    val tp1 = new TopicPartition("topic1", 0)
    val tp2 = new TopicPartition("topic2", 1)

    val range1 = AcquiredRecordRange(Set(100L, 105L, 110L), 1000L, 31000L, 1)
    val range2 = AcquiredRecordRange(Set(200L, 210L), 2000L, 32000L, 2)

    val offset = KafkaShareSourceOffset(
      shareGroupId = "my-share-group",
      batchId = 10L,
      acquiredRecords = Map(tp1 -> range1, tp2 -> range2)
    )

    val json = offset.json
    val parsed = KafkaShareSourceOffset(json)

    assert(parsed.shareGroupId === offset.shareGroupId)
    assert(parsed.batchId === offset.batchId)
    assert(parsed.acquiredRecords.size === 2)
    assert(parsed.acquiredRecords(tp1).offsets === range1.offsets)
    assert(parsed.acquiredRecords(tp2).offsets === range2.offsets)
  }

  test("KafkaShareSourceOffset - getAllRecordKeys") {
    val tp1 = new TopicPartition("topic1", 0)
    val tp2 = new TopicPartition("topic2", 1)

    val range1 = AcquiredRecordRange(Set(100L, 105L), 1000L, 31000L, 1)
    val range2 = AcquiredRecordRange(Set(200L), 2000L, 32000L, 1)

    val offset = KafkaShareSourceOffset("group", 1L, Map(tp1 -> range1, tp2 -> range2))
    val keys = offset.getAllRecordKeys

    assert(keys.size === 3)
    assert(keys.contains(RecordKey(tp1, 100L)))
    assert(keys.contains(RecordKey(tp1, 105L)))
    assert(keys.contains(RecordKey(tp2, 200L)))
  }

  test("KafkaShareSourceOffset - totalRecords") {
    val tp1 = new TopicPartition("topic1", 0)
    val tp2 = new TopicPartition("topic2", 1)

    val range1 = AcquiredRecordRange(Set(100L, 105L, 110L), 1000L, 31000L, 1)
    val range2 = AcquiredRecordRange(Set(200L, 210L), 2000L, 32000L, 1)

    val offset = KafkaShareSourceOffset("group", 1L, Map(tp1 -> range1, tp2 -> range2))

    assert(offset.totalRecords === 5)
  }

  test("KafkaShareSourceOffset - hasRecords and partitionsWithRecords") {
    val tp1 = new TopicPartition("topic1", 0)
    val tp2 = new TopicPartition("topic2", 1)

    val emptyOffset = KafkaShareSourceOffset.empty("group")
    assert(!emptyOffset.hasRecords)
    assert(emptyOffset.partitionsWithRecords.isEmpty)

    val range = AcquiredRecordRange(Set(100L), 1000L, 31000L, 1)
    val nonEmptyOffset = KafkaShareSourceOffset("group", 1L, Map(tp1 -> range))

    assert(nonEmptyOffset.hasRecords)
    assert(nonEmptyOffset.partitionsWithRecords === Set(tp1))
  }

  test("KafkaShareSourceOffset - withAcquiredRecord") {
    val tp = new TopicPartition("topic1", 0)
    val offset = KafkaShareSourceOffset.empty("group")

    val updated = offset.withAcquiredRecord(tp, 100L, 1000L, 30000L)

    assert(updated.acquiredRecords.contains(tp))
    assert(updated.acquiredRecords(tp).contains(100L))
    assert(offset.acquiredRecords.isEmpty) // Original unchanged
  }

  test("KafkaShareSourceOffset - withAcknowledgedRecord") {
    val tp = new TopicPartition("topic1", 0)
    val range = AcquiredRecordRange(Set(100L, 105L), 1000L, 31000L, 1)
    val offset = KafkaShareSourceOffset("group", 1L, Map(tp -> range))

    val updated = offset.withAcknowledgedRecord(tp, 100L)

    assert(updated.acquiredRecords(tp).offsets === Set(105L))
  }

  test("KafkaShareSourceOffset - withAcknowledgedRecord removes empty partition") {
    val tp = new TopicPartition("topic1", 0)
    val range = AcquiredRecordRange(Set(100L), 1000L, 31000L, 1)
    val offset = KafkaShareSourceOffset("group", 1L, Map(tp -> range))

    val updated = offset.withAcknowledgedRecord(tp, 100L)

    assert(!updated.acquiredRecords.contains(tp))
  }
}


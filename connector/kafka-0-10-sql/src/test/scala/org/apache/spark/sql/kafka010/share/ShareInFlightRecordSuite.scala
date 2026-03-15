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
 * Unit tests for ShareInFlightRecord and ShareInFlightBatch.
 */
class ShareInFlightRecordSuite extends SparkFunSuite {

  test("AcknowledgmentType - values exist") {
    assert(AcknowledgmentType.ACCEPT.toString === "ACCEPT")
    assert(AcknowledgmentType.RELEASE.toString === "RELEASE")
    assert(AcknowledgmentType.REJECT.toString === "REJECT")
  }

  test("ShareInFlightBatch - add and retrieve records") {
    val batch = new ShareInFlightBatch(1L)
    val key1 = RecordKey("topic", 0, 100L)
    val key2 = RecordKey("topic", 0, 101L)

    // Create mock in-flight records
    val record1 = createMockInFlightRecord(key1, 1000L, 31000L)
    val record2 = createMockInFlightRecord(key2, 1000L, 31000L)

    batch.addRecord(record1)
    batch.addRecord(record2)

    assert(batch.totalRecords === 2)
    assert(batch.pending === 2)
    assert(batch.accepted === 0)
    assert(!batch.isComplete)
  }

  test("ShareInFlightBatch - acknowledge with ACCEPT") {
    val batch = new ShareInFlightBatch(1L)
    val key = RecordKey("topic", 0, 100L)
    val record = createMockInFlightRecord(key, 1000L, 31000L)

    batch.addRecord(record)
    assert(batch.pending === 1)

    val result = batch.acknowledge(key, AcknowledgmentType.ACCEPT)
    assert(result)
    assert(batch.pending === 0)
    assert(batch.accepted === 1)
    assert(batch.isComplete)
  }

  test("ShareInFlightBatch - acknowledge with RELEASE") {
    val batch = new ShareInFlightBatch(1L)
    val key = RecordKey("topic", 0, 100L)
    val record = createMockInFlightRecord(key, 1000L, 31000L)

    batch.addRecord(record)
    batch.acknowledge(key, AcknowledgmentType.RELEASE)

    assert(batch.pending === 0)
    assert(batch.released === 1)
    assert(batch.accepted === 0)
  }

  test("ShareInFlightBatch - acknowledge with REJECT") {
    val batch = new ShareInFlightBatch(1L)
    val key = RecordKey("topic", 0, 100L)
    val record = createMockInFlightRecord(key, 1000L, 31000L)

    batch.addRecord(record)
    batch.acknowledge(key, AcknowledgmentType.REJECT)

    assert(batch.pending === 0)
    assert(batch.rejected === 1)
  }

  test("ShareInFlightBatch - cannot acknowledge same record twice") {
    val batch = new ShareInFlightBatch(1L)
    val key = RecordKey("topic", 0, 100L)
    val record = createMockInFlightRecord(key, 1000L, 31000L)

    batch.addRecord(record)
    assert(batch.acknowledge(key, AcknowledgmentType.ACCEPT))
    assert(!batch.acknowledge(key, AcknowledgmentType.RELEASE)) // Should return false
  }

  test("ShareInFlightBatch - acknowledgeAllAsAccept") {
    val batch = new ShareInFlightBatch(1L)

    (0 until 5).foreach { i =>
      val key = RecordKey("topic", 0, i.toLong)
      batch.addRecord(createMockInFlightRecord(key, 1000L, 31000L))
    }

    assert(batch.pending === 5)

    val count = batch.acknowledgeAllAsAccept()
    assert(count === 5)
    assert(batch.pending === 0)
    assert(batch.accepted === 5)
    assert(batch.isComplete)
  }

  test("ShareInFlightBatch - releaseAllPending") {
    val batch = new ShareInFlightBatch(1L)

    (0 until 5).foreach { i =>
      val key = RecordKey("topic", 0, i.toLong)
      batch.addRecord(createMockInFlightRecord(key, 1000L, 31000L))
    }

    // Acknowledge some
    batch.acknowledge(RecordKey("topic", 0, 0L), AcknowledgmentType.ACCEPT)
    batch.acknowledge(RecordKey("topic", 0, 1L), AcknowledgmentType.ACCEPT)

    assert(batch.pending === 3)

    val released = batch.releaseAllPending()
    assert(released === 3)
    assert(batch.pending === 0)
    assert(batch.released === 3)
    assert(batch.accepted === 2)
  }

  test("ShareInFlightBatch - getPendingRecords") {
    val batch = new ShareInFlightBatch(1L)

    (0 until 3).foreach { i =>
      val key = RecordKey("topic", 0, i.toLong)
      batch.addRecord(createMockInFlightRecord(key, 1000L, 31000L))
    }

    batch.acknowledge(RecordKey("topic", 0, 1L), AcknowledgmentType.ACCEPT)

    val pending = batch.getPendingRecords
    assert(pending.size === 2)
    assert(pending.exists(_.recordKey.offset === 0L))
    assert(pending.exists(_.recordKey.offset === 2L))
  }

  test("ShareInFlightBatch - getRecordsByPartition") {
    val batch = new ShareInFlightBatch(1L)
    val tp0 = new TopicPartition("topic", 0)
    val tp1 = new TopicPartition("topic", 1)

    batch.addRecord(createMockInFlightRecord(RecordKey(tp0, 100L), 1000L, 31000L))
    batch.addRecord(createMockInFlightRecord(RecordKey(tp0, 101L), 1000L, 31000L))
    batch.addRecord(createMockInFlightRecord(RecordKey(tp1, 200L), 1000L, 31000L))

    val byPartition = batch.getRecordsByPartition

    assert(byPartition.size === 2)
    assert(byPartition(tp0).size === 2)
    assert(byPartition(tp1).size === 1)
  }

  test("ShareInFlightBatch - getAcknowledgmentsForCommit") {
    val batch = new ShareInFlightBatch(1L)
    val tp = new TopicPartition("topic", 0)

    batch.addRecord(createMockInFlightRecord(RecordKey(tp, 100L), 1000L, 31000L))
    batch.addRecord(createMockInFlightRecord(RecordKey(tp, 101L), 1000L, 31000L))
    batch.addRecord(createMockInFlightRecord(RecordKey(tp, 102L), 1000L, 31000L))

    batch.acknowledge(RecordKey(tp, 100L), AcknowledgmentType.ACCEPT)
    batch.acknowledge(RecordKey(tp, 101L), AcknowledgmentType.RELEASE)

    val acks = batch.getAcknowledgmentsForCommit

    assert(acks.size === 1) // One partition
    assert(acks(tp).size === 2) // Two acknowledged records
    assert(acks(tp)(100L) === AcknowledgmentType.ACCEPT)
    assert(acks(tp)(101L) === AcknowledgmentType.RELEASE)
  }

  test("ShareInFlightBatch - clear") {
    val batch = new ShareInFlightBatch(1L)

    (0 until 5).foreach { i =>
      val key = RecordKey("topic", 0, i.toLong)
      batch.addRecord(createMockInFlightRecord(key, 1000L, 31000L))
    }

    batch.acknowledgeAllAsAccept()
    assert(batch.totalRecords === 5)

    batch.clear()
    assert(batch.totalRecords === 0)
    assert(batch.pending === 0)
    assert(batch.accepted === 0)
  }

  test("ShareInFlightManager - create and get batch") {
    val manager = new ShareInFlightManager()

    val batch1 = manager.createBatch(1L)
    val batch2 = manager.createBatch(2L)

    assert(manager.getBatch(1L).isDefined)
    assert(manager.getBatch(2L).isDefined)
    assert(manager.getBatch(3L).isEmpty)
  }

  test("ShareInFlightManager - getOrCreateBatch") {
    val manager = new ShareInFlightManager()

    val batch1 = manager.getOrCreateBatch(1L)
    val batch2 = manager.getOrCreateBatch(1L) // Should return same batch

    assert(batch1 eq batch2)
  }

  test("ShareInFlightManager - removeBatch") {
    val manager = new ShareInFlightManager()

    manager.createBatch(1L)
    assert(manager.getBatch(1L).isDefined)

    val removed = manager.removeBatch(1L)
    assert(removed.isDefined)
    assert(manager.getBatch(1L).isEmpty)
  }

  test("ShareInFlightManager - getIncompleteBatches") {
    val manager = new ShareInFlightManager()

    val batch1 = manager.createBatch(1L)
    val batch2 = manager.createBatch(2L)

    batch1.addRecord(createMockInFlightRecord(RecordKey("t", 0, 0L), 1000L, 31000L))
    batch2.addRecord(createMockInFlightRecord(RecordKey("t", 0, 1L), 1000L, 31000L))

    batch1.acknowledgeAllAsAccept() // Complete batch1

    val incomplete = manager.getIncompleteBatches
    assert(incomplete.size === 1)
    assert(incomplete.head.batchId === 2L)
  }

  test("ShareInFlightManager - releaseAll") {
    val manager = new ShareInFlightManager()

    val batch1 = manager.createBatch(1L)
    val batch2 = manager.createBatch(2L)

    (0 until 3).foreach { i =>
      batch1.addRecord(createMockInFlightRecord(RecordKey("t", 0, i.toLong), 1000L, 31000L))
    }
    (0 until 2).foreach { i =>
      batch2.addRecord(createMockInFlightRecord(RecordKey("t", 0, (i + 10).toLong), 1000L, 31000L))
    }

    val released = manager.releaseAll()
    assert(released === 5)
    assert(manager.totalPending === 0)
  }

  test("ShareInFlightManager - totalPending") {
    val manager = new ShareInFlightManager()

    val batch1 = manager.createBatch(1L)
    val batch2 = manager.createBatch(2L)

    (0 until 3).foreach { i =>
      batch1.addRecord(createMockInFlightRecord(RecordKey("t", 0, i.toLong), 1000L, 31000L))
    }
    (0 until 2).foreach { i =>
      batch2.addRecord(createMockInFlightRecord(RecordKey("t", 0, (i + 10).toLong), 1000L, 31000L))
    }

    assert(manager.totalPending === 5)

    batch1.acknowledgeAllAsAccept()
    assert(manager.totalPending === 2)
  }

  // Helper method to create mock in-flight records
  private def createMockInFlightRecord(
      key: RecordKey,
      acquiredAt: Long,
      lockExpiresAt: Long): ShareInFlightRecord = {
    ShareInFlightRecord(
      recordKey = key,
      record = null, // We don't need actual ConsumerRecord for these tests
      acquiredAt = acquiredAt,
      lockExpiresAt = lockExpiresAt,
      deliveryCount = 1,
      acknowledgment = None
    )
  }
}


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

import java.io.File
import java.nio.file.Files

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.kafka010.share.exactlyonce._

/**
 * Integration tests for Kafka Share Source fault tolerance.
 *
 * These tests verify:
 * 1. At-least-once semantics via acquisition lock expiry
 * 2. Recovery from driver failure
 * 3. Checkpoint-based deduplication for exactly-once
 * 4. Two-phase commit coordinator
 */
class KafkaShareSourceFaultToleranceSuite extends SparkFunSuite {

  private var tempDir: File = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Files.createTempDirectory("kafka-share-test").toFile
  }

  override def afterEach(): Unit = {
    if (tempDir != null) {
      deleteRecursively(tempDir)
    }
    super.afterEach()
  }

  // ==================== Offset Tracking Tests ====================

  test("KafkaShareSourceOffset - non-sequential offset tracking") {
    // Simulate share group behavior where offsets are not sequential
    val tp = new TopicPartition("test-topic", 0)

    // Share consumer might receive offsets 100, 105, 110 (not sequential)
    val range = AcquiredRecordRange(Set(100L, 105L, 110L), 1000L, 31000L, 1)
    val offset = KafkaShareSourceOffset("group", 1L, Map(tp -> range))

    assert(offset.totalRecords === 3)
    assert(offset.acquiredRecords(tp).contains(100L))
    assert(offset.acquiredRecords(tp).contains(105L))
    assert(offset.acquiredRecords(tp).contains(110L))
    // Offset 101 was given to another consumer
    assert(!offset.acquiredRecords(tp).contains(101L))
  }

  test("KafkaShareSourceOffset - multiple partitions with non-sequential offsets") {
    val tp0 = new TopicPartition("test-topic", 0)
    val tp1 = new TopicPartition("test-topic", 1)

    // Consumer A got offsets from tp0: 100, 102, 104
    // Consumer B (different share member) got tp0: 101, 103, 105
    // This consumer got tp1: 200, 201
    val range0 = AcquiredRecordRange(Set(100L, 102L, 104L), 1000L, 31000L, 1)
    val range1 = AcquiredRecordRange(Set(200L, 201L), 1000L, 31000L, 1)

    val offset = KafkaShareSourceOffset("group", 1L, Map(tp0 -> range0, tp1 -> range1))

    assert(offset.totalRecords === 5)
    assert(offset.partitionsWithRecords.size === 2)
  }

  // ==================== Acknowledgment State Tests ====================

  test("ShareInFlightBatch - track acknowledgments for non-sequential offsets") {
    val batch = new ShareInFlightBatch(1L)
    val tp = new TopicPartition("topic", 0)

    // Add non-sequential offsets
    Seq(100L, 105L, 110L, 115L).foreach { offset =>
      val key = RecordKey(tp, offset)
      batch.addRecord(ShareInFlightRecord(
        recordKey = key,
        record = null,
        acquiredAt = 1000L,
        lockExpiresAt = 31000L,
        deliveryCount = 1,
        acknowledgment = None
      ))
    }

    // Acknowledge some (simulating out-of-order processing)
    batch.acknowledge(RecordKey(tp, 110L), AcknowledgmentType.ACCEPT)
    batch.acknowledge(RecordKey(tp, 100L), AcknowledgmentType.ACCEPT)
    batch.acknowledge(RecordKey(tp, 105L), AcknowledgmentType.RELEASE) // Needs retry

    val acks = batch.getAcknowledgmentsForCommit
    assert(acks(tp).size === 3)
    assert(acks(tp)(100L) === AcknowledgmentType.ACCEPT)
    assert(acks(tp)(105L) === AcknowledgmentType.RELEASE)
    assert(acks(tp)(110L) === AcknowledgmentType.ACCEPT)

    // 115 is still pending
    assert(batch.pending === 1)
  }

  // ==================== Recovery Tests ====================

  test("ShareRecoveryManager - recovery from checkpoint") {
    val checkpointPath = new File(tempDir, "checkpoint").getAbsolutePath

    // Create initial state
    val manager1 = new ShareRecoveryManager("test-group", checkpointPath)
    val writer = manager1.getCheckpointWriter

    val tp = new TopicPartition("topic", 0)
    val range = AcquiredRecordRange(Set(100L, 105L), 1000L, 31000L, 1)
    val offset = KafkaShareSourceOffset("test-group", 5L, Map(tp -> range))

    writer.write(offset)

    // Simulate restart - create new manager
    val manager2 = new ShareRecoveryManager("test-group", checkpointPath)
    val recoveryState = manager2.recover()

    assert(!recoveryState.isCleanStart)
    assert(recoveryState.startBatchId === 6L) // Resume from next batch
    assert(recoveryState.lastCommittedOffset.isDefined)
    assert(recoveryState.lastCommittedOffset.get.batchId === 5L)

    // Pending records should be flagged for release (lock expiry)
    assert(recoveryState.pendingRecords.size === 2)
    assert(recoveryState.pendingRecords.contains(RecordKey(tp, 100L)))
    assert(recoveryState.pendingRecords.contains(RecordKey(tp, 105L)))
  }

  test("ShareRecoveryManager - clean start with no checkpoint") {
    val checkpointPath = new File(tempDir, "new-checkpoint").getAbsolutePath
    val manager = new ShareRecoveryManager("test-group", checkpointPath)

    val recoveryState = manager.recover()

    assert(recoveryState.isCleanStart)
    assert(recoveryState.startBatchId === 0L)
    assert(recoveryState.lastCommittedOffset.isEmpty)
    assert(recoveryState.pendingRecords.isEmpty)
  }

  // ==================== Checkpoint Deduplication Tests ====================

  test("CheckpointDedupManager - deduplicate redelivered records") {
    val checkpointPath = new File(tempDir, "dedup-checkpoint").getAbsolutePath
    val manager = new CheckpointDedupManager(checkpointPath)
    manager.initialize()

    val key1 = RecordKey("topic", 0, 100L)
    val key2 = RecordKey("topic", 0, 101L)

    // First delivery
    manager.startBatch(1L)
    assert(!manager.isProcessed(key1))
    assert(!manager.isProcessed(key2))

    manager.markPending(key1)
    manager.markPending(key2)
    manager.markProcessed(key1)
    manager.commitBatch(1L)

    // key1 is now processed, key2 was not completed (simulating failure)

    // Redelivery - key1 should be skipped
    manager.startBatch(2L)
    assert(manager.isProcessed(key1)) // Should be deduplicated
    assert(!manager.isProcessed(key2)) // Was not committed, needs reprocessing
  }

  test("CheckpointDedupManager - rollback releases pending records") {
    val checkpointPath = new File(tempDir, "dedup-checkpoint2").getAbsolutePath
    val manager = new CheckpointDedupManager(checkpointPath)
    manager.initialize()

    val key1 = RecordKey("topic", 0, 100L)
    val key2 = RecordKey("topic", 0, 101L)

    manager.startBatch(1L)
    manager.markPending(key1)
    manager.markPending(key2)

    // Simulate failure - rollback
    val rolledBack = manager.rollbackBatch(1L)

    assert(rolledBack.size === 2)
    assert(rolledBack.contains(key1))
    assert(rolledBack.contains(key2))

    // These should NOT be marked as processed
    assert(!manager.isProcessed(key1))
    assert(!manager.isProcessed(key2))
  }

  // ==================== Two-Phase Commit Tests ====================

  test("TwoPhaseCommitCoordinator - successful commit flow") {
    val checkpointPath = new File(tempDir, "2pc-checkpoint").getAbsolutePath
    val coordinator = new TwoPhaseCommitCoordinator("test-group", checkpointPath)

    val handle = coordinator.beginTransaction(1L)
    val key = RecordKey("topic", 0, 100L)

    handle.recordAck(key, AcknowledgmentType.ACCEPT)

    // Phase 1: Prepare
    assert(handle.prepare())
    assert(coordinator.getTransactionPhase(1L).contains(TwoPhaseCommitCoordinator.Phase.PREPARED))

    // Note: Cannot test Phase 2 without a real Kafka consumer
    // In a real test, we would:
    // assert(handle.commit(consumer))
    // assert(coordinator.getTransactionPhase(1L).isEmpty) // Completed and cleaned up

    coordinator.close()
  }

  test("TwoPhaseCommitCoordinator - transaction phases") {
    val checkpointPath = new File(tempDir, "2pc-checkpoint2").getAbsolutePath
    val coordinator = new TwoPhaseCommitCoordinator("test-group", checkpointPath)

    // STARTED phase
    val handle = coordinator.beginTransaction(1L)
    assert(coordinator.getTransactionPhase(1L).contains(TwoPhaseCommitCoordinator.Phase.STARTED))

    // Cannot prepare without being in STARTED state after already preparing
    handle.recordAck(RecordKey("topic", 0, 100L), AcknowledgmentType.ACCEPT)
    assert(handle.prepare())
    assert(coordinator.getTransactionPhase(1L).contains(TwoPhaseCommitCoordinator.Phase.PREPARED))

    // Cannot prepare again
    assert(!coordinator.prepareTransaction(1L))

    coordinator.close()
  }

  // ==================== Lock Expiry Tests ====================

  test("AcquisitionLock - detect expired locks") {
    val tp = new TopicPartition("topic", 0)

    // Lock acquired at time 1000, expires at 31000 (30 second timeout)
    val range = AcquiredRecordRange(Set(100L, 105L), 1000L, 31000L, 1)

    assert(!range.isLockExpired(1000L))
    assert(!range.isLockExpired(15000L))
    assert(!range.isLockExpired(30999L))
    assert(range.isLockExpired(31000L))
    assert(range.isLockExpired(32000L))
  }

  test("ShareInFlightRecord - lock expiry detection") {
    val key = RecordKey("topic", 0, 100L)
    val record = ShareInFlightRecord(
      recordKey = key,
      record = null,
      acquiredAt = 1000L,
      lockExpiresAt = 31000L,
      deliveryCount = 1,
      acknowledgment = None
    )

    assert(!record.isLockExpired(1000L))
    assert(!record.isLockExpired(30000L))
    assert(record.isLockExpired(31000L))
    assert(record.isLockExpired(35000L))

    assert(record.remainingLockTimeMs(1000L) === 30000L)
    assert(record.remainingLockTimeMs(30000L) === 1000L)
    assert(record.remainingLockTimeMs(31000L) === 0L)
    assert(record.remainingLockTimeMs(32000L) === 0L)
  }

  // ==================== Helper Methods ====================

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}


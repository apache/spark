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
 * Unit tests for ShareStateBatch and related classes.
 */
class ShareStateBatchSuite extends SparkFunSuite {

  test("ShareStateBatch - basic creation and properties") {
    val batch = ShareStateBatch(100L, 150L, DeliveryState.ACQUIRED, 1)

    assert(batch.firstOffset === 100L)
    assert(batch.lastOffset === 150L)
    assert(batch.deliveryState === DeliveryState.ACQUIRED)
    assert(batch.deliveryCount === 1)
    assert(batch.recordCount === 51L)
  }

  test("ShareStateBatch - contains offset check") {
    val batch = ShareStateBatch(100L, 150L, DeliveryState.ACQUIRED, 1)

    assert(batch.contains(100L))
    assert(batch.contains(125L))
    assert(batch.contains(150L))
    assert(!batch.contains(99L))
    assert(!batch.contains(151L))
  }

  test("ShareStateBatch - single offset creation") {
    val batch = ShareStateBatch.single(100L, DeliveryState.ACKNOWLEDGED, 2)

    assert(batch.firstOffset === 100L)
    assert(batch.lastOffset === 100L)
    assert(batch.recordCount === 1L)
    assert(batch.deliveryCount === 2)
  }

  test("ShareStateBatch - merge adjacent batches with same state") {
    val batch1 = ShareStateBatch(100L, 120L, DeliveryState.ACKNOWLEDGED, 1)
    val batch2 = ShareStateBatch(121L, 150L, DeliveryState.ACKNOWLEDGED, 1)

    assert(batch1.canMergeWith(batch2))
    val merged = batch1.mergeWith(batch2)

    assert(merged.firstOffset === 100L)
    assert(merged.lastOffset === 150L)
    assert(merged.recordCount === 51L)
  }

  test("ShareStateBatch - cannot merge non-adjacent batches") {
    val batch1 = ShareStateBatch(100L, 120L, DeliveryState.ACKNOWLEDGED, 1)
    val batch2 = ShareStateBatch(125L, 150L, DeliveryState.ACKNOWLEDGED, 1)

    assert(!batch1.canMergeWith(batch2))
  }

  test("ShareStateBatch - cannot merge batches with different states") {
    val batch1 = ShareStateBatch(100L, 120L, DeliveryState.ACKNOWLEDGED, 1)
    val batch2 = ShareStateBatch(121L, 150L, DeliveryState.ACQUIRED, 1)

    assert(!batch1.canMergeWith(batch2))
  }

  test("ShareStateBatch - split at offset") {
    val batch = ShareStateBatch(100L, 150L, DeliveryState.ACQUIRED, 1)
    val (left, right) = batch.splitAt(125L)

    assert(left.firstOffset === 100L)
    assert(left.lastOffset === 125L)
    assert(right.firstOffset === 126L)
    assert(right.lastOffset === 150L)
  }

  test("ShareStateBatch - withState creates copy with new state") {
    val original = ShareStateBatch(100L, 150L, DeliveryState.ACQUIRED, 1)
    val acknowledged = original.withState(DeliveryState.ACKNOWLEDGED)

    assert(acknowledged.deliveryState === DeliveryState.ACKNOWLEDGED)
    assert(original.deliveryState === DeliveryState.ACQUIRED) // Original unchanged
  }

  test("ShareStateBatch - withIncrementedDeliveryCount") {
    val original = ShareStateBatch(100L, 150L, DeliveryState.AVAILABLE, 1)
    val incremented = original.withIncrementedDeliveryCount()

    assert(incremented.deliveryCount === 2)
    assert(original.deliveryCount === 1) // Original unchanged
  }

  test("ShareStateBatch - validation rejects invalid ranges") {
    intercept[IllegalArgumentException] {
      ShareStateBatch(150L, 100L, DeliveryState.ACQUIRED, 1) // lastOffset < firstOffset
    }

    intercept[IllegalArgumentException] {
      ShareStateBatch(100L, 150L, DeliveryState.ACQUIRED, -1) // Negative delivery count
    }
  }

  test("DeliveryState - fromByte conversion") {
    assert(DeliveryState.fromByte(0) === DeliveryState.AVAILABLE)
    assert(DeliveryState.fromByte(1) === DeliveryState.ACQUIRED)
    assert(DeliveryState.fromByte(2) === DeliveryState.ACKNOWLEDGED)
    assert(DeliveryState.fromByte(4) === DeliveryState.ARCHIVED)

    intercept[IllegalArgumentException] {
      DeliveryState.fromByte(99)
    }
  }

  test("SharePartitionState - basic creation") {
    val tp = new TopicPartition("test-topic", 0)
    val batches = Seq(
      ShareStateBatch(100L, 120L, DeliveryState.ACKNOWLEDGED, 1),
      ShareStateBatch(121L, 150L, DeliveryState.ACQUIRED, 1)
    )

    val state = SharePartitionState(tp, 1, 5, 0, 100L, batches)

    assert(state.topicPartition === tp)
    assert(state.snapshotEpoch === 1)
    assert(state.stateEpoch === 5)
    assert(state.startOffset === 100L)
    assert(state.stateBatches.size === 2)
  }

  test("SharePartitionState - getState for offset") {
    val tp = new TopicPartition("test-topic", 0)
    val batches = Seq(
      ShareStateBatch(100L, 120L, DeliveryState.ACKNOWLEDGED, 1),
      ShareStateBatch(121L, 150L, DeliveryState.ACQUIRED, 1)
    )

    val state = SharePartitionState(tp, 1, 5, 0, 100L, batches)

    assert(state.getState(99L).isEmpty) // Below startOffset
    assert(state.getState(110L).isDefined)
    assert(state.getState(110L).get.deliveryState === DeliveryState.ACKNOWLEDGED)
    assert(state.getState(130L).isDefined)
    assert(state.getState(130L).get.deliveryState === DeliveryState.ACQUIRED)
  }

  test("SharePartitionState - getAcquiredOffsets") {
    val tp = new TopicPartition("test-topic", 0)
    val batches = Seq(
      ShareStateBatch(100L, 110L, DeliveryState.ACKNOWLEDGED, 1),
      ShareStateBatch(111L, 120L, DeliveryState.ACQUIRED, 1)
    )

    val state = SharePartitionState(tp, 1, 5, 0, 100L, batches)
    val acquired = state.getAcquiredOffsets

    assert(acquired.size === 10) // 111 to 120 inclusive
    assert(acquired.contains(111L))
    assert(acquired.contains(120L))
    assert(!acquired.contains(100L))
  }

  test("SharePartitionState - highestOffset") {
    val tp = new TopicPartition("test-topic", 0)
    val batches = Seq(
      ShareStateBatch(100L, 120L, DeliveryState.ACKNOWLEDGED, 1),
      ShareStateBatch(121L, 150L, DeliveryState.ACQUIRED, 1)
    )

    val state = SharePartitionState(tp, 1, 5, 0, 100L, batches)

    assert(state.highestOffset === 150L)
  }

  test("SharePartitionState - empty batches") {
    val tp = new TopicPartition("test-topic", 0)
    val state = SharePartitionState(tp, 1, 5, 0, 100L, Seq.empty)

    assert(state.highestOffset === 99L) // startOffset - 1
    assert(state.getAcquiredOffsets.isEmpty)
  }
}


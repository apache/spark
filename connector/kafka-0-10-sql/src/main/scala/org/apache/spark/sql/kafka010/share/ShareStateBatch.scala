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

/**
 * Represents the delivery state of a record in a share group.
 * Mirrors Kafka's RecordState enum.
 */
object DeliveryState extends Enumeration {
  type DeliveryState = Value
  val AVAILABLE = Value(0, "AVAILABLE")      // Record is available for acquisition
  val ACQUIRED = Value(1, "ACQUIRED")        // Record is acquired by a consumer
  val ACKNOWLEDGED = Value(2, "ACKNOWLEDGED") // Record has been acknowledged (processed)
  val ARCHIVED = Value(4, "ARCHIVED")        // Record is archived (dead-lettered)

  def fromByte(b: Byte): DeliveryState = b match {
    case 0 => AVAILABLE
    case 1 => ACQUIRED
    case 2 => ACKNOWLEDGED
    case 4 => ARCHIVED
    case _ => throw new IllegalArgumentException(s"Unknown delivery state: $b")
  }
}

/**
 * Represents a contiguous range of offsets with their delivery state and count.
 * This mirrors Kafka's PersisterStateBatch which is the fundamental unit for
 * tracking state in share groups.
 *
 * Unlike traditional consumer groups that track a single offset per partition,
 * share groups track ranges of offsets with different states because:
 * 1. Multiple consumers can acquire records from the same partition concurrently
 * 2. Records can be acknowledged out of order
 * 3. Records can be released (redelivered) without affecting other records
 *
 * @param firstOffset Start of the offset range (inclusive)
 * @param lastOffset End of the offset range (inclusive)
 * @param deliveryState Current state of records in this range
 * @param deliveryCount Number of times records in this range have been delivered
 */
case class ShareStateBatch(
    firstOffset: Long,
    lastOffset: Long,
    deliveryState: DeliveryState.DeliveryState,
    deliveryCount: Short) {

  require(firstOffset <= lastOffset,
    s"firstOffset ($firstOffset) must be <= lastOffset ($lastOffset)")
  require(deliveryCount >= 0, s"deliveryCount must be non-negative: $deliveryCount")

  /** Returns the number of records in this batch */
  def recordCount: Long = lastOffset - firstOffset + 1

  /** Check if a specific offset is within this batch */
  def contains(offset: Long): Boolean = offset >= firstOffset && offset <= lastOffset

  /** Check if this batch can be merged with another batch */
  def canMergeWith(other: ShareStateBatch): Boolean = {
    // Batches can be merged if they are adjacent and have the same state/count
    (this.lastOffset + 1 == other.firstOffset || other.lastOffset + 1 == this.firstOffset) &&
      this.deliveryState == other.deliveryState &&
      this.deliveryCount == other.deliveryCount
  }

  /** Merge with another adjacent batch with the same state */
  def mergeWith(other: ShareStateBatch): ShareStateBatch = {
    require(canMergeWith(other), s"Cannot merge non-adjacent batches: $this and $other")
    ShareStateBatch(
      math.min(this.firstOffset, other.firstOffset),
      math.max(this.lastOffset, other.lastOffset),
      this.deliveryState,
      this.deliveryCount
    )
  }

  /**
   * Split this batch at the given offset, returning two batches.
   * The offset will be the lastOffset of the first batch.
   */
  def splitAt(offset: Long): (ShareStateBatch, ShareStateBatch) = {
    require(contains(offset) && offset < lastOffset,
      s"Cannot split at offset $offset - must be within range [$firstOffset, $lastOffset)")
    (
      ShareStateBatch(firstOffset, offset, deliveryState, deliveryCount),
      ShareStateBatch(offset + 1, lastOffset, deliveryState, deliveryCount)
    )
  }

  /** Create a copy with updated delivery state */
  def withState(newState: DeliveryState.DeliveryState): ShareStateBatch = {
    copy(deliveryState = newState)
  }

  /** Create a copy with incremented delivery count */
  def withIncrementedDeliveryCount(): ShareStateBatch = {
    copy(deliveryCount = (deliveryCount + 1).toShort)
  }

  override def toString: String = {
    s"ShareStateBatch[$firstOffset-$lastOffset: $deliveryState, count=$deliveryCount]"
  }
}

object ShareStateBatch {
  /**
   * Create a batch for a single offset
   */
  def single(offset: Long, state: DeliveryState.DeliveryState, count: Short = 1): ShareStateBatch = {
    ShareStateBatch(offset, offset, state, count)
  }

  /**
   * Create an ACQUIRED batch for a range of offsets
   */
  def acquired(firstOffset: Long, lastOffset: Long, deliveryCount: Short = 1): ShareStateBatch = {
    ShareStateBatch(firstOffset, lastOffset, DeliveryState.ACQUIRED, deliveryCount)
  }

  /**
   * Create an ACKNOWLEDGED batch for a range of offsets
   */
  def acknowledged(firstOffset: Long, lastOffset: Long, deliveryCount: Short = 1): ShareStateBatch = {
    ShareStateBatch(firstOffset, lastOffset, DeliveryState.ACKNOWLEDGED, deliveryCount)
  }
}

/**
 * Container for share group state for a specific topic partition.
 * This mirrors Kafka's ShareGroupOffset which is persisted to __share_group_state topic.
 *
 * @param snapshotEpoch Epoch for snapshot versioning
 * @param stateEpoch Epoch for state changes (incremented on each state modification)
 * @param leaderEpoch Leader epoch for fencing stale coordinators
 * @param startOffset The lowest offset that is still tracked (offset before this are complete)
 * @param stateBatches List of state batches tracking non-complete offsets
 */
case class SharePartitionState(
    topicPartition: TopicPartition,
    snapshotEpoch: Int,
    stateEpoch: Int,
    leaderEpoch: Int,
    startOffset: Long,
    stateBatches: Seq[ShareStateBatch]) {

  /**
   * Get the state for a specific offset.
   * Returns None if the offset is below startOffset (already complete).
   */
  def getState(offset: Long): Option[ShareStateBatch] = {
    if (offset < startOffset) {
      None // Already completed/archived
    } else {
      stateBatches.find(_.contains(offset))
    }
  }

  /**
   * Get all ACQUIRED offsets in this partition state
   */
  def getAcquiredOffsets: Seq[Long] = {
    stateBatches
      .filter(_.deliveryState == DeliveryState.ACQUIRED)
      .flatMap(batch => batch.firstOffset to batch.lastOffset)
  }

  /**
   * Get the highest tracked offset
   */
  def highestOffset: Long = {
    if (stateBatches.isEmpty) startOffset - 1
    else stateBatches.map(_.lastOffset).max
  }
}


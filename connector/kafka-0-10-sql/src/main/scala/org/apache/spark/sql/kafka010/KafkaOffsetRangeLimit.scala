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

package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.kafka010.KafkaSourceProvider.StrategyOnNoMatchStartingOffset

/**
 * Objects that represent desired offset range limits for starting,
 * ending, and specific offsets.
 */
private[kafka010] sealed trait KafkaOffsetRangeLimit

/**
 * Represents the desire to bind to the earliest offsets in Kafka
 */
private[kafka010] case object EarliestOffsetRangeLimit extends KafkaOffsetRangeLimit

/**
 * Represents the desire to bind to the latest offsets in Kafka
 */
private[kafka010] case object LatestOffsetRangeLimit extends KafkaOffsetRangeLimit

/**
 * Represents the desire to bind to specific offsets. A offset == -1 binds to the
 * latest offset, and offset == -2 binds to the earliest offset.
 *
 * `topicOffsets` holds topic-level bindings, written in the JSON as "earliest" or "latest" in
 * place of a topic's per-partition object. They are expanded against the partitions discovered
 * for the topic when the offsets are resolved, so they keep working across repartitioning.
 */
private[kafka010] case class SpecificOffsetRangeLimit(
    partitionOffsets: Map[TopicPartition, Long],
    topicOffsets: Map[String, Long] = Map.empty) extends KafkaOffsetRangeLimit {

  /**
   * Expands the topic-level bindings against `assignedPartitions` and returns the resulting
   * offset per topic-partition. `assignedPartitions` is by-name so that a fully enumerated
   * limit never pays for discovering the partitions.
   */
  def resolve(assignedPartitions: => Set[TopicPartition]): Map[TopicPartition, Long] = {
    if (topicOffsets.isEmpty) {
      partitionOffsets
    } else {
      val partitions = assignedPartitions
      val expanded = partitions.collect {
        case tp if topicOffsets.contains(tp.topic) => tp -> topicOffsets(tp.topic)
      }.toMap
      val unmatchedTopics = topicOffsets.keySet.diff(expanded.keySet.map(_.topic))
      if (unmatchedTopics.nonEmpty) {
        throw KafkaExceptions.topicOffsetDoesNotMatchAssigned(
          unmatchedTopics, partitions.map(_.topic))
      }
      expanded ++ partitionOffsets
    }
  }
}

/**
 * Represents the desire to bind to earliest offset which timestamp for the offset is equal or
 * greater than specific timestamp.
 */
private[kafka010] case class SpecificTimestampRangeLimit(
    topicTimestamps: Map[TopicPartition, Long],
    strategyOnNoMatchingStartingOffset: StrategyOnNoMatchStartingOffset.Value)
  extends KafkaOffsetRangeLimit

/**
 * Represents the desire to bind to earliest offset which timestamp for the offset is equal or
 * greater than specific timestamp. This applies the timestamp to the all topics/partitions.
 */
private[kafka010] case class GlobalTimestampRangeLimit(
    timestamp: Long,
    strategyOnNoMatchingStartingOffset: StrategyOnNoMatchStartingOffset.Value)
  extends KafkaOffsetRangeLimit

private[kafka010] object KafkaOffsetRangeLimit {
  /**
   * Used to denote offset range limits that are resolved via Kafka
   */
  val LATEST = -1L // indicates resolution to the latest offset
  val EARLIEST = -2L // indicates resolution to the earliest offset
}

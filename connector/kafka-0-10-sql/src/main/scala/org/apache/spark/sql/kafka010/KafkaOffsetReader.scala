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

import java.{util => ju}

import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kafka010.KafkaSourceProvider.StrategyOnNoMatchStartingOffset

/**
 * Base trait to fetch offsets from Kafka. The implementations are
 * [[KafkaOffsetReaderConsumer]] and [[KafkaOffsetReaderAdmin]].
 */
private[kafka010] trait KafkaOffsetReader {

  // These are needed here because of KafkaSourceProviderSuite
  private[kafka010] val maxOffsetFetchAttempts: Int
  private[kafka010] val offsetFetchAttemptIntervalMs: Long

  // This is needed here because of KafkaContinuousStream
  val driverKafkaParams: ju.Map[String, Object]

  /**
   * Closes the connection to Kafka, and cleans up state.
   */
  def close(): Unit

  /**
   * Fetch the partition offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]] and [[KafkaOffsetRangeLimit]].
   */
  def fetchPartitionOffsets(
      offsetRangeLimit: KafkaOffsetRangeLimit,
      isStartingOffsets: Boolean): Map[TopicPartition, Long]

  /**
   * Resolves the specific offsets based on Kafka seek positions.
   * This method resolves offset value -1 to the latest and -2 to the
   * earliest Kafka seek position.
   *
   * @param partitionOffsets the specific offsets to resolve
   * @param reportDataLoss callback to either report or log data loss depending on setting
   */
  def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long],
      reportDataLoss: String => Unit): KafkaSourceOffset

  /**
   * Resolves the specific offsets based on timestamp per topic-partition.
   * The returned offset for each partition is the earliest offset whose timestamp is greater
   * than or equal to the given timestamp in the corresponding partition.
   *
   * If the matched offset doesn't exist, the behavior depends on the destination and the option:
   *
   * - isStartingOffsets = false => implementation should provide the offset same as 'latest'
   * - isStartingOffsets = true  => implementation should follow the strategy on non-matching
   *                                starting offset, passed as `strategyOnNoMatchStartingOffset`
   *
   * @param partitionTimestamps the timestamp per topic-partition.
   */
  def fetchSpecificTimestampBasedOffsets(
      partitionTimestamps: Map[TopicPartition, Long],
      isStartingOffsets: Boolean,
      strategyOnNoMatchStartingOffset: StrategyOnNoMatchStartingOffset.Value)
    : KafkaSourceOffset

  /**
   * Resolves the specific offsets based on timestamp per all topic-partitions being subscribed.
   * The returned offset for each partition is the earliest offset whose timestamp is greater
   * than or equal to the given timestamp in the corresponding partition.
   *
   * If the matched offset doesn't exist, the behavior depends on the destination and the option:
   *
   * - isStartingOffsets = false => implementation should provide the offset same as 'latest'
   * - isStartingOffsets = true  => implementation should follow the strategy on non-matching
   *                                starting offset, passed as `strategyOnNoMatchStartingOffset`
   *
   * @param timestamp the timestamp.
   */
  def fetchGlobalTimestampBasedOffsets(
      timestamp: Long,
      isStartingOffsets: Boolean,
      strategyOnNoMatchingStartingOffset: StrategyOnNoMatchStartingOffset.Value)
    : KafkaSourceOffset

  /**
   * Fetch the earliest offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]].
   */
  def fetchEarliestOffsets(): Map[TopicPartition, Long]

  /**
   * Fetch the latest offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]].
   *
   * In order to avoid unknown issues, we use the given `knownOffsets` to audit the
   * latest offsets returned by Kafka. If we find some incorrect offsets (a latest offset is less
   * than an offset in `knownOffsets`), we will retry at most `maxOffsetFetchAttempts` times. When
   * a topic is recreated, the latest offsets may be less than offsets in `knownOffsets`. We cannot
   * distinguish this with issues like KAFKA-7703, so we just return whatever we get from Kafka
   * after retrying.
   */
  def fetchLatestOffsets(knownOffsets: Option[PartitionOffsetMap]): PartitionOffsetMap

  /**
   * Fetch the earliest offsets for specific topic partitions.
   * The return result may not contain some partitions if they are deleted.
   */
  def fetchEarliestOffsets(newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long]

  /**
   * Return the offset ranges for a Kafka batch query. If `minPartitions` is set, this method may
   * split partitions to respect it. Since offsets can be early and late binding which are evaluated
   * on the executors, in order to divvy up the partitions we need to perform some substitutions. We
   * don't want to send exact offsets to the executors, because data may age out before we can
   * consume the data. This method makes some approximate splitting, and replaces the special offset
   * values in the final output.
   */
  def getOffsetRangesFromUnresolvedOffsets(
      startingOffsets: KafkaOffsetRangeLimit,
      endingOffsets: KafkaOffsetRangeLimit): Seq[KafkaOffsetRange]

  /**
   * Return the offset ranges for a Kafka streaming batch. If `minPartitions` is set, this method
   * may split partitions to respect it. If any data lost issue is detected, `reportDataLoss` will
   * be called.
   */
  def getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets: PartitionOffsetMap,
      untilPartitionOffsets: PartitionOffsetMap,
      reportDataLoss: String => Unit): Seq[KafkaOffsetRange]
}

private[kafka010] object KafkaOffsetReader extends Logging {
  def build(
      consumerStrategy: ConsumerStrategy,
      driverKafkaParams: ju.Map[String, Object],
      readerOptions: CaseInsensitiveMap[String],
      driverGroupIdPrefix: String): KafkaOffsetReader = {
    if (SQLConf.get.useDeprecatedKafkaOffsetFetching) {
      logDebug("Creating old and deprecated Consumer based offset reader")
      new KafkaOffsetReaderConsumer(consumerStrategy, driverKafkaParams, readerOptions,
        driverGroupIdPrefix)
    } else {
      logDebug("Creating new Admin based offset reader")
      new KafkaOffsetReaderAdmin(consumerStrategy, driverKafkaParams, readerOptions,
        driverGroupIdPrefix)
    }
  }
}

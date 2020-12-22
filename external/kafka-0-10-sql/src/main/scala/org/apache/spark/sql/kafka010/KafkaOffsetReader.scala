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

/**
 * Base trait to fetch offsets from Kafka. The implementations are
 * [[KafkaOffsetReaderConsumer]] and [[KafkaOffsetReaderAdmin]].
 * Please see the documentation and API description there.
 */
private[kafka010] trait KafkaOffsetReader {

  // These are needed here because of KafkaSourceProviderSuite
  private[kafka010] val maxOffsetFetchAttempts: Int
  private[kafka010] val offsetFetchAttemptIntervalMs: Long

  // This is needed here because of KafkaContinuousStream
  val driverKafkaParams: ju.Map[String, Object]

  def close(): Unit
  def fetchPartitionOffsets(
      offsetRangeLimit: KafkaOffsetRangeLimit,
      isStartingOffsets: Boolean): Map[TopicPartition, Long]
  def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long],
      reportDataLoss: String => Unit): KafkaSourceOffset
  def fetchSpecificTimestampBasedOffsets(
      partitionTimestamps: Map[TopicPartition, Long],
      failsOnNoMatchingOffset: Boolean): KafkaSourceOffset
  def fetchEarliestOffsets(): Map[TopicPartition, Long]
  def fetchLatestOffsets(knownOffsets: Option[PartitionOffsetMap]): PartitionOffsetMap
  def fetchEarliestOffsets(newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long]
  def getOffsetRangesFromUnresolvedOffsets(
      startingOffsets: KafkaOffsetRangeLimit,
      endingOffsets: KafkaOffsetRangeLimit): Seq[KafkaOffsetRange]
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

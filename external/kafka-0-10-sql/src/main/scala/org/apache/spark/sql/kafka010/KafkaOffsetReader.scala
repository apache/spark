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
 * This class is just a wrapper for the subsequent implementations
 * [[KafkaOffsetReaderConsumer]] and [[KafkaOffsetReaderAdmin]].
 * Please see the documentation and API description there.
 */
private[kafka010] class KafkaOffsetReader(
    consumerStrategy: ConsumerStrategy,
    override val driverKafkaParams: ju.Map[String, Object],
    readerOptions: CaseInsensitiveMap[String],
    driverGroupIdPrefix: String)
  extends KafkaOffsetReaderBase(
    consumerStrategy,
    driverKafkaParams,
    readerOptions,
    driverGroupIdPrefix) with Logging {

  protected override val impl = if (SQLConf.get.useDeprecatedKafkaOffsetFetching) {
    logDebug("Creating old and deprecated Consumer based offset reader")
    new KafkaOffsetReaderConsumer(consumerStrategy, driverKafkaParams, readerOptions,
      driverGroupIdPrefix)
  } else {
    logDebug("Creating new Admin based offset reader")
    new KafkaOffsetReaderAdmin(consumerStrategy, driverKafkaParams, readerOptions,
      driverGroupIdPrefix)
  }
}

private[kafka010] abstract class KafkaOffsetReaderBase(
    consumerStrategy: ConsumerStrategy,
    val driverKafkaParams: ju.Map[String, Object],
    readerOptions: CaseInsensitiveMap[String],
    driverGroupIdPrefix: String) extends Logging {

  // This is a super ugly construct but the other option would be to duplicate the API in
  // KafkaOffsetReader
  protected val impl: KafkaOffsetReaderBase = null

  // These are needed here because of KafkaSourceProviderSuite

  private[kafka010] val maxOffsetFetchAttempts =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_NUM_RETRY, "3").toInt

  private[kafka010] val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_RETRY_INTERVAL_MS, "1000").toLong

  override def toString(): String = consumerStrategy.toString

  def close(): Unit = {
    impl.close()
  }

  def fetchTopicPartitions(): Set[TopicPartition] = {
    impl.fetchTopicPartitions()
  }

  def fetchPartitionOffsets(
      offsetRangeLimit: KafkaOffsetRangeLimit,
      isStartingOffsets: Boolean): Map[TopicPartition, Long] = {
    impl.fetchPartitionOffsets(offsetRangeLimit, isStartingOffsets)
  }

  def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long],
      reportDataLoss: String => Unit): KafkaSourceOffset = {
    impl.fetchSpecificOffsets(partitionOffsets, reportDataLoss)
  }

  def fetchSpecificTimestampBasedOffsets(
      partitionTimestamps: Map[TopicPartition, Long],
      failsOnNoMatchingOffset: Boolean): KafkaSourceOffset = {
    impl.fetchSpecificTimestampBasedOffsets(partitionTimestamps, failsOnNoMatchingOffset)
  }

  def fetchEarliestOffsets(): Map[TopicPartition, Long] = {
    impl.fetchEarliestOffsets()
  }

  def fetchLatestOffsets(knownOffsets: Option[PartitionOffsetMap]): PartitionOffsetMap = {
    impl.fetchLatestOffsets(knownOffsets)
  }

  def fetchEarliestOffsets(newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    impl.fetchEarliestOffsets(newPartitions)
  }

  def getOffsetRangesFromUnresolvedOffsets(
      startingOffsets: KafkaOffsetRangeLimit,
      endingOffsets: KafkaOffsetRangeLimit): Seq[KafkaOffsetRange] = {
    impl.getOffsetRangesFromUnresolvedOffsets(startingOffsets, endingOffsets)
  }

  def getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets: PartitionOffsetMap,
      untilPartitionOffsets: PartitionOffsetMap,
      reportDataLoss: String => Unit): Seq[KafkaOffsetRange] = {
    impl.getOffsetRangesFromResolvedOffsets(fromPartitionOffsets, untilPartitionOffsets,
      reportDataLoss)
  }
}

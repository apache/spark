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

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.TOPIC_PARTITION_OFFSET
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kafka010.KafkaSourceProvider.StrategyOnNoMatchStartingOffset
import org.apache.spark.util.ArrayImplicits._

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
      reportDataLoss: (String, () => Throwable) => Unit): KafkaSourceOffset

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
      reportDataLoss: (String, () => Throwable) => Unit): Seq[KafkaOffsetRange]
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

private[kafka010] abstract class KafkaOffsetReaderBase extends KafkaOffsetReader with Logging {
  protected val rangeCalculator: KafkaOffsetRangeCalculator

  private def getSortedExecutorList: Array[String] = {
    def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
      if (a.host == b.host) {
        a.executorId > b.executorId
      } else {
        a.host > b.host
      }
    }

    val bm = SparkEnv.get.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  override def getOffsetRangesFromResolvedOffsets(
      fromPartitionOffsets: PartitionOffsetMap,
      untilPartitionOffsets: PartitionOffsetMap,
      reportDataLoss: (String, () => Throwable) => Unit): Seq[KafkaOffsetRange] = {
    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionInitialOffsets = fetchEarliestOffsets(newPartitions.toSeq)
    if (newPartitionInitialOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionInitialOffsets.keySet)
      reportDataLoss(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed",
        () => KafkaExceptions.initialOffsetNotFoundForPartitions(deletedPartitions))
    }
    logInfo(log"Partitions added: ${MDC(TOPIC_PARTITION_OFFSET, newPartitionInitialOffsets)}")
    newPartitionInitialOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      reportDataLoss(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed",
        () => KafkaExceptions.addedPartitionDoesNotStartFromZero(p, o))
    }

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      val (message, config) =
        if (driverKafkaParams.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
          (s"$deletedPartitions are gone.${KafkaSourceProvider.CUSTOM_GROUP_ID_ERROR_MESSAGE}",
            Some(ConsumerConfig.GROUP_ID_CONFIG))
        } else {
          (s"$deletedPartitions are gone. Some data may have been missed.", None)
        }

      reportDataLoss(
        message,
        () => KafkaExceptions.partitionsDeleted(deletedPartitions, config))
    }

    // Use the until partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = untilPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionInitialOffsets.contains(tp) || fromPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    val fromOffsets = fromPartitionOffsets ++ newPartitionInitialOffsets
    val untilOffsets = untilPartitionOffsets
    val ranges = topicPartitions.map { tp =>
      val fromOffset = fromOffsets(tp)
      val untilOffset = untilOffsets(tp)
      if (untilOffset < fromOffset) {
        reportDataLoss(
          s"Partition $tp's offset was changed from " +
            s"$fromOffset to $untilOffset. This could be either 1) a user error that the start " +
            "offset is set beyond available offset when starting query, or 2) the kafka " +
            "topic-partition is deleted and re-created.",
          () => KafkaExceptions.partitionOffsetChanged(tp, fromOffset, untilOffset))
      }
      KafkaOffsetRange(tp, fromOffset, untilOffset, preferredLoc = None)
    }
    rangeCalculator.getRanges(ranges, getSortedExecutorList.toImmutableArraySeq)
  }
}

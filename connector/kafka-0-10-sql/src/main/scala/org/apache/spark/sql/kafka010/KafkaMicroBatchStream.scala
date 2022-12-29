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
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Network.NETWORK_TIMEOUT
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming._
import org.apache.spark.sql.kafka010.KafkaSourceProvider._
import org.apache.spark.sql.kafka010.MockedSystemClock.currentMockSystemTime
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{Clock, ManualClock, SystemClock, UninterruptibleThread, Utils}

/**
 * A [[MicroBatchStream]] that reads data from Kafka.
 *
 * The [[KafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
 * a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
 * example if the last record in a Kafka topic "t", partition 2 is offset 5, then
 * KafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
 * with the semantics of `KafkaConsumer.position()`.
 *
 * Zero data lost is not guaranteed when topics are deleted. If zero data lost is critical, the user
 * must make sure all messages in a topic have been processed when deleting a topic.
 *
 * There is a known issue caused by KAFKA-1894: the query using Kafka maybe cannot be stopped.
 * To avoid this issue, you should make sure stopping the query before stopping the Kafka brokers
 * and not use wrong broker addresses.
 */
private[kafka010] class KafkaMicroBatchStream(
    private[kafka010] val kafkaOffsetReader: KafkaOffsetReader,
    executorKafkaParams: ju.Map[String, Object],
    options: CaseInsensitiveStringMap,
    metadataPath: String,
    startingOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean)
  extends SupportsTriggerAvailableNow with ReportsSourceMetrics with MicroBatchStream with Logging {

  private[kafka010] val pollTimeoutMs = options.getLong(
    KafkaSourceProvider.CONSUMER_POLL_TIMEOUT,
    SparkEnv.get.conf.get(NETWORK_TIMEOUT) * 1000L)

  private[kafka010] val maxOffsetsPerTrigger = Option(options.get(
    KafkaSourceProvider.MAX_OFFSET_PER_TRIGGER)).map(_.toLong)

  private[kafka010] val minOffsetPerTrigger = Option(options.get(
    KafkaSourceProvider.MIN_OFFSET_PER_TRIGGER)).map(_.toLong)

  private[kafka010] val maxTriggerDelayMs =
    Utils.timeStringAsMs(Option(options.get(
      KafkaSourceProvider.MAX_TRIGGER_DELAY)).getOrElse(DEFAULT_MAX_TRIGGER_DELAY))

  // this allows us to mock system clock for testing purposes
  private[kafka010] val clock: Clock = if (options.containsKey(MOCK_SYSTEM_TIME)) {
    new MockedSystemClock
  } else {
    new SystemClock
  }

  private var lastTriggerMillis = 0L

  private val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)

  private var latestPartitionOffsets: PartitionOffsetMap = _

  private var allDataForTriggerAvailableNow: PartitionOffsetMap = _

  /**
   * Lazily initialize `initialPartitionOffsets` to make sure that `KafkaConsumer.poll` is only
   * called in StreamExecutionThread. Otherwise, interrupting a thread while running
   * `KafkaConsumer.poll` may hang forever (KAFKA-1894).
   */
  override def initialOffset(): Offset = {
    KafkaSourceOffset(getOrCreateInitialPartitionOffsets())
  }

  override def getDefaultReadLimit: ReadLimit = {
    if (minOffsetPerTrigger.isDefined && maxOffsetsPerTrigger.isDefined) {
      ReadLimit.compositeLimit(Array(
        ReadLimit.minRows(minOffsetPerTrigger.get, maxTriggerDelayMs),
        ReadLimit.maxRows(maxOffsetsPerTrigger.get)))
    } else if (minOffsetPerTrigger.isDefined) {
      ReadLimit.minRows(minOffsetPerTrigger.get, maxTriggerDelayMs)
    } else {
      // TODO (SPARK-37973) Directly call super.getDefaultReadLimit when scala issue 12523 is fixed
      maxOffsetsPerTrigger.map(ReadLimit.maxRows).getOrElse(ReadLimit.allAvailable())
    }
  }

  override def reportLatestOffset(): Offset = {
    Option(KafkaSourceOffset(latestPartitionOffsets)).filterNot(_.partitionToOffsets.isEmpty).orNull
  }

  override def latestOffset(): Offset = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  override def latestOffset(start: Offset, readLimit: ReadLimit): Offset = {
    val startPartitionOffsets = start.asInstanceOf[KafkaSourceOffset].partitionToOffsets

    // Use the pre-fetched list of partition offsets when Trigger.AvailableNow is enabled.
    latestPartitionOffsets = if (allDataForTriggerAvailableNow != null) {
      allDataForTriggerAvailableNow
    } else {
      kafkaOffsetReader.fetchLatestOffsets(Some(startPartitionOffsets))
    }

    val limits: Seq[ReadLimit] = readLimit match {
      case rows: CompositeReadLimit => rows.getReadLimits
      case rows => Seq(rows)
    }

    val offsets = if (limits.exists(_.isInstanceOf[ReadAllAvailable])) {
      // ReadAllAvailable has the highest priority
      latestPartitionOffsets
    } else {
      val lowerLimit = limits.find(_.isInstanceOf[ReadMinRows]).map(_.asInstanceOf[ReadMinRows])
      val upperLimit = limits.find(_.isInstanceOf[ReadMaxRows]).map(_.asInstanceOf[ReadMaxRows])

      lowerLimit.flatMap { limit =>
        // checking if we need to skip batch based on minOffsetPerTrigger criteria
        val skipBatch = delayBatch(
          limit.minRows, latestPartitionOffsets, startPartitionOffsets, limit.maxTriggerDelayMs)
        if (skipBatch) {
          logDebug(
            s"Delaying batch as number of records available is less than minOffsetsPerTrigger")
          Some(startPartitionOffsets)
        } else {
          None
        }
      }.orElse {
        // checking if we need to adjust a range of offsets based on maxOffsetPerTrigger criteria
        upperLimit.map { limit =>
          rateLimit(limit.maxRows(), startPartitionOffsets, latestPartitionOffsets)
        }
      }.getOrElse(latestPartitionOffsets)
    }

    Option(KafkaSourceOffset(offsets)).filterNot(_.partitionToOffsets.isEmpty).orNull
  }

  /** Checks if we need to skip this trigger based on minOffsetsPerTrigger & maxTriggerDelay */
  private def delayBatch(
      minLimit: Long,
      latestOffsets: Map[TopicPartition, Long],
      currentOffsets: Map[TopicPartition, Long],
      maxTriggerDelayMs: Long): Boolean = {
    // Checking first if the maxbatchDelay time has passed
    if ((clock.getTimeMillis() - lastTriggerMillis) >= maxTriggerDelayMs) {
      logDebug("Maximum wait time is passed, triggering batch")
      lastTriggerMillis = clock.getTimeMillis()
      false
    } else {
      val newRecords = latestOffsets.flatMap {
        case (topic, offset) =>
          Some(topic -> (offset - currentOffsets.getOrElse(topic, 0L)))
      }.values.sum.toDouble
      if (newRecords < minLimit) true else {
        lastTriggerMillis = clock.getTimeMillis()
        false
      }
    }
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startPartitionOffsets = start.asInstanceOf[KafkaSourceOffset].partitionToOffsets
    val endPartitionOffsets = end.asInstanceOf[KafkaSourceOffset].partitionToOffsets

    if (allDataForTriggerAvailableNow != null) {
      verifyEndOffsetForTriggerAvailableNow(endPartitionOffsets)
    }

    val offsetRanges = kafkaOffsetReader.getOffsetRangesFromResolvedOffsets(
      startPartitionOffsets,
      endPartitionOffsets,
      reportDataLoss
    )

    // Generate factories based on the offset ranges
    offsetRanges.map { range =>
      KafkaBatchInputPartition(range, executorKafkaParams, pollTimeoutMs,
        failOnDataLoss, includeHeaders)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    KafkaBatchReaderFactory
  }

  override def deserializeOffset(json: String): Offset = {
    KafkaSourceOffset(JsonUtils.partitionOffsets(json))
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    kafkaOffsetReader.close()
  }

  override def toString(): String = s"KafkaV2[$kafkaOffsetReader]"

  override def metrics(latestConsumedOffset: Optional[Offset]): ju.Map[String, String] = {
    KafkaMicroBatchStream.metrics(latestConsumedOffset, latestPartitionOffsets)
  }

  /**
   * Read initial partition offsets from the checkpoint, or decide the offsets and write them to
   * the checkpoint.
   */
  private def getOrCreateInitialPartitionOffsets(): PartitionOffsetMap = {
    // Make sure that `KafkaConsumer.poll` is only called in StreamExecutionThread.
    // Otherwise, interrupting a thread while running `KafkaConsumer.poll` may hang forever
    // (KAFKA-1894).
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    // SparkSession is required for getting Hadoop configuration for writing to checkpoints
    assert(SparkSession.getActiveSession.nonEmpty)

    val metadataLog =
      new KafkaSourceInitialOffsetWriter(SparkSession.getActiveSession.get, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchLatestOffsets(None))
        case SpecificOffsetRangeLimit(p) =>
          kafkaOffsetReader.fetchSpecificOffsets(p, reportDataLoss)
        case SpecificTimestampRangeLimit(p, strategy) =>
          kafkaOffsetReader.fetchSpecificTimestampBasedOffsets(p,
            isStartingOffsets = true, strategy)
        case GlobalTimestampRangeLimit(ts, strategy) =>
          kafkaOffsetReader.fetchGlobalTimestampBasedOffsets(ts,
            isStartingOffsets = true, strategy)
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
      limit: Long,
      from: PartitionOffsetMap,
      until: PartitionOffsetMap): PartitionOffsetMap = {
    lazy val fromNew = kafkaOffsetReader.fetchEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
    val sizes = until.flatMap {
      case (tp, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(tp).orElse(fromNew.get(tp)).flatMap { begin =>
          val size = end - begin
          logDebug(s"rateLimit $tp size is $size")
          if (size > 0) Some(tp -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (tp, end) =>
          tp -> sizes.get(tp).map { size =>
            val begin = from.getOrElse(tp, fromNew(tp))
            val prorate = limit * (size / total)
            // Don't completely starve small topicpartitions
            val prorateLong = (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
            // need to be careful of integer overflow
            // therefore added canary checks where to see if off variable could be overflowed
            // refer to [https://issues.apache.org/jira/browse/SPARK-26718]
            val off = if (prorateLong > Long.MaxValue - begin) {
              Long.MaxValue
            } else {
              begin + prorateLong
            }
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  /**
   * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`.
   * Otherwise, just log a warning.
   */
  private def reportDataLoss(message: String): Unit = {
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE")
    } else {
      logWarning(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE")
    }
  }

  private def verifyEndOffsetForTriggerAvailableNow(
      endPartitionOffsets: Map[TopicPartition, Long]): Unit = {
    val tpsForPrefetched = allDataForTriggerAvailableNow.keySet
    val tpsForEndOffset = endPartitionOffsets.keySet

    if (tpsForPrefetched != tpsForEndOffset) {
      throw KafkaExceptions.mismatchedTopicPartitionsBetweenEndOffsetAndPrefetched(
        tpsForPrefetched, tpsForEndOffset)
    }

    val endOffsetHasGreaterThanPrefetched = {
      allDataForTriggerAvailableNow.keySet.exists { tp =>
        val offsetFromPrefetched = allDataForTriggerAvailableNow(tp)
        val offsetFromEndOffset = endPartitionOffsets(tp)
        offsetFromEndOffset > offsetFromPrefetched
      }
    }
    if (endOffsetHasGreaterThanPrefetched) {
      throw KafkaExceptions.endOffsetHasGreaterOffsetForTopicPartitionThanPrefetched(
        allDataForTriggerAvailableNow, endPartitionOffsets)
    }

    val latestOffsets = kafkaOffsetReader.fetchLatestOffsets(Some(endPartitionOffsets))
    val tpsForLatestOffsets = latestOffsets.keySet

    if (!tpsForEndOffset.subsetOf(tpsForLatestOffsets)) {
      throw KafkaExceptions.lostTopicPartitionsInEndOffsetWithTriggerAvailableNow(
        tpsForLatestOffsets, tpsForEndOffset)
    }

    val endOffsetHasGreaterThenLatest = {
      tpsForEndOffset.exists { tp =>
        val offsetFromLatest = latestOffsets(tp)
        val offsetFromEndOffset = endPartitionOffsets(tp)
        offsetFromEndOffset > offsetFromLatest
      }
    }
    if (endOffsetHasGreaterThenLatest) {
      throw KafkaExceptions
        .endOffsetHasGreaterOffsetForTopicPartitionThanLatestWithTriggerAvailableNow(
          latestOffsets, endPartitionOffsets)
    }
  }

  override def prepareForTriggerAvailableNow(): Unit = {
    allDataForTriggerAvailableNow = kafkaOffsetReader.fetchLatestOffsets(
      Some(getOrCreateInitialPartitionOffsets()))
  }
}

object KafkaMicroBatchStream extends Logging {

  /**
   * Compute the difference of offset per partition between latestAvailablePartitionOffsets
   * and partition offsets in the latestConsumedOffset.
   * Report min/max/avg offsets behind the latest for all the partitions in the Kafka stream.
   *
   * Because of rate limit, latest consumed offset per partition can be smaller than
   * the latest available offset per partition.
   * @param latestConsumedOffset latest consumed offset
   * @param latestAvailablePartitionOffsets latest available offset per partition
   * @return the generated metrics map
   */
  def metrics(
      latestConsumedOffset: Optional[Offset],
      latestAvailablePartitionOffsets: PartitionOffsetMap): ju.Map[String, String] = {
    val offset = Option(latestConsumedOffset.orElse(null))

    if (offset.nonEmpty && latestAvailablePartitionOffsets != null) {
      val consumedPartitionOffsets = offset.map(KafkaSourceOffset(_)).get.partitionToOffsets
      val offsetsBehindLatest = latestAvailablePartitionOffsets
        .map(partitionOffset => partitionOffset._2 - consumedPartitionOffsets(partitionOffset._1))
      if (offsetsBehindLatest.nonEmpty) {
        val avgOffsetBehindLatest = offsetsBehindLatest.sum.toDouble / offsetsBehindLatest.size
        return Map[String, String](
          "minOffsetsBehindLatest" -> offsetsBehindLatest.min.toString,
          "maxOffsetsBehindLatest" -> offsetsBehindLatest.max.toString,
          "avgOffsetsBehindLatest" -> avgOffsetBehindLatest.toString).asJava
      }
    }
    ju.Collections.emptyMap()
  }
}

/**
 * To return a mocked system clock for testing purposes
 */
private[kafka010] class MockedSystemClock extends ManualClock {
  override def getTimeMillis(): Long = {
    currentMockSystemTime
  }
}

private[kafka010] object MockedSystemClock {
  var currentMockSystemTime = 0L

  def advanceCurrentSystemTime(advanceByMillis: Long): Unit = {
    currentMockSystemTime += advanceByMillis
  }

  def reset(): Unit = {
    currentMockSystemTime = 0L
  }
}

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
import java.util.concurrent.TimeoutException

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetOutOfRangeException}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.TaskContext
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{ERROR, OFFSETS, TIP}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReader, ContinuousPartitionReaderFactory, ContinuousStream, Offset, PartitionOffset}
import org.apache.spark.sql.kafka010.KafkaSourceProvider._
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A [[ContinuousStream]] for data from kafka.
 *
 * @param offsetReader  a reader used to get kafka offsets. Note that the actual data will be
 *                      read by per-task consumers generated later.
 * @param kafkaParams   String params for per-task Kafka consumers.
 * @param options Params which are not Kafka consumer params.
 * @param metadataPath Path to a directory this reader can use for writing metadata.
 * @param initialOffsets The Kafka offsets to start reading data at.
 * @param failOnDataLoss Flag indicating whether reading should fail in data loss
 *                       scenarios, where some offsets after the specified initial ones can't be
 *                       properly read.
 */
class KafkaContinuousStream(
    private[kafka010] val offsetReader: KafkaOffsetReader,
    kafkaParams: ju.Map[String, Object],
    options: CaseInsensitiveStringMap,
    metadataPath: String,
    initialOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean)
  extends ContinuousStream with Logging {

  private[kafka010] val pollTimeoutMs =
    options.getLong(KafkaSourceProvider.CONSUMER_POLL_TIMEOUT, 512)
  private val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)

  // Initialized when creating reader factories. If this diverges from the partitions at the latest
  // offsets, we need to reconfigure.
  // Exposed outside this object only for unit tests.
  @volatile private[sql] var knownPartitions: Set[TopicPartition] = _


  override def initialOffset(): Offset = {
    val offsets = initialOffsets match {
      case EarliestOffsetRangeLimit => KafkaSourceOffset(offsetReader.fetchEarliestOffsets())
      case LatestOffsetRangeLimit => KafkaSourceOffset(offsetReader.fetchLatestOffsets(None))
      case SpecificOffsetRangeLimit(p) => offsetReader.fetchSpecificOffsets(p, reportDataLoss)
      case SpecificTimestampRangeLimit(p, strategy) =>
        offsetReader.fetchSpecificTimestampBasedOffsets(p, isStartingOffsets = true, strategy)
      case GlobalTimestampRangeLimit(ts, strategy) =>
        offsetReader.fetchGlobalTimestampBasedOffsets(ts, isStartingOffsets = true, strategy)
    }
    logInfo(log"Initial offsets: ${MDC(OFFSETS, offsets)}")
    offsets
  }

  override def deserializeOffset(json: String): Offset = {
    KafkaSourceOffset(JsonUtils.partitionOffsets(json))
  }

  override def planInputPartitions(start: Offset): Array[InputPartition] = {
    val oldStartPartitionOffsets = start.asInstanceOf[KafkaSourceOffset].partitionToOffsets

    val currentPartitionSet = offsetReader.fetchEarliestOffsets().keySet
    val newPartitions = currentPartitionSet.diff(oldStartPartitionOffsets.keySet)
    val newPartitionOffsets = offsetReader.fetchEarliestOffsets(newPartitions.toSeq)

    val deletedPartitions = oldStartPartitionOffsets.keySet.diff(currentPartitionSet)
    if (deletedPartitions.nonEmpty) {
      val (message, config) =
        if (offsetReader.driverKafkaParams.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
          (s"$deletedPartitions are gone. ${CUSTOM_GROUP_ID_ERROR_MESSAGE}",
            Some(ConsumerConfig.GROUP_ID_CONFIG))
        } else {
          (s"$deletedPartitions are gone. Some data may have been missed.", None)
        }

      reportDataLoss(
        message,
        () => KafkaExceptions.partitionsDeleted(deletedPartitions, config))
    }

    val startOffsets = newPartitionOffsets ++
      oldStartPartitionOffsets.filter { case (k, _) => !deletedPartitions.contains(k) }
    knownPartitions = startOffsets.keySet

    startOffsets.toSeq.map {
      case (topicPartition, start) =>
        KafkaContinuousInputPartition(
          topicPartition, start, kafkaParams, pollTimeoutMs, failOnDataLoss, includeHeaders)
    }.toArray
  }

  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = {
    KafkaContinuousReaderFactory
  }

  /** Stop this source and free any resources it has allocated. */
  def stop(): Unit = synchronized {
    offsetReader.close()
  }

  override def commit(end: Offset): Unit = {}

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    val mergedMap = offsets.map {
      case KafkaSourcePartitionOffset(p, o) => Map(p -> o)
    }.reduce(_ ++ _)
    KafkaSourceOffset(mergedMap)
  }

  override def needsReconfiguration(): Boolean = {
    offsetReader.fetchLatestOffsets(None).keySet != knownPartitions
  }

  override def toString(): String = s"KafkaSource[$offsetReader]"

  /**
   * If `failOnDataLoss` is true, this method will throw the exception.
   * Otherwise, just log a warning.
   */
  private def reportDataLoss(message: String, getException: () => Throwable): Unit = {
    if (failOnDataLoss) {
      throw getException()
    } else {
      logWarning(log"${MDC(ERROR, message)}. ${MDC(TIP, INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE)}")
    }
  }
}

/**
 * An input partition for continuous Kafka processing. This will be serialized and transformed
 * into a full reader on executors.
 *
 * @param topicPartition The (topic, partition) pair this task is responsible for.
 * @param startOffset The offset to start reading from within the partition.
 * @param kafkaParams Kafka consumer params to use.
 * @param pollTimeoutMs The timeout for Kafka consumer polling.
 * @param failOnDataLoss Flag indicating whether data reader should fail if some offsets
 *                       are skipped.
 * @param includeHeaders Flag indicating whether to include Kafka records' headers.
 */
case class KafkaContinuousInputPartition(
  topicPartition: TopicPartition,
  startOffset: Long,
  kafkaParams: ju.Map[String, Object],
  pollTimeoutMs: Long,
  failOnDataLoss: Boolean,
  includeHeaders: Boolean) extends InputPartition

object KafkaContinuousReaderFactory extends ContinuousPartitionReaderFactory {
  override def createReader(partition: InputPartition): ContinuousPartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[KafkaContinuousInputPartition]
    new KafkaContinuousPartitionReader(
      p.topicPartition, p.startOffset, p.kafkaParams, p.pollTimeoutMs,
      p.failOnDataLoss, p.includeHeaders)
  }
}

/**
 * A per-task data reader for continuous Kafka processing.
 *
 * @param topicPartition The (topic, partition) pair this data reader is responsible for.
 * @param startOffset The offset to start reading from within the partition.
 * @param kafkaParams Kafka consumer params to use.
 * @param pollTimeoutMs The timeout for Kafka consumer polling.
 * @param failOnDataLoss Flag indicating whether data reader should fail if some offsets
 *                       are skipped.
 */
class KafkaContinuousPartitionReader(
    topicPartition: TopicPartition,
    startOffset: Long,
    kafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    includeHeaders: Boolean) extends ContinuousPartitionReader[InternalRow] {
  private val consumer = KafkaDataConsumer.acquire(topicPartition, kafkaParams)
  private val unsafeRowProjector = new KafkaRecordToRowConverter()
    .toUnsafeRowProjector(includeHeaders)

  private var nextKafkaOffset = startOffset
  private var currentRecord: ConsumerRecord[Array[Byte], Array[Byte]] = _

  override def next(): Boolean = {
    var r: ConsumerRecord[Array[Byte], Array[Byte]] = null
    while (r == null) {
      if (TaskContext.get().isInterrupted() || TaskContext.get().isCompleted()) return false
      // Our consumer.get is not interruptible, so we have to set a low poll timeout, leaving
      // interrupt points to end the query rather than waiting for new data that might never come.
      try {
        r = consumer.get(
          nextKafkaOffset,
          untilOffset = Long.MaxValue,
          pollTimeoutMs,
          failOnDataLoss)
      } catch {
        // We didn't read within the timeout. We're supposed to block indefinitely for new data, so
        // swallow and ignore this.
        case _: TimeoutException | _: org.apache.kafka.common.errors.TimeoutException =>

        // This is a failOnDataLoss exception. Retry if nextKafkaOffset is within the data range,
        // or if it's the endpoint of the data range (i.e. the "true" next offset).
        case e: KafkaIllegalStateException if e.getCause.isInstanceOf[OffsetOutOfRangeException] =>
          val range = consumer.getAvailableOffsetRange()
          if (range.latest >= nextKafkaOffset && range.earliest <= nextKafkaOffset) {
            // retry
          } else {
            throw e
          }
      }
    }
    nextKafkaOffset = r.offset + 1
    currentRecord = r
    true
  }

  override def get(): UnsafeRow = {
    unsafeRowProjector(currentRecord)
  }

  override def getOffset(): KafkaSourcePartitionOffset = {
    KafkaSourcePartitionOffset(topicPartition, nextKafkaOffset)
  }

  override def close(): Unit = {
    consumer.release()
  }
}

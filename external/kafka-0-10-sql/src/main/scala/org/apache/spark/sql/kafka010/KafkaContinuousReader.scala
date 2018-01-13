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

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetOutOfRangeException}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.kafka010.KafkaSource.{INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE, INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.streaming.reader.{ContinuousDataReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * A [[ContinuousReader]] for data from kafka.
 *
 * @param offsetReader  a reader used to get kafka offsets. Note that the actual data will be
 *                      read by per-task consumers generated later.
 * @param kafkaParams   String params for per-task Kafka consumers.
 * @param sourceOptions The [[org.apache.spark.sql.sources.v2.DataSourceV2Options]] params which
 *                      are not Kafka consumer params.
 * @param metadataPath Path to a directory this reader can use for writing metadata.
 * @param initialOffsets The Kafka offsets to start reading data at.
 * @param failOnDataLoss Flag indicating whether reading should fail in data loss
 *                       scenarios, where some offsets after the specified initial ones can't be
 *                       properly read.
 */
class KafkaContinuousReader(
    offsetReader: KafkaOffsetReader,
    kafkaParams: ju.Map[String, Object],
    sourceOptions: Map[String, String],
    metadataPath: String,
    initialOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean)
  extends ContinuousReader with SupportsScanUnsafeRow with Logging {

  private lazy val session = SparkSession.getActiveSession.get
  private lazy val sc = session.sparkContext

  // Initialized when creating read tasks. If this diverges from the partitions at the latest
  // offsets, we need to reconfigure.
  // Exposed outside this object only for unit tests.
  private[sql] var knownPartitions: Set[TopicPartition] = _

  override def readSchema: StructType = KafkaOffsetReader.kafkaSchema

  private var offset: Offset = _
  override def setOffset(start: ju.Optional[Offset]): Unit = {
    offset = start.orElse {
      val offsets = initialOffsets match {
        case EarliestOffsetRangeLimit => KafkaSourceOffset(offsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit => KafkaSourceOffset(offsetReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(p) => offsetReader.fetchSpecificOffsets(p, reportDataLoss)
      }
      logInfo(s"Initial offsets: $offsets")
      offsets
    }
  }

  override def getStartOffset(): Offset = offset

  override def deserializeOffset(json: String): Offset = {
    KafkaSourceOffset(JsonUtils.partitionOffsets(json))
  }

  override def createUnsafeRowReadTasks(): ju.List[ReadTask[UnsafeRow]] = {
    import scala.collection.JavaConverters._

    val oldStartPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(offset)

    val currentPartitionSet = offsetReader.fetchEarliestOffsets().keySet
    val newPartitions = currentPartitionSet.diff(oldStartPartitionOffsets.keySet)
    val newPartitionOffsets = offsetReader.fetchEarliestOffsets(newPartitions.toSeq)

    val deletedPartitions = oldStartPartitionOffsets.keySet.diff(currentPartitionSet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"Some partitions were deleted: $deletedPartitions")
    }

    val startOffsets = newPartitionOffsets ++
      oldStartPartitionOffsets.filterKeys(!deletedPartitions.contains(_))
    knownPartitions = startOffsets.keySet

    startOffsets.toSeq.map {
      case (topicPartition, start) =>
        KafkaContinuousReadTask(
          topicPartition, start, kafkaParams, failOnDataLoss)
          .asInstanceOf[ReadTask[UnsafeRow]]
    }.asJava
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
    knownPartitions != null && offsetReader.fetchLatestOffsets().keySet != knownPartitions
  }

  override def toString(): String = s"KafkaSource[$offsetReader]"

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
}

/**
 * A read task for continuous Kafka processing. This will be serialized and transformed into a
 * full reader on executors.
 *
 * @param topicPartition The (topic, partition) pair this task is responsible for.
 * @param startOffset The offset to start reading from within the partition.
 * @param kafkaParams Kafka consumer params to use.
 * @param failOnDataLoss Flag indicating whether data reader should fail if some offsets
 *                       are skipped.
 */
case class KafkaContinuousReadTask(
    topicPartition: TopicPartition,
    startOffset: Long,
    kafkaParams: ju.Map[String, Object],
    failOnDataLoss: Boolean) extends ReadTask[UnsafeRow] {
  override def createDataReader(): KafkaContinuousDataReader = {
    new KafkaContinuousDataReader(topicPartition, startOffset, kafkaParams, failOnDataLoss)
  }
}

/**
 * A per-task data reader for continuous Kafka processing.
 *
 * @param topicPartition The (topic, partition) pair this data reader is responsible for.
 * @param startOffset The offset to start reading from within the partition.
 * @param kafkaParams Kafka consumer params to use.
 * @param failOnDataLoss Flag indicating whether data reader should fail if some offsets
 *                       are skipped.
 */
class KafkaContinuousDataReader(
    topicPartition: TopicPartition,
    startOffset: Long,
    kafkaParams: ju.Map[String, Object],
    failOnDataLoss: Boolean) extends ContinuousDataReader[UnsafeRow] {
  private val topic = topicPartition.topic
  private val kafkaPartition = topicPartition.partition
  private val consumer = CachedKafkaConsumer.createUncached(topic, kafkaPartition, kafkaParams)

  private val sharedRow = new UnsafeRow(7)
  private val bufferHolder = new BufferHolder(sharedRow)
  private val rowWriter = new UnsafeRowWriter(bufferHolder, 7)

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
          pollTimeoutMs = 1000,
          failOnDataLoss)
      } catch {
        // We didn't read within the timeout. We're supposed to block indefinitely for new data, so
        // swallow and ignore this.
        case _: TimeoutException =>
          
        // This is a failOnDataLoss exception. Retry if nextKafkaOffset is within the data range,
        // or if it's the endpoint of the data range (i.e. the "true" next offset).
        case e: IllegalStateException  if e.getCause.isInstanceOf[OffsetOutOfRangeException] =>
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
    bufferHolder.reset()

    if (currentRecord.key == null) {
      rowWriter.setNullAt(0)
    } else {
      rowWriter.write(0, currentRecord.key)
    }
    rowWriter.write(1, currentRecord.value)
    rowWriter.write(2, UTF8String.fromString(currentRecord.topic))
    rowWriter.write(3, currentRecord.partition)
    rowWriter.write(4, currentRecord.offset)
    rowWriter.write(5,
      DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(currentRecord.timestamp)))
    rowWriter.write(6, currentRecord.timestampType.id)
    sharedRow.setTotalSize(bufferHolder.totalSize)
    sharedRow
  }

  override def getOffset(): KafkaSourcePartitionOffset = {
    KafkaSourcePartitionOffset(topicPartition, nextKafkaOffset)
  }

  override def close(): Unit = {
    consumer.close()
  }
}

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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, SerializedOffset}
import org.apache.spark.sql.kafka010.KafkaSource.{INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE, INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE, VERSION}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.unsafe.types.UTF8String

class ContinuousKafkaReader(
    kafkaReader: KafkaOffsetReader,
    executorKafkaParams: java.util.Map[String, Object],
    sourceOptions: Map[String, String],
    metadataPath: String,
    initialOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean)
  extends ContinuousReader with SupportsScanUnsafeRow with Logging {

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    val mergedMap = offsets.map {
      case KafkaSourcePartitionOffset(p, o) => Map(p -> o)
    }.reduce(_ ++ _)
    KafkaSourceOffset(mergedMap)
  }

  private lazy val session = SparkSession.getActiveSession.get
  private lazy val sc = session.sparkContext

  private lazy val pollTimeoutMs = sourceOptions.getOrElse(
    "kafkaConsumer.pollTimeoutMs",
    sc.conf.getTimeAsMs("spark.network.timeout", "120s").toString
  ).toLong

  private val maxOffsetsPerTrigger =
    sourceOptions.get("maxOffsetsPerTrigger").map(_.toLong)

  /**
   * Lazily initialize `initialPartitionOffsets` to make sure that `KafkaConsumer.poll` is only
   * called in StreamExecutionThread. Otherwise, interrupting a thread while running
   * `KafkaConsumer.poll` may hang forever (KAFKA-1894).
   */
  private lazy val initialPartitionOffsets = {
      val offsets = initialOffsets match {
        case EarliestOffsetRangeLimit => KafkaSourceOffset(kafkaReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit => KafkaSourceOffset(kafkaReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(p) => fetchAndVerify(p)
      }
      logInfo(s"Initial offsets: $offsets")
      offsets.partitionToOffsets
  }

  private def fetchAndVerify(specificOffsets: Map[TopicPartition, Long]) = {
    val result = kafkaReader.fetchSpecificOffsets(specificOffsets)
    specificOffsets.foreach {
      case (tp, off) if off != KafkaOffsetRangeLimit.LATEST &&
        off != KafkaOffsetRangeLimit.EARLIEST =>
        if (result(tp) != off) {
          reportDataLoss(
            s"startingOffsets for $tp was $off but consumer reset to ${result(tp)}")
        }
      case _ =>
      // no real way to check that beginning or end is reasonable
    }
    KafkaSourceOffset(result)
  }

  // Initialized when creating read tasks. If this diverges from the partitions at the latest
  // offsets, we need to reconfigure.
  private var knownPartitions: Set[TopicPartition] = _

  override def readSchema: StructType = KafkaOffsetReader.kafkaSchema

  private var offset: Offset = _
  override def setOffset(start: java.util.Optional[Offset]): Unit = {
    offset = start.orElse {
      val offsets = initialOffsets match {
        case EarliestOffsetRangeLimit => KafkaSourceOffset(kafkaReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit => KafkaSourceOffset(kafkaReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(p) => fetchAndVerify(p)
      }
      logInfo(s"Initial offsets: $offsets")
      offsets
    }
  }

  override def getStartOffset(): Offset = offset

  override def deserializeOffset(json: String): Offset = {
    KafkaSourceOffset(JsonUtils.partitionOffsets(json))
  }

  override def createUnsafeRowReadTasks(): java.util.List[ReadTask[UnsafeRow]] = {
    import scala.collection.JavaConverters._

    val oldStartOffsets = KafkaSourceOffset.getPartitionOffsets(offset)

    val newPartitions =
      kafkaReader.fetchLatestOffsets().keySet.diff(oldStartOffsets.keySet)
    val newPartitionOffsets = kafkaReader.fetchEarliestOffsets(newPartitions.toSeq)
    val startOffsets = oldStartOffsets ++ newPartitionOffsets

    knownPartitions = startOffsets.keySet

    startOffsets.toSeq.map {
      case (topicPartition, start) =>
        ContinuousKafkaReadTask(
          topicPartition, start, executorKafkaParams, pollTimeoutMs, failOnDataLoss)
          .asInstanceOf[ReadTask[UnsafeRow]]
    }.asJava
  }

  /** Stop this source and free any resources it has allocated. */
  def stop(): Unit = synchronized {
    kafkaReader.close()
  }

  override def commit(end: Offset): Unit = {}

  override def needsReconfiguration(): Boolean = {
    knownPartitions != null && kafkaReader.fetchLatestOffsets().keySet != knownPartitions
  }

  override def toString(): String = s"KafkaSource[$kafkaReader]"

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

case class ContinuousKafkaReadTask(
    topicPartition: TopicPartition,
    start: Long,
    kafkaParams: java.util.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean)
  extends ReadTask[UnsafeRow] {
  override def createDataReader(): ContinuousKafkaDataReader = {
    new ContinuousKafkaDataReader(topicPartition, start, kafkaParams, pollTimeoutMs, failOnDataLoss)
  }
}

class ContinuousKafkaDataReader(
    topicPartition: TopicPartition,
    start: Long,
    kafkaParams: java.util.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean)
  extends ContinuousDataReader[UnsafeRow] {
  private val topic = topicPartition.topic
  private val kafkaPartition = topicPartition.partition
  private val consumer = CachedKafkaConsumer.createUncached(topic, kafkaPartition, kafkaParams)

  private val closed = new AtomicBoolean(false)

  private var nextKafkaOffset = start match {
    case s if s >= 0 => s
    case KafkaOffsetRangeLimit.EARLIEST => consumer.getAvailableOffsetRange().earliest
    case _ => throw new IllegalArgumentException(s"Invalid start Kafka offset $start.")
  }
  private var currentRecord: ConsumerRecord[Array[Byte], Array[Byte]] = _

  override def next(): Boolean = {
    var r: ConsumerRecord[Array[Byte], Array[Byte]] = null
    while (r == null) {
      r = consumer.get(
        nextKafkaOffset,
        untilOffset = Long.MaxValue,
        pollTimeoutMs = Long.MaxValue,
        failOnDataLoss)
    }
    nextKafkaOffset = r.offset + 1
    currentRecord = r
    true
  }

  val sharedRow = new UnsafeRow(7)
  val bufferHolder = new BufferHolder(sharedRow)
  val rowWriter = new UnsafeRowWriter(bufferHolder, 7)

  override def get(): UnsafeRow = {
    bufferHolder.reset()

    if (currentRecord.key == null) {
      rowWriter.isNullAt(0)
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

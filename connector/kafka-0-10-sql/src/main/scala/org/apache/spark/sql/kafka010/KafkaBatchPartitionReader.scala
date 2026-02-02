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

import org.apache.kafka.common.record.TimestampType

import org.apache.spark.TaskContext
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus
import org.apache.spark.sql.execution.streaming.runtime.{MicroBatchExecution, StreamExecution}
import org.apache.spark.sql.kafka010.consumer.{KafkaDataConsumer, KafkaDataConsumerIterator}

/** A [[InputPartition]] for reading Kafka data in a batch based streaming query. */
private[kafka010] case class KafkaBatchInputPartition(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    includeHeaders: Boolean) extends InputPartition {
  override def preferredLocations(): Array[String] = {
    offsetRange.preferredLoc.map(Array(_)).getOrElse(Array())
  }
}

private[kafka010] object KafkaBatchReaderFactory extends PartitionReaderFactory with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[KafkaBatchInputPartition]

    val taskCtx = TaskContext.get()
    val queryId = taskCtx.getLocalProperty(StreamExecution.QUERY_ID_KEY)
    val batchId = taskCtx.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY)
    logInfo(log"Creating Kafka reader " +
      log"topicPartition=${MDC(TOPIC_PARTITION, p.offsetRange.topicPartition)} " +
      log"fromOffset=${MDC(FROM_OFFSET, p.offsetRange.fromOffset)}} " +
      log"untilOffset=${MDC(UNTIL_OFFSET, p.offsetRange.untilOffset)}, " +
      log"for query queryId=${MDC(QUERY_ID, queryId)} batchId=${MDC(BATCH_ID, batchId)} " +
      log"taskId=${MDC(TASK_ATTEMPT_ID, TaskContext.get().taskAttemptId())} " +
      log"partitionId=${MDC(PARTITION_ID, TaskContext.get().partitionId())}")

    KafkaBatchPartitionReader(p.offsetRange, p.executorKafkaParams, p.pollTimeoutMs,
      p.failOnDataLoss, p.includeHeaders)
  }
}

/** A [[PartitionReader]] for reading Kafka data in a micro-batch streaming query. */
private case class KafkaBatchPartitionReader(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    includeHeaders: Boolean)
  extends SupportsRealTimeRead[InternalRow] with Logging {

  private val consumer = KafkaDataConsumer.acquire(offsetRange.topicPartition, executorKafkaParams)

  private val rangeToRead = resolveRange(offsetRange)
  private val unsafeRowProjector = new KafkaRecordToRowConverter()
    .toUnsafeRowProjector(includeHeaders)

  private var nextOffset = rangeToRead.fromOffset
  private var nextRow: UnsafeRow = _
  private var iteratorForRealTimeMode: Option[KafkaDataConsumerIterator] = None

  // Boolean flag that indicates whether we have logged the type of timestamp (i.e. create time,
  // log-append time, etc.) for the Kafka source. We log upon reading the first record, and we
  // then skip logging for subsequent records.
  private var timestampTypeLogged = false

  override def next(): Boolean = {
    if (nextOffset < rangeToRead.untilOffset) {
      val record = consumer.get(nextOffset, rangeToRead.untilOffset, pollTimeoutMs, failOnDataLoss)
      if (record != null) {
        nextRow = unsafeRowProjector(record)
        nextOffset = record.offset + 1
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def nextWithTimeout(timeoutMs: java.lang.Long): RecordStatus = {
    if (!iteratorForRealTimeMode.isDefined) {
      logInfo(s"Getting a new kafka consuming iterator for ${offsetRange.topicPartition} " +
        s"starting from ${nextOffset}, timeoutMs ${timeoutMs}")
      iteratorForRealTimeMode = Some(consumer.getIterator(nextOffset))
    }
    assert(iteratorForRealTimeMode.isDefined)
    val nextRecord = iteratorForRealTimeMode.get.nextWithTimeout(timeoutMs)
    nextRecord.foreach { record =>

      nextRow = unsafeRowProjector(record)
      nextOffset = record.offset + 1
      if (record.timestampType() == TimestampType.LOG_APPEND_TIME ||
        record.timestampType() == TimestampType.CREATE_TIME) {
        if (!timestampTypeLogged) {
          logInfo(log"Kafka source record timestamp type is " +
            log"${MDC(LogKeys.TIMESTAMP_COLUMN_NAME, record.timestampType())}")
          timestampTypeLogged = true
        }

        RecordStatus.newStatusWithArrivalTimeMs(record.timestamp())
      } else {
        RecordStatus.newStatusWithoutArrivalTime(true)
      }
    }
    RecordStatus.newStatusWithoutArrivalTime(nextRecord.isDefined)
  }

  override def getOffset(): KafkaSourcePartitionOffset = {
    KafkaSourcePartitionOffset(offsetRange.topicPartition, nextOffset)
  }

  override def get(): UnsafeRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit = {
    consumer.release()
  }

  private def resolveRange(range: KafkaOffsetRange): KafkaOffsetRange = {
    if (range.fromOffset < 0 || range.untilOffset < 0) {
      // Late bind the offset range
      val availableOffsetRange = consumer.getAvailableOffsetRange()
      val fromOffset = if (range.fromOffset < 0) {
        assert(range.fromOffset == KafkaOffsetRangeLimit.EARLIEST,
          s"earliest offset ${range.fromOffset} does not equal ${KafkaOffsetRangeLimit.EARLIEST}")
        availableOffsetRange.earliest
      } else {
        range.fromOffset
      }
      val untilOffset = if (range.untilOffset < 0) {
        assert(range.untilOffset == KafkaOffsetRangeLimit.LATEST,
          s"latest offset ${range.untilOffset} does not equal ${KafkaOffsetRangeLimit.LATEST}")
        availableOffsetRange.latest
      } else {
        range.untilOffset
      }
      KafkaOffsetRange(range.topicPartition, fromOffset, untilOffset, None)
    } else {
      range
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    val offsetOutOfRange = new CustomTaskMetric {
      override def name(): String = "offsetOutOfRange"
      override def value(): Long = consumer.getNumOffsetOutOfRange()
    }
    val dataLoss = new CustomTaskMetric {
      override def name(): String = "dataLoss"
      override def value(): Long = consumer.getNumDataLoss()
    }
    Array(offsetOutOfRange, dataLoss)
  }
}

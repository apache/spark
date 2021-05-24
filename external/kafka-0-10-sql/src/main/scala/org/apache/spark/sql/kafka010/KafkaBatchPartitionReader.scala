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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer

/** A [[InputPartition]] for reading Kafka data in a batch based streaming query. */
private[kafka010] case class KafkaBatchInputPartition(
    offsetRange: KafkaOffsetRange,
    executorKafkaParams: ju.Map[String, Object],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean,
    includeHeaders: Boolean) extends InputPartition

private[kafka010] object KafkaBatchReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[KafkaBatchInputPartition]
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
    includeHeaders: Boolean) extends PartitionReader[InternalRow] with Logging {

  private val consumer = KafkaDataConsumer.acquire(offsetRange.topicPartition, executorKafkaParams)

  private val rangeToRead = resolveRange(offsetRange)
  private val unsafeRowProjector = new KafkaRecordToRowConverter()
    .toUnsafeRowProjector(includeHeaders)

  private var nextOffset = rangeToRead.fromOffset
  private var nextRow: UnsafeRow = _

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

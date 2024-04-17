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

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.LogKey.{FROM_OFFSET, PARTITION_ID, TOPIC}
import org.apache.spark.internal.MDC
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator

/** Partition of the KafkaSourceRDD */
private[kafka010] case class KafkaSourceRDDPartition(
  index: Int, offsetRange: KafkaOffsetRange) extends Partition


/**
 * An RDD that reads data from Kafka based on offset ranges across multiple partitions.
 * Additionally, it allows preferred locations to be set for each topic + partition, so that
 * the [[KafkaSource]] can ensure the same executor always reads the same topic + partition
 * and cached KafkaConsumers (see [[KafkaDataConsumer]] can be used read data efficiently.
 *
 * @param sc the [[SparkContext]]
 * @param executorKafkaParams Kafka configuration for creating KafkaConsumer on the executors
 * @param offsetRanges Offset ranges that define the Kafka data belonging to this RDD
 */
private[kafka010] class KafkaSourceRDD(
    sc: SparkContext,
    executorKafkaParams: ju.Map[String, Object],
    offsetRanges: Seq[KafkaOffsetRange],
    pollTimeoutMs: Long,
    failOnDataLoss: Boolean)
  extends RDD[ConsumerRecord[Array[Byte], Array[Byte]]](sc, Nil) {

  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) => new KafkaSourceRDDPartition(i, o) }.toArray
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[KafkaSourceRDDPartition]
    part.offsetRange.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
  }

  override def compute(
      thePart: Partition,
      context: TaskContext): Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val sourcePartition = thePart.asInstanceOf[KafkaSourceRDDPartition]
    val consumer = KafkaDataConsumer.acquire(
      sourcePartition.offsetRange.topicPartition, executorKafkaParams)

    val range = resolveRange(consumer, sourcePartition.offsetRange)
    assert(
      range.fromOffset <= range.untilOffset,
      s"Beginning offset ${range.fromOffset} is after the ending offset ${range.untilOffset} " +
        s"for topic ${range.topic} partition ${range.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged")
    if (range.fromOffset == range.untilOffset) {
      logInfo(log"Beginning offset ${MDC(FROM_OFFSET, range.fromOffset)} is the same as ending " +
        log"offset skipping ${MDC(TOPIC, range.topic)} ${MDC(PARTITION_ID, range.partition)}")
      consumer.release()
      Iterator.empty
    } else {
      val underlying = new NextIterator[ConsumerRecord[Array[Byte], Array[Byte]]]() {
        var requestOffset = range.fromOffset

        override def getNext(): ConsumerRecord[Array[Byte], Array[Byte]] = {
          if (requestOffset >= range.untilOffset) {
            // Processed all offsets in this partition.
            finished = true
            null
          } else {
            val r = consumer.get(requestOffset, range.untilOffset, pollTimeoutMs, failOnDataLoss)
            if (r == null) {
              // Losing some data. Skip the rest offsets in this partition.
              finished = true
              null
            } else {
              requestOffset = r.offset + 1
              r
            }
          }
        }

        override protected def close(): Unit = {
          consumer.release()
        }
      }
      // Release consumer, either by removing it or indicating we're no longer using it
      context.addTaskCompletionListener[Unit] { _ =>
        underlying.closeIfNeeded()
      }
      underlying
    }
  }

  private def resolveRange(consumer: KafkaDataConsumer, range: KafkaOffsetRange) = {
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
      KafkaOffsetRange(range.topicPartition,
        fromOffset, untilOffset, range.preferredLoc)
    } else {
      range
    }
  }
}

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

package org.apache.spark.streaming.kafka010

import java.{util => ju}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging


protected class KafkaConsumerHolder[K, V](private val kafkaParams: ju.Map[String, Object]) {
  private var closed = false
  private var consumers: mutable.Map[TopicPartition, Consumer[K, V]] = mutable.Map()

  /**
   * Get a consumer from the pool. Creates one if needed.
   * Creates 1 consmuer per TopicPartition to be able to fetch multiple partitions
   * at the same time.
   */
  def getConsumer(tp: TopicPartition): Consumer[K, V] = {
    this.synchronized {
      assert(!closed, "Consumers have been closed, can't open new.")
      if (!consumers.contains(tp)) {
        val c = new KafkaConsumer[K, V](kafkaParams)
        c.assign(Set(tp).asJava)
        consumers(tp) = c
      }
      consumers(tp)
    }
  }

  def close(): Unit = {
    this.synchronized {
      closed = true
      consumers.foreach(_._2.close())
    }
  }
}

/**
 * If we endup on an empty offset (transaction marker or abort transaction),
 * a call to c.poll() won't return any data and we can't tell if it's because
 * we missed data or the offset is empty.
 * To prevent that, we change the offset range to always end on an offset with
 * data. The range can be increased or reduced to the next living offset.
 * @param rewind how many offset we'll try to rewind before seeking offset with data
 * @param kafkaParams kafka params. Isolation level must be read_committed
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
class OffsetWithRecordRewinder[K, V](
    rewind: Long,
    kafkaParams: ju.Map[String, Object]
  ) extends Logging with Serializable {

  if (!kafkaParams.containsKey("isolation.level") ||
    kafkaParams.get("isolation.level") != "read_committed") {
    throw new IllegalStateException("DirectStream only support read_committed." +
      "Please add isolation.level = read_committed to your kafka configuration")
  }

  lazy val holder = new KafkaConsumerHolder[K, V](kafkaParams)

  def close(): Unit = {
    holder.close()
  }

  def rewind(currentOffsets: Map[TopicPartition, Long], tp: TopicPartition,
             o: Long): (TopicPartition, Long) = {
    val consumer = holder.getConsumer(tp)
    val rewindOffset = rewindUntilDataExist(currentOffsets, tp, o, 1, rewind, consumer)
    (tp, rewindOffset)
  }

  // Try to poll the last message if any.
  def rewindUntilDataExist(currentOffsets: Map[TopicPartition, Long], tp: TopicPartition,
                           toOffset: Long, iteration: Int, rewind: Long,
                           consumer: Consumer[K, V]): Long = {
    if (toOffset <= currentOffsets(tp)) {
      logDebug(s"can't rewind further: $toOffset <= ${currentOffsets(tp)}. Stop")
      return toOffset
    }
    val nextStart = toOffset - Math.pow(rewind, iteration).toLong
    val startOffsetScan = Math.max(currentOffsets(tp), nextStart)
    logDebug(s"searching the next non-empty offset in ${tp} from offset $startOffsetScan")
    val records: ConsumerRecords[K, V] = seekAndPoll(consumer, tp, startOffsetScan)
    val smallestRecordOffset = records.iterator().asScala
      .foldLeft(startOffsetScan)((maxOffset, r) => r.offset() match {
        // we get an offset after our range, but our max is already after the range, we take the min
        case recordOffset if recordOffset >= toOffset && maxOffset > toOffset => maxOffset
        // we get an offset after our range, take it (it'll increase the range).
        case recordOffset if recordOffset >= toOffset => recordOffset
        // we get an offset bigger than the max but before our range, take it.
        case recordOffset if recordOffset > maxOffset => recordOffset
        // we get an offset smaller than the max, ignore it.
        case _ => maxOffset
      })
    // we have at least 1 offset with data, use this one as final range
    if(smallestRecordOffset > startOffsetScan) {
      smallestRecordOffset
    } else {
      // if we don't get any data, try to rewind faster
      rewindUntilDataExist(currentOffsets, tp, startOffsetScan, iteration + 1, rewind, consumer)
    }
  }

  protected def seekAndPoll(c: Consumer[K, V], tp: TopicPartition, startOffsetScan: Long) = {
    c.seek(tp, startOffsetScan)
    // Try to get the last records. We want at least 1 living record
    // Will be slow if all range data is aborted transactions
    c.poll(1000)
  }
}

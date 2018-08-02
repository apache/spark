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

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.kafka.common.TopicPartition

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging


/**
 * If we endup on an empty offset (transaction marker or abort transaction),
 * a call to c.poll() won't return any data and we can't tell if it's because
 * we missed data or the offset is empty.
 * To prevent that, we change the offset range to always end on an offset with
 * data. The range can be increased or reduced to the next living offset.
 * @param kafkaParams kafka params. Isolation level must be read_committed
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
class OffsetWithRecordScanner[K, V](
    kafkaParams: ju.Map[String, Object],
    cacheInitialCapacity: Int,
    cacheMaxCapacity: Int,
    cacheLoadFactor: Float,
    useConsumerCache: Boolean
  ) extends Logging with Serializable {

  if (kafkaParams.containsKey("isolation.level") &&
    kafkaParams.get("isolation.level") != "read_committed") {
    throw new IllegalStateException("DirectStream only support read_committed." +
      "Please add isolation.level = read_committed to your kafka configuration")
  }

  def getLastOffsetAndCount(fromOffset: Long, tp: TopicPartition,
                            toOffset: Long): (Long, Long) = {
    if (toOffset <= fromOffset) {
      return (toOffset, 0)
    }
    logDebug(s"Initial offsets for $tp: [$fromOffset, $toOffset]")
    KafkaDataConsumer.init(cacheInitialCapacity, cacheMaxCapacity, cacheLoadFactor)
    val c = KafkaDataConsumer.acquire[K, V](tp, kafkaParams, TaskContext.get, useConsumerCache)
    val (o, size) = iterateUntilLastOrEmpty(fromOffset, 0, c, toOffset - fromOffset)
    c.release()
    logDebug(s"Rectified offset for $tp: [$fromOffset, $o]. Real size=$size")
    (o, size)
  }

  final def iterateUntilLastOrEmpty(lastOffset: Long, count: Long,
                                    c: KafkaDataConsumer[K, V], rangeSize: Long): (Long, Long) = {
    iterate(lastOffset - 1, count, c, rangeSize)
  }

  @tailrec
  final def iterate(lastOffset: Long, count: Long,
                              c: KafkaDataConsumer[K, V], rangeSize: Long): (Long, Long) = {
    getNext(c) match {
      // No more records or can't get new one. stop here
      case None => (lastOffset + 1, count)
      // We have enough records. stop here
      case Some(r) if count + 1 >= rangeSize => (r.offset() + 1, count + 1)
      case Some(r) => iterate(r.offset(), count + 1, c, rangeSize)
    }
  }

  protected def getNext(c: KafkaDataConsumer[K, V]) = {
    c.next(1000)
  }
}

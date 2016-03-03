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

package org.apache.spark.streaming.kafka

import java.util.{Collections, Properties}

import scala.collection.JavaConverters._
import scala.reflect._

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{Logging, TaskContext}
import org.apache.spark.util.NextIterator

private class NewKafkaRDDIterator[
K: ClassTag,
V: ClassTag,
R: ClassTag] private[spark] (
  part: KafkaRDDPartition,
  context: TaskContext,
  kafkaParams: Map[String, String],
  messageHandler: ConsumerRecord[K, V] => R,
  pollTime: Long = KafkaUtils.DEFAULT_NEW_KAFKA_API_POLL_TIME) extends NextIterator[R] with
  Logging {

    context.addTaskCompletionListener { context => closeIfNeeded() }

    log.info(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    val consumer = new KafkaConsumer[K, V](props)
    val tp = new TopicPartition(part.topic, part.partition)
    consumer.assign(Collections.singletonList[TopicPartition](tp))

    var requestOffset = part.fromOffset
    var iter: Iterator[ConsumerRecord[K, V]] = null
    consumer.seek(tp, requestOffset)

    override def close(): Unit = {
      if (consumer != null) {
        consumer.close()
      }
    }

    private def fetchBatch: Iterator[ConsumerRecord[K, V]] = {
      consumer.seek(new TopicPartition(part.topic, part.partition), requestOffset)
      var recs: ConsumerRecords[K, V] = null
      do {
        recs = consumer.poll(pollTime)
      } while (recs.isEmpty && requestOffset < part.untilOffset)
      recs.records(new TopicPartition(part.topic, part.partition)).iterator().asScala
    }

    override def getNext(): R = {
      if (requestOffset == part.untilOffset) {
        finished = true
        null.asInstanceOf[R]
      }

      if (iter == null || !iter.hasNext) {
        iter = fetchBatch
      }

      if (!iter.hasNext) {
        assert(requestOffset == part.untilOffset, KafkaUtils.errRanOutBeforeEnd(part))
        finished = true
        null.asInstanceOf[R]
      } else {
        val item: ConsumerRecord[K, V] = iter.next()
        if (item.offset >= part.untilOffset) {
          assert(item.offset == part.untilOffset, KafkaUtils.errOvershotEnd(item.offset, part))
          finished = true
          null.asInstanceOf[R]
        } else {
          requestOffset = item.offset() + 1
          messageHandler(item)
        }
      }
    }
}

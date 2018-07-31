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

package org.apache.spark.streaming.kafka010.consumer.sync

import java.{util => ju}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.streaming.kafka010.consumer.SparkKafkaConsumer

class SyncSparkKafkaConsumer [K, V](
                                      val topicPartition: TopicPartition,
                                      val kafkaParams: ju.Map[String, Object],
                                      pollTimeoutMs: Long
                                      ) extends SparkKafkaConsumer[K, V] {

  private[kafka010] val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)
    .asInstanceOf[String]

  lazy val consumer = createConsumer

  @volatile private var buffer = ju.Collections.emptyListIterator[ConsumerRecord[K, V]]()
  @volatile private var nextOffset = SyncSparkKafkaConsumer.UNKNOWN_OFFSET

  /** Create a KafkaConsumer to fetch records for `topicPartition` */
  private def createConsumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer[K, V](kafkaParams)
    val topics = ju.Arrays.asList(topicPartition)
    c.assign(topics)
    logDebug(s"$topicPartition Created new kafka consumer with assigned topics $topics")
    c
  }


  override def ensureOffset(offset: Long): Unit = {
    nextOffset = offset
    buffer = ju.Collections.emptyListIterator[ConsumerRecord[K, V]]()
    seek(offset)
  }

  override def getNextRecord(): ConsumerRecord[K, V] = {
    var record: ConsumerRecord[K, V] = null
    if (!buffer.hasNext) {
      poll(pollTimeoutMs)
    }
    if (buffer.hasNext) {
      record = buffer.next()
      nextOffset = record.offset() + 1
    }
    record
  }

  override def moveToPrevious(): ConsumerRecord[K, V] = buffer.previous()

  override def getNextOffset(): Long = nextOffset

  override def close(): Unit = consumer.close()

  override def getTimeout(): Long = pollTimeoutMs

  private def seek(offset: Long): Unit = {
    logDebug(s"Seeking to $topicPartition $offset")
    consumer.seek(topicPartition, offset)
  }

  private def poll(timeout: Long): Unit = {
    val p = consumer.poll(timeout)
    val r = p.records(topicPartition)
    logDebug(s"Polled ${p.partitions()}  ${r.size}")
    buffer = r.listIterator
  }
}

object SyncSparkKafkaConsumer {
  val UNKNOWN_OFFSET: Long = -2
}

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

package org.apache.spark.streaming.kafka010.consumer.async

import java.{util => ju}
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.streaming.kafka010.consumer.SparkKafkaConsumer

class AsyncSparkKafkaConsumer[K, V](
                                        val topicPartition: TopicPartition,
                                        val kafkaParams: ju.Map[String, Object],
                                        val maintainBufferMin: Int,
                                        val pollTimeoutMs: Long
                                      ) extends SparkKafkaConsumer[K, V] {

  /** We need to maintain lastRecord to revert buffer to previous state
   * in case of compacted iteration
   */
  @volatile private var lastRecord: ConsumerRecord[K, V] = null

  // Pre-initialize async task and output to empty list
  // This tuple will cleaned-up and updated as required
  var asyncPollTask: AsyncPollTask[K, V] = null

  private def createNewPollTask(offset: Long) = {
    new AsyncPollTask[K, V](
      topicPartition, ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor()),
      createConsumer, maintainBufferMin, pollTimeoutMs, offset
    )
  }

  /**
   * Ensure that next record have expected offset.
   * If not clear the buffer and async poll task, and reset seek offset
   */
  override def ensureOffset(offset: Long): Unit = {
    if (asyncPollTask == null) {
      asyncPollTask = createNewPollTask(offset)
    }
    else if (asyncPollTask.getNextOffset() != offset) {
      logDebug(s"Seeking to offset $offset")
      asyncPollTask.close()
      asyncPollTask = createNewPollTask(offset)
    }
  }

  /**
   * Get next record from buffer.
   *  First ensure buffer have data.
   *  Then return first record from buffer.
   */
  override def getNextRecord(): ConsumerRecord[K, V] = {
    lastRecord = asyncPollTask.getNextRecord(pollTimeoutMs)
    lastRecord
  }

  /**
   * Add last record back to buffer to revert the state.
   */
  override def moveToPrevious(): ConsumerRecord[K, V] = {
    asyncPollTask.revertLastRecord(lastRecord)
    lastRecord
  }

  override def getNextOffset(): Long = {
    if (asyncPollTask == null) {
      AsyncSparkKafkaConsumer.UNKNOWN_OFFSET
    }
    else {
      asyncPollTask.getNextOffset()
    }
  }

  /** Create a KafkaConsumer to fetch records for `topicPartition` */
  private def createConsumer: KafkaConsumer[K, V] = {
    val c = new KafkaConsumer[K, V](kafkaParams)
    val topics = ju.Arrays.asList(topicPartition)
    c.assign(topics)
    logDebug(s"$topicPartition Created new kafka consumer with assigned topics $topics")
    c
  }

  override def getTimeout(): Long = pollTimeoutMs

  override def close(): Unit = {
    logDebug(s"$topicPartition Closing consumers")
    asyncPollTask.close()
  }
}

private[kafka010] object AsyncSparkKafkaConsumer {
  val UNKNOWN_OFFSET = -2L
}

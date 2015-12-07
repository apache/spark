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

package org.apache.spark.streaming.kafka.v09

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.util.ThreadUtils

import scala.collection.Map
import scala.reflect.ClassTag

/**
  * Input stream that pulls messages from a Kafka Broker.
  *
  * @param kafkaParams Map of kafka configuration parameters.
  *                    See: http://kafka.apache.org/configuration.html
  * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
  *               in its own thread.
  * @param storageLevel RDD storage level.
  */
private[streaming]
class KafkaInputDStream[
K: ClassTag,
V: ClassTag](
              @transient ssc_ : StreamingContext,
              kafkaParams: Map[String, String],
              topics: Map[String, Int],
              useReliableReceiver: Boolean,
              storageLevel: StorageLevel
            ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging {

  def getReceiver(): Receiver[(K, V)] = {
    if (!useReliableReceiver) {
      logInfo("[!] Using 0.9 KafkaReceiver")
      new KafkaReceiver[K, V](kafkaParams, topics, storageLevel)
    } else {
      logInfo("[!] Using 0.9 ReliableKafkaReceiver")
      new ReliableKafkaReceiver[K, V](kafkaParams, topics, storageLevel)
    }
  }
}

private[streaming]
class KafkaReceiver[
K: ClassTag,
V: ClassTag](
              kafkaParams: Map[String, String],
              topics: Map[String, Int],
              storageLevel: StorageLevel
            ) extends Receiver[(K, V)](storageLevel) with Logging {

  private var kafkaCluster: KafkaCluster[_, _] = null

  private val KAFKA_DEFAULT_POLL_TIME: String = "0"
  private val pollTime = kafkaParams.get("spark.kafka.poll.time")
    .getOrElse(KAFKA_DEFAULT_POLL_TIME).toInt

  def onStop() {
  }

  def onStart() {

    logInfo("Starting Kafka Consumer Stream with group: " + kafkaParams("group.id"))

    // Kafka connection properties
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    kafkaCluster = new KafkaCluster[K, V](kafkaParams.toMap)

    val executorPool = ThreadUtils.newDaemonFixedThreadPool(topics.values.sum,
      "KafkaMessageHandler")
    try {
      // Start the messages handler for each partition
      val topicAndPartitions = kafkaCluster.getPartitions(topics.keys.toSet).right.toOption
      val iter = topicAndPartitions.get.iterator

      while (iter.hasNext) {
        val topicAndPartition = iter.next()
        val newConsumer = new KafkaConsumer[K, V](props)
        newConsumer.subscribe(Collections.singletonList[String](topicAndPartition.topic))
        executorPool.submit(new MessageHandler(newConsumer))
      }
    } finally {
      executorPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  // Handles Kafka messages
  private class MessageHandler(consumer: KafkaConsumer[K, V])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      try {
        while (true) {
          val records: ConsumerRecords[K, V] = this.consumer.poll(pollTime)
          val iterator = records.iterator()
          while (iterator.hasNext) {
            val record: ConsumerRecord[K, V] = iterator.next()
            store((record.key, record.value()))
          }
        }
      } catch {
        case e: Throwable => reportError("Error handling message; exiting", e)
      }
    }
  }

}


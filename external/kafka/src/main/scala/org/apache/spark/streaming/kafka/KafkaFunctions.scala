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

import scala.reflect.ClassTag

import kafka.serializer.{Decoder, StringDecoder}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

/**
 * Extra Kafka input stream functions available on [[org.apache.spark.streaming.StreamingContext]]
 * through implicit conversion. Import org.apache.spark.streaming.kafka._ to use these functions.
 */
class KafkaFunctions(ssc: StreamingContext) {
  /**
   * Create an input stream that pulls messages from a Kafka Broker.
   * @param zkQuorum Zookeper quorum (hostname:port,hostname:port,..).
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *               in its own thread.
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def kafkaStream(
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
    kafkaStream[String, String, StringDecoder, StringDecoder](
      kafkaParams,
      topics,
      storageLevel)
  }

  /**
   * Create an input stream that pulls messages from a Kafka Broker.
   * @param kafkaParams Map of kafka configuration paramaters.
   *                    See: http://kafka.apache.org/configuration.html
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *               in its own thread.
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def kafkaStream[K: ClassTag, V: ClassTag, U <: Decoder[_]: Manifest, T <: Decoder[_]: Manifest](
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): DStream[(K, V)] = {
    val inputStream = new KafkaInputDStream[K, V, U, T](ssc, kafkaParams, topics, storageLevel)
    ssc.registerInputStream(inputStream)
    inputStream
  }
}

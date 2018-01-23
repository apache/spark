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

import java.util

import scala.collection.concurrent.TrieMap

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

private[kafka010] object KafkaConsumerPool extends Logging {

  private lazy val map = TrieMap[String, CachedKafkaConsumer]()
  private lazy val capacity = SparkEnv.get.conf.getInt("spark.sql.kafkaConsumerPool.capacity", 64)

  def borrowConsumer(topic: String, partition: Int, kafkaParams: util.Map[String, Object],
      reuseConsumer: Boolean): CachedKafkaConsumer = synchronized {
    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]
    assert(groupId != null)
    map.remove(topic + partition + groupId).getOrElse {
      CachedKafkaConsumer.createConsumer(topic, partition, kafkaParams, reuseConsumer)
    }
  }

  def returnConsumer(cachedKafkaConsumer: CachedKafkaConsumer): Unit = synchronized {
    val partition = cachedKafkaConsumer.topicPartition.partition()
    val reuseConsumer = cachedKafkaConsumer.reuseConsumer
    val groupId = cachedKafkaConsumer.groupId
    val topic = cachedKafkaConsumer.topicPartition.topic()
    if (reuseConsumer && map.size < capacity) {
      if (map.contains(topic + partition + groupId)) {
        // Ideally, this should never happen.
        map.remove(topic + partition + groupId).foreach(_.close())
        logWarning(s"""consumer for same partition already exists, closing previously stored
            consumer for topic-partition: ${cachedKafkaConsumer.topicPartition},
           $cachedKafkaConsumer""")
      }
      map.put(topic + partition + groupId, cachedKafkaConsumer)
    } else {
      if (reuseConsumer) {
        logWarning(s"Consumer pool has reached it's max capacity, closing consumer:" +
          s" $cachedKafkaConsumer")
      }
      cachedKafkaConsumer.close()
    }
  }

  // For testing.
  def close(): Unit = synchronized {
    map.values.foreach(_.close())
    map.clear()
  }
}

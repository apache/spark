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

import java.{ util => ju }

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Subscribe to a collection of topics.
 * @param topics collection of topics to subscribe
 * @param kafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
 * configuration parameters</a> to be used on driver. The same params will be used on executors,
 * with minor automatic modifications applied.
 *  Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
 * TopicPartition, the committed offset (if applicable) or kafka param
 * auto.offset.reset will be used.
 */
@Experimental
private[kafka010] case class SubscribeStrategy[K, V] private(
    topics: ju.Collection[String],
    kafkaParams: ju.Map[String, Object],
    offsets: ju.Map[TopicPartition, java.lang.Long]
  ) extends ConsumerStrategy[K, V] {

  def this(
      topics: collection.Iterable[String],
      kafkaParams: collection.Map[String, Object],
      offsets: collection.Map[TopicPartition, scala.Long]) = this(
    topics.asJavaCollection,
    Utils.asJavaMap(kafkaParams),
    Utils.asJavaMap(offsets.mapValues(l => new java.lang.Long(l))))

  def executorKafkaParams(): ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: ju.Map[TopicPartition, java.lang.Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.subscribe(topics)
    if (currentOffsets.isEmpty) {
      offsets.asScala.foreach { case (topicPartition, offset) =>
          consumer.seek(topicPartition, offset)
      }
    }

    consumer
  }
}

/**
 * :: Experimental ::
 * Assign a fixed collection of TopicPartitions
 * @param topicPartitions collection of TopicPartitions to assign
 * @param kafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
 * configuration parameters</a> to be used on driver. The same params will be used on executors,
 * with minor automatic modifications applied.
 *  Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
 * TopicPartition, the committed offset (if applicable) or kafka param
 * auto.offset.reset will be used.
 */
@Experimental
private[kafka010] case class AssignStrategy[K, V] private(
    topicPartitions: ju.Collection[TopicPartition],
    kafkaParams: ju.Map[String, Object],
    offsets: ju.Map[TopicPartition, java.lang.Long]
  ) extends ConsumerStrategy[K, V] {

  def this(
      topicPartitions: collection.Iterable[TopicPartition],
      kafkaParams: collection.Map[String, Object],
      offsets: collection.Map[TopicPartition, scala.Long]) = this(
    topicPartitions.asJavaCollection,
    Utils.asJavaMap(kafkaParams),
    Utils.asJavaMap(offsets.mapValues(l => new java.lang.Long(l))))

  def executorKafkaParams(): ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: ju.Map[TopicPartition, java.lang.Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.assign(topicPartitions)
    if (currentOffsets.isEmpty) {
      offsets.asScala.foreach { case (topicPartition, offset) =>
          consumer.seek(topicPartition, offset)
      }
    }

    consumer
  }
}



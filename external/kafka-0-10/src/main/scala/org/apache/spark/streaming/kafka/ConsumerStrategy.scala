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

import java.{ util => ju }

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

/**
 * Choice of how to create and configure underlying Kafka Consumers on driver and executors.
 * Kafka 0.10 consumers can require additional, sometimes complex, setup after object
 *  instantiation. This interface encapsulates that process, and allows it to be checkpointed.
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
trait ConsumerStrategy[K, V] {
  /**
   * Kafka <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
   * configuration parameters</a> to be used on executors. Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   */
  def executorKafkaParams: ju.Map[String, Object]

  /**
   * Must return a fully configured Kafka Consumer, including subscribed or assigned topics.
   * This consumer will be used on the driver to query for offsets only, not messages.
   * @param currentOffsets A map from TopicPartition to offset, indicating how far the driver
   * has successfully read.  Will be empty on initial start, possibly non-empty on restart from
   * checkpoint.
   * TODO: is strategy or dstream responsible for seeking on checkpoint restart
   */
  def onStart(currentOffsets: Map[TopicPartition, Long]): Consumer[K, V]
}

/**
 * Subscribe to a collection of topics.
 * @param topics collection of topics to subscribe
 * @param kafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
 * configuration parameters</a> to be used on driver. The same parameters will be used on executors,
 * with minor automatic modifications applied.
 *  Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 */
case class Subscribe[K: ClassTag, V: ClassTag](
    topics: ju.Collection[java.lang.String],
    kafkaParams: ju.Map[String, Object]
  ) extends ConsumerStrategy[K, V] {

  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: Map[TopicPartition, Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.subscribe(topics)
    consumer
  }
}

object Subscribe {
  def create[K, V](
      keyClass: Class[K],
      valueClass: Class[V],
      topics: ju.Collection[java.lang.String],
      kafkaParams: ju.Map[String, Object]
  ): Subscribe[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    Subscribe[K, V](topics, kafkaParams)
  }
}

/**
 * Assign a fixed collection of TopicPartitions
 * @param topicPartitions collection of TopicPartitions to subscribe
 * @param kafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
 * configuration parameters</a> to be used on driver. The same parameters will be used on executors,
 * with minor automatic modifications applied.
 *  Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 */
case class Assign[K: ClassTag, V: ClassTag](
    topicPartitions: ju.Collection[TopicPartition],
    kafkaParams: ju.Map[String, Object]
  ) extends ConsumerStrategy[K, V] {

  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: Map[TopicPartition, Long]): Consumer[K, V] = {
    val consumer = new KafkaConsumer[K, V](kafkaParams)
    consumer.assign(topicPartitions)
    consumer
  }
}

object Assign {
  def create[K, V](
      keyClass: Class[K],
      valueClass: Class[V],
      topicPartitions: ju.Collection[TopicPartition],
      kafkaParams: ju.Map[String, Object]
  ): Assign[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    Assign[K, V](topicPartitions, kafkaParams)
  }
}


/**
 * Set offsets on initial startup only, after another strategy has configured consumer
 * @param offsets: offsets to begin at
 * @param init: ConsumerStrategy responsible for instantiation and initial config
 */
case class FromOffsets[K: ClassTag, V: ClassTag](
    offsets: Map[TopicPartition, Long],
    init: ConsumerStrategy[K, V]
) extends ConsumerStrategy[K, V] {
  def executorKafkaParams: ju.Map[String, Object] = init.executorKafkaParams

  def onStart(currentOffsets: Map[TopicPartition, Long]): Consumer[K, V] = {
    val consumer = init.onStart(currentOffsets)

    if (currentOffsets.isEmpty) {
      offsets.foreach { case (topicPartition, offset) =>
          consumer.seek(topicPartition, offset)
      }
    }
    consumer
  }
}

object FromOffsets {
  def create[K, V](
      keyClass: Class[K],
      valueClass: Class[V],
      offsets: ju.Map[TopicPartition, Long],
      init: ConsumerStrategy[K, V]
  ): FromOffsets[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    val off = Map(offsets.asScala.toSeq: _*)
    FromOffsets[K, V](off, init)
  }
}

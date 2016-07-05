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

import java.{ lang => jl, util => ju }

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition

import org.apache.spark.annotation.Experimental


/**
 * :: Experimental ::
 * Choice of how to create and configure underlying Kafka Consumers on driver and executors.
 * See [[ConsumerStrategies]] to obtain instances.
 * Kafka 0.10 consumers can require additional, sometimes complex, setup after object
 *  instantiation. This interface encapsulates that process, and allows it to be checkpointed.
 * @tparam K type of Kafka message key
 * @tparam V type of Kafka message value
 */
@Experimental
abstract class ConsumerStrategy[K, V] {
  /**
   * Kafka <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
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
   */
  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V]
}

/**
 * Subscribe to a collection of topics.
 * @param topics collection of topics to subscribe
 * @param kafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
 * configuration parameters</a> to be used on driver. The same params will be used on executors,
 * with minor automatic modifications applied.
 *  Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
 * TopicPartition, the committed offset (if applicable) or kafka param
 * auto.offset.reset will be used.
 */
private case class Subscribe[K, V](
    topics: ju.Collection[jl.String],
    kafkaParams: ju.Map[String, Object],
    offsets: ju.Map[TopicPartition, jl.Long]
  ) extends ConsumerStrategy[K, V] {

  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V] = {
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
 * Assign a fixed collection of TopicPartitions
 * @param topicPartitions collection of TopicPartitions to assign
 * @param kafkaParams Kafka
 * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
 * configuration parameters</a> to be used on driver. The same params will be used on executors,
 * with minor automatic modifications applied.
 *  Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
 * TopicPartition, the committed offset (if applicable) or kafka param
 * auto.offset.reset will be used.
 */
private case class Assign[K, V](
    topicPartitions: ju.Collection[TopicPartition],
    kafkaParams: ju.Map[String, Object],
    offsets: ju.Map[TopicPartition, jl.Long]
  ) extends ConsumerStrategy[K, V] {

  def executorKafkaParams: ju.Map[String, Object] = kafkaParams

  def onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]): Consumer[K, V] = {
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

/**
 * :: Experimental ::
 * object for obtaining instances of [[ConsumerStrategy]]
 */
@Experimental
object ConsumerStrategies {
  /**
   *  :: Experimental ::
   * Subscribe to a collection of topics.
   * @param topics collection of topics to subscribe
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
   * TopicPartition, the committed offset (if applicable) or kafka param
   * auto.offset.reset will be used.
   */
  @Experimental
  def Subscribe[K, V](
      topics: Iterable[jl.String],
      kafkaParams: collection.Map[String, Object],
      offsets: collection.Map[TopicPartition, Long]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](
      new ju.ArrayList(topics.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      new ju.HashMap[TopicPartition, jl.Long](offsets.mapValues(l => new jl.Long(l)).asJava))
  }

  /**
   *  :: Experimental ::
   * Subscribe to a collection of topics.
   * @param topics collection of topics to subscribe
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   */
  @Experimental
  def Subscribe[K, V](
      topics: Iterable[jl.String],
      kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](
      new ju.ArrayList(topics.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /**
   *  :: Experimental ::
   * Subscribe to a collection of topics.
   * @param topics collection of topics to subscribe
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
   * TopicPartition, the committed offset (if applicable) or kafka param
   * auto.offset.reset will be used.
   */
  @Experimental
  def Subscribe[K, V](
      topics: ju.Collection[jl.String],
      kafkaParams: ju.Map[String, Object],
      offsets: ju.Map[TopicPartition, jl.Long]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](topics, kafkaParams, offsets)
  }

  /**
   *  :: Experimental ::
   * Subscribe to a collection of topics.
   * @param topics collection of topics to subscribe
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   */
  @Experimental
  def Subscribe[K, V](
      topics: ju.Collection[jl.String],
      kafkaParams: ju.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Subscribe[K, V](topics, kafkaParams, ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /**
   *  :: Experimental ::
   * Assign a fixed collection of TopicPartitions
   * @param topicPartitions collection of TopicPartitions to assign
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
   * TopicPartition, the committed offset (if applicable) or kafka param
   * auto.offset.reset will be used.
   */
  @Experimental
  def Assign[K, V](
      topicPartitions: Iterable[TopicPartition],
      kafkaParams: collection.Map[String, Object],
      offsets: collection.Map[TopicPartition, Long]): ConsumerStrategy[K, V] = {
    new Assign[K, V](
      new ju.ArrayList(topicPartitions.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      new ju.HashMap[TopicPartition, jl.Long](offsets.mapValues(l => new jl.Long(l)).asJava))
  }

  /**
   *  :: Experimental ::
   * Assign a fixed collection of TopicPartitions
   * @param topicPartitions collection of TopicPartitions to assign
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   */
  @Experimental
  def Assign[K, V](
      topicPartitions: Iterable[TopicPartition],
      kafkaParams: collection.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Assign[K, V](
      new ju.ArrayList(topicPartitions.asJavaCollection),
      new ju.HashMap[String, Object](kafkaParams.asJava),
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

  /**
   *  :: Experimental ::
   * Assign a fixed collection of TopicPartitions
   * @param topicPartitions collection of TopicPartitions to assign
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   * @param offsets: offsets to begin at on initial startup.  If no offset is given for a
   * TopicPartition, the committed offset (if applicable) or kafka param
   * auto.offset.reset will be used.
   */
  @Experimental
  def Assign[K, V](
      topicPartitions: ju.Collection[TopicPartition],
      kafkaParams: ju.Map[String, Object],
      offsets: ju.Map[TopicPartition, jl.Long]): ConsumerStrategy[K, V] = {
    new Assign[K, V](topicPartitions, kafkaParams, offsets)
  }

  /**
   *  :: Experimental ::
   * Assign a fixed collection of TopicPartitions
   * @param topicPartitions collection of TopicPartitions to assign
   * @param kafkaParams Kafka
   * <a href="http://kafka.apache.org/documentation.html#newconsumerconfigs">
   * configuration parameters</a> to be used on driver. The same params will be used on executors,
   * with minor automatic modifications applied.
   *  Requires "bootstrap.servers" to be set
   * with Kafka broker(s) specified in host1:port1,host2:port2 form.
   */
  @Experimental
  def Assign[K, V](
      topicPartitions: ju.Collection[TopicPartition],
      kafkaParams: ju.Map[String, Object]): ConsumerStrategy[K, V] = {
    new Assign[K, V](
      topicPartitions,
      kafkaParams,
      ju.Collections.emptyMap[TopicPartition, jl.Long]())
  }

}

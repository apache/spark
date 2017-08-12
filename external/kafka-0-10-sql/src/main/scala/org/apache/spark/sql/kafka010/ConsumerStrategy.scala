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

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

/**
 * Subscribe allows you to subscribe to a fixed collection of topics.
 * SubscribePattern allows you to use a regex to specify topics of interest.
 * Note that unlike the 0.8 integration, using Subscribe or SubscribePattern
 * should respond to adding partitions during a running stream.
 * Finally, Assign allows you to specify a fixed collection of partitions.
 * All three strategies have overloaded constructors that allow you to specify
 * the starting offset for a particular partition.
 */
sealed trait ConsumerStrategy {
  /** Create a [[KafkaConsumer]] and subscribe to topics according to a desired strategy */
  def createConsumer(kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]]
}

/**
 * Specify a fixed collection of partitions.
 */
case class AssignStrategy(partitions: Array[TopicPartition]) extends ConsumerStrategy {
  override def createConsumer(
      kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    consumer.assign(ju.Arrays.asList(partitions: _*))
    consumer
  }

  override def toString: String = s"Assign[${partitions.mkString(", ")}]"
}

/**
 * Subscribe to a fixed collection of topics.
 */
case class SubscribeStrategy(topics: Seq[String]) extends ConsumerStrategy {
  override def createConsumer(
      kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    consumer.subscribe(topics.asJava)
    consumer
  }

  override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
}

/**
 * Use a regex to specify topics of interest.
 */
case class SubscribePatternStrategy(topicPattern: String) extends ConsumerStrategy {
  override def createConsumer(
      kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
    consumer.subscribe(
      ju.regex.Pattern.compile(topicPattern),
      new NoOpConsumerRebalanceListener())
    consumer
  }

  override def toString: String = s"SubscribePattern[$topicPattern]"
}

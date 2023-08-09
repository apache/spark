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
import scala.collection.mutable

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.{KafkaConfigUpdater, KafkaRedactionUtil}

/**
 * Subscribe allows you to subscribe to a fixed collection of topics.
 * SubscribePattern allows you to use a regex to specify topics of interest.
 * Note that unlike the 0.8 integration, using Subscribe or SubscribePattern
 * should respond to adding partitions during a running stream.
 * Finally, Assign allows you to specify a fixed collection of partitions.
 * All three strategies have overloaded constructors that allow you to specify
 * the starting offset for a particular partition.
 */
private[kafka010] sealed trait ConsumerStrategy extends Logging {
  /** Create a [[KafkaConsumer]] and subscribe to topics according to a desired strategy */
  def createConsumer(kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]]

  /** Creates an [[org.apache.kafka.clients.admin.AdminClient]] */
  def createAdmin(kafkaParams: ju.Map[String, Object]): Admin = {
    val updatedKafkaParams = setAuthenticationConfigIfNeeded(kafkaParams)
    logDebug(s"Admin params: ${KafkaRedactionUtil.redactParams(updatedKafkaParams.asScala.toSeq)}")
    Admin.create(updatedKafkaParams)
  }

  /** Returns the assigned or subscribed [[TopicPartition]] */
  def assignedTopicPartitions(admin: Admin): Set[TopicPartition]

  /**
   * Updates the parameters with security if needed.
   * Added a function to hide internals and reduce code duplications because all strategy uses it.
   */
  protected def setAuthenticationConfigIfNeeded(kafkaParams: ju.Map[String, Object]) =
    KafkaConfigUpdater("source", kafkaParams.asScala.toMap)
      .setAuthenticationConfigIfNeeded()
      .build()

  protected def retrieveAllPartitions(admin: Admin, topics: Set[String]): Set[TopicPartition] = {
    admin.describeTopics(topics.asJava).all().get().asScala.filterNot(_._2.isInternal).flatMap {
      case (topic, topicDescription) =>
        topicDescription.partitions().asScala.map { topicPartitionInfo =>
          val partition = topicPartitionInfo.partition()
          logDebug(s"Partition found: $topic:$partition")
          new TopicPartition(topic, partition)
        }
    }.toSet
  }
}

/**
 * Specify a fixed collection of partitions.
 */
private[kafka010] case class AssignStrategy(partitions: Array[TopicPartition])
    extends ConsumerStrategy with Logging {
  override def createConsumer(
      kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
    val updatedKafkaParams = setAuthenticationConfigIfNeeded(kafkaParams)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](updatedKafkaParams)
    consumer.assign(ju.Arrays.asList(partitions: _*))
    consumer
  }

  override def assignedTopicPartitions(admin: Admin): Set[TopicPartition] = {
    val topics = partitions.map(_.topic()).toSet
    logDebug(s"Topics for assignment: $topics")
    retrieveAllPartitions(admin, topics).filter(partitions.contains(_))
  }

  override def toString: String = s"Assign[${partitions.mkString(", ")}]"
}

/**
 * Subscribe to a fixed collection of topics.
 */
private[kafka010] case class SubscribeStrategy(topics: Seq[String])
    extends ConsumerStrategy with Logging {
  override def createConsumer(
      kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
    val updatedKafkaParams = setAuthenticationConfigIfNeeded(kafkaParams)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](updatedKafkaParams)
    consumer.subscribe(topics.asJava)
    consumer
  }

  override def assignedTopicPartitions(admin: Admin): Set[TopicPartition] = {
    retrieveAllPartitions(admin, topics.toSet)
  }

  override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
}

/**
 * Use a regex to specify topics of interest.
 */
private[kafka010] case class SubscribePatternStrategy(topicPattern: String)
    extends ConsumerStrategy with Logging {
  private val topicRegex = topicPattern.r

  override def createConsumer(
      kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
    val updatedKafkaParams = setAuthenticationConfigIfNeeded(kafkaParams)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](updatedKafkaParams)
    consumer.subscribe(ju.regex.Pattern.compile(topicPattern), new NoOpConsumerRebalanceListener())
    consumer
  }

  override def assignedTopicPartitions(admin: Admin): Set[TopicPartition] = {
    logDebug(s"Topic pattern: $topicPattern")
    var topics = mutable.Seq.empty[String]
    // listTopics is not listing internal topics by default so no filter needed
    admin.listTopics().listings().get().asScala.foreach { topicListing =>
      val name = topicListing.name()
      if (topicRegex.findFirstIn(name).isDefined) {
        logDebug(s"Topic matches pattern: $name")
        topics :+= name
      }
    }
    retrieveAllPartitions(admin, topics.toSet)
  }

  override def toString: String = s"SubscribePattern[$topicPattern]"
}

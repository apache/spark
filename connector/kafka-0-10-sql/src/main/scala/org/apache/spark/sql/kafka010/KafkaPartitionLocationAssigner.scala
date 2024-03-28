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

import scala.jdk.CollectionConverters._

import org.apache.kafka.common.{ Node, PartitionInfo, TopicPartition, TopicPartitionInfo }

import org.apache.spark.util.Utils

case class ExecutorDescription(id: String, host: String)

case class PartitionDescription(
  topic: String,
  partition: Int,
  leader: Node,
  replicas: Array[Node],
  isr: Array[Node]) {
  def toTopicPartition: TopicPartition = new TopicPartition(topic, partition)

  private def nodeString(n: Node): String = n.host()

  override def toString: String =
    s"""
      |PartitionDescription(
      |  $topic,
      |  $partition,
      |  ${nodeString(leader)},
      |  ${replicas.map(nodeString(_)).mkString(",")},
      |  ${isr.map(nodeString(_)).mkString(",")})
      |""".stripMargin
}

object PartitionDescription {
  def fromPartitionInfo(pi: PartitionInfo): PartitionDescription =
    PartitionDescription(
      pi.topic(),
      pi.partition(),
      pi.leader(),
      pi.replicas(),
      pi.inSyncReplicas())

  def fromTopicPartitionInfo(topic: String, tpi: TopicPartitionInfo): PartitionDescription =
    PartitionDescription(
      topic,
      tpi.partition(),
      tpi.leader(),
      tpi.replicas().asScala.toArray,
      tpi.isr().asScala.toArray)
}

trait KafkaPartitionLocationAssigner {
  /**
  * Returns a map of what executors are eligible for each Kafka
  * partition. This map need not be exhaustive; if a given Kafka
  * partition is not associated with any executors, the partition
  * will be consistently assigned to any available executor.
  *
  * @param partDescrs     Partition information per topic and partition
  *                       subscribed. This collection is provided sorted by
  *                       topic, then partition
  *
  * @param knownExecutors Executor addresses for this app. This
  *                       collection is provided sorted to aid in
  *                       consistently associating partitions with
  *                       executors
  */
  def getLocationPreferences(
    partDescrs: Array[PartitionDescription],
    knownExecutors: Array[ExecutorDescription]):
    Map[PartitionDescription, Array[ExecutorDescription]]
}

object KafkaPartitionLocationAssigner {
  def instance(maybeClassName: Option[String]): KafkaPartitionLocationAssigner =
    maybeClassName.map { className =>
      Utils.classForName[KafkaPartitionLocationAssigner](className)
        .getConstructor()
        .newInstance()
    }.getOrElse(DefaultKafkaPartitionLocationAssigner)
}

object DefaultKafkaPartitionLocationAssigner extends KafkaPartitionLocationAssigner {
  def getLocationPreferences(
    partDescrs: Array[PartitionDescription],
    knownExecutors: Array[ExecutorDescription]):
    Map[PartitionDescription, Array[ExecutorDescription]] =
    Map.empty
}


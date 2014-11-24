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

package org.apache.spark.rdd.kafka

import scala.util.control.NonFatal
import java.util.Properties
import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetRequestInfo, TopicMetadataRequest, TopicMetadataResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}

/**
  * Convenience methods for interacting with a Kafka cluster.
  * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">configuration parameters</a>.
  *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
  *   NOT zookeeper servers, specified in host1:port1,host2:port2 form
  */
class KafkaCluster(val kafkaParams: Map[String, String]) {
  val brokers: Array[(String, Int)] =
    kafkaParams.get("metadata.broker.list")
      .orElse(kafkaParams.get("bootstrap.servers"))
      .getOrElse(throw new Exception("Must specify metadata.broker.list or bootstrap.servers"))
      .split(",").map { hp =>
        val hpa = hp.split(":")
        (hpa(0), hpa(1).toInt)
      }

  val config: ConsumerConfig = KafkaCluster.consumerConfig(kafkaParams)

  def connect(host: String, port: Int): SimpleConsumer =
    new SimpleConsumer(host, port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)

  def connect(hostAndPort: (String, Int)): SimpleConsumer =
    connect(hostAndPort._1, hostAndPort._2)

  def connectLeader(topic: String, partition: Int): Option[SimpleConsumer] =
    findLeader(topic, partition).map(connect)

  def findLeader(topic: String, partition: Int): Option[(String, Int)] = {
    val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, config.clientId, Seq(topic))
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp)
        val resp: TopicMetadataResponse = consumer.send(req)
        resp.topicsMetadata.find(_.topic == topic).flatMap { t =>
          t.partitionsMetadata.find(_.partitionId == partition)
        }.foreach { partitionMeta =>
          partitionMeta.leader.foreach { leader =>
            return Some((leader.host, leader.port))
          }
        }
      } catch {
        case NonFatal(e) =>
      } finally {
        if (consumer != null) consumer.close()
      }
    }
    None
  }

  def getLatestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.LatestTime)

  def getEarliestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.EarliestTime)

  def getLeaderOffsets(topicAndPartitions: Set[TopicAndPartition], before: Long): Map[TopicAndPartition, Long] =
    getLeaderOffsets(topicAndPartitions, before, 1).map { kv =>
      // mapValues isnt serializable, see SI-7005
      kv._1 -> kv._2.head
    }

  def getLeaderOffsets(topicAndPartitions: Set[TopicAndPartition], before: Long, maxNumOffsets: Int): Map[TopicAndPartition, Seq[Long]] = {
    var result = Map[TopicAndPartition, Seq[Long]]()
    val req = OffsetRequest(
      topicAndPartitions.map(tp => tp -> PartitionOffsetRequestInfo(before, 1)).toMap
    )
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp)
        val resp: OffsetResponse = consumer.getOffsetsBefore(req)
        val respParts = resp.partitionErrorAndOffsets
        val needed = topicAndPartitions.diff(result.keys.toSet)
        needed.foreach { tp =>
          respParts.get(tp).foreach { errAndOffsets =>
            if (errAndOffsets.error == ErrorMapping.NoError) {
              result += tp -> errAndOffsets.offsets
            }
          }
        }
        if (result.keys.size == topicAndPartitions.size) {
          return result
        }
      } catch {
        case NonFatal(e) =>
      } finally {
        if (consumer != null) consumer.close()
      }
    }
    val missing = topicAndPartitions.diff(result.keys.toSet)
    throw new Exception(s"Couldn't find offsets for ${missing}")
  }

  def getConsumerOffsets(topicAndPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = ???

  def setConsumerOffsets(offsets: Map[TopicAndPartition, Long]): Unit = ???
}

object KafkaCluster {
  /** Make a consumer config without requiring group.id or zookeeper.connect,
    * since communicating with brokers also needs common settings such as timeout
    */
  def consumerConfig(kafkaParams: Map[String, String]): ConsumerConfig = {
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    Seq("zookeeper.connect", "group.id").foreach { s =>
      if (!props.contains(s))
      props.setProperty(s, "")
    }
    new ConsumerConfig(props)
  }
}

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

import java.util
import java.util.Collections

import scala.collection.JavaConverters._
import scala.reflect._

import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetResetStrategy}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Convenience methods for interacting with a Kafka cluster, using the new Kafka consumer API.
 * See <a href="https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol">
 * A Guide To The Kafka Protocol</a> for more details on individual api calls.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>.
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form
 */
@DeveloperApi
class NewKafkaCluster[K: ClassTag, V: ClassTag](val kafkaParams: Map[String, String])
  extends Serializable {

  @transient
  protected var consumer: KafkaConsumer[K, V] = null

  def getLatestOffsets(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
    getOffsetsWithoutLeaders(topicPartitions, OffsetResetStrategy.LATEST)
  }

  def getEarliestOffsets(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
    getOffsetsWithoutLeaders(topicPartitions, OffsetResetStrategy.EARLIEST)
  }

  def getPartitions(topics: Set[String]): Set[TopicPartition] = {
    getPartitionInfo(topics).map { pi =>
      new TopicPartition(pi.topic(), pi.partition())
    }
  }

  def getPartitionsLeader(topics: Set[String]): Map[TopicPartition, (String, Int)] = {
    getPartitionInfo(topics).map { pi =>
      new TopicPartition(pi.topic, pi.partition) -> (pi.leader.host, pi.leader.port)
    }.toMap
  }

  def getPartitionInfo(topics: Set[String]): Set[PartitionInfo] = {
    withConsumer { consumer =>
      topics.flatMap { topic =>
        Option(consumer.partitionsFor(topic)) match {
          case None =>
            throw new SparkException("Topic doesn't exist " + topic)
          case Some(piList) => piList.asScala.toList
        }
      }
    }.asInstanceOf[Set[PartitionInfo]]
  }

  def setConsumerOffsets(offsets: Map[TopicPartition, Long]): Unit = {
    val topicPartOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    val topicPartition = offsets.map(tpl => tpl._1).toSeq

    withConsumer(consumer => {
      consumer.assign(Collections.emptyList[TopicPartition])
      consumer.assign(topicPartition.asJava)

      for ((topicAndPart, offset) <- offsets) {
        val topicPartition = topicAndPart
        val offsetAndMetadata = new OffsetAndMetadata(offset)
        topicPartOffsets.put(topicPartition, offsetAndMetadata)
      }

      consumer.commitSync(topicPartOffsets)
    })
  }

  def getCommittedOffsets(topicPartitions: Set[TopicPartition]):
    Map[TopicPartition, Long] = {
    withConsumer(consumer => {
      consumer.assign(topicPartitions.toList.asJava)
      topicPartitions.map( tp => {
        val offsetAndMetadata = consumer.committed(tp)
        Option(offsetAndMetadata) match {
          case None => throw new SparkException(s"Topic $tp hasn't committed offsets")
          case Some(om) => tp -> om.offset()
        }
      }
      ).toMap
    }).asInstanceOf[Map[TopicPartition, Long]]
  }

  def getLatestOffsetsWithLeaders(
      topicPartitions: Set[TopicPartition]
    ): Map[TopicPartition, LeaderOffset] = {
    getOffsets(topicPartitions, OffsetResetStrategy.LATEST)
  }

  private def getOffsetsWithoutLeaders(
      topicPartitions: Set[TopicPartition],
      offsetResetType: OffsetResetStrategy
    ): Map[TopicPartition, Long] = {
    getOffsets(topicPartitions, offsetResetType)
      .map { t => (t._1, t._2.offset) }
  }

  def getOffsets(topicPartitions: Set[TopicPartition], resetStrategy: OffsetResetStrategy):
    Map[TopicPartition, LeaderOffset] = {
    val topics = topicPartitions.map { _.topic }
    val tplMap = getPartitionsLeader(topics)
    withConsumer{ consumer =>
      consumer.assign(topicPartitions.toList.asJava)
      resetStrategy match {
        case OffsetResetStrategy.EARLIEST => consumer.seekToBeginning(topicPartitions.toList: _*)
        case OffsetResetStrategy.LATEST => consumer.seekToEnd(topicPartitions.toList: _*)
        case _ => throw new SparkException("Unknown OffsetResetStrategy " + resetStrategy)
      }
      topicPartitions.map { tp =>
        val pos = consumer.position(tp)
        tp -> new LeaderOffset(tplMap(tp)._1, tplMap(tp)._2, pos)
      }.toMap

    }.asInstanceOf[Map[TopicPartition, LeaderOffset]]
  }

  def findLeaders(
    topicPartitions: Set[TopicPartition]
  ): Map[TopicPartition, (String, Int)] = {
    val topics = topicPartitions.map { _.topic }
    getPartitionsLeader(topics)
  }

  private def withConsumer(fn: KafkaConsumer[K, V] => Any): Any = {
    if (consumer == null) {
      consumer = new KafkaConsumer[K, V](kafkaParams.asInstanceOf[Map[String, Object]].asJava)
    }
    fn(consumer)
  }

  def close(): Unit = {
    if (consumer != null) {
      consumer.close()
      consumer = null
    }
  }

}

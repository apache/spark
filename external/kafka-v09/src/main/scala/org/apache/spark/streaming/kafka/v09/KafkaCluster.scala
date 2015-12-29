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

package org.apache.spark.streaming.kafka.v09

import java.util
import java.util.{Collections}

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.{OffsetResetStrategy, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkException

import scala.collection.JavaConverters._
import scala.reflect._
import scala.util.control.NonFatal

/**
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 *                    configuration parameters</a>.
 *                    Requires "bootstrap.servers" to be set with Kafka broker(s),
 *                    NOT zookeeper servers, specified in host1:port1,host2:port2 form
 */
private[spark]
class KafkaCluster[K: ClassTag, V: ClassTag](val kafkaParams: Map[String, String])
  extends Serializable {

  import KafkaCluster.{toTopicPart}

  def getLatestOffsets(topicPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    seek(topicPartitions, OffsetResetStrategy.LATEST)
  }

  def getEarliestOffsets(topicPartitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    seek(topicPartitions, OffsetResetStrategy.EARLIEST)
  }

  def getPartitions(topics: Set[String]): Set[TopicAndPartition] = {
    withConsumer { consumer => {
        val partInfo = topics.flatMap {
          topic => Option(consumer.partitionsFor(topic)) match {
            case None => throw new SparkException("Topic doesn't exist " + topic)
            case Some(partInfoList) => partInfoList.asScala.toList
          }
        }
        val topicPartitions: Set[TopicAndPartition] = partInfo.map { partition =>
          new TopicAndPartition(partition.topic(), partition.partition())
        }
        topicPartitions
      }
    }.asInstanceOf[Set[TopicAndPartition]]
  }

  def setConsumerOffsets(offsets: Map[TopicAndPartition, Long]): Unit = {
    val topicPartOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    val topicAndPartition = offsets.map(tpl => tpl._1).toSeq

    withConsumer(consumer => {
      consumer.assign(Collections.emptyList[TopicPartition])
      consumer.assign(topicAndPartition.map(tp => toTopicPart(tp)).asJava)

      for ((topicAndPart, offset) <- offsets) {
        val topicPartition = toTopicPart(topicAndPart)
        val offsetAndMetadata = new OffsetAndMetadata(offset)
        topicPartOffsets.put(topicPartition, offsetAndMetadata)
      }

      consumer.commitSync(topicPartOffsets)
    })
  }

  def getCommittedOffsets(topicAndPartitions: Set[TopicAndPartition]):
    Map[TopicAndPartition, Long] = {
    withConsumer(consumer => {
      val topicPartitions = topicAndPartitions.map(tp => toTopicPart(tp))
      consumer.assign(topicPartitions.toList.asJava)
      topicAndPartitions.map( tp => {
        val offsetAndMetadata = consumer.committed(toTopicPart(tp))
        Option(offsetAndMetadata) match {
          case None => throw new SparkException(s"Topic $tp hasn't committed offsets")
          case Some(om) => tp -> om.offset()
        }
      }
      ).toMap
    }).asInstanceOf[Map[TopicAndPartition, Long]]
  }

  def seek(topicAndPartitions: Set[TopicAndPartition], resetStrategy: OffsetResetStrategy):
    Map[TopicAndPartition, Long] = {
    withConsumer(consumer => {
      val topicPartitions = topicAndPartitions.map(tp => toTopicPart(tp))
      consumer.assign(topicPartitions.toList.asJava)
      resetStrategy match {
        case OffsetResetStrategy.EARLIEST => consumer.seekToBeginning(topicPartitions.toArray: _*)
        case OffsetResetStrategy.LATEST => consumer.seekToEnd(topicPartitions.toArray: _*)
        case _ => throw new SparkException("Unknown OffsetResetStrategy " + resetStrategy)
      }
      topicAndPartitions.map(
        tp => tp -> (consumer.position(toTopicPart(tp)))
      ).toMap
    }).asInstanceOf[Map[TopicAndPartition, Long]]
  }

  private def withConsumer(fn: KafkaConsumer[K, V] => Any): Any = {
    var consumer: KafkaConsumer[K, V] = null
    try {
      consumer = new KafkaConsumer[K, V](kafkaParams.asInstanceOf[Map[String, Object]].asJava)
      fn(consumer)
    } finally {
      if (consumer != null) {
        consumer.close()
      }
    }
  }

  def close(): Unit = {
  }

}

object KafkaCluster {

  def toTopicPart(topicAndPartition: TopicAndPartition): TopicPartition = {
    new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
  }

  def toTopicAndPart(topicPartition: TopicPartition): TopicAndPartition = {
    TopicAndPartition(topicPartition.topic, topicPartition.partition)
  }
}

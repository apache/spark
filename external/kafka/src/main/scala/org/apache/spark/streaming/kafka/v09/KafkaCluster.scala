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
import java.util.{Properties, Collections}

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.v09.KafkaCluster.SeekType.SeekType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect._
import scala.util.control.NonFatal

/**
  * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
  *                    configuration parameters</a>.
  *                    Requires "metadata.broker.list" or "bootstrap.servers"
  *                    to be set with Kafka broker(s),
  *                    NOT zookeeper servers, specified in host1:port1,host2:port2 form
  */
private[spark]
class KafkaCluster[K: ClassTag, V: ClassTag](val kafkaParams: Map[String, String])
  extends Serializable {
  import KafkaCluster.{Err, SeekType, toTopicPart}

  def getLatestOffsets(topicPartitions: Set[TopicAndPartition]):
  Either[Err, Map[TopicAndPartition, Long]] = {
    seek(topicPartitions, SeekType.End)
  }

  def getEarliestOffsets(topicPartitions: Set[TopicAndPartition]):
  Either[Err, Map[TopicAndPartition, Long]] = {
    seek(topicPartitions, SeekType.Beginning)
  }

  def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] = {
    val errs = new Err
    var result: Either[Err, Set[TopicAndPartition]] = null

    withConsumer(errs)(consumer => {
      try {
        val partInfo = topics.flatMap(topic => consumer.partitionsFor(topic).asScala)
        val topicPartitions: Set[TopicAndPartition] = partInfo.map { partition =>
          new TopicAndPartition(partition.topic(), partition.partition())
        }
        result = Right(topicPartitions)
      } catch {
        case NonFatal(e) => {
          errs.append(e)
          result = Left(errs)
        }
      }
    })
    result
  }

  def setConsumerOffsets(offsets: Map[TopicAndPartition, Long]): Unit = {
    val errs = new Err
    val topicPartOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    val topicAndPartition = offsets.map(tpl => tpl._1).toSeq

    withConsumer(errs)(consumer => {
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

  def getCommittedOffset(topicAndPartition: TopicAndPartition): Either[Err, Long] = {
    val errs = new Err
    var result: Either[Err, Long] = null

    withConsumer(errs)(consumer => {
      try {
        val topicPartition = toTopicPart(topicAndPartition)
        consumer.assign(util.Arrays.asList(topicPartition))
        result = Right(consumer.committed(topicPartition).offset())
      } catch {
        case NonFatal(e) => {
          errs.append(e)
          result = Left(errs)
        }
      }
    })
    result
  }

  def getCommittedOffsets(topicAndPartitions: Set[TopicAndPartition]):
  Either[Err, Map[TopicAndPartition, Long]] = {
    val errs = new Err
    var result: Either[Err, Map[TopicAndPartition, Long]] = null

    withConsumer(errs)(consumer => {
      try {
        val topicPartitions = topicAndPartitions.map(tp => toTopicPart(tp))
        consumer.assign(topicPartitions.toList.asJava)
        result = Right(topicAndPartitions.map(
          tp => tp -> (consumer.committed(toTopicPart(tp))).offset()).toMap)
      } catch {
        case NonFatal(e) => {
          errs.append(e)
          result = Left(errs)
        }
      }
    })
    result
  }

  def seek(topicAndPartitions: Set[TopicAndPartition], seekType: SeekType):
  Either[Err, Map[TopicAndPartition, Long]] = {
    val errs = new Err
    var result: Either[Err, Map[TopicAndPartition, Long]] = null

    withConsumer(errs)(consumer => {
      try {
        val topicPartitions = topicAndPartitions.map(tp => toTopicPart(tp))
        consumer.assign(topicPartitions.toList.asJava)
        seekType match {
          case SeekType.Beginning => consumer.seekToBeginning(topicPartitions.toArray: _*)
          case SeekType.End => consumer.seekToEnd(topicPartitions.toArray: _*)
        }
        result = Right(topicAndPartitions.map(
          tp => tp -> (consumer.position(toTopicPart(tp)))
        ).toMap)
      } catch {
        case NonFatal(e) => {
          errs.append(e)
          result = Left(errs)
        }
      }
    })
    result

  }

  private def withConsumer(errs: Err)(fn: KafkaConsumer[K, V] => Any): Unit = {
    var consumer: KafkaConsumer[K, V] = null
    try {
      val props = new Properties()
      kafkaParams.foreach(param => props.put(param._1, param._2))
      consumer = new KafkaConsumer[K, V](props)
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

  object SeekType extends Enumeration {
    type SeekType = Value
    val Beginning, End = Value
  }

  type Err = ArrayBuffer[Throwable]

  /** If the result is right, return it, otherwise throw SparkException */
  def checkErrors[T](result: Either[Err, T]): T = {
    result.fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
  }

  def toTopicPart(topicAndPartition: TopicAndPartition): TopicPartition = {
    new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
  }

  def toTopicAndPart(topicPartition: TopicPartition): TopicAndPartition = {
    TopicAndPartition(topicPartition.topic, topicPartition.partition)
  }
}

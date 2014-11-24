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
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import kafka.api.{OffsetCommitRequest, OffsetRequest, OffsetFetchRequest, PartitionOffsetRequestInfo, TopicMetadata, TopicMetadataRequest, TopicMetadataResponse}
import kafka.common.{ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}

/**
  * Convenience methods for interacting with a Kafka cluster.
  * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">configuration parameters</a>.
  *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
  *   NOT zookeeper servers, specified in host1:port1,host2:port2 form
  */
class KafkaCluster(val kafkaParams: Map[String, String]) {
  type Err = ArrayBuffer[Throwable]

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

  def connectLeader(topic: String, partition: Int): Either[Err, SimpleConsumer] =
    findLeader(topic, partition).right.map(connect)

  def findLeader(topic: String, partition: Int): Either[Err, (String, Int)] = {
    val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, config.clientId, Seq(topic))
    val errs = new Err
    withBrokers(errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      resp.topicsMetadata.find(_.topic == topic).flatMap { t =>
        t.partitionsMetadata.find(_.partitionId == partition)
      }.foreach { partitionMeta =>
        partitionMeta.leader.foreach { leader =>
          return Right((leader.host, leader.port))
        }
      }
    }
    Left(errs)
  }

  def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] =
    getPartitionMetadata(topics).right.map { r =>
      r.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.map { pm =>
          TopicAndPartition(tm.topic, pm.partitionId)
        }    
      }
    }

  def getPartitionMetadata(topics: Set[String]): Either[Err, Set[TopicMetadata]] = {
    val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
    val errs = new Err
    withBrokers(errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      // error codes here indicate missing / just created topic,
      // repeating on a different broker wont be useful
      return Right(resp.topicsMetadata.toSet)
    }
    Left(errs)
  }

  def getLatestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, Long]] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.LatestTime)

  def getEarliestLeaderOffsets(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, Long]] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.EarliestTime)

  def getLeaderOffsets(topicAndPartitions: Set[TopicAndPartition], before: Long): Either[Err, Map[TopicAndPartition, Long]] =
    getLeaderOffsets(topicAndPartitions, before, 1).right.map { r =>
      r.map { kv =>
        // mapValues isnt serializable, see SI-7005
        kv._1 -> kv._2.head
      }
    }

  def getLeaderOffsets(
    topicAndPartitions: Set[TopicAndPartition],
    before: Long,
    maxNumOffsets: Int
  ): Either[Err, Map[TopicAndPartition, Seq[Long]]] = {
    var result = Map[TopicAndPartition, Seq[Long]]()
    val req = OffsetRequest(
      topicAndPartitions.map(tp => tp -> PartitionOffsetRequestInfo(before, 1)).toMap
    )
    val errs = new Err
    withBrokers(errs) { consumer =>
      val resp = consumer.getOffsetsBefore(req)
      val respMap = resp.partitionErrorAndOffsets
      val needed = topicAndPartitions.diff(result.keys.toSet)
      needed.foreach { tp =>
        respMap.get(tp).foreach { errAndOffsets =>
          if (errAndOffsets.error == ErrorMapping.NoError) {
            result += tp -> errAndOffsets.offsets
          } else {
            errs.append(ErrorMapping.exceptionFor(errAndOffsets.error))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return Right(result)
      }
    }
    val missing = topicAndPartitions.diff(result.keys.toSet)
    errs.append(new Exception(s"Couldn't find offsets for ${missing}"))
    Left(errs)
  }

  def getConsumerOffsets(groupId: String, topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, Long]] = {
    getConsumerOffsetMetadata(groupId, topicAndPartitions).right.map { r =>
      r.map { kv =>
        kv._1 -> kv._2.offset
      }
    }
  }

  def getConsumerOffsetMetadata(
    groupId: String,
    topicAndPartitions: Set[TopicAndPartition]
  ): Either[Err, Map[TopicAndPartition, OffsetMetadataAndError]] = {
    var result = Map[TopicAndPartition, OffsetMetadataAndError]()
    val req = OffsetFetchRequest(groupId, topicAndPartitions.toSeq)
    val errs = new Err
    withBrokers(errs) { consumer =>
      val resp = consumer.fetchOffsets(req)
      val respMap = resp.requestInfo
      val needed = topicAndPartitions.diff(result.keys.toSet)
      needed.foreach { tp =>
        respMap.get(tp).foreach { offsetMeta =>
          if (offsetMeta.error == ErrorMapping.NoError) {
            result += tp -> offsetMeta
          } else {
            errs.append(ErrorMapping.exceptionFor(offsetMeta.error))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return Right(result)
      }
    }
    val missing = topicAndPartitions.diff(result.keys.toSet)
    errs.append(new Exception(s"Couldn't find offsets for ${missing}"))
    Left(errs)
  }

  def setConsumerOffsets(groupId: String, offsets: Map[TopicAndPartition, Long]): Unit = {
    setConsumerOffsetMetadata(groupId, offsets.map { kv =>
      kv._1 -> OffsetMetadataAndError(kv._2)
    })
  }

  def setConsumerOffsetMetadata(
    groupId: String,
    metadata: Map[TopicAndPartition, OffsetMetadataAndError]
  ): Either[Err, Map[TopicAndPartition, Short]] = {
    var result = Map[TopicAndPartition, Short]()
    val req = OffsetCommitRequest(groupId, metadata)
    val errs = new Err
    val topicAndPartitions = metadata.keys.toSet
    withBrokers(errs) { consumer =>
      val resp = consumer.commitOffsets(req)
      val respMap = resp.requestInfo
      val needed = topicAndPartitions.diff(result.keys.toSet)
      needed.foreach { tp =>
        respMap.get(tp).foreach { err =>
          if (err == ErrorMapping.NoError) {
            result += tp -> err
          } else {
            errs.append(ErrorMapping.exceptionFor(err))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return Right(result)
      }
    }
    val missing = topicAndPartitions.diff(result.keys.toSet)
    errs.append(new Exception(s"Couldn't set offsets for ${missing}"))
    Left(errs)
  }

  private def withBrokers(errs: Err)(fn: SimpleConsumer => Any): Unit = {
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp)
        fn(consumer)
      } catch {
        case NonFatal(e) =>
          errs.append(e)
      } finally {
        if (consumer != null) consumer.close()
      }
    }
  }
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

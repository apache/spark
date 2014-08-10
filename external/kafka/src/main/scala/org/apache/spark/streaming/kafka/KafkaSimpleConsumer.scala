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

import kafka.api.FetchRequestBuilder
import kafka.api.TopicMetadataRequest
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet
import org.apache.spark.Logging
import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZKStringSerializer
import scala.collection.mutable.ArrayBuffer
import org.apache.zookeeper.CreateMode
import org.I0Itec.zkclient.DataUpdater

@serializable
class KafkaSimpleConsumer(
    zkQuorum: String, 
    groupId: String, 
    topic: String, 
    partition: Int, 
    maxBatchByteSize: Int
  ) extends Logging {
  private var brokers: Seq[String] = _
  private var leader: String = _
  private var consumer: SimpleConsumer = null
  private val soTimeout = 60000
  private val bufferSize = maxBatchByteSize
  private var replicaBrokers: Seq[(String)] = null

  private def init(): Unit = {
    brokers = KafkaSimpleConsumer.getBrokers(zkQuorum)
    val data = findLeaderAndReplicaBrokers(brokers)
    leader = data._1
    replicaBrokers = data._2
    val ipPort = leader.split(":")
    val ip = ipPort(0)
    val port = ipPort(1).toInt
    consumer = new SimpleConsumer(ip, port, soTimeout, bufferSize, groupId)
  }

  private def getOffset(consumer: SimpleConsumer, topic: String,
    partition: Int, whichTime: Long, clientId: Int): Long = {
    val topicAndPartition = new TopicAndPartition(topic, partition);
    consumer.earliestOrLatestOffset(topicAndPartition, whichTime, clientId)
  }

  def getEarliestOffset(): Long = {
    if (consumer == null) {
      init()
    }
    return getOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime, 1)
  }

  def getLatestOffset(): Long = {
    if (consumer == null) {
      init()
    }
    return getOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime, 1)
  }

  private def findNewLeader(
      oldLeader: String, 
      replicaBrokers: Seq[String]
    ): (String, Seq[String]) = {
    for (i <- 0 until 3) {
      var goToSleep = false
      try {
        val data = findLeaderAndReplicaBrokers(replicaBrokers)
        val newLeader = data._1
        if (oldLeader.equalsIgnoreCase(newLeader) && i == 0) {
          goToSleep = true
        }
        return data
      } catch {
        case _: Throwable => goToSleep = true
      }
      if (goToSleep) {
        try {
          Thread.sleep(1000 * (i + 1))
        } catch {
          case _: Throwable =>
        }
      }
    }
    throw new Exception("Unable to find new leader after Broker failure. Exiting")
  }

  def fetch(startPositionOffset: Long): ByteBufferMessageSet = {
    if (consumer == null) {
      init()
    }
    val builder = new FetchRequestBuilder()
    val req = builder.addFetch(topic, partition, startPositionOffset, maxBatchByteSize)
      .clientId(groupId).build()
    val fetchResponse = consumer.fetch(req)
    var numErrors = 0
    if (fetchResponse.hasError) {
      numErrors = numErrors + 1
      val code = fetchResponse.errorCode(topic, partition)
      if (numErrors > 5) {
        throw new Exception("Error fetching data from the Broker:" + leader + " Reason: " + code)
      }
      if (code == ErrorMapping.OffsetOutOfRangeCode) {
        return fetch(getLatestOffset())
      }
      close
      val data = findNewLeader(leader, replicaBrokers)
      leader = data._1
      replicaBrokers = data._2
      init()
      return fetch(startPositionOffset)
    }
    fetchResponse.messageSet(topic, partition)
  }

  private def findLeaderAndReplicaBrokers(broker: String): (String, Seq[(String)]) = {
    var result: (String, Seq[String]) = null
    val tmp = broker.split(":")
    val ip = tmp(0)
    val port = tmp(1).toInt
    var consumer: SimpleConsumer = null
    try {
      consumer = new SimpleConsumer(ip, port, 100000, 64 * 1024, "leaderLookup")
      val req = new TopicMetadataRequest(Seq(topic), 1)
      val resp = consumer.send(req);
      val metaData = resp.topicsMetadata
      for (
        item <- metaData if (result == null);
        part <- item.partitionsMetadata if (part.partitionId == partition)
      ) {
        part.leader match {
          case Some(leader) => {
            result = (leader.host + ":" + leader.port, part.replicas.map(brk => (brk.host + ":" 
                + brk.port)))
          }
          case None =>
        }
      }
      result
    } catch {
      case e: Throwable => throw new Exception("Error communicating with Broker [" + broker
        + "] to find Leader for [" + topic + "] Reason: " + e)
    } finally {
      if (consumer != null) {
        consumer.close
      }
    }
  }

  private def findLeaderAndReplicaBrokers(brokers: Seq[String]): (String, Seq[(String)]) = {
    var result: (String, Seq[String]) = null
    for (broker <- brokers if (result == null)) {
      result = findLeaderAndReplicaBrokers(broker)
    }
    if (result == null) {
      throw new Exception("not found leader.")
    } else {
      result
    }
  }

  def close: Unit = {
    if (consumer != null) {
      consumer.close
    }
  }

  def commitOffsetToZookeeper(offset: Long) {
    val dir = "/consumers/" + groupId + "/offsets/" + topic + "/" + partition
    val zk = new ZkClient(zkQuorum, 30 * 1000, 30 * 1000, ZKStringSerializer)
    try {
      if (zk.exists(dir) == false) {
        zk.createPersistent(dir,true)
      }
      zk.writeData(dir, offset.toString)
    } catch {
      case e: Throwable => logWarning("Error saving Kafka offset to Zookeeper dir: " + dir, e)
    } finally {
      zk.close()
    }
  }
}

object KafkaSimpleConsumer extends Logging {
  private def getBrokerFromJson(json: String): String = {
    import scala.util.parsing.json.JSON
    val broker = JSON.parseFull(json)
    broker match {
      case Some(m: Map[String, Any]) =>
        m("host") + ":" + (m("port").asInstanceOf[Double].toInt).toString
      case _ => throw new Exception("incorrect broker info in zookeeper")
    }
  }

  def getBrokers(zkQuorum: String): Seq[String] = {
    val list = new ArrayBuffer[String]()
    val dir = "/brokers/ids"
    val zk = new ZkClient(zkQuorum, 30 * 1000, 30 * 1000, ZKStringSerializer)
    try {
      if (zk.exists(dir)) {
        val ids = zk.getChildren(dir)
        import scala.collection.JavaConversions._
        for (id <- ids) {
          val json = zk.readData[String](dir + "/" + id)
          list.append(getBrokerFromJson(json))
        }
      }
    } catch {
      case e: Throwable => logWarning("Error reading Kafka brokers Zookeeper data", e)
    } finally {
      zk.close()
    }
    list.toSeq
  }

  def getEndOffsetPositionFromZookeeper(groupId: String, zkQuorum: String, topic: String, 
      partition: Int): Long = {
    val dir = "/consumers/" + groupId + "/offsets/" + topic + "/" + partition
    val zk = new ZkClient(zkQuorum, 30 * 1000, 30 * 1000, ZKStringSerializer)
    try {
      if (zk.exists(dir)) {
        val offset = zk.readData[String](dir)
        return offset.toInt
      }
    } catch {
      case e: Throwable => logWarning("Error reading Kafka brokers Zookeeper data", e)
    } finally {
      zk.close()
    }
    0L
  }

  def getTopicPartitionList(zkQuorum: String, topic: String): Seq[Int] = {
    val list = new ArrayBuffer[Int]()
    val dir = "/brokers/topics/" + topic + "/partitions"
    val zk = new ZkClient(zkQuorum, 30 * 1000, 30 * 1000, ZKStringSerializer)
    try {
      if (zk.exists(dir)) {
        val ids = zk.getChildren(dir)
        import scala.collection.JavaConversions._
        for (id <- ids) {
          list.append(id.toInt)
        }
      }
    } catch {
      case e: Throwable => logWarning("Error reading Kafka partitions list Zookeeper data", e)
    } finally {
      zk.close()
    }
    list.toSeq
  }
}


package org.apache.spark.streaming.kafka

import kafka.api.FetchRequestBuilder
import kafka.api.TopicMetadataRequest
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.ByteBufferMessageSet

@serializable
class KafkaSimpleConsumer(brokers: Seq[String], topic: String, partition: Int, maxBatchByteSize: Int) {
  private var leader: String = _
  private var clientName: String = _
  private var consumer: SimpleConsumer = null
  private val soTimeout = 60000
  private var bufferSize = 1024
  private var replicaBrokers: Seq[(String)] = null;

  private def init(): Unit = {
    val data = KafkaSimpleConsumer.findLeaderAndReplicaBrokers(brokers, topic, partition)
    leader = data._1
    replicaBrokers = data._2
    val ipPort = leader.split(":")
    val ip = ipPort(0)
    val port = ipPort(1).toInt
    clientName = "client-" + topic + "-" + partition;
    consumer = new SimpleConsumer(ip, port, soTimeout, bufferSize, clientName);
  }

  def getEarliestOffset(): Long = {
    if (consumer == null) {
      init();
    }
    return KafkaSimpleConsumer.getOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime, 1);
  }

  def getLatestOffset(): Long = {
    if (consumer == null) {
      init();
    }
    return KafkaSimpleConsumer.getOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime, 1);
  }

  def findNewLeader(oldLeader: String,
    replicaBrokers: Seq[String], topic: String, partition: Int): (String, Seq[String]) = {
    for (i <- 0 until 3) {
      var goToSleep = false;
      try {
        val data = KafkaSimpleConsumer.findLeaderAndReplicaBrokers(
          replicaBrokers, topic, partition);
        val newLeader = data._1
        if (oldLeader.equalsIgnoreCase(newLeader) && i == 0) {
          goToSleep = true
        }
        return data
      } catch {
        case _: Throwable => goToSleep = true;
      }
      if (goToSleep) {
        try {
          Thread.sleep(1000 * (i + 1));
        } catch {
          case _: Throwable =>
        }
      }
    }
    throw new Exception(
      "Unable to find new leader after Broker failure. Exiting");
  }

  def fetch(startPositionOffset: Long): ByteBufferMessageSet = {
    if (consumer == null) {
      init();
    }
    val builder = new FetchRequestBuilder();
    val req = builder
      .addFetch(topic, partition, startPositionOffset, maxBatchByteSize)
      .clientId(clientName).build();
    val fetchResponse = consumer.fetch(req);
    var numErrors = 0;
    if (fetchResponse.hasError) {
      numErrors = numErrors + 1
      val code = fetchResponse.errorCode(topic, partition);
      if (numErrors > 5) {
        throw new Exception("Error fetching data from the Broker:"
          + leader + " Reason: " + code);
      }
      if (code == ErrorMapping.OffsetOutOfRangeCode) {
        return fetch(getLatestOffset());
      }
      close
      val data = findNewLeader(leader, replicaBrokers, topic, partition)
      leader = data._1
      replicaBrokers = data._2
      init()
      return fetch(startPositionOffset)
    }
    val set = fetchResponse
      .messageSet(topic, partition);
    return set;

  }

  def close: Unit = {
    if (consumer != null) {
      consumer.close
    }
  }
}

object KafkaSimpleConsumer {
  def findLeaderAndReplicaBrokers(brokers: Seq[String], topic: String, partition: Int): (String, Seq[(String)]) = {
    for (broker <- brokers) {
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
          item <- metaData;
          part <- item.partitionsMetadata if (part.partitionId == partition)
        ) {
          part.leader match {
            case Some(leader) => {
              return (leader.host + ":" + leader.port, part.replicas.map(brk => (brk.host + ":" + brk.port)))
            }
            case None =>
          }
        }
      } catch {
        case e: Throwable => throw new Exception("Error communicating with Broker [" + broker
          + "] to find Leader for [" + topic + "] Reason: " + e);
      } finally {
        if (consumer != null)
          consumer.close
      }
    }
    throw new Exception("not found leader.");
  }

  def getOffset(consumer: SimpleConsumer, topic: String,
    partition: Int, whichTime: Long, clientId: Int): Long = {
    val topicAndPartition = new TopicAndPartition(topic, partition);
    consumer.earliestOrLatestOffset(topicAndPartition, whichTime, clientId)
  }
}
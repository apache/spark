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
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.{Collections, Properties}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{Logging, SparkEnv}

import scala.collection.{Map, mutable}
import scala.reflect.ClassTag

/**
 * ReliableKafkaReceiver offers the ability to reliably store data into BlockManager
 * without loss.
 * It is turned off by default and will be enabled when
 * spark.streaming.receiver.writeAheadLog.enable is true. The difference compared to KafkaReceiver
 * is that this receiver manages topic-partition/offset itself and updates the offset information
 * after data is reliably stored as write-ahead log. Offsets will only be updated when data is
 * reliably stored, so the potential data loss problem of KafkaReceiver can be eliminated.
 *
 * Note: ReliableKafkaReceiver will set auto.commit.enable to false to turn off automatic offset
 * commit mechanism in Kafka consumer. So setting this configuration manually within kafkaParams
 * will not take effect.
 */
private[streaming]
class ReliableKafkaReceiver[K: ClassTag, V: ClassTag](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    storageLevel: StorageLevel) extends Receiver[(K, V)](storageLevel) with Logging {

  private val groupId = kafkaParams("group.id")
  private val AUTO_OFFSET_COMMIT = ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG

  private def conf = SparkEnv.get.conf

  private var kafkaCluster: KafkaCluster[_, _] = null
  private val lock: ReentrantLock = new ReentrantLock()

  private var props: Properties = null

  private val KAFKA_DEFAULT_POLL_TIME: String = "0"
  private val pollTime = kafkaParams.get("spark.kafka.poll.time")
   .getOrElse(KAFKA_DEFAULT_POLL_TIME).toInt

  /**
   * A HashMap to manage the offset for each topic/partition, this HashMap is called in
   * synchronized block, so mutable HashMap will not meet concurrency issue.
   */
  private var topicPartitionOffsetMap: mutable.HashMap[TopicAndPartition, Long] = null

  /** A concurrent HashMap to store the stream block id and related offset snapshot. */
  private var blockOffsetMap: ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]] = null

  /**
   * Manage the BlockGenerator in receiver itself for better managing block store and offset
   * commit.
   */
  private var blockGenerator: BlockGenerator = null

  /** Thread pool running the handlers for receiving message from multiple topics and partitions. */
  private var messageHandlerThreadPool: ThreadPoolExecutor = null

  private var topicAndPartitionConsumerMap:
    mutable.HashMap[TopicAndPartition, KafkaConsumer[K, V]] = null

  private var consumerAndLockMap:
    mutable.HashMap[KafkaConsumer[K, V], ReentrantLock] = null

  override def onStart(): Unit = {
    logInfo(s"Starting Kafka Consumer Stream with group: $groupId")
    // Initialize the topic-partition / offset hash map.
    topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]

    topicAndPartitionConsumerMap = new mutable.HashMap[TopicAndPartition, KafkaConsumer[K, V]]
    consumerAndLockMap = new mutable.HashMap[KafkaConsumer[K, V], ReentrantLock]

    // Initialize the stream block id / offset snapshot hash map.
    blockOffsetMap = new ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]]()

    // Initialize the block generator for storing Kafka message.
    blockGenerator = supervisor.createBlockGenerator(new GeneratedBlockHandler)

    if (kafkaParams.contains(AUTO_OFFSET_COMMIT) && kafkaParams(AUTO_OFFSET_COMMIT) == "true") {
      logWarning(s"$AUTO_OFFSET_COMMIT should be set to false in ReliableKafkaReceiver, " +
       "otherwise we will manually set it to false to turn off auto offset commit in Kafka")
    }

    props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    // Manually set "auto.commit.enable" to "false" no matter user explicitly set it to true,
    // we have to make sure this property is set to false to turn off auto commit mechanism in
    // Kafka.
    props.setProperty(AUTO_OFFSET_COMMIT, "false")

    messageHandlerThreadPool = ThreadUtils.newDaemonFixedThreadPool(
      topics.values.sum, "KafkaMessageHandler")

    blockGenerator.start()

    kafkaCluster = new KafkaCluster[K, V](kafkaParams.toMap)

    try {
      // Start the messages handler for each partition
      val topicAndPartitions = kafkaCluster.getPartitions(topics.keys.toSet).right.toOption
      val iter = topicAndPartitions.get.iterator
      while (iter.hasNext) {
        val topicAndPartition = iter.next()
        val newConsumer = new KafkaConsumer[K, V](props)
        topicAndPartitionConsumerMap.put(
          topicAndPartition,
          newConsumer
        )
        consumerAndLockMap.put(newConsumer, new ReentrantLock())
        newConsumer.subscribe(Collections.singletonList[String](topicAndPartition.topic))
        messageHandlerThreadPool.submit(new MessageHandler(newConsumer))
      }
    } finally {
      messageHandlerThreadPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  override def onStop(): Unit = {
    if (messageHandlerThreadPool != null) {
      messageHandlerThreadPool.shutdown()
      messageHandlerThreadPool = null
    }

    if (blockGenerator != null) {
      blockGenerator.stop()
      blockGenerator = null
    }

    if (topicPartitionOffsetMap != null) {
      topicPartitionOffsetMap.clear()
      topicPartitionOffsetMap = null
    }

    if (blockOffsetMap != null) {
      blockOffsetMap.clear()
      blockOffsetMap = null
    }
  }

  /** Store a Kafka message and the associated metadata as a tuple. */
  private def storeMessageAndMetadata(msgAndMetadata: MessageAndMetadata[K, V]): Unit = {
    val topicAndPartition = TopicAndPartition(msgAndMetadata.topic, msgAndMetadata.partition)
    val data = (msgAndMetadata.key, msgAndMetadata.message)
    val metadata = (topicAndPartition, msgAndMetadata.offset)
    blockGenerator.addDataWithCallback(data, metadata)
  }

  private def storeConsumerRecord(consumerRecord: ConsumerRecord[K, V]): Unit = {
    val topicAndPartition = TopicAndPartition(consumerRecord.topic, consumerRecord.partition)
    val data = (consumerRecord.key, consumerRecord.value())
    val metadata = (topicAndPartition, consumerRecord.offset)
    blockGenerator.addDataWithCallback(data, metadata)
  }

  /** Update stored offset */
  private def updateOffset(topicAndPartition: TopicAndPartition, offset: Long): Unit = {
    topicPartitionOffsetMap.put(topicAndPartition, offset)
  }

  /**
   * Remember the current offsets for each topic and partition. This is called when a block is
   * generated.
   */
  private def rememberBlockOffsets(blockId: StreamBlockId): Unit = {
    // Get a snapshot of current offset map and store with related block id.
    val offsetSnapshot = topicPartitionOffsetMap.toMap
    blockOffsetMap.put(blockId, offsetSnapshot)
    topicPartitionOffsetMap.clear()
  }

  /**
   * Store the ready-to-be-stored block and commit the related offsets to Kafka. This method
   * will try a fixed number of times to push the block. If the push fails,
   * the receiver is stopped.
   */
  private def storeBlockAndCommitOffset(
    blockId: StreamBlockId,
    arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
    var count = 0
    var pushed = false
    var exception: Exception = null
    while (!pushed && count <= 3) {
      try {
        store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[(K, V)]])
        pushed = true
      } catch {
        case ex: Exception =>
          count += 1
          exception = ex
      }
    }
    if (pushed) {
      Option(blockOffsetMap.get(blockId)).foreach(commitOffset)
      blockOffsetMap.remove(blockId)
    } else {
      stop("Error while storing block into Spark", exception)
    }
  }

  /**
   * Commit the offset of Kafka's topic/partition
   */
  private def commitOffset(offsetMap: Map[TopicAndPartition, Long]): Unit = {
    val offsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    for ((topicAndPart, offset) <- offsetMap) {
      val kafkaConsumer = topicAndPartitionConsumerMap.getOrElse(topicAndPart,
        throw new RuntimeException(s"Failed to get consumer for $topicAndPart"))

      val topicPartition = new TopicPartition(topicAndPart.topic, topicAndPart.partition)
      val offsetAndMetadata = new OffsetAndMetadata(offset)
      offsets.put(topicPartition, offsetAndMetadata)
      val lock = consumerAndLockMap(kafkaConsumer)
      lock.lock()
      try {
        kafkaConsumer.commitSync(offsets)
      } finally {
        lock.unlock()
      }
    }

  }

  /** Class to handle received Kafka message. */
  private final class MessageHandler(consumer: KafkaConsumer[K, V]) extends Runnable {
    override def run(): Unit = {
      var records: ConsumerRecords[K, V] = null
      val lock = consumerAndLockMap(consumer)
      while (!isStopped) {
        try {
          while (true) {
            lock.lock()
            try {
              records = consumer.poll(pollTime)
            } finally {
              lock.unlock()
            }
            val iterator = records.iterator()
            while (iterator.hasNext) {
              val record: ConsumerRecord[K, V] = iterator.next()
              storeConsumerRecord(record)
            }
          }
        } catch {
          case e: Exception => {
            reportError("Error handling message", e)
          }

        }
      }
    }
  }

  /** Class to handle blocks generated by the block generator. */
  private final class GeneratedBlockHandler extends BlockGeneratorListener {

    def onAddData(data: Any, metadata: Any): Unit = {
      // Update the offset of the data that was added to the generator
      if (metadata != null) {
        val (topicAndPartition, offset) = metadata.asInstanceOf[(TopicAndPartition, Long)]
        updateOffset(topicAndPartition, offset)
      }
    }

    def onGenerateBlock(blockId: StreamBlockId): Unit = {
      // Remember the offsets of topics/partitions when a block has been generated
      rememberBlockOffsets(blockId)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      // Store block and commit the blocks offset
      storeBlockAndCommitOffset(blockId, arrayBuffer)
    }

    def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }

}

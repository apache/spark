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

import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, Executors}

import scala.collection.Map
import scala.collection.mutable
import scala.reflect.{classTag, ClassTag}

import kafka.common.TopicAndPartition
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}
import kafka.serializer.Decoder
import kafka.utils.{ZkUtils, ZKGroupTopicDirs, ZKStringSerializer, VerifiableProperties}
import org.I0Itec.zkclient.ZkClient

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.storage.{StreamBlockId, StorageLevel}
import org.apache.spark.streaming.receiver.{BlockGeneratorListener, BlockGenerator, Receiver}

private[streaming]
class ReliableKafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    storageLevel: StorageLevel)
    extends Receiver[Any](storageLevel) with Logging {

  /** High level consumer to connect to Kafka */
  private var consumerConnector: ConsumerConnector = null

  /** zkClient to connect to Zookeeper to commit the offsets */
  private var zkClient: ZkClient = null

  private val groupId = kafkaParams("group.id")

  private lazy val env = SparkEnv.get

  private val AUTO_OFFSET_COMMIT = "auto.commit.enable"

  /** A HashMap to manage the offset for each topic/partition, this HashMap is called in
    * synchronized block, so mutable HashMap will not meet concurrency issue */
  private lazy val topicPartitionOffsetMap = new mutable.HashMap[TopicAndPartition, Long]

  /** A concurrent HashMap to store the stream block id and related offset snapshot */
  private lazy val blockOffsetMap =
    new ConcurrentHashMap[StreamBlockId, Map[TopicAndPartition, Long]]

  private lazy val blockGeneratorListener = new BlockGeneratorListener {
    override def onStoreData(data: Any, metadata: Any): Unit = {
      if (metadata != null) {
        val kafkaMetadata = metadata.asInstanceOf[(TopicAndPartition, Long)]
        topicPartitionOffsetMap.put(kafkaMetadata._1, kafkaMetadata._2)
      }
    }

    override def onGenerateBlock(blockId: StreamBlockId): Unit = {
      // Get a snapshot of current offset map and store with related block id. Since this hook
      // function is called in synchronized block, so we can get the snapshot without explicit lock.
      val offsetSnapshot = topicPartitionOffsetMap.toMap
      blockOffsetMap.put(blockId, offsetSnapshot)
      topicPartitionOffsetMap.clear()
    }

    override def onPushBlock(blockId: StreamBlockId, arrayBuffer: mutable.ArrayBuffer[_]): Unit = {
      store(arrayBuffer.asInstanceOf[mutable.ArrayBuffer[Any]])

      // Commit and remove the related offsets.
      Option(blockOffsetMap.get(blockId)).foreach { offsetMap =>
        commitOffset(offsetMap)
      }
      blockOffsetMap.remove(blockId)
    }

    override def onError(message: String, throwable: Throwable): Unit = {
      reportError(message, throwable)
    }
  }

  /** Manage the BlockGenerator in receiver itself for better managing block store and offset
    * commit */
  private lazy val blockGenerator = new BlockGenerator(blockGeneratorListener, streamId, env.conf)

  override def onStop(): Unit = {
    if (consumerConnector != null) {
      consumerConnector.shutdown()
      consumerConnector = null
    }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    blockGenerator.stop()
  }

  override def onStart(): Unit = {
    logInfo(s"Starting Kafka Consumer Stream with group: $groupId")

    if (kafkaParams.contains(AUTO_OFFSET_COMMIT) && kafkaParams(AUTO_OFFSET_COMMIT) == "true") {
      logWarning(s"$AUTO_OFFSET_COMMIT should be set to false in ReliableKafkaReceiver, " +
        "otherwise we will manually set it to false to turn off auto offset commit in Kafka")
    }

    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    // Manually set "auto.commit.enable" to "false" no matter user explicitly set it to true,
    // we have to make sure this property is set to false to turn off auto commit mechanism in
    // Kafka.
    props.setProperty(AUTO_OFFSET_COMMIT, "false")

    val consumerConfig = new ConsumerConfig(props)

    assert(consumerConfig.autoCommitEnable == false)

    logInfo(s"Connecting to Zookeeper: ${consumerConfig.zkConnect}")
    consumerConnector = Consumer.create(consumerConfig)
    logInfo(s"Connected to Zookeeper: ${consumerConfig.zkConnect}")

    zkClient = new ZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs,
      consumerConfig.zkConnectionTimeoutMs, ZKStringSerializer)

    // start BlockGenerator
    blockGenerator.start()

    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]

    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    val topicMessageStreams = consumerConnector.createMessageStreams(
      topics, keyDecoder, valueDecoder)

    val executorPool = Executors.newFixedThreadPool(topics.values.sum)

    try {
      topicMessageStreams.values.foreach { streams =>
        streams.foreach { stream =>
          executorPool.submit(new Runnable {
            override def run(): Unit = {
              logInfo(s"Starting message process thread ${Thread.currentThread().getId}.")
              try {
                for (msgAndMetadata <- stream) {
                  val topicAndPartition = TopicAndPartition(
                    msgAndMetadata.topic, msgAndMetadata.partition)
                  val metadata = (topicAndPartition, msgAndMetadata.offset)

                  blockGenerator += ((msgAndMetadata.key, msgAndMetadata.message), metadata)
                }
              } catch {
                case e: Throwable => logError("Error handling message; existing", e)
              }
            }
          })
        }
      }
    } finally {
      executorPool.shutdown()
    }
  }

  /**
   * Commit the offset of Kafka's topic/partition, the commit mechanism follow Kafka 0.8.x's
   * metadata schema in Zookeeper.
   */
  private def commitOffset(offsetMap: Map[TopicAndPartition, Long]): Unit = {
    if (zkClient == null) {
      logError(s"zkClient $zkClient should be initialized at started")
      return
    }

    for ((topicAndPart, offset) <- offsetMap) {
      try {
        val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPart.topic)
        val zkPath = s"${topicDirs.consumerOffsetDir}/${topicAndPart.partition}"

        ZkUtils.updatePersistentPath(zkClient, zkPath, offset.toString)
      } catch {
        case t: Throwable => logWarning(s"Exception during commit offset $offset for topic" +
          s"${topicAndPart.topic}, partition ${topicAndPart.partition}", t)
      }

      logInfo(s"Committed offset ${offset} for topic ${topicAndPart.topic}, " +
        s"partition ${topicAndPart.partition}")
    }
  }
}

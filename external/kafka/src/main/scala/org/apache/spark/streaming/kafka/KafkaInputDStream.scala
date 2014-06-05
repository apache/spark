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

import scala.collection.Map
import scala.reflect.ClassTag

import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer._
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient._

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

/**
 * Input stream that pulls messages from a Kafka Broker.
 *
 * @param kafkaParams Map of kafka configuration parameters.
 *                    See: http://kafka.apache.org/configuration.html
 * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
 * in its own thread.
 * @param storageLevel RDD storage level.
 */
private[streaming]
class KafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: Manifest,
  T <: Decoder[_]: Manifest](
    @transient ssc_ : StreamingContext,
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging {

  def getReceiver(): Receiver[(K, V)] = {
    new KafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
        .asInstanceOf[Receiver[(K, V)]]
  }
}

private[streaming]
class KafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: Manifest,
  T <: Decoder[_]: Manifest](
    kafkaParams: Map[String, String],
    topics: Map[String, Int],
    storageLevel: StorageLevel
  ) extends Receiver[Any](storageLevel) with Logging {

  // Connection to Kafka
  var consumerConnector : ConsumerConnector = null

  def onStop() {
    if (consumerConnector != null) {
      consumerConnector.shutdown()
    }
  }

  def onStart() {

    logInfo("Starting Kafka Consumer Stream with group: " + kafkaParams("group.id"))

    // Kafka connection properties
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    val zkConnect = kafkaParams("zookeeper.connect")
    // Create the connection to the cluster
    logInfo("Connecting to Zookeeper: " + zkConnect)
    val consumerConfig = new ConsumerConfig(props)
    consumerConnector = Consumer.create(consumerConfig)
    logInfo("Connected to " + zkConnect)

    // When auto.offset.reset is defined, it is our responsibility to try and whack the
    // consumer group zk node.
    if (kafkaParams.contains("auto.offset.reset")) {
      tryZookeeperConsumerGroupCleanup(zkConnect, kafkaParams("group.id"))
    }

    val keyDecoder = manifest[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]
    val valueDecoder = manifest[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    // Create Threads for each Topic/Message Stream we are listening
    val topicMessageStreams = consumerConnector.createMessageStreams(
      topics, keyDecoder, valueDecoder)

    val executorPool = Executors.newFixedThreadPool(topics.values.sum)
    try {
      // Start the messages handler for each partition
      topicMessageStreams.values.foreach { streams =>
        streams.foreach { stream => executorPool.submit(new MessageHandler(stream)) }
      }
    } finally {
      executorPool.shutdown() // Just causes threads to terminate after work is done
    }
  }

  // Handles Kafka Messages
  private class MessageHandler[K: ClassTag, V: ClassTag](stream: KafkaStream[K, V])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      try {
        for (msgAndMetadata <- stream) {
          store((msgAndMetadata.key, msgAndMetadata.message))
        }
      } catch {
        case e: Throwable => logError("Error handling message; exiting", e)
      }
    }
  }

  // It is our responsibility to delete the consumer group when specifying auto.offset.reset. This
  // is because Kafka 0.7.2 only honors this param when the group is not in zookeeper.
  //
  // The kafka high level consumer doesn't expose setting offsets currently, this is a trick copied
  // from Kafka's ConsoleConsumer. See code related to 'auto.offset.reset' when it is set to
  // 'smallest'/'largest':
  // scalastyle:off
  // https://github.com/apache/kafka/blob/0.7.2/core/src/main/scala/kafka/consumer/ConsoleConsumer.scala
  // scalastyle:on
  private def tryZookeeperConsumerGroupCleanup(zkUrl: String, groupId: String) {
    val dir = "/consumers/" + groupId
    logInfo("Cleaning up temporary Zookeeper data under " + dir + ".")
    val zk = new ZkClient(zkUrl, 30*1000, 30*1000, ZKStringSerializer)
    try {
      zk.deleteRecursive(dir)
    } catch {
      case e: Throwable => logWarning("Error cleaning up temporary Zookeeper data", e)
    } finally {
      zk.close()
    }
  }
}

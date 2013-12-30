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

package org.apache.spark.streaming.dstream

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer._
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient._

import scala.collection.Map
import scala.reflect.ClassTag

/**
 * Input stream that pulls messages from a Kafka Broker.
 *
 * @param kafkaParams Map of kafka configuration paramaters. See: http://kafka.apache.org/configuration.html
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
  ) extends NetworkInputDStream[(K, V)](ssc_) with Logging {

  def getReceiver(): NetworkReceiver[(K, V)] = {
    new KafkaReceiver[K, V, U, T](kafkaParams, topics, storageLevel)
        .asInstanceOf[NetworkReceiver[(K, V)]]
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
  ) extends NetworkReceiver[Any] {

  // Handles pushing data into the BlockManager
  lazy protected val blockGenerator = new BlockGenerator(storageLevel)
  // Connection to Kafka
  var consumerConnector : ConsumerConnector = null

  def onStop() {
    blockGenerator.stop()
  }

  def onStart() {

    blockGenerator.start()

    // In case we are using multiple Threads to handle Kafka Messages
    val executorPool = Executors.newFixedThreadPool(topics.values.reduce(_ + _))

    logInfo("Starting Kafka Consumer Stream with group: " + kafkaParams("group.id"))

    // Kafka connection properties
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    // Create the connection to the cluster
    logInfo("Connecting to Zookeper: " + kafkaParams("zookeeper.connect"))
    val consumerConfig = new ConsumerConfig(props)
    consumerConnector = Consumer.create(consumerConfig)
    logInfo("Connected to " + kafkaParams("zookeeper.connect"))

    // When autooffset.reset is defined, it is our responsibility to try and whack the
    // consumer group zk node.
    if (kafkaParams.contains("auto.offset.reset")) {
      tryZookeeperConsumerGroupCleanup(kafkaParams("zookeeper.connect"), kafkaParams("group.id"))
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


    // Start the messages handler for each partition
    topicMessageStreams.values.foreach { streams =>
      streams.foreach { stream => executorPool.submit(new MessageHandler(stream)) }
    }
  }

  // Handles Kafka Messages
  private class MessageHandler[K: ClassTag, V: ClassTag](stream: KafkaStream[K, V])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      for (msgAndMetadata <- stream) {
        blockGenerator += (msgAndMetadata.key, msgAndMetadata.message)
      }
    }
  }

  // It is our responsibility to delete the consumer group when specifying autooffset.reset. This is because
  // Kafka 0.7.2 only honors this param when the group is not in zookeeper.
  //
  // The kafka high level consumer doesn't expose setting offsets currently, this is a trick copied from Kafkas'
  // ConsoleConsumer. See code related to 'autooffset.reset' when it is set to 'smallest'/'largest':
  // https://github.com/apache/kafka/blob/0.7.2/core/src/main/scala/kafka/consumer/ConsoleConsumer.scala
  private def tryZookeeperConsumerGroupCleanup(zkUrl: String, groupId: String) {
    try {
      val dir = "/consumers/" + groupId
      logInfo("Cleaning up temporary zookeeper data under " + dir + ".")
      val zk = new ZkClient(zkUrl, 30*1000, 30*1000, ZKStringSerializer)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case _ : Throwable => // swallow
    }
  }
}

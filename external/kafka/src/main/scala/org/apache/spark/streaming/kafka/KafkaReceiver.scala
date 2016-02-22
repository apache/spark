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
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}

import kafka.consumer._
import kafka.serializer.Decoder
import kafka.utils.{VerifiableProperties, ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.receiver.{BlockGenerator, BlockGeneratorListener, Receiver}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{Logging, SparkEnv}

import scala.collection.{Map, mutable}
import scala.reflect.{ClassTag, classTag}

/**
  * KafkaReceiver.
  */
private[streaming]
class KafkaReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_] : ClassTag,
  T <: Decoder[_] : ClassTag](
     kafkaParams: Map[String, String],
     topics: Map[String, Int],
     storageLevel: StorageLevel,
     useWhiteListTopicFilter: Boolean = false
   ) extends Receiver[(K, V)](storageLevel) with Logging {

  // Connection to Kafka
  var consumerConnector: ConsumerConnector = null

  def onStop() {
    if (consumerConnector != null) {
      consumerConnector.shutdown()
      consumerConnector = null
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

    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[K]]

    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(consumerConfig.props)
      .asInstanceOf[Decoder[V]]

    val messageHandlerThreadPool =
      ThreadUtils.newDaemonFixedThreadPool(topics.values.sum, "KafkaMessageHandler")

    try {
      createMessageHandlers(keyDecoder, valueDecoder).foreach(messageHandlerThreadPool.submit)
    } finally {
      // Just causes threads to terminate after work is done
      messageHandlerThreadPool.shutdown()
    }
  }

  private def createMessageHandlers(
      keyDecoder: Decoder[K], valueDecoder: Decoder[V]) : Iterable[MessageHandler] = {
    if (!useWhiteListTopicFilter) {
      val topicMessageStreams = consumerConnector.createMessageStreams(
        topics, keyDecoder, valueDecoder)

      topicMessageStreams.values.flatten { streams =>
        streams.map { stream =>
          new MessageHandler(stream)
        }
      }
    } else {
      val topicsHeadOption = topics.headOption
      assert(topicsHeadOption.isDefined)

      val topicsHead = topicsHeadOption.get
      val topicFilter = Whitelist(topicsHead._1);

      val topicMessageStreams = consumerConnector.createMessageStreamsByFilter(
        topicFilter, topicsHead._2, keyDecoder, valueDecoder)

      topicMessageStreams.map { stream =>
        new MessageHandler(stream)
      }
    }
  }

  // Handles Kafka messages
  private class MessageHandler(stream: KafkaStream[K, V])
    extends Runnable {
    def run() {
      logInfo("Starting MessageHandler.")
      try {
        val streamIterator = stream.iterator()
        while (streamIterator.hasNext()) {
          val msgAndMetadata = streamIterator.next()
          store((msgAndMetadata.key, msgAndMetadata.message))
        }
      } catch {
        case e: Throwable => reportError("Error handling message; exiting", e)
      }
    }
  }

}

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

import scala.reflect.{classTag, ClassTag}

import kafka.api.{FetchRequestBuilder, FetchResponse}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig, ConsumerConnector, SimpleConsumer}
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, DStreamGraph, StreamingContext}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.streaming.scheduler.rate.RateEstimator
import org.apache.spark.util.{NextIterator, ThreadUtils}


/**
 * Input stream that pulls messages from a Kafka Broker.
 *
 * @param kafkaParams Map of kafka configuration parameters.
 *                    See: http://kafka.apache.org/configuration.html
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
 * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
 *  starting point of the stream
 * @param storageLevel RDD storage level.
 */
private[streaming]
class KafkaDirectInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    ssc_ : StreamingContext,
    kafkaParams: Map[String, String],
    val fromOffsets: Map[TopicAndPartition, Long],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[(K, V)](ssc_) with Logging {

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka direct receiver stream [$id]"

  def getReceiver(): Receiver[(K, V)] = {
    new KafkaDirectReceiver[K, V, U, T](ssc_.conf, context.graph.batchDuration, kafkaParams,
      fromOffsets, storageLevel)
  }
}

private[streaming]
class KafkaDirectReceiver[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag](
    conf: SparkConf,
    duration: Duration,
    kafkaParams: Map[String, String],
    val fromOffsets: Map[TopicAndPartition, Long],
    storageLevel: StorageLevel
  ) extends Receiver[(K, V)](storageLevel) with KafkaDirect with Logging {

  override val maxRetries = conf.getInt(
    "spark.streaming.kafka.maxRetries", 1)

  override val maxRateLimitPerPartition: Int = conf.getInt(
      "spark.streaming.kafka.maxRatePerPartition", 0)

  override def maxMessagesPerPartition: Option[Long] = {
    val numPartitions = fromOffsets.keys.size

    val effectiveRateLimitPerPartition =
      Math.max(0, maxRateLimitPerPartition)

    if (effectiveRateLimitPerPartition > 0 && duration != null) {
      val secsPerBatch = duration.milliseconds.toDouble / 1000
      Some((secsPerBatch * effectiveRateLimitPerPartition).toLong)
    } else {
      None
    }
  }

  def onStop() {
  }

  def onStart() {
    logInfo("Starting Kafka Consumer Direct Stream")

    val executorPool =
      ThreadUtils.newDaemonFixedThreadPool(fromOffsets.keys.size, "KafkaMessageHandler")
    try {
      // Start the messages handler
      fromOffsets.keys.foreach { partition =>
        executorPool.submit(new MessageHandler(partition, fromOffsets(partition)))
      }
    } finally {
      executorPool.shutdown() // Just causes thread to terminate after work is done
    }
  }

  // Handles Kafka messages
  private class MessageHandler(partition: TopicAndPartition, fromOffset: Long)
    extends Runnable {
    val kc = new KafkaCluster(kafkaParams)
    var currentOffset: Long = fromOffset
    def run() {
      logInfo("Starting MessageHandler.")

      while(true) {
        try {
          val untilOffsets =
            clamp(latestLeaderOffsets(kc, Set(partition), maxRetries), (_) => currentOffset)
          if (currentOffset != untilOffsets(partition).offset) {
            val iter =
              new KafkaIterator[K, V, U, T](
                kc,
                None,
                None,
                partition.topic,
                partition.partition,
                currentOffset,
                untilOffsets(partition).offset)

            var next: MessageAndMetadata[K, V] = iter.getNext()
            while (next != null) {
              store((next.key, next.message))
              next = iter.getNext()
            }
            currentOffset = untilOffsets(partition).offset
          }
        } catch {
          case e: Throwable =>
            reportError("Error handling message; exiting", e)
        }
      }
    }
  }
}

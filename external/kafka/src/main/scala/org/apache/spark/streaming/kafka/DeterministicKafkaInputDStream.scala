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

import scala.annotation.tailrec
import scala.reflect.{classTag, ClassTag}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.kafka.{KafkaCluster, KafkaRDD}
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._

/** A stream of {@link org.apache.spark.rdd.kafka.KafkaRDD} where
  * each given Kafka topic/partition corresponds to an RDD partition.
  * The spark configuration spark.streaming.receiver.maxRate gives the maximum number of messages
  * per second that each '''partition''' will accept.
  * Starting offsets are specified in advance,
  * and this DStream is not responsible for committing offsets,
  * so that you can control exactly-once semantics.
  * For an easy interface to Kafka-managed offsets,
  *  see {@link org.apache.spark.rdd.kafka.KafkaCluster}
  * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
  * configuration parameters</a>.
  *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
  *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
  * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
  *  starting point of the stream
  * @param messageHandler function for translating each message into the desired type
  * @param maxRetries maximum number of times in a row to retry getting leaders' offsets
  */
class DeterministicKafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag,
  R: ClassTag](
    @transient ssc_ : StreamingContext,
    val kafkaParams: Map[String, String],
    val fromOffsets: Map[TopicAndPartition, Long],
    messageHandler: MessageAndMetadata[K, V] => R,
    maxRetries: Int = 1
) extends InputDStream[R](ssc_) with Logging {

  private val kc = new KafkaCluster(kafkaParams)

  private val maxMessagesPerPartition: Option[Long] = {
    val ratePerSec = context.sparkContext.getConf.getInt("spark.streaming.receiver.maxRate", 0)
    if (ratePerSec > 0) {
      val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
      Some((secsPerBatch * ratePerSec).toLong)
    } else {
      None
    }
  }

  private var currentOffsets = fromOffsets

  @tailrec
  private def latestLeaderOffsets(retries: Int): Map[TopicAndPartition, Long] = {
    val o = kc.getLatestLeaderOffsets(currentOffsets.keySet)
    // Either.fold would confuse @tailrec, do it manually
    if (o.isLeft) {
      val err = o.left.get.toString
      if (retries <= 0) {
        throw new Exception(err)
      } else {
        log.error(err)
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        latestLeaderOffsets(retries - 1)
      }
    } else {
      o.right.get
    }
  }

  private def clamp(leaderOffsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, Long] = {
    maxMessagesPerPartition.map { mmp =>
      leaderOffsets.map { kv =>
        kv._1 -> Math.min(currentOffsets(kv._1) + mmp, kv._2)
      }
    }.getOrElse(leaderOffsets)
  }

  override def compute(validTime: Time): Option[KafkaRDD[K, V, U, T, R]] = {
    val untilOffsets = clamp(latestLeaderOffsets(maxRetries))
    val rdd = new KafkaRDD[K, V, U, T, R](
      context.sparkContext, kafkaParams, currentOffsets, untilOffsets, messageHandler)

    currentOffsets = untilOffsets
    Some(rdd)
  }

  override def start(): Unit = {
  }

  def stop(): Unit = {
  }
}

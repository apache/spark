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

import scala.collection.mutable
import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._

/**
 * A stream of {@link org.apache.spark.streaming.kafka.KafkaRDD} where
 * each given Kafka topic/partition corresponds to an RDD partition.
 * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
 * of messages
 * per second that each '''partition''' will accept.
 * Starting offsets are specified in advance,
 * and this DStream is not responsible for committing offsets,
 * so that you can control exactly-once semantics.
 *
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 *                    configuration parameters</a>.
 *                    Requires "metadata.broker.list" or "bootstrap.servers" to be set
 *                    with Kafka broker(s),
 *                    NOT zookeeper servers, specified in host1:port1,host2:port2 form.
 * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
 *                    starting point of the stream
 */
private[streaming]
class NewDirectKafkaInputDStream[
K: ClassTag,
V: ClassTag,
R: ClassTag](
  _ssc: StreamingContext,
  val kafkaParams: Map[String, String],
  val fromOffsets: Map[TopicPartition, Long],
  messageHandler: ConsumerRecord[K, V] => R
) extends DirectKafkaInputDStreamBase[K, V, scala.Null, scala.Null, R](_ssc, kafkaParams,
  KafkaUtils.newTopicPartitionToOld(fromOffsets)) {

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Kafka (new consumer API) direct stream [$id]"

  protected[streaming] override val checkpointData =
    new NewDirectKafkaInputDStreamCheckpointData

  protected var kc = new NewKafkaCluster[K, V](kafkaParams)

  protected final override def latestLeaderOffsets(retries: Int): Map[TopicAndPartition,
    LeaderOffset] = {
    KafkaUtils.newTopicPartitionToOld(
      kc.getLatestOffsetsWithLeaders(
        KafkaUtils.oldTopicPartitionToNew(currentOffsets).keySet
      )
    )
  }

  override def getRdd(untilOffsets: Map[TopicAndPartition, LeaderOffset]): KafkaRDDBase[K, V,
    scala.Null, scala.Null, R] = {
    NewKafkaRDD[K, V, R](context.sparkContext, kafkaParams, KafkaUtils.oldTopicPartitionToNew
    (currentOffsets), KafkaUtils.oldTopicPartitionToNew(untilOffsets), messageHandler)
  }

  override def stop(): Unit = {
    if (kc != null) {
      kc.close()
      kc = null
    }
  }

  private[streaming]
  class NewDirectKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time) {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = kv._2.asInstanceOf[NewKafkaRDD[K, V, R]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time) {}

    override def restore() {
      // this is assuming that the topics don't change during execution, which is true currently
      val topics = fromOffsets.keySet
      val leaders = kc.findLeaders(topics)
      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring KafkaRDD for time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> new NewKafkaRDD[K, V, R](
          context.sparkContext, kafkaParams, b.map(OffsetRange(_)), leaders, messageHandler)
      }
    }
  }
}

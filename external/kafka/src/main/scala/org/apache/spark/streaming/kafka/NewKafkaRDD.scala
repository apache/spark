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

import scala.reflect.ClassTag

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import org.apache.spark._

/**
 * A batch-oriented interface for consuming from Kafka.
 * Starting and ending offsets are specified in advance,
 * so that you can control exactly-once semantics.
 *
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 *                    configuration parameters</a>. Requires "bootstrap.servers" to be set
 *                    with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 */
private[kafka]
class NewKafkaRDD[K: ClassTag, V: ClassTag, R: ClassTag] private[spark](
  sc: SparkContext,
  kafkaParams: Map[String, String],
  val offsetRanges: Array[OffsetRange],
  leaders: Map[TopicPartition, (String, Int)],
  messageHandler: ConsumerRecord[K, V] => R
) extends KafkaRDDBase[K, V, scala.Null, scala.Null, R](sc, kafkaParams, offsetRanges, KafkaUtils
  .newTopicPartitionToOld(leaders)) with
  Logging with HasOffsetRanges {

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map {
      case (o, i) =>
        val (host, port) = leaders(new TopicPartition(o.topic, o.partition))
        new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset, host, port)
    }.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[R] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    assert(part.fromOffset <= part.untilOffset, KafkaUtils.errBeginAfterEnd(part))
    if (part.fromOffset == part.untilOffset) {
      log.info(s"Beginning offset ${part.fromOffset} is the same as ending offset " +
        s"skipping ${part.topic} ${part.partition}")
      Iterator.empty
    } else {
      val pollTime = SparkEnv.get.conf.getLong("spark.kafka.poll.time", KafkaUtils
        .DEFAULT_NEW_KAFKA_API_POLL_TIME)
      new NewKafkaRDDIterator[K, V, R](part, context, kafkaParams, messageHandler, pollTime)
    }
  }

}

private[kafka]
object NewKafkaRDD {

  /**
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *                    configuration parameters</a>.
   *                    Requires "bootstrap.servers" to be set with Kafka broker(s),
   *                    NOT zookeeper servers, specified in host1:port1,host2:port2 form.
   * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
   *                    starting point of the batch
   * @param untilOffsets per-topic/partition Kafka offsets defining the (exclusive)
   *                     ending point of the batch
   */
  def apply[K: ClassTag, V: ClassTag, R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicPartition, Long],
      untilOffsets: Map[TopicPartition, LeaderOffset],
      messageHandler: ConsumerRecord[K, V] => R): NewKafkaRDD[K, V, R] = {
    val leaders = untilOffsets.map { case (tp, lo) =>
      tp -> (lo.host, lo.port)
    }.toMap

    val offsetRanges = fromOffsets.map {
      case (tp, fo) =>
        val uo = untilOffsets(tp)
        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }.toArray

    new NewKafkaRDD[K, V, R](sc, kafkaParams, offsetRanges, leaders, messageHandler)
  }
}

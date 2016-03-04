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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{classTag, ClassTag}

import java.{ util => ju }

import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord }
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{Logging, Partition, SparkContext, SparkException, TaskContext}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.ExecutorCacheTaskLocation

/**
 * A batch-oriented interface for consuming from Kafka.
 * Starting and ending offsets are specified in advance,
 * so that you can control exactly-once semantics.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 * @param messageHandler function for translating each message into the desired type
 */

class KafkaRDD[
  K: ClassTag,
  V: ClassTag,
  R: ClassTag] private[spark] (
    sc: SparkContext,
    kafkaParams: ju.Map[String, Object],
    val offsetRanges: Array[OffsetRange],
    leaders: Map[TopicPartition, (String, Int)],
    messageHandler: ConsumerRecord[K, V] => R
  ) extends RDD[R](sc, Nil) with Logging with HasOffsetRanges {
  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
        val (host, port) = leaders(new TopicPartition(o.topic, o.partition))
        new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset, host, port)
    }.toArray
  }

  override def count(): Long = offsetRanges.map(_.count).sum

  override def countApprox(
      timeout: Long,
      confidence: Double = 0.95
  ): PartialResult[BoundedDouble] = {
    val c = count
    new PartialResult(new BoundedDouble(c, 1.0, c, c), true)
  }

  override def isEmpty(): Boolean = count == 0L

  override def take(num: Int): Array[R] = {
    val nonEmptyPartitions = this.partitions
      .map(_.asInstanceOf[KafkaRDDPartition])
      .filter(_.count > 0)

    if (num < 1 || nonEmptyPartitions.size < 1) {
      return new Array[R](0)
    }

    // Determine in advance how many messages need to be taken from each partition
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.count)
        result + (part.index -> taken.toInt)
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[R]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[R]) => it.take(parts(tc.partitionId)).toArray,
      parts.keys.toArray)
    res.foreach(buf ++= _)
    buf.toArray
  }

  // TODO is there a better way to get executors
  @transient private var sortedExecutors: Array[ExecutorCacheTaskLocation] = null
  private def executors(): Array[ExecutorCacheTaskLocation] = {
    if (null == sortedExecutors) {
      val bm = sparkContext.env.blockManager
      sortedExecutors = bm.master.getPeers(bm.blockManagerId).toArray
        .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
        .sortWith((a, b) => a.host > b.host || a.executorId > b.executorId)
    }
    sortedExecutors
  }

  // non-negative modulus, from java 8 math
  private def floorMod(a: Int, b: Int): Int = ((a % b) + b) % b

  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    // want lead broker if an executor is running on it, otherwise stable exec to use caching
    // TODO what if broker host is ip and executor is name, or vice versa
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    val allExecs = executors()
    val brokerExecs = allExecs.filter(_.host == part.host)
    val execs = if (brokerExecs.isEmpty) allExecs else brokerExecs
    val index = floorMod(part.topic.hashCode * 41 + part.partition.hashCode, execs.length)
    val chosen = execs(index)

    Seq(chosen.toString)
  }

  private def errBeginAfterEnd(part: KafkaRDDPartition): String =
    s"Beginning offset ${part.fromOffset} is after the ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition}. " +
      "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

  override def compute(thePart: Partition, context: TaskContext): Iterator[R] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    assert(part.fromOffset <= part.untilOffset, errBeginAfterEnd(part))
    if (part.fromOffset == part.untilOffset) {
      log.info(s"Beginning offset ${part.fromOffset} is the same as ending offset " +
        s"skipping ${part.topic} ${part.partition}")
      Iterator.empty
    } else {
      new KafkaRDDIterator(part, context)
    }
  }

  private class KafkaRDDIterator(
      part: KafkaRDDPartition,
      context: TaskContext) extends Iterator[R] {

    log.info(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

    val pollTimeout = sparkContext.getConf.getLong("spark.streaming.kafka.consumer.poll.ms", 10)

    val consumer = {
      CachedKafkaConsumer.init(sparkContext.getConf)
      if (context.attemptNumber > 0) {
        // just in case the prior attempt failure was cache related
        CachedKafkaConsumer.remove(groupId, part.topic, part.partition)
      }
      CachedKafkaConsumer.get[K, V](groupId, part.topic, part.partition, kafkaParams)
    }

    var requestOffset = part.fromOffset

    override def hasNext(): Boolean = requestOffset < part.untilOffset

    override def next(): R = {
      assert(hasNext(), "Can't call getNext() once untilOffset has been reached")
      // XXX TODO is messageHandler even useful any more?
      // Don't think it can catch serialization problems with the new consumer in an efficient way
      val r = messageHandler(consumer.get(requestOffset, pollTimeout))
      requestOffset += 1
      r
    }
  }
}

private[kafka]
object KafkaRDD {
  import KafkaCluster.LeaderOffset

  /**
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   * configuration parameters</a>.
   *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
   *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
   * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
   *  starting point of the batch
   * @param untilOffsets per-topic/partition Kafka offsets defining the (exclusive)
   *  ending point of the batch
   * @param messageHandler function for translating each message into the desired type
   */
  def apply[
    K: ClassTag,
    V: ClassTag,
    R: ClassTag](
      sc: SparkContext,
      kafkaParams: ju.Map[String, Object],
      fromOffsets: Map[TopicPartition, Long],
      untilOffsets: Map[TopicPartition, LeaderOffset],
      messageHandler: ConsumerRecord[K, V] => R
    ): KafkaRDD[K, V, R] = {
    val leaders = untilOffsets.map { case (tp, lo) =>
        tp -> (lo.host, lo.port)
    }.toMap

    val offsetRanges = fromOffsets.map { case (tp, fo) =>
        val uo = untilOffsets(tp)
        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }.toArray

    new KafkaRDD[K, V, R](sc, kafkaParams, offsetRanges, leaders, messageHandler)
  }
}

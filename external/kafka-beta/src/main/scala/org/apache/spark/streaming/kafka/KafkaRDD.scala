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
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.htmll#newconsumerconfigs">
 * configuration parameters</a>. Requires "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 */

class KafkaRDD[
  K: ClassTag,
  V: ClassTag] private[kafka] (
    sc: SparkContext,
    val kafkaParams: ju.Map[String, Object],
    val offsetRanges: Array[OffsetRange],
    val preferredHosts: ju.Map[TopicPartition, String]
) extends RDD[ConsumerRecord[K, V]](sc, Nil) with Logging with HasOffsetRanges {

  assert("none" ==
    kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).asInstanceOf[String],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG +
      " must be set to none for executor kafka params, else messages may not match offsetRange")

  assert(false ==
    kafkaParams.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).asInstanceOf[Boolean],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG +
      " must be set to false for executor kafka params, else offsets may commit before processing")

  private val pollTimeout = conf.getLong("spark.streaming.kafka.consumer.poll.ms", 64)
  private val cacheInitialCapacity =
    conf.getInt("spark.streaming.kafka.consumer.cache.initialCapacity", 16)
  private val cacheMaxCapacity =
    conf.getInt("spark.streaming.kafka.consumer.cache.maxCapacity", 64)
  private val cacheLoadFactor =
    conf.getDouble("spark.streaming.kafka.consumer.cache.loadFactor", 0.75).toFloat

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
        new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset)
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

  override def take(num: Int): Array[ConsumerRecord[K, V]] = {
    val nonEmptyPartitions = this.partitions
      .map(_.asInstanceOf[KafkaRDDPartition])
      .filter(_.count > 0)

    if (num < 1 || nonEmptyPartitions.size < 1) {
      return new Array[ConsumerRecord[K, V]](0)
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

    val buf = new ArrayBuffer[ConsumerRecord[K, V]]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[ConsumerRecord[K, V]]) =>
      it.take(parts(tc.partitionId)).toArray, parts.keys.toArray
    )
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
    // TODO what about hosts specified by ip vs name
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    val allExecs = executors()
    val tp = part.topicPartition
    val prefHost = preferredHosts.get(tp)
    val prefExecs = if (null == prefHost) allExecs else allExecs.filter(_.host == prefHost)
    val execs = if (prefExecs.isEmpty) allExecs else prefExecs
    val index = floorMod(tp.hashCode, execs.length)
    val chosen = execs(index)

    Seq(chosen.toString)
  }

  private def errBeginAfterEnd(part: KafkaRDDPartition): String =
    s"Beginning offset ${part.fromOffset} is after the ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition}. " +
      "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

  override def compute(thePart: Partition, context: TaskContext): Iterator[ConsumerRecord[K, V]] = {
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
      context: TaskContext) extends Iterator[ConsumerRecord[K, V]] {

    log.info(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val groupId = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

    val consumer = {
      CachedKafkaConsumer.init(cacheInitialCapacity, cacheMaxCapacity, cacheLoadFactor)
      if (context.attemptNumber > 1) {
        // just in case the prior attempt failures were cache related
        CachedKafkaConsumer.remove(groupId, part.topic, part.partition)
      }
      CachedKafkaConsumer.get[K, V](groupId, part.topic, part.partition, kafkaParams)
    }

    var requestOffset = part.fromOffset

    override def hasNext(): Boolean = requestOffset < part.untilOffset

    override def next(): ConsumerRecord[K, V] = {
      assert(hasNext(), "Can't call getNext() once untilOffset has been reached")
      val r = consumer.get(requestOffset, pollTimeout)
      requestOffset += 1
      r
    }
  }
}

object KafkaRDD extends Logging {
  private[kafka] def fixKafkaParams(kafkaParams: ju.HashMap[String, Object]): Unit = {
    log.warn(s"overriding ${ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG} to false for executor")
    kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false: java.lang.Boolean)

    log.warn(s"overriding ${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} to none for executor")
    kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")

    // this probably doesnt matter since executors are manually assigned partitions, but just in case
    if (null != kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG)) {
      val id = kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG) + "-executor"
      log.warn(s"overriding ${ConsumerConfig.GROUP_ID_CONFIG} to ${id} for executor")
      kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, id)
    }
  }

  def apply[
    K: ClassTag,
    V: ClassTag](
    sc: SparkContext,
    kafkaParams: ju.Map[String, Object],
    offsetRanges: Array[OffsetRange],
    preferredHosts: ju.Map[TopicPartition, String]
    ): KafkaRDD[K, V] = {
    val kp = new ju.HashMap[String, Object](kafkaParams)
    val osr = offsetRanges.clone()
    val ph = new ju.HashMap[TopicPartition, String](preferredHosts)

    new KafkaRDD[K, V](sc, kp, osr, ph)
  }
}

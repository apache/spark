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

package org.apache.spark.sql.kafka010

import java.{util => ju}

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.kafka010.KafkaSourceRDD._
import org.apache.spark.storage.StorageLevel


private[kafka010]
case class KafkaSourceRDDPartition(index: Int, offsetRange: OffsetRange) extends Partition

/**
 * An RDD that reads data from Kafka based on offset ranges across multiple partitions.
 * Additionally, it allows preferred locations to be set for each topic + partition, so that
 * the [[KafkaSource]] can ensure the same executor always reads the same topic + partition
 * and cached KafkaConsuemrs (see [[CachedKafkaConsumer]] can be used read data efficiently.
 *
 * Note that this is a simplified version of the [[org.apache.spark.streaming.kafka010.KafkaRDD]].
 *
 * @param executorKafkaParams Kafka configuration for creating KafkaConsumer on the executors
 * @param offsetRanges Offset ranges that define the Kafka data belonging to this RDD
 * @param sourceOptions Options provided through the source
 */
private[kafka010] class KafkaSourceRDD(
    sc: SparkContext,
    executorKafkaParams: ju.Map[String, Object],
    offsetRanges: Seq[OffsetRange],
    sourceOptions: Map[String, String])
  extends RDD[ConsumerRecord[Array[Byte], Array[Byte]]](sc, Nil) {

  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) => new KafkaSourceRDDPartition(i, o) }.toArray
  }

  override def count(): Long = offsetRanges.map(_.size).sum

  override def isEmpty(): Boolean = count == 0L

  override def take(num: Int): Array[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val nonEmptyPartitions =
      this.partitions.map(_.asInstanceOf[KafkaSourceRDDPartition]).filter(_.offsetRange.size > 0)

    if (num < 1 || nonEmptyPartitions.isEmpty) {
      return new Array[ConsumerRecord[Array[Byte], Array[Byte]]](0)
    }

    // Determine in advance how many messages need to be taken from each partition
    val parts = nonEmptyPartitions.foldLeft(Map[Int, Int]()) { (result, part) =>
      val remain = num - result.values.sum
      if (remain > 0) {
        val taken = Math.min(remain, part.offsetRange.size)
        result + (part.index -> taken.toInt)
      } else {
        result
      }
    }

    val buf = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    val res = context.runJob(
      this,
      (tc: TaskContext, it: Iterator[ConsumerRecord[Array[Byte], Array[Byte]]]) =>
      it.take(parts(tc.partitionId)).toArray, parts.keys.toArray
    )
    res.foreach(buf ++= _)
    buf.toArray
  }

  override def compute(
      thePart: Partition,
      context: TaskContext): Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val range = thePart.asInstanceOf[KafkaSourceRDDPartition].offsetRange
    assert(
      range.fromOffset <= range.untilOffset,
      s"Beginning offset ${range.fromOffset} is after the ending offset ${range.untilOffset} " +
        s"for topic ${range.topic} partition ${range.partition}. " +
        "You either provided an invalid fromOffset, or the Kafka topic has been damaged")
    if (range.fromOffset == range.untilOffset) {
      logInfo(s"Beginning offset ${range.fromOffset} is the same as ending offset " +
        s"skipping ${range.topic} ${range.partition}")
      Iterator.empty
    } else {
      new KafkaRDDIterator(range, executorKafkaParams, sourceOptions, context)
    }
  }
}

private[kafka010] object KafkaSourceRDD {

  /** Offset range that one partition of the KafkaSourceRDD has to read */
  case class OffsetRange(
      topicPartition: TopicPartition,
      fromOffset: Long,
      untilOffset: Long,
      preferredLoc: Option[String]) {
    def topic: String = topicPartition.topic
    def partition: Int = topicPartition.partition
    def size: Long = untilOffset - fromOffset
  }

  /**
   * An iterator that fetches messages directly from Kafka for the offsets in partition.
   * Uses a cached consumer where possible to take advantage of prefetching
   */
  private class KafkaRDDIterator(
      part: OffsetRange,
      executorKafkaParams: ju.Map[String, Object],
      options: Map[String, String],
      context: TaskContext)
    extends Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] with Logging {

    logInfo(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val groupId = executorKafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).asInstanceOf[String]

    // Time between polling for more data by the KafkaConsumer in the executor. This should not
    // require much configuration as data should already be available in Kafka when the executors
    // are polling.
    private val pollTimeout = getLong(options, "consumer.pollMs", 512)

    // Configurations for initializing the cache of KafkaConsumers.
    private val cacheInitialCapacity =
      SparkEnv.get.conf.getInt("kafka.consumer.cache.initialCapacity", 16)
    private val cacheMaxCapacity =
      SparkEnv.get.conf.getInt("kafka.consumer.cache.maxCapacity", 64)
    private val cacheLoadFactor =
      SparkEnv.get.conf.getDouble("kafka.consumer.cache.loadFactor", 0.75).toFloat

    // Initialize the cache if not already done, and get a cached KafkaConsumer
    CachedKafkaConsumer.init(cacheInitialCapacity, cacheMaxCapacity, cacheLoadFactor)
    if (context.attemptNumber > 1) {
      // Just in case the prior attempt failures were cache related
      CachedKafkaConsumer.remove(groupId, part.topic, part.partition)
    }
    val consumer =
      CachedKafkaConsumer.get(groupId, part.topic, part.partition, executorKafkaParams)

    var requestOffset = part.fromOffset

    override def hasNext(): Boolean = requestOffset < part.untilOffset

    override def next(): ConsumerRecord[Array[Byte], Array[Byte]] = {
      assert(hasNext(), "Can't call next() once untilOffset has been reached")
      val r = consumer.get(requestOffset, pollTimeout)
      requestOffset += 1
      r
    }
  }

  def getLong(options: Map[String, String], name: String, defaultValue: Long): Long = {
    options.get(name).map { str =>
      Try(str.toLong).getOrElse {
        throw new IllegalArgumentException("Option '$name' must be a long")
      }
    }.getOrElse(defaultValue)
  }
}

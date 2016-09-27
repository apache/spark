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

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetOutOfRangeException}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/** Offset range that one partition of the KafkaSourceRDD has to read */
private[kafka010] case class KafkaSourceRDDOffsetRange(
    topicPartition: TopicPartition,
    fromOffset: Long,
    untilOffset: Long,
    preferredLoc: Option[String]) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition
  def size: Long = untilOffset - fromOffset
}


/** Partition of the KafkaSourceRDD */
private[kafka010] case class KafkaSourceRDDPartition(
  index: Int, offsetRange: KafkaSourceRDDOffsetRange) extends Partition


/**
 * An RDD that reads data from Kafka based on offset ranges across multiple partitions.
 * Additionally, it allows preferred locations to be set for each topic + partition, so that
 * the [[KafkaSource]] can ensure the same executor always reads the same topic + partition
 * and cached KafkaConsuemrs (see [[CachedKafkaConsumer]] can be used read data efficiently.
 *
 * Note that this is a simplified version of the org.apache.spark.streaming.kafka010.KafkaRDD.
 *
 * @param executorKafkaParams Kafka configuration for creating KafkaConsumer on the executors
 * @param offsetRanges Offset ranges that define the Kafka data belonging to this RDD
 */
private[kafka010] class KafkaSourceRDD(
    sc: SparkContext,
    executorKafkaParams: ju.Map[String, Object],
    offsetRanges: Seq[KafkaSourceRDDOffsetRange])
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

      val consumer = CachedKafkaConsumer.getOrCreate(
        range.topic, range.partition, executorKafkaParams)
      var requestOffset = range.fromOffset

      logDebug(s"Creating iterator for $range")

      new Iterator[ConsumerRecord[Array[Byte], Array[Byte]]]() {

        private var prefetch: ConsumerRecord[Array[Byte], Array[Byte]] = _

        private def fetchNext(): ConsumerRecord[Array[Byte], Array[Byte]] = {
          try {
            val r = consumer.get(requestOffset)
            requestOffset += 1
            r
          } catch {
            case e: OffsetOutOfRangeException =>
              logWarning(s"${range.topicPartition} was deleted, some data may have been missed")
              null
          }
        }

        override def hasNext(): Boolean = {
          if (prefetch == null && requestOffset < range.untilOffset) {
            prefetch = fetchNext()
          }
          prefetch != null
        }

        override def next(): ConsumerRecord[Array[Byte], Array[Byte]] = {
          assert(hasNext(), "Can't call next() once untilOffset has been reached")
          val r = prefetch
          prefetch = null
          r
        }
      }
    }
  }
}

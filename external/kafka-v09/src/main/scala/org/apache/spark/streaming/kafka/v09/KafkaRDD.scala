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

package org.apache.spark.streaming.kafka.v09

import java.util.{Collections, Properties}

import kafka.common.TopicAndPartition
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.v09.KafkaCluster.toTopicPart
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * A batch-oriented interface for consuming from Kafka.
  * Starting and ending offsets are specified in advance,
  * so that you can control exactly-once semantics.
  * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
  * configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers" to be set
  * with Kafka broker(s) specified in host1:port1,host2:port2 form.
  * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
  */
private[kafka]
class KafkaRDD[K: ClassTag, V: ClassTag, R: ClassTag] private[spark](
          sc: SparkContext,
          kafkaParams: Map[String, String],
          val offsetRanges: Array[OffsetRange],
          messageHandler: ConsumerRecord[K, V] => R
     ) extends RDD[R](sc, Nil) with Logging with HasOffsetRanges {

  private val KAFKA_DEFAULT_POLL_TIME: String = "0"
  private val pollTime = kafkaParams.get("spark.kafka.poll.time")
    .getOrElse(KAFKA_DEFAULT_POLL_TIME).toInt

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

  private def errBeginAfterEnd(part: KafkaRDDPartition): String =
    s"Beginning offset ${part.fromOffset} is after the ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition}. " +
      "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

  private def errRanOutBeforeEnd(part: KafkaRDDPartition): String =
    s"Ran out of messages before reaching ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
      " This should not happen, and indicates that messages may have been lost"

  private def errOvershotEnd(itemOffset: Long, part: KafkaRDDPartition): String =
    s"Got ${itemOffset} > ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
      " This should not happen, and indicates a message may have been skipped"

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
                                  context: TaskContext) extends NextIterator[R] {

    context.addTaskCompletionListener{ context => closeIfNeeded() }

    log.info(s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset}")

    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))

    val consumer = new KafkaConsumer[K, V](props)
    val tp = new TopicAndPartition(part.topic, part.partition)
    consumer.assign(Collections.singletonList[TopicPartition](toTopicPart(tp)))

    var requestOffset = part.fromOffset
    var iter: java.util.Iterator[ConsumerRecord[K, V]] = null
    consumer.seek(toTopicPart(tp), requestOffset)

    override def close(): Unit = {
      if (consumer != null) {
        consumer.close()
      }
    }

    override def getNext(): R = {
      if (iter == null || !iter.hasNext) {
        iter = consumer.poll(pollTime).iterator()
      }

      if (!iter.hasNext) {
        finished = true
        null.asInstanceOf[R]
      } else {
        val item: ConsumerRecord[K, V] = iter.next()
        if (item.offset >= part.untilOffset) {
          assert(item.offset == part.untilOffset, errOvershotEnd(item.offset, part))
          finished = true
          null.asInstanceOf[R]
        } else {
          requestOffset = item.offset() + 1
          messageHandler(item)
        }
      }
    }
  }
}

private[kafka]
object KafkaRDD {

  /**
    * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
    * configuration parameters</a>.
    *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
    *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
    * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
    *  starting point of the batch
    * @param untilOffsets per-topic/partition Kafka offsets defining the (exclusive)
    *  ending point of the batch
    */
  def apply[
  K: ClassTag,
  V: ClassTag,
  R: ClassTag](
                sc: SparkContext,
                kafkaParams: Map[String, String],
                fromOffsets: Map[TopicAndPartition, Long],
                untilOffsets: Map[TopicAndPartition, Long],
                messageHandler: ConsumerRecord[K, V] => R
              ): KafkaRDD[K, V, R] = {
    val offsetRanges = fromOffsets.map { case (tp, fo) =>
      val uo = untilOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo, uo)
    }.toArray

    new KafkaRDD[K, V, R](sc, kafkaParams, offsetRanges, messageHandler)
  }
}

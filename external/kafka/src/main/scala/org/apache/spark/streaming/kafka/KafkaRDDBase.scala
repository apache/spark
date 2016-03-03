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
import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import kafka.serializer.Decoder

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD

/**
  * A batch-oriented interface for consuming from Kafka.
  * Starting and ending offsets are specified in advance,
  * so that you can control exactly-once semantics.
  *
  * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
  * configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers" to be set
  * with Kafka broker(s) specified in host1:port1,host2:port2 form.
  * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
  */
private[kafka]
abstract class KafkaRDDBase[
K: ClassTag,
V: ClassTag,
U <: Decoder[_] : ClassTag,
T <: Decoder[_] : ClassTag,
R: ClassTag] private[spark](
  sc: SparkContext,
  kafkaParams: Map[String, String],
  offsetRanges: Array[OffsetRange],
  leaders: Map[TopicAndPartition, (String, Int)]
) extends RDD[R](sc, Nil) with Logging with HasOffsetRanges {

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

  override def getPreferredLocations(thePart: Partition): Seq[String] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    // TODO is additional hostname resolution necessary here
    if (part.host != null) {
      Seq(part.host)
    } else {
      Seq()
    }
  }

}


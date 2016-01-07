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

import kafka.common.TopicAndPartition
import kafka.serializer._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{Batch, StreamingRelation, Offset, Source}
import org.apache.spark.sql.types.StructType





private[kafka]
case class KafkaSourceOffset(offsets: Map[TopicAndPartition, Long]) extends Offset {
  /**
    * Returns a negative integer, zero, or a positive integer as this object is less than, equal to,
    * or greater than the specified object.
    */
  override def compareTo(other: Offset): Int = other match {
    case KafkaSourceOffset(otherOffsets) =>
      val allTopicAndPartitions = (this.offsets.keySet ++ otherOffsets.keySet).toSeq

      val comparisons = allTopicAndPartitions.map { tp =>
        (this.offsets.get(tp), otherOffsets.get(tp)) match {

          case (Some(a), Some(b)) =>
            if (a < b) {
              -1
            } else if (a > b) {
              1
            } else {
              0
            }
          case (None, None) => 0
          case (None, _) => -1
          case (_, None) => 1
        }
      }
      val signs = comparisons.distinct
      if (signs.size != 1) {
        throw new IllegalArgumentException(
          s"Invalid comparison between to sets of Kafka offets: $this <=> $other")
      }
      signs.head
    case _ =>
      throw new IllegalArgumentException(s"Cannot compare $this <=> $other")
  }

  override def toString(): String = offsets.toSeq.mkString("[", ", ", "]")
}

private[kafka] object KafkaSourceOffset {
  def fromOffset(offset: Offset): KafkaSourceOffset = {
    offset match {
      case o: KafkaSourceOffset => o
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to $getClass")
    }
  }
}


private[kafka] case class KafkaSource(
  topics: Set[String], params: Map[String, String])(implicit sqlContext: SQLContext) extends Source {

  type OffsetMap = Map[TopicAndPartition, Long]
  implicit private val encoder = ExpressionEncoder.tuple(
    ExpressionEncoder[Array[Byte]](), ExpressionEncoder[Array[Byte]]())

  @transient private val logicalPlan = StreamingRelation(this)
  @transient private val kc = new KafkaCluster(params)
  @transient private val topicAndPartitions = KafkaCluster.checkErrors(kc.getPartitions(topics))
  @transient private lazy val initialOffsets = getInitialOffsets()

  override def schema: StructType = encoder.schema

  /**
    * Returns the next batch of data that is available after `start`, if any is available.
    */
  override def getNextBatch(start: Option[Offset]): Option[Batch] = {
    val latestOffset = getLatestOffsets()
    val offsetRanges = getOffsetRanges(
      start.map(KafkaSourceOffset.fromOffset(_).offsets), latestOffset)

    val kafkaParams = params
    val encodingFunc = encoder.toRow _
    val sparkContext = sqlContext.sparkContext

    println("Creating DF with offset ranges: " + offsetRanges)
    if (offsetRanges.nonEmpty) {
      val rdd = KafkaUtils.createRDD[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
        sparkContext, kafkaParams, offsetRanges.toArray)
      Some(new Batch(KafkaSourceOffset(latestOffset), sqlContext.createDataset(rdd).toDF))
    } else {
      None
    }
  }

  def toDS(): Dataset[(Array[Byte], Array[Byte])] = {
    toDF.as[(Array[Byte], Array[Byte])]
  }

  def toDF(): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }

  private def getOffsetRanges(start: Option[OffsetMap], end: OffsetMap): Seq[OffsetRange] = {
    // Get the offsets from which to start reading. If its none, find initial offsets to start from.
    val fromOffsets = start.getOrElse { initialOffsets }

    // Get the latest offsets
    val untilOffsets = getLatestOffsets()

    // Get all the partitions referenced in both sets of offsets
    val allTopicAndPartitions = (fromOffsets.keySet ++ untilOffsets.keySet).toSeq

    // For each partition, figure out the non-empty ranges of offsets
    allTopicAndPartitions.flatMap { tp =>
      (fromOffsets.get(tp), untilOffsets.get(tp)) match {

        // Data was read till fromOffset and needs to be read till untilOffset
        case (Some(fromOffset), Some(untilOffset)) =>
          if (untilOffset > fromOffset) {
            Some(OffsetRange(tp, fromOffset, untilOffset))
          } else None

        case _ =>
          None
      }
    }
  }

  private def getLatestOffsets(): OffsetMap = {
    val partitionLeaders = KafkaCluster.checkErrors(kc.findLeaders(topicAndPartitions))
    val leadersAndOffsets = KafkaCluster.checkErrors(kc.getLatestLeaderOffsets(topicAndPartitions))
    println("Getting offsets " + leadersAndOffsets)
    leadersAndOffsets.map { x => (x._1, x._2.offset) }
  }

  private def getInitialOffsets(): OffsetMap = {
    if (params.get("auto.offset.reset").map(_.toLowerCase) == Some("smallest")) {
      KafkaCluster.checkErrors(kc.getEarliestLeaderOffsets(topicAndPartitions)).mapValues(_.offset)
    } else Map.empty
  }

  override def toString(): String = s"KafkaSource[${topics.mkString(", ")}]"
}

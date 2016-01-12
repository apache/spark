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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.{Batch, Offset, Source, StreamingRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}


/** An [[Offset]] for the [[KafkaSource]]. */
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
          case (None, _) => -1
          case (_, None) => 1
        }
      }
      val nonZeroSigns = comparisons.filter { _ != 0 }.toSet
      nonZeroSigns.size match {
        case 0 => 0 // if both empty or only 0s
        case 1 => nonZeroSigns.head // if there are only (0s and 1s) or (0s and -1s)
        case _ => // there are both 1s and -1s
          throw new IllegalArgumentException(
            s"Invalid comparison between non-linear histories: $this <=> $other")
      }

    case _ =>
      throw new IllegalArgumentException(s"Cannot compare $this <=> $other")
  }

  /** Returns a set of offset ranges between `this` and `other` */
  def to(other: KafkaSourceOffset): Seq[OffsetRange] = {

    // Get all the partitions referenced in both sets of offsets
    val allTopicAndPartitions = (this.offsets.keySet ++ other.offsets.keySet).toSeq

    // For each partition, figure out the non-empty ranges of offsets
    allTopicAndPartitions.flatMap { tp =>
      (this.offsets.get(tp), other.offsets.get(tp)) match {

        // Data was read till fromOffset and needs to be read till untilOffset
        case (Some(fromOffset), Some(untilOffset)) =>
          if (untilOffset > fromOffset) {
            Some(OffsetRange(tp, fromOffset, untilOffset))
          } else None

        // TODO: Support cases where topic+partitions are missing from one. Can happen in case of
        // repartitioning.

        case _ =>
          None
      }
    }
  }

  override def toString(): String = {
    offsets.toSeq.sortBy(_._1.topic).mkString("[", ", ", "]")
  }
}

/** Companion object of the [[KafkaSourceOffset]] */
private[kafka] object KafkaSourceOffset {

  /** Returns [[KafkaSourceOffset]] from a Option[Offset]. */
  def from(offsetOption: Option[Offset]): Option[KafkaSourceOffset] = {
    offsetOption.map { offset =>
      offset match {
        case o: KafkaSourceOffset => o
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
      }
    }
  }

  /**
   * Returns [[KafkaSourceOffset]] from a variable sequence of (topic, partitionId, offset)
   * tuples.
   */
  def apply(data: (String, Int, Long)*): KafkaSourceOffset = {
    val map = data.map { case (topic, partition, offset) =>
        TopicAndPartition(topic, partition) -> offset }.toMap
    KafkaSourceOffset(map)
  }
}


/** A [[Source]] that reads data from Kafka */
private[kafka] case class KafkaSource(
    topics: Set[String],
    params: Map[String, String])(implicit sqlContext: SQLContext) extends Source with Logging {

  type OffsetMap = Map[TopicAndPartition, Long]

  implicit private val encoder = ExpressionEncoder.tuple(
    ExpressionEncoder[Array[Byte]](), ExpressionEncoder[Array[Byte]]())

  @transient private val logicalPlan = StreamingRelation(this)
  @transient private val kc = new KafkaCluster(params)
  @transient private val topicAndPartitions = KafkaCluster.checkErrors(kc.getPartitions(topics))
  @transient private[kafka] val initialOffsets = getInitialOffsets()

  override def schema: StructType = encoder.schema

  /** Returns the next batch of data that is available after `start`, if any is available. */
  override def getNextBatch(start: Option[Offset]): Option[Batch] = {
    val beginOffset: KafkaSourceOffset = KafkaSourceOffset.from(start).getOrElse(initialOffsets)
    val latestOffset = getLatestOffsets()
    logDebug(s"Latest offset: $latestOffset")

    val offsetRanges = beginOffset to latestOffset
    val kafkaParams = params
    val encodingFunc = encoder.toRow _
    val sparkContext = sqlContext.sparkContext

    if (offsetRanges.nonEmpty) {
      val rdd = KafkaUtils.createRDD[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
        sparkContext, kafkaParams, offsetRanges.toArray)
      logInfo(s"Creating DF with offset ranges: $offsetRanges")
      Some(new Batch(latestOffset, sqlContext.createDataset(rdd).toDF))
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

  /** Get latest offsets from Kafka. */
  private def getLatestOffsets(): KafkaSourceOffset = {
    val partitionLeaders = KafkaCluster.checkErrors(kc.findLeaders(topicAndPartitions))
    val leadersAndOffsets = KafkaCluster.checkErrors(kc.getLatestLeaderOffsets(topicAndPartitions))
    KafkaSourceOffset(leadersAndOffsets.map { x => (x._1, x._2.offset) })
  }

  /** Get the initial offsets from Kafka for the source to start from. */
  private def getInitialOffsets(): KafkaSourceOffset = {
    if (params.get("auto.offset.reset").map(_.toLowerCase) == Some("smallest")) {
      val offsetMap = KafkaCluster.checkErrors(
        kc.getEarliestLeaderOffsets(topicAndPartitions)).mapValues(_.offset)
      KafkaSourceOffset(offsetMap)
    } else {
      getLatestOffsets()
    }
  }

  override def toString(): String = s"KafkaSource[${topics.mkString(", ")}]"
}

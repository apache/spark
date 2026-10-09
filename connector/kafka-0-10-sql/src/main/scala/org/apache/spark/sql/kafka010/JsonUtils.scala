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

import java.util.Locale

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.kafka.common.TopicPartition
import org.json4s.{Formats, JObject, JString, NoTypeHints}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

/**
 * Utilities for converting Kafka related objects to and from json.
 */
private object JsonUtils {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * Read TopicPartitions from json string
   */
  def partitions(str: String): Array[TopicPartition] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).flatMap {  case (topic, parts) =>
          parts.map { part =>
            new TopicPartition(topic, part)
          }
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  /**
   * Write TopicPartitions as json string
   */
  def partitions(partitions: Iterable[TopicPartition]): String = {
    val result = new HashMap[String, List[Int]]
    partitions.foreach { tp =>
      val parts: List[Int] = result.getOrElse(tp.topic, Nil)
      result += tp.topic -> (tp.partition::parts)
    }
    Serialization.write(result)
  }

  /**
   * Read per-TopicPartition offsets from json string
   */
  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
          partOffsets.map { case (part, offset) =>
              new TopicPartition(topic, part) -> offset
          }
      }
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /**
   * Read the offsets of the `startingOffsets` / `endingOffsets` json string. On top of the
   * per-TopicPartition form read by [[partitionOffsets]], a topic may bind to "earliest" or
   * "latest" as a whole, e.g. {"topicA":"earliest","topicB":{"0":23,"1":-1}}. Such topic-level
   * values are returned unexpanded in `SpecificOffsetRangeLimit.topicOffsets`, since the
   * partitions of the topic are only known once the offsets get resolved against Kafka.
   */
  def specificOffsets(str: String): SpecificOffsetRangeLimit = {
    def fail(): Nothing = throw new IllegalArgumentException(
      s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}} or
         |{"topicA":"earliest","topicB":"latest"}, got $str""".stripMargin)

    val partitionOffsets = new HashMap[TopicPartition, Long]
    val topicOffsets = new HashMap[String, Long]
    try {
      parse(str) match {
        case JObject(topics) =>
          topics.foreach {
            case (topic, JString(value)) =>
              topicOffsets += topic -> (value.toLowerCase(Locale.ROOT) match {
                case "earliest" => KafkaOffsetRangeLimit.EARLIEST
                case "latest" => KafkaOffsetRangeLimit.LATEST
                case _ => fail()
              })
            case (topic, partOffsets) =>
              partOffsets.extract[Map[Int, Long]].foreach { case (part, offset) =>
                partitionOffsets += new TopicPartition(topic, part) -> offset
              }
          }
        case _ => fail()
      }
    } catch {
      case NonFatal(_) => fail()
    }
    val bothForms = topicOffsets.keySet.intersect(partitionOffsets.keySet.map(_.topic))
    if (bothForms.nonEmpty) {
      throw new IllegalArgumentException(
        s"""Topic(s) ${bothForms.toSeq.sorted.mkString(", ")} are given both a topic-level offset
           |and per-partition offsets, only one of the two forms is allowed per topic,
           |got $str""".stripMargin)
    }
    SpecificOffsetRangeLimit(partitionOffsets.toMap, topicOffsets.toMap)
  }

  def partitionTimestamps(str: String): Map[TopicPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partTimestamps) =>
        partTimestamps.map { case (part, timestamp) =>
          new TopicPartition(topic, part) -> timestamp
        }
      }
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA": {"0": 123456789, "1": 123456789},
             |"topicB": {"0": 123456789, "1": 123456789}}, got $str""".stripMargin)
    }
  }

  /**
   * Write per-TopicPartition offsets as json string
   */
  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long]): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val order: Ordering[TopicPartition] = (x: TopicPartition, y: TopicPartition) => {
      Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
    }
    val partitions = partitionOffsets.keySet.toSeq.sorted  // sort for more determinism
    partitions.foreach { tp =>
        val off = partitionOffsets(tp)
        val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
        parts += tp.partition -> off
        result += tp.topic -> parts
    }
    Serialization.write(result)
  }

  def partitionTimestamps(topicTimestamps: Map[TopicPartition, Long]): String = {
    // For now it's same as partitionOffsets
    partitionOffsets(topicTimestamps)
  }
}

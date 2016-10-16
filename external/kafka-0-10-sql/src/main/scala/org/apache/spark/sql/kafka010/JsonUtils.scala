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

import java.io.Writer

import scala.collection.mutable.{ ArrayBuffer, HashMap }
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node._
import org.apache.kafka.common.TopicPartition

/*
 * Utilities for converting Kafka related objects to and from json.
 */
private object JsonUtils {
  private val mapper = new ObjectMapper()

  /* Read TopicPartitions from json string */
  def partitions(str: String): Array[TopicPartition] = {
    try {
      val res = new ArrayBuffer[TopicPartition]()
      val topics = mapper.readTree(str).fields
      while (topics.hasNext) {
        val node = topics.next
        val topic = node.getKey
        val parts = node.getValue.elements
        while (parts.hasNext) {
          res.append(new TopicPartition(topic, parts.next().asInt))
        }
      }
      res.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  /* Write TopicPartitions as json */
  def partitions(partitions: Iterable[TopicPartition], writer: Writer): Unit = {
    val root = mapper.createObjectNode()
    partitions.foreach { tp =>
      var topic = root.get(tp.topic)
      if (null == topic) {
        root.set(tp.topic, mapper.createArrayNode())
        topic = root.get(tp.topic)
      }
      topic.asInstanceOf[ArrayNode].add(tp.partition)
    }
    mapper.writeValue(writer, root)
  }

  /* Read per-TopicPartition offsets from json string */
  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    try {
      val res = new HashMap[TopicPartition, Long]
      val topics = mapper.readTree(str).fields
      while (topics.hasNext) {
        val node = topics.next
        val topic = node.getKey
        val parts = node.getValue.fields
        while (parts.hasNext) {
          val node = parts.next
          val part = node.getKey.toInt
          val offset = node.getValue.asLong
          res += new TopicPartition(topic, part) -> offset
        }
      }
      res.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /* Write per-TopicPartition offsets as json */
  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long], writer: Writer): Unit = {
    val root = mapper.createObjectNode()
    partitionOffsets.foreach { case (tp, off) =>
        var topic = root.get(tp.topic)
        if (null == topic) {
          root.set(tp.topic, mapper.createObjectNode())
          topic = root.get(tp.topic)
        }
        topic.asInstanceOf[ObjectNode].set(tp.partition.toString, new LongNode(off))
    }
    mapper.writeValue(writer, root)
  }
}

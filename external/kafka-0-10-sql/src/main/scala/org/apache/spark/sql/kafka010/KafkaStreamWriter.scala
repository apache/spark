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

import scala.collection.JavaConverters._

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.kafka010.KafkaWriter.validateQuery
import org.apache.spark.sql.sources.v2.CustomMetrics
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.{StreamWriter, SupportsCustomWriterMetrics}
import org.apache.spark.sql.types.StructType

/**
 * Dummy commit message. The DataSourceV2 framework requires a commit message implementation but we
 * don't need to really send one.
 */
case class KafkaWriterCommitMessage(minOffset: KafkaSourceOffset, maxOffset: KafkaSourceOffset)
  extends WriterCommitMessage

/**
 * A [[StreamWriter]] for Kafka writing. Responsible for generating the writer factory.
 *
 * @param topic The topic this writer is responsible for. If None, topic will be inferred from
 *              a `topic` field in the incoming data.
 * @param producerParams Parameters for Kafka producers in each task.
 * @param schema The schema of the input data.
 */
class KafkaStreamWriter(
    topic: Option[String], producerParams: Map[String, String], schema: StructType)
  extends StreamWriter with SupportsCustomWriterMetrics {

  private var customMetrics: KafkaWriterCustomMetrics = _

  validateQuery(schema.toAttributes, producerParams.toMap[String, Object].asJava, topic)

  override def createWriterFactory(): KafkaStreamWriterFactory =
    KafkaStreamWriterFactory(topic, producerParams, schema)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    customMetrics = KafkaWriterCustomMetrics(messages)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def getCustomMetrics: KafkaWriterCustomMetrics = {
    customMetrics
  }

}

/**
 * A [[DataWriterFactory]] for Kafka writing. Will be serialized and sent to executors to generate
 * the per-task data writers.
 * @param topic The topic that should be written to. If None, topic will be inferred from
 *              a `topic` field in the incoming data.
 * @param producerParams Parameters for Kafka producers in each task.
 * @param schema The schema of the input data.
 */
case class KafkaStreamWriterFactory(
    topic: Option[String], producerParams: Map[String, String], schema: StructType)
  extends DataWriterFactory[InternalRow] {

  override def createDataWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new KafkaStreamDataWriter(topic, producerParams, schema.toAttributes)
  }
}

/**
 * A [[DataWriter]] for Kafka writing. One data writer will be created in each partition to
 * process incoming rows.
 *
 * @param targetTopic The topic that this data writer is targeting. If None, topic will be inferred
 *                    from a `topic` field in the incoming data.
 * @param producerParams Parameters to use for the Kafka producer.
 * @param inputSchema The attributes in the input data.
 */
class KafkaStreamDataWriter(
    targetTopic: Option[String], producerParams: Map[String, String], inputSchema: Seq[Attribute])
  extends KafkaRowWriter(inputSchema, targetTopic) with DataWriter[InternalRow] {
  import scala.collection.JavaConverters._

  private lazy val producer = CachedKafkaProducer.getOrCreate(
    new java.util.HashMap[String, Object](producerParams.asJava))

  def write(row: InternalRow): Unit = {
    checkForErrors()
    sendRow(row, producer)
  }

  def commit(): WriterCommitMessage = {
    // Send is asynchronous, but we can't commit until all rows are actually in Kafka.
    // This requires flushing and then checking that no callbacks produced errors.
    // We also check for errors before to fail as soon as possible - the check is cheap.
    checkForErrors()
    producer.flush()
    checkForErrors()
    val minOffset: KafkaSourceOffset = KafkaSourceOffset(minOffsetAccumulator.toMap)
    val maxOffset: KafkaSourceOffset = KafkaSourceOffset(maxOffsetAccumulator.toMap)
    KafkaWriterCommitMessage(minOffset, maxOffset)
  }

  def abort(): Unit = {}

  def close(): Unit = {
    checkForErrors()
    if (producer != null) {
      producer.flush()
      checkForErrors()
      CachedKafkaProducer.close(new java.util.HashMap[String, Object](producerParams.asJava))
    }
  }
}

private[kafka010] case class KafkaWriterCustomMetrics(
    minOffset: KafkaSourceOffset,
    maxOffset: KafkaSourceOffset) extends CustomMetrics {
  override def json(): String = {
    val jsonVal = ("minOffset" -> parse(minOffset.json)) ~
      ("maxOffset" -> parse(maxOffset.json))
    compact(render(jsonVal))
  }

  override def toString: String = json()
}

private[kafka010] object KafkaWriterCustomMetrics {

  import Math.{min, max}

  def apply(messages: Array[WriterCommitMessage]): KafkaWriterCustomMetrics = {
    val minMax = collate(messages)
    KafkaWriterCustomMetrics(minMax._1, minMax._2)
  }

  private def collate(messages: Array[WriterCommitMessage]):
      (KafkaSourceOffset, KafkaSourceOffset) = {

    messages.headOption.flatMap {
      case x: KafkaWriterCommitMessage =>
        val lower = messages.map(_.asInstanceOf[KafkaWriterCommitMessage])
          .map(_.minOffset).reduce(collateLower)
        val higher = messages.map(_.asInstanceOf[KafkaWriterCommitMessage])
          .map(_.maxOffset).reduce(collateHigher)
        Some((lower, higher))
      case _ => throw new IllegalArgumentException()
    }.getOrElse((KafkaSourceOffset(), KafkaSourceOffset()))
  }

  private def collateHigher(o1: KafkaSourceOffset, o2: KafkaSourceOffset): KafkaSourceOffset = {
    collate(o1, o2, max)
  }

  private def collateLower(o1: KafkaSourceOffset, o2: KafkaSourceOffset): KafkaSourceOffset = {
    collate(o1, o2, min)
  }

  private def collate(
      o1: KafkaSourceOffset,
      o2: KafkaSourceOffset,
      collator: (Long, Long) => Long): KafkaSourceOffset = {
    val thisOffsets = o1.partitionToOffsets
    val thatOffsets = o2.partitionToOffsets
    val collated = (thisOffsets.keySet ++ thatOffsets.keySet)
      .map(key =>
        if (!thatOffsets.contains(key)) {
          key -> thisOffsets(key)
        } else if (!thisOffsets.contains(key)) {
          key -> thatOffsets(key)
        } else {
          key -> collator(thisOffsets(key), thatOffsets(key))
        }
      ).toMap
    new KafkaSourceOffset(collated)
  }
}

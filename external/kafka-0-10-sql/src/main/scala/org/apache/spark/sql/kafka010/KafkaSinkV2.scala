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

import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.kafka010.KafkaSourceProvider.{kafkaParamsForProducer, TOPIC_OPTION_KEY}
import org.apache.spark.sql.sources.v2.streaming.writer.ContinuousWriter
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}

class ContinuousKafkaWriter(
    topic: Option[String], producerParams: Map[String, String], schema: StructType)
  extends ContinuousWriter with SupportsWriteInternalRow {

  override def createInternalRowWriterFactory(): KafkaWriterFactory =
    KafkaWriterFactory(topic, producerParams, schema)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

case class KafkaWriterFactory(
    topic: Option[String], producerParams: Map[String, String], schema: StructType)
  extends DataWriterFactory[InternalRow] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[InternalRow] = {
    new KafkaDataWriter(topic, producerParams, schema.toAttributes)
  }
}

case class KafkaWriterCommitMessage() extends WriterCommitMessage {}

class KafkaDataWriter(
    topic: Option[String], producerParams: Map[String, String], inputSchema: Seq[Attribute])
  extends DataWriter[InternalRow] {
  import scala.collection.JavaConverters._

  @volatile private var failedWrite: Exception = _
  private val projection = createProjection
  private lazy val producer = CachedKafkaProducer.getOrCreate(
    new java.util.HashMap[String, Object](producerParams.asJava))

  private val callback = new Callback() {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (failedWrite == null && e != null) {
        failedWrite = e
      }
    }
  }

  def write(row: InternalRow): Unit = {
    checkForErrors()
    val projectedRow = projection(row)
    val topic = projectedRow.getUTF8String(0)
    val key = projectedRow.getBinary(1)
    val value = projectedRow.getBinary(2)

    if (topic == null) {
      throw new NullPointerException(s"null topic present in the data. Use the " +
        s"${KafkaSourceProvider.TOPIC_OPTION_KEY} option for setting a default topic.")
    }
    val record = new ProducerRecord[Array[Byte], Array[Byte]](topic.toString, key, value)
    producer.send(record, callback)
  }

  def commit(): WriterCommitMessage = {
    // Send is asynchronous, but we can't commit until all rows are actually in Kafka.
    // This requires flushing and then checking that no callbacks produced errors.
    producer.flush()
    checkForErrors()
    KafkaWriterCommitMessage()
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

  private def createProjection: UnsafeProjection = {
    val topicExpression = topic.map(Literal(_)).orElse {
      inputSchema.find(_.name == KafkaWriter.TOPIC_ATTRIBUTE_NAME)
    }.getOrElse {
      throw new IllegalStateException(s"topic option required when no " +
        s"'${KafkaWriter.TOPIC_ATTRIBUTE_NAME}' attribute is present")
    }
    topicExpression.dataType match {
      case StringType => // good
      case t =>
        throw new IllegalStateException(s"${KafkaWriter.TOPIC_ATTRIBUTE_NAME} " +
          s"attribute unsupported type $t. ${KafkaWriter.TOPIC_ATTRIBUTE_NAME} " +
          "must be a StringType")
    }
    val keyExpression = inputSchema.find(_.name == KafkaWriter.KEY_ATTRIBUTE_NAME)
      .getOrElse(Literal(null, BinaryType))
    keyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"${KafkaWriter.KEY_ATTRIBUTE_NAME} " +
          s"attribute unsupported type $t")
    }
    val valueExpression = inputSchema
      .find(_.name == KafkaWriter.VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'${KafkaWriter.VALUE_ATTRIBUTE_NAME}' not found")
    )
    valueExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"${KafkaWriter.VALUE_ATTRIBUTE_NAME} " +
          s"attribute unsupported type $t")
    }
    UnsafeProjection.create(
      Seq(topicExpression, Cast(keyExpression, BinaryType),
        Cast(valueExpression, BinaryType)), inputSchema)
  }

  private def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }
}

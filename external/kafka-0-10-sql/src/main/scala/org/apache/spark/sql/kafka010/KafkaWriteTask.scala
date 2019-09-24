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

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.types.{BinaryType, MapType, StringType}

/**
 * Writes out data in a single Spark task, without any concerns about how
 * to commit or abort tasks. Exceptions thrown by the implementation of this class will
 * automatically trigger task aborts.
 */
private[kafka010] class KafkaWriteTask(
    producerConfiguration: ju.Map[String, Object],
    inputSchema: Seq[Attribute],
    topic: Option[String],
    includeHeaders: Boolean = false) extends KafkaRowWriter(inputSchema, topic, includeHeaders) {
  // used to synchronize with Kafka callbacks
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  /**
   * Writes key value data out to topics.
   */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    producer = CachedKafkaProducer.getOrCreate(producerConfiguration)
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      sendRow(currentRow, producer)
    }
  }

  def close(): Unit = {
    checkForErrors()
    if (producer != null) {
      producer.flush()
      checkForErrors()
      producer = null
    }
  }
}

private[kafka010] abstract class KafkaRowWriter(
    inputSchema: Seq[Attribute], topic: Option[String], includeHeaders: Boolean) {

  // used to synchronize with Kafka callbacks
  @volatile protected var failedWrite: Exception = _
  protected val projection = createProjection

  private val callback = new Callback() {
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (failedWrite == null && e != null) {
        failedWrite = e
      }
    }
  }

  /**
   * Send the specified row to the producer, with a callback that will save any exception
   * to failedWrite. Note that send is asynchronous; subclasses must flush() their producer before
   * assuming the row is in Kafka.
   */
  protected def sendRow(
      row: InternalRow, producer: KafkaProducer[Array[Byte], Array[Byte]]): Unit = {
    val projectedRow = projection(row)
    val topic = projectedRow.getUTF8String(0)
    val key = projectedRow.getBinary(1)
    val value = projectedRow.getBinary(2)
    if (topic == null) {
      throw new NullPointerException(s"null topic present in the data. Use the " +
        s"${KafkaSourceProvider.TOPIC_OPTION_KEY} option for setting a default topic.")
    }
    val headers: RecordHeaders = if (includeHeaders) {
      val rowHeaders = projectedRow.getMap(3)
      val len = rowHeaders.numElements()
      val headers = new RecordHeaders()
      val keyArray = rowHeaders.keyArray()
      val valueArray = rowHeaders.valueArray()
      var i = 0
      while (i < len) {
        val key = keyArray.getUTF8String(i).toString
        if (valueArray.isNullAt(i)) {
          headers.add(key, null)
        } else {
          headers.add(key, valueArray.getUTF8String(i).toString.getBytes())
        }
        i += 1
      }
      headers
    } else {
      null
    }

    if (topic == null) {
      throw new NullPointerException(s"null topic present in the data. Use the " +
          s"${KafkaSourceProvider.TOPIC_OPTION_KEY} option for setting a default topic.")
    }

    val record = new ProducerRecord[Array[Byte], Array[Byte]](
      topic.toString, null.asInstanceOf[Integer], null.asInstanceOf[Long], key, value, headers
    )

    producer.send(record, callback)
  }

  protected def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }

  private def createProjection = {
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
          s"must be a ${StringType.catalogString}")
    }
    val keyExpression = inputSchema.find(_.name == KafkaWriter.KEY_ATTRIBUTE_NAME)
      .getOrElse(Literal(null, BinaryType))
    keyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"${KafkaWriter.KEY_ATTRIBUTE_NAME} " +
          s"attribute unsupported type ${t.catalogString}")
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
          s"attribute unsupported type ${t.catalogString}")
    }
    if (includeHeaders) {
      val headerExpression = inputSchema
          .find(_.name == KafkaWriter.HEADERS_ATTRIBUTE_NAME).getOrElse(
        throw new IllegalStateException("Required attribute " +
            s"'${KafkaWriter.HEADERS_ATTRIBUTE_NAME}' not found")
      )
      headerExpression.dataType match {
        case MapType(StringType, StringType, _) => // good
        case _ =>
          throw new AnalysisException(s"${KafkaWriter.HEADERS_ATTRIBUTE_NAME} attribute type must" +
              s" be a ${MapType(StringType, StringType, valueContainsNull = false).catalogString}")
      }
      UnsafeProjection.create(
        Seq(topicExpression, Cast(keyExpression, BinaryType),
          Cast(valueExpression, BinaryType), headerExpression), inputSchema)
    } else {
      UnsafeProjection.create(
        Seq(topicExpression, Cast(keyExpression, BinaryType),
          Cast(valueExpression, BinaryType)), inputSchema)
    }
  }
}


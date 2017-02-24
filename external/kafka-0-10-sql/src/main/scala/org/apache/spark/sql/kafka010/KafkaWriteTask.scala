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

import scala.collection.mutable.ListBuffer

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.ByteArraySerializer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, If, IsNull, Literal, UnsafeProjection}
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A simple trait for writing out data in a single Spark task, without any concerns about how
 * to commit or abort tasks. Exceptions thrown by the implementation of this class will
 * automatically trigger task aborts.
 */
private[kafka010] class KafkaWriteTask(
    producerConfiguration: ju.Map[String, Object],
    inputSchema: Seq[Attribute],
    defaultTopic: Option[String]) {
  // var failedWrites = ListBuffer.empty[Throwable]
  @volatile var failedWrite: Exception = null
  val topicExpression = inputSchema.find(p =>
    p.name == KafkaWriter.TOPIC_ATTRIBUTE_NAME).getOrElse(
      if (defaultTopic == None) {
        throw new IllegalStateException(s"Default topic required when no " +
          s"'${KafkaWriter.TOPIC_ATTRIBUTE_NAME}' attribute is present")
      } else {
        Literal(null, StringType)
      }
    ).map{c =>
      if (defaultTopic == None) {
        c   // return null if we can't fall back on a default value
      } else {
        // fall back on a default value in case we evaluate c to null
        If(IsNull(c), Literal(UTF8String.fromString(defaultTopic.get), StringType), c)
      }
    }
  val keyExpression = inputSchema.find(p =>
    p.name == KafkaWriter.KEY_ATTRIBUTE_NAME).getOrElse(
      Literal(null, BinaryType)
    )
  keyExpression.dataType match {
    case StringType | BinaryType => // good
    case t =>
      throw new IllegalStateException(s"${KafkaWriter.KEY_ATTRIBUTE_NAME} " +
        s"attribute unsupported type $t")
  }
  val valueExpression = inputSchema.find(p =>
    p.name == KafkaWriter.VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException(s"Required attribute " +
        s"'${KafkaWriter.VALUE_ATTRIBUTE_NAME}' not found")
    )
  valueExpression.dataType match {
    case StringType | BinaryType => // good
    case t =>
      throw new IllegalStateException(s"${KafkaWriter.VALUE_ATTRIBUTE_NAME} " +
        s"attribute unsupported type $t")
  }
  val projection = UnsafeProjection.create(topicExpression ++
    Seq(Cast(keyExpression, BinaryType), Cast(valueExpression, BinaryType)), inputSchema)

  // Create a Kafka Producer
  producerConfiguration.put("key.serializer", classOf[ByteArraySerializer].getName)
  producerConfiguration.put("value.serializer", classOf[ByteArraySerializer].getName)
  val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfiguration)

  /**
   * Writes key value data out to topics.
   */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      val projectedRow = projection(currentRow)
      val topic = projectedRow.get(0, StringType).toString
      val key = projectedRow.get(1, BinaryType).asInstanceOf[Array[Byte]]
      val value = projectedRow.get(2, BinaryType).asInstanceOf[Array[Byte]]
      val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)
      val callback = new Callback() {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (failedWrite == null && e != null) {
            failedWrite = e
          }
        }
      }
      producer.send(record, callback)
    }
  }

  def close(): Unit = {
    checkForErrors()
    producer.close()
    checkForErrors()
  }

  private def checkForErrors() = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }
}


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

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, UnsafeProjection}
import org.apache.spark.sql.kafka010.producer.{CachedKafkaProducer, InternalKafkaProducerPool}
import org.apache.spark.sql.types.BinaryType

/**
 * Writes out data in a single Spark task, without any concerns about how
 * to commit or abort tasks. Exceptions thrown by the implementation of this class will
 * automatically trigger task aborts.
 */
private[kafka010] class KafkaWriteTask(
    producerConfiguration: ju.Map[String, Object],
    inputSchema: Seq[Attribute],
    topic: Option[String]) extends KafkaRowWriter(inputSchema, topic) {
  // used to synchronize with Kafka callbacks
  private var producer: Option[CachedKafkaProducer] = None

  /**
   * Writes key value data out to topics.
   */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    producer = Some(InternalKafkaProducerPool.acquire(producerConfiguration))
    val internalProducer = producer.get.producer
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      sendRow(currentRow, internalProducer)
    }
  }

  def close(): Unit = {
    try {
      checkForErrors()
      producer.foreach { p =>
        p.producer.flush()
        checkForErrors()
      }
    } finally {
      producer.foreach(InternalKafkaProducerPool.release)
      producer = None
    }
  }
}

private[kafka010] abstract class KafkaRowWriter(
    inputSchema: Seq[Attribute], topic: Option[String]) {

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
    val partition: Integer =
      if (projectedRow.isNullAt(4)) null else projectedRow.getInt(4)
    val record = if (projectedRow.isNullAt(3)) {
      new ProducerRecord[Array[Byte], Array[Byte]](topic.toString, partition, key, value)
    } else {
      val headerArray = projectedRow.getArray(3)
      val headers = (0 until headerArray.numElements()).map { i =>
        val struct = headerArray.getStruct(i, 2)
        new RecordHeader(struct.getUTF8String(0).toString, struct.getBinary(1))
          .asInstanceOf[Header]
      }
      new ProducerRecord[Array[Byte], Array[Byte]](
        topic.toString, partition, key, value, headers.asJava)
    }
    producer.send(record, callback)
  }

  protected def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }

  private def createProjection = {
    UnsafeProjection.create(
      Seq(
        KafkaWriter.topicExpression(inputSchema, topic),
        Cast(KafkaWriter.keyExpression(inputSchema), BinaryType),
        Cast(KafkaWriter.valueExpression(inputSchema), BinaryType),
        KafkaWriter.headersExpression(inputSchema),
        KafkaWriter.partitionExpression(inputSchema)
      ),
      inputSchema
    )
  }
}


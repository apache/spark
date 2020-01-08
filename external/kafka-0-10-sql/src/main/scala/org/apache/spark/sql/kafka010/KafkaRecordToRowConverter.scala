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

import java.sql.Timestamp

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** A simple class for converting Kafka ConsumerRecord to InternalRow/UnsafeRow */
private[kafka010] class KafkaRecordToRowConverter {
  import KafkaRecordToRowConverter._

  private val toUnsafeRowWithoutHeaders = UnsafeProjection.create(schemaWithoutHeaders)
  private val toUnsafeRowWithHeaders = UnsafeProjection.create(schemaWithHeaders)

  val toInternalRowWithoutHeaders: Record => InternalRow =
    (cr: Record) => InternalRow(
      cr.key, cr.value, UTF8String.fromString(cr.topic), cr.partition, cr.offset,
      DateTimeUtils.fromJavaTimestamp(new Timestamp(cr.timestamp)), cr.timestampType.id
    )

  val toInternalRowWithHeaders: Record => InternalRow =
    (cr: Record) => InternalRow(
      cr.key, cr.value, UTF8String.fromString(cr.topic), cr.partition, cr.offset,
      DateTimeUtils.fromJavaTimestamp(new Timestamp(cr.timestamp)), cr.timestampType.id,
      if (cr.headers.iterator().hasNext) {
        new GenericArrayData(cr.headers.iterator().asScala
          .map(header =>
            InternalRow(UTF8String.fromString(header.key()), header.value())
          ).toArray)
      } else {
        null
      }
    )

  def toUnsafeRowWithoutHeadersProjector: Record => UnsafeRow =
    (cr: Record) => toUnsafeRowWithoutHeaders(toInternalRowWithoutHeaders(cr))

  def toUnsafeRowWithHeadersProjector: Record => UnsafeRow =
    (cr: Record) => toUnsafeRowWithHeaders(toInternalRowWithHeaders(cr))

  def toUnsafeRowProjector(includeHeaders: Boolean): Record => UnsafeRow = {
    if (includeHeaders) toUnsafeRowWithHeadersProjector else toUnsafeRowWithoutHeadersProjector
  }
}

private[kafka010] object KafkaRecordToRowConverter {
  type Record = ConsumerRecord[Array[Byte], Array[Byte]]

  val headersType = ArrayType(StructType(Array(
    StructField("key", StringType),
    StructField("value", BinaryType))))

  private val schemaWithoutHeaders = new StructType(Array(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)
  ))

  private val schemaWithHeaders =
    new StructType(schemaWithoutHeaders.fields :+ StructField("headers", headersType))

  def kafkaSchema(includeHeaders: Boolean): StructType = {
    if (includeHeaders) schemaWithHeaders else schemaWithoutHeaders
  }
}

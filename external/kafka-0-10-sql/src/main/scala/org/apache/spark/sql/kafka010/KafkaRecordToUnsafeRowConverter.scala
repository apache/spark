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

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String

/** A simple class for converting Kafka ConsumerRecord to UnsafeRow */
private[kafka010] class KafkaRecordToUnsafeRowConverter {
  private val rowWriter = new UnsafeRowWriter(7)

  def toUnsafeRow(record: ConsumerRecord[Array[Byte], Array[Byte]]): UnsafeRow = {
    rowWriter.reset()
    rowWriter.zeroOutNullBytes()

    if (record.key == null) {
      rowWriter.setNullAt(0)
    } else {
      rowWriter.write(0, record.key)
    }
    if (record.value == null) {
      rowWriter.setNullAt(1)
    } else {
      rowWriter.write(1, record.value)
    }
    rowWriter.write(2, UTF8String.fromString(record.topic))
    rowWriter.write(3, record.partition)
    rowWriter.write(4, record.offset)
    rowWriter.write(
      5,
      DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(record.timestamp)))
    rowWriter.write(6, record.timestampType.id)
    rowWriter.getRow()
  }
}

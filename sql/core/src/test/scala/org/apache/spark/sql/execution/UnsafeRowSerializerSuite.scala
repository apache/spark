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

package org.apache.spark.sql.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, UnsafeRowConverter}
import org.apache.spark.sql.catalyst.util.ObjectPool
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.PlatformDependent

class UnsafeRowSerializerSuite extends SparkFunSuite {

  private def toUnsafeRow(
      row: Row,
      schema: Array[DataType],
      objPool: ObjectPool = null): UnsafeRow = {
    val internalRow = CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow]
    val rowConverter = new UnsafeRowConverter(schema)
    val rowSizeInBytes = rowConverter.getSizeRequirement(internalRow)
    val byteArray = new Array[Byte](rowSizeInBytes)
    rowConverter.writeRow(
      internalRow, byteArray, PlatformDependent.BYTE_ARRAY_OFFSET, rowSizeInBytes, objPool)
    val unsafeRow = new UnsafeRow()
    unsafeRow.pointTo(
      byteArray, PlatformDependent.BYTE_ARRAY_OFFSET, row.length, rowSizeInBytes, objPool)
    unsafeRow
  }

  test("toUnsafeRow() test helper method") {
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    assert(row.getString(0) === unsafeRow.get(0).toString)
    assert(row.getInt(1) === unsafeRow.getInt(1))
  }

  test("basic row serialization") {
    val rows = Seq(Row("Hello", 1), Row("World", 2))
    val unsafeRows = rows.map(row => toUnsafeRow(row, Array(StringType, IntegerType)))
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val baos = new ByteArrayOutputStream()
    val serializerStream = serializer.serializeStream(baos)
    for (unsafeRow <- unsafeRows) {
      serializerStream.writeKey(0)
      serializerStream.writeValue(unsafeRow)
    }
    serializerStream.close()
    val deserializerIter = serializer.deserializeStream(
      new ByteArrayInputStream(baos.toByteArray)).asKeyValueIterator
    for (expectedRow <- unsafeRows) {
      val actualRow = deserializerIter.next().asInstanceOf[(Integer, UnsafeRow)]._2
      assert(expectedRow.getSizeInBytes === actualRow.getSizeInBytes)
      assert(expectedRow.getString(0) === actualRow.getString(0))
      assert(expectedRow.getInt(1) === actualRow.getInt(1))
    }
    assert(!deserializerIter.hasNext)
  }
}

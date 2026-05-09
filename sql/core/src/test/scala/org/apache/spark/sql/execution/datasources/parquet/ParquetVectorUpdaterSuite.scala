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

package org.apache.spark.sql.execution.datasources.parquet

import java.nio.{ByteBuffer, ByteOrder}
import java.time.ZoneOffset

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._

/**
 * Correctness tests for the INT32 -> Long widening Updater (`IntegerToLongUpdater`)
 * on the Parquet vectorized read path.
 *
 * Covers boundary batch lengths, sign-extension on negative INT32 values, the singular
 * `readValue` path, and the factory's long-decimal dispatch
 * (INT32 + DECIMAL(p<=9) -> DecimalType(p in (9, 18])).
 */
class ParquetVectorUpdaterSuite extends SparkFunSuite {

  // INT32 column descriptor with no logical-type annotation; matches what the production
  // factory uses for plain INT32 -> Long widening.
  private val int32Descriptor: ColumnDescriptor = {
    val pt = Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("col")
    new ColumnDescriptor(Array("col"), pt, 0, 1)
  }

  // INT32 column descriptor annotated as DECIMAL(precision, scale); routes the factory's
  // INT32 dispatch through the `canReadAsLongDecimal` branch when target precision is in
  // (9, 18].
  private def int32DecimalDescriptor(precision: Int, scale: Int): ColumnDescriptor = {
    val pt = Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL)
      .as(LogicalTypeAnnotation.decimalType(scale, precision))
      .named("col")
    new ColumnDescriptor(Array("col"), pt, 0, 1)
  }

  private def newFactory(desc: ColumnDescriptor): ParquetVectorUpdaterFactory =
    ParquetTestAccess.newFactory(
      desc.getPrimitiveType.getLogicalTypeAnnotation,
      ZoneOffset.UTC, "CORRECTED", "UTC", "CORRECTED", "UTC")

  private def plainIntBytes(values: Array[Int]): Array[Byte] = {
    val buf = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < values.length) { buf.putInt(values(i)); i += 1 }
    buf.array()
  }

  private def newPlainReader(bytes: Array[Byte], numValues: Int): VectorizedPlainValuesReader = {
    val r = new VectorizedPlainValuesReader
    r.initFromPage(numValues, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    r
  }

  // Reads `values.length` INT32s through `IntegerToLongUpdater.readValues` and returns the
  // resulting long column.
  private def readViaUpdater(values: Array[Int]): Array[Long] = {
    val fac = newFactory(int32Descriptor)
    val updater = fac.getUpdater(int32Descriptor, DataTypes.LongType)
    // OnHeapColumnVector requires capacity >= 1 even when nothing is written into it.
    val out = new OnHeapColumnVector(values.length.max(1), DataTypes.LongType)
    val reader = newPlainReader(plainIntBytes(values), values.length)
    updater.readValues(values.length, 0, out, reader)
    val result = new Array[Long](values.length)
    var i = 0
    while (i < values.length) { result(i) = out.getLong(i); i += 1 }
    result
  }

  // Test data: a mix of positive, negative, zero, MIN/MAX values to catch sign-extension bugs.
  private def signedSampleValues(n: Int): Array[Int] = {
    val out = new Array[Int](n)
    var i = 0
    while (i < n) {
      out(i) = i match {
        case _ if i % 5 == 0 => Int.MinValue + i
        case _ if i % 5 == 1 => -1
        case _ if i % 5 == 2 => 0
        case _ if i % 5 == 3 => Int.MaxValue - i
        case _ => i * 13 - 7
      }
      i += 1
    }
    out
  }

  private def expectedWiden(values: Array[Int]): Array[Long] = values.map(_.toLong)

  // ---- Boundary-length correctness: empty, sub-batch, batch-aligned, multi-batch ----

  for (n <- Seq(0, 1, 7, 8, 9, 17, 1024, 4097)) {
    test(s"IntegerToLongUpdater produces correct widened output (total=$n)") {
      val input = signedSampleValues(n)
      assert(readViaUpdater(input) === expectedWiden(input))
    }
  }

  // ---- readValue (singular) path is separate from readValues ----

  test("IntegerToLongUpdater: readValue widens a single INT32 -> Long") {
    // The singular readValue is invoked from the RLE/PACKED def-level decoder for runs
    // of length 1, which calls `readInteger()` directly rather than the bulk method.
    // Pinned here so a future change that conflates the two paths is caught at unit level.
    val input = Array(0, 1, -1, 42, Int.MinValue, Int.MaxValue)
    val fac = newFactory(int32Descriptor)
    val updater = fac.getUpdater(int32Descriptor, DataTypes.LongType)
    val out = new OnHeapColumnVector(input.length, DataTypes.LongType)
    val reader = newPlainReader(plainIntBytes(input), input.length)
    var i = 0
    while (i < input.length) {
      updater.readValue(i, out, reader)
      i += 1
    }
    val actual = (0 until input.length).map(out.getLong).toArray
    assert(actual === input.map(_.toLong))
  }

  // ---- Sign-extension: negative INT32 must become negative INT64 ----

  test("IntegerToLongUpdater: negative INT32 sign-extends to negative INT64") {
    val input = Array(Int.MinValue, -1, -42, 0, Int.MaxValue)
    assert(readViaUpdater(input) ===
      Array[Long](Int.MinValue.toLong, -1L, -42L, 0L, Int.MaxValue.toLong))
  }

  // ---- Long-decimal dispatch: factory routes INT32+DECIMAL(p<=9) -> IntegerToLongUpdater
  //      when the Spark target is a DecimalType(precision in (9, 18]) ----

  test("IntegerToLongUpdater handles INT32 -> DecimalType(p<=18) via canReadAsLongDecimal") {
    // Parquet caps INT32 DECIMAL precision at 9 (max digits in int32), so the source is
    // DECIMAL(9, 0); the Spark target DecimalType(15, 0) is a long-decimal (precision in
    // (9, 18]). The factory routes this through `canReadAsLongDecimal` to
    // IntegerToLongUpdater, which writes via `putLong` exactly like the LongType case.
    // This test confirms the dispatch wiring stays intact for long-decimal targets.
    val desc = int32DecimalDescriptor(precision = 9, scale = 0)
    val targetType = DataTypes.createDecimalType(15, 0)
    val input = Array(0, 1, 42, -7, Int.MinValue, Int.MaxValue, 1234567)

    val fac = newFactory(desc)
    val updater = fac.getUpdater(desc, targetType)
    val out = new OnHeapColumnVector(input.length, targetType)
    val reader = newPlainReader(plainIntBytes(input), input.length)
    updater.readValues(input.length, 0, out, reader)

    val actual = (0 until input.length).map(out.getLong).toArray
    assert(actual === input.map(_.toLong))
  }
}

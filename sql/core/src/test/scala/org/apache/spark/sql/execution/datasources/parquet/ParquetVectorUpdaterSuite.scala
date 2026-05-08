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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Correctness tests for `ParquetVectorUpdater` implementations that take a bulk
 * read+convert path when the run length meets a configurable threshold.
 *
 * The tests verify that for every batch length the bulk-path output is byte-for-byte
 * identical to the per-row path that has historically been the only implementation.
 * They cover boundaries around the threshold (below, at, above, and well above) plus
 * sign-extension and empty-batch edge cases.
 */
class ParquetVectorUpdaterSuite extends SparkFunSuite {

  // INT32 column descriptor with no logical-type annotation; matches what the production
  // factory uses for plain INT32 -> Long widening.
  private val int32Descriptor: ColumnDescriptor = {
    val pt = Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("col")
    new ColumnDescriptor(Array("col"), pt, 0, 1)
  }

  // INT32 column descriptor annotated as DECIMAL(precision, scale); routes the factory's
  // INT32 dispatch through the `canReadAsLongDecimal` branch when precision <= 18.
  private def int32DecimalDescriptor(precision: Int, scale: Int): ColumnDescriptor = {
    val pt = Types.primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL)
      .as(LogicalTypeAnnotation.decimalType(scale, precision))
      .named("col")
    new ColumnDescriptor(Array("col"), pt, 0, 1)
  }

  private def newFactory(
      desc: ColumnDescriptor,
      bulkThreshold: Int): ParquetVectorUpdaterFactory =
    ParquetTestAccess.newFactory(
      desc.getPrimitiveType.getLogicalTypeAnnotation,
      ZoneOffset.UTC, "CORRECTED", "UTC", "CORRECTED", "UTC", bulkThreshold)

  private def newFactory(bulkThreshold: Int): ParquetVectorUpdaterFactory =
    newFactory(int32Descriptor, bulkThreshold)

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
  // resulting long column. `bulkThreshold` controls whether the bulk or per-row path runs.
  private def readViaUpdater(values: Array[Int], bulkThreshold: Int): Array[Long] = {
    val fac = newFactory(bulkThreshold)
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

  // Per-row reference: re-implements the legacy loop so the test does not depend on the
  // updater's private internals. Sign-extension (int -> long) is the JVM default cast.
  private def expectedWiden(values: Array[Int]): Array[Long] = values.map(_.toLong)

  // SQLConf default sourced from the conf entry so test code does not duplicate the literal.
  private val defaultThreshold: Int =
    SQLConf.PARQUET_VECTORIZED_UPDATER_BULK_THRESHOLD.defaultValue.get

  // ---- Equivalence: bulk path (low threshold) vs per-row path (high threshold) ----

  for (n <- Seq(0, 1, 7, 8, 9, 17, 1024, 4097)) {
    test(s"IntegerToLongUpdater: bulk and per-row produce identical output (total=$n)") {
      val input = signedSampleValues(n)
      val expected = expectedWiden(input)

      // Force per-row: threshold higher than any input length we test.
      val perRow = readViaUpdater(input, bulkThreshold = Int.MaxValue)
      assert(perRow === expected, s"per-row path produced wrong values for total=$n")

      // Force bulk: threshold = 1 means every non-empty call goes through the bulk path.
      val bulk = readViaUpdater(input, bulkThreshold = 1)
      assert(bulk === expected, s"bulk path produced wrong values for total=$n")

      // Default threshold: both paths exercised across the test suite.
      val dflt = readViaUpdater(input, bulkThreshold = defaultThreshold)
      assert(dflt === expected, s"default-threshold path produced wrong values for total=$n")
    }
  }

  // ---- readValue (singular) is not gated by bulkThreshold and must always widen ----

  test("IntegerToLongUpdater: readValue widens a single INT32 -> Long regardless of threshold") {
    // The singular readValue path is not gated by bulkThreshold; it is invoked from the
    // RLE/PACKED def-level decoder for runs of length 1. This test pins down its behavior
    // so a future change that accidentally routes readValue through the bulk gate is
    // caught at unit level.
    val input = Array(0, 1, -1, 42, Int.MinValue, Int.MaxValue)
    Seq(1, defaultThreshold, Int.MaxValue).foreach { threshold =>
      val fac = newFactory(threshold)
      val updater = fac.getUpdater(int32Descriptor, DataTypes.LongType)
      val out = new OnHeapColumnVector(input.length, DataTypes.LongType)
      val reader = newPlainReader(plainIntBytes(input), input.length)
      var i = 0
      while (i < input.length) {
        updater.readValue(i, out, reader)
        i += 1
      }
      val actual = (0 until input.length).map(out.getLong).toArray
      assert(actual === input.map(_.toLong),
        s"readValue mismatch at threshold=$threshold: actual=${actual.mkString(",")}")
    }
  }

  // ---- Threshold gate: behavior boundary at total == bulkThreshold ----

  // Spy that delegates to a plain reader and counts which path the Updater entered:
  // `bulkCalls` for `readIntegersAsLongs`, `perRowCalls` for `readInteger`. Lets the test
  // distinguish a `>=` gate from a `>` mis-type, which a pure correctness assertion cannot.
  // Only the Updater's own calls on the spy are counted; reads the bulk-path impl performs
  // internally on the delegate's buffer bypass the spy. Other scalar reads inherit the
  // throwing default from `VectorizedReaderBase`/`ValuesReader` so future test extensions
  // to a different Updater fail fast instead of silently no-oping.
  private class SpyReader(delegate: VectorizedPlainValuesReader)
    extends VectorizedReaderBase {
    var bulkCalls: Int = 0
    var perRowCalls: Int = 0
    override def readInteger(): Int = {
      perRowCalls += 1
      delegate.readInteger()
    }
    override def readIntegersAsLongs(
        total: Int,
        c: org.apache.spark.sql.execution.vectorized.WritableColumnVector,
        rowId: Int): Unit = {
      bulkCalls += 1
      delegate.readIntegersAsLongs(total, c, rowId)
    }
  }

  private def runWithSpy(
      values: Array[Int],
      bulkThreshold: Int): SpyReader = {
    val fac = newFactory(bulkThreshold)
    val updater = fac.getUpdater(int32Descriptor, DataTypes.LongType)
    val out = new OnHeapColumnVector(values.length.max(1), DataTypes.LongType)
    val spy = new SpyReader(newPlainReader(plainIntBytes(values), values.length))
    updater.readValues(values.length, 0, out, spy)
    spy
  }

  test("IntegerToLongUpdater: threshold gate is `>=` (total == threshold takes bulk)") {
    // Arbitrary non-default threshold, chosen to disambiguate from the SQLConf default
    // value covered by the hygiene test. The gate semantic is independent of which
    // specific positive integer threshold is configured.
    val threshold = 5

    // total == threshold: must take bulk path exactly once.
    val atThreshold = runWithSpy(signedSampleValues(threshold), bulkThreshold = threshold)
    assert(atThreshold.bulkCalls === 1, "total == threshold should call bulk path once")
    assert(atThreshold.perRowCalls === 0, "total == threshold should not call per-row path")

    // total == threshold - 1: must take per-row path for every value.
    val belowCount = threshold - 1
    val belowThreshold = runWithSpy(signedSampleValues(belowCount), bulkThreshold = threshold)
    assert(belowThreshold.bulkCalls === 0,
      "total < threshold must not call bulk path")
    assert(belowThreshold.perRowCalls === belowCount,
      s"total < threshold must call per-row once per value (expected $belowCount)")

    // Threshold = 1: any non-empty batch takes bulk.
    val alwaysBulk = runWithSpy(signedSampleValues(1), bulkThreshold = 1)
    assert(alwaysBulk.bulkCalls === 1)
    assert(alwaysBulk.perRowCalls === 0)
  }

  // ---- Sign-extension: negative INT32 must become negative INT64 ----

  test("IntegerToLongUpdater: negative INT32 sign-extends to negative INT64") {
    val input = Array(Int.MinValue, -1, -42, 0, Int.MaxValue)
    val out = readViaUpdater(input, bulkThreshold = 1)
    assert(out === Array[Long](Int.MinValue.toLong, -1L, -42L, 0L, Int.MaxValue.toLong))
  }

  // ---- Hygiene: SQLConf default and the legacy-ctor literal must agree ----

  test("PARQUET_VECTORIZED_UPDATER_BULK_THRESHOLD default matches legacy-ctor literal") {
    // The legacy `VectorizedParquetRecordReader(7-arg)` ctor delegates with a hardcoded
    // bulkThreshold of 8. SQLConf catalyst cannot reference a constant declared in sql/core,
    // so the literal is duplicated. This test catches drift when one side is updated alone.
    // (`ParquetTestAccess.newFactory` reads the SQLConf default directly, so it does not
    // duplicate the literal and is not part of the drift surface.)
    assert(SQLConf.PARQUET_VECTORIZED_UPDATER_BULK_THRESHOLD.defaultValue.contains(8),
      "If you change this, also update the literal in VectorizedParquetRecordReader's " +
        "legacy 7-arg constructor.")
  }

  // ---- Long-decimal dispatch: factory routes INT32+DECIMAL(p<=18) -> IntegerToLongUpdater ----

  test("IntegerToLongUpdater handles INT32 -> DecimalType(p<=18) via canReadAsLongDecimal") {
    // Parquet limits INT32 storage to precision <= 9 (max digits in int32), so the source
    // must be DECIMAL(9, 0); the Spark target DecimalType(15, 0) widens to a long-decimal
    // (precision > 9 and <= 18). The factory routes this through `canReadAsLongDecimal` and
    // returns IntegerToLongUpdater. The bulk path writes via `putLong`, identical to the
    // LongType case; this test confirms the dispatch wiring stays intact when the SQL
    // target is a long-decimal rather than LongType.
    val desc = int32DecimalDescriptor(precision = 9, scale = 0)
    val targetType = DataTypes.createDecimalType(15, 0)

    val input = Array(0, 1, 42, -7, Int.MinValue, Int.MaxValue, 1234567)

    Seq(1, defaultThreshold, Int.MaxValue).foreach { threshold =>
      val fac = newFactory(desc, threshold)
      val updater = fac.getUpdater(desc, targetType)
      val out = new OnHeapColumnVector(input.length, targetType)
      val reader = newPlainReader(plainIntBytes(input), input.length)
      updater.readValues(input.length, 0, out, reader)
      val actual = (0 until input.length).map(out.getLong).toArray
      val expected = input.map(_.toLong)
      assert(actual === expected,
        s"Mismatch at bulkThreshold=$threshold: actual=${actual.mkString(",")} " +
          s"expected=${expected.mkString(",")}")
    }
  }
}

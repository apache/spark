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

import java.nio.ByteBuffer
import java.util.PrimitiveIterator

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReaderTestUtils._
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.IntegerType

/**
 * Focused correctness tests for `VectorizedRleValuesReader.readBatch` PACKED-mode decoding,
 * covering patterns the P0 optimization cares about: run boundaries, batch boundaries, and
 * nested def-level grouping. Uses the same reflection bridge as the benchmark.
 */
class VectorizedRleValuesReaderSuite extends SparkFunSuite {

  import VectorizedRleValuesReaderSuite._

  test("PACKED: alternating null/non-null (many single-element runs)") {
    val n = 1024
    val defLevels = Array.tabulate(n)(i => i & 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = n, withDefLevels = false)
  }

  test("PACKED: 4-element runs aligned to 8-group boundaries") {
    val n = 1024
    val defLevels = Array.tabulate(n)(i => if ((i / 4) % 2 == 0) 0 else 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = n, withDefLevels = false)
  }

  test("PACKED: long null and non-null runs within PACKED blocks") {
    // 7-long null then 1 non-null then 7-long null ... forces PACKED (each 8-group is mixed),
    // with asymmetric run lengths typical of realistic sparse data.
    val pattern = Array.fill(7)(0) ++ Array(1)
    val defLevels = Array.fill(128)(pattern).flatten.take(1024)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("PACKED: runs span batch boundaries (state carries across readBatch calls)") {
    // 32-long null run starting at position 100 spans multiple 64-row batches.
    val n = 512
    val defLevels = Array.tabulate(n) { i =>
      if (i >= 100 && i < 132) 0 // null run
      else if ((i / 4) % 2 == 0) 0 // PACKED-forcing background pattern
      else 1
    }
    runAndAssert(defLevels, maxDef = 1, batchSize = 64, withDefLevels = false)
  }

  test("PACKED with defLevels: nested column maxDef=3 with mixed def-level values") {
    // Simulates a nested column where def-level values 0, 1, 2 all mean null (at different
    // nesting levels) and 3 means non-null. Tests that readValuesN groups by exact def-level
    // value so per-level null semantics are preserved.
    val pattern = Array(0, 1, 2, 3, 0, 1, 2, 3, 1, 2, 0, 3)
    val defLevels = Array.fill(64)(pattern).flatten.take(768)
    runAndAssert(defLevels, maxDef = 3, batchSize = 256, withDefLevels = true)
  }

  test("PACKED: cross-batch continuity with defLevels") {
    val n = 256
    val defLevels = Array.tabulate(n)(i => if ((i / 3) % 2 == 0) 0 else 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = 64, withDefLevels = true)
  }

  test("RLE fast path: single long run, no nulls") {
    val defLevels = Array.fill(1024)(1)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("RLE fast path: single long run, all nulls") {
    val defLevels = Array.fill(1024)(0)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("PACKED group larger than initial 16-int currentBuffer (triggers buffer grow)") {
    // ~1024 alternating values produce one PACKED block well beyond the initial 16-int buffer,
    // exercising `new int[currentCount]` in readNextGroup.
    val defLevels = Array.tabulate(1024)(i => i & 1)
    runAndAssert(defLevels, maxDef = 1, batchSize = 1024, withDefLevels = false)
  }

  test("RLE + PACKED mixed in a single page") {
    val defLevels =
      Array.fill(200)(1) ++ Array.tabulate(256)(i => i & 1) ++ Array.fill(200)(0)
    runAndAssert(defLevels, maxDef = 1, batchSize = 256, withDefLevels = true)
  }

  test("required column (maxDef=0): single implicit RLE run") {
    val defLevels = Array.fill(64)(0)
    runAndAssert(defLevels, maxDef = 0, batchSize = 64, withDefLevels = false)
  }

  test("PACKED: row-index filtering with contiguous included range") {
    val n = 256
    val defLevels = Array.tabulate(n)(i => i & 1)
    runAndAssertFiltered(defLevels, maxDef = 1, includedPositions = (50 to 200).toArray)
  }

  test("PACKED: row-index filtering with multiple disjoint ranges") {
    val n = 256
    val defLevels = Array.tabulate(n)(i => if ((i / 3) % 2 == 0) 0 else 1)
    val included = ((10 to 30) ++ (80 to 120) ++ (200 to 240)).toArray
    runAndAssertFiltered(defLevels, maxDef = 1, includedPositions = included)
  }

  test("multi-page: reader reinitialized between pages, state carried via resetForNewPage") {
    val page1 = Array.tabulate(128)(i => i & 1)
    val page2 = Array.fill(64)(1) ++ Array.tabulate(64)(i => i & 1)
    runAndAssertMultiPage(Seq(page1, page2), maxDef = 1, batchSize = 64)
  }
}

private object VectorizedRleValuesReaderSuite {

  /**
   * Runs readBatch end-to-end and asserts null-bits, non-null values, and def levels.
   * Each batch uses a fresh output vector since `state.valueOffset` resets to 0 per batch,
   * mirroring production where `VectorizedColumnReader` hands in a batch-sized vector.
   */
  // Non-trivial value formula: off-by-one mismatches won't coincidentally align.
  private def valueAt(idx: Int): Int = idx * 100 + 7

  private def runAndAssert(
      defLevels: Array[Int],
      maxDef: Int,
      batchSize: Int,
      withDefLevels: Boolean): Unit = {
    val n = defLevels.length
    val bitWidth = if (maxDef == 0) 0 else 32 - Integer.numberOfLeadingZeros(maxDef)
    // When bitWidth == 0 (required column), the reader treats the page as an implicit RLE run
    // of zeros and never consumes bytes; the encoded array is a placeholder.
    val encoded = if (bitWidth == 0) Array.emptyByteArray else encodeRle(defLevels, bitWidth)
    val nonNullCount = defLevels.count(_ == maxDef)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    val reader = new VectorizedRleValuesReader(bitWidth, false)
    reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
    val state = ParquetReadStateTestAccess.newState(intColumnDescriptor(maxDef), maxDef == 0)
    ParquetReadStateTestAccess.resetForNewPage(state, n, 0L)

    var produced = 0
    var expectedValueIdx = 0
    while (produced < n) {
      val toRead = math.min(batchSize, n - produced)
      val values = new OnHeapColumnVector(toRead, IntegerType)
      val defLevelsVec = new OnHeapColumnVector(toRead, IntegerType)
      ParquetReadStateTestAccess.resetForNewBatch(state, toRead)
      val defLevelsArg: WritableColumnVector = if (withDefLevels) defLevelsVec else null
      ParquetReadStateTestAccess.readBatch(
        reader, state, values, defLevelsArg, valueReader, integerUpdater)

      var expectedNullsInBatch = 0
      var i = 0
      while (i < toRead) {
        val absPos = produced + i
        if (defLevels(absPos) == maxDef) {
          assert(!values.isNullAt(i), s"pos $absPos should be non-null")
          val expected = valueAt(expectedValueIdx)
          assert(
            values.getInt(i) == expected,
            s"pos $absPos value mismatch: got ${values.getInt(i)}, expected $expected")
          expectedValueIdx += 1
        } else {
          assert(values.isNullAt(i), s"pos $absPos should be null")
          expectedNullsInBatch += 1
        }
        if (withDefLevels) {
          assert(
            defLevelsVec.getInt(i) == defLevels(absPos),
            s"defLevel at pos $absPos: got ${defLevelsVec.getInt(i)}, " +
              s"expected ${defLevels(absPos)}")
        }
        i += 1
      }
      assert(
        values.numNulls() == expectedNullsInBatch,
        s"batch starting at $produced: numNulls ${values.numNulls()}, " +
          s"expected $expectedNullsInBatch")
      produced += toRead
    }
  }

  /**
   * Variant of `runAndAssert` that passes a `rowIndexes` iterator so the reader only emits
   * rows at the listed positions. Verifies that skipped value positions advance the value
   * reader correctly and that included rows map to the expected values in order.
   */
  private def runAndAssertFiltered(
      defLevels: Array[Int],
      maxDef: Int,
      includedPositions: Array[Int]): Unit = {
    val n = defLevels.length
    val bitWidth = if (maxDef == 0) 0 else 32 - Integer.numberOfLeadingZeros(maxDef)
    val encoded = if (bitWidth == 0) Array.emptyByteArray else encodeRle(defLevels, bitWidth)
    val nonNullCount = defLevels.count(_ == maxDef)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    val reader = new VectorizedRleValuesReader(bitWidth, false)
    reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
    val state = ParquetReadStateTestAccess.newState(
      intColumnDescriptor(maxDef), maxDef == 0, longIterator(includedPositions))
    ParquetReadStateTestAccess.resetForNewPage(state, n, 0L)

    val size = includedPositions.length
    val values = new OnHeapColumnVector(size, IntegerType)
    ParquetReadStateTestAccess.resetForNewBatch(state, size)
    ParquetReadStateTestAccess.readBatch(reader, state, values, null, valueReader, integerUpdater)

    val prefixNonNulls = defLevels.scanLeft(0) { (c, d) =>
      c + (if (d == maxDef) 1 else 0)
    }
    var j = 0
    while (j < size) {
      val p = includedPositions(j)
      if (defLevels(p) == maxDef) {
        assert(!values.isNullAt(j), s"included pos $p (output $j) should be non-null")
        val expected = valueAt(prefixNonNulls(p))
        assert(
          values.getInt(j) == expected,
          s"included pos $p (output $j): got ${values.getInt(j)}, expected $expected")
      } else {
        assert(values.isNullAt(j), s"included pos $p (output $j) should be null")
      }
      j += 1
    }
  }

  /**
   * Simulates a column chunk with multiple pages: the same reader instance is reused, pointing
   * to fresh encoded bytes per page and with `resetForNewPage` called between pages.
   */
  private def runAndAssertMultiPage(
      pages: Seq[Array[Int]],
      maxDef: Int,
      batchSize: Int): Unit = {
    val bitWidth = if (maxDef == 0) 0 else 32 - Integer.numberOfLeadingZeros(maxDef)
    val reader = new VectorizedRleValuesReader(bitWidth, false)
    val state =
      ParquetReadStateTestAccess.newState(intColumnDescriptor(maxDef), maxDef == 0)

    var pageFirstRow = 0L
    pages.foreach { pageDefLevels =>
      val pageN = pageDefLevels.length
      val encoded = if (bitWidth == 0) Array.emptyByteArray else encodeRle(pageDefLevels, bitWidth)
      val nonNullCount = pageDefLevels.count(_ == maxDef)
      val plainBytes = plainIntBytes(nonNullCount)(valueAt)

      reader.initFromPage(pageN, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
      val valueReader = new VectorizedPlainValuesReader
      valueReader.initFromPage(
        nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
      ParquetReadStateTestAccess.resetForNewPage(state, pageN, pageFirstRow)

      var produced = 0
      var expectedValueIdx = 0
      while (produced < pageN) {
        val toRead = math.min(batchSize, pageN - produced)
        val values = new OnHeapColumnVector(toRead, IntegerType)
        ParquetReadStateTestAccess.resetForNewBatch(state, toRead)
        ParquetReadStateTestAccess.readBatch(
          reader, state, values, null, valueReader, integerUpdater)

        var i = 0
        while (i < toRead) {
          val absPos = produced + i
          if (pageDefLevels(absPos) == maxDef) {
            assert(!values.isNullAt(i), s"page@$pageFirstRow pos $absPos should be non-null")
            val expected = valueAt(expectedValueIdx)
            assert(values.getInt(i) == expected)
            expectedValueIdx += 1
          } else {
            assert(values.isNullAt(i), s"page@$pageFirstRow pos $absPos should be null")
          }
          i += 1
        }
        produced += toRead
      }
      pageFirstRow += pageN
    }
  }

  private def longIterator(values: Array[Int]): PrimitiveIterator.OfLong =
    new PrimitiveIterator.OfLong {
      private var idx = 0
      override def hasNext: Boolean = idx < values.length
      override def nextLong(): Long = { val v = values(idx).toLong; idx += 1; v }
    }
}

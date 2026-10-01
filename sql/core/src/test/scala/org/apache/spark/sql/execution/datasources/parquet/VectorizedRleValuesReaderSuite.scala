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
import java.util.concurrent.{Callable, ExecutionException, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.parquet.bytes.{ByteBufferInputStream, BytesInput, BytesUtils}
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReaderTestUtils._
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.util.ThreadUtils

/**
 * Focused correctness tests for `VectorizedRleValuesReader.readBatch` PACKED-mode decoding,
 * covering patterns the P0 optimization cares about: run boundaries, batch boundaries, and
 * nested def-level grouping. Uses the same reflection bridge as the benchmark.
 *
 * The SPARK-59832 tests check that corrupted or truncated input fails with a
 * `ParquetDecodingException` instead of returning wrong values, looping forever or allocating
 * a huge buffer, and that reads ending exactly at the end of the encoded values still work.
 */
class VectorizedRleValuesReaderSuite extends SparkFunSuite {

  import VectorizedRleValuesReaderSuite._

  // Runs the reads that may never return before SPARK-59832. A thread per read, so that a read
  // stuck in a loop does not block the following ones.
  private lazy val executor = ThreadUtils.newDaemonCachedThreadPool("rle-reader-suite")

  override def afterAll(): Unit = {
    try {
      executor.shutdownNow()
    } finally {
      super.afterAll()
    }
  }

  /**
   * Runs `f` in another thread, so that a reader that never returns fails the test, and checks
   * that it fails with a `ParquetDecodingException` whose message contains `expected`.
   */
  private def interceptCorrupted(expected: String)(f: => Any): Unit = {
    val future = executor.submit(new Callable[Any] {
      override def call(): Any = f
    })
    val e = intercept[ParquetDecodingException] {
      try {
        future.get(30, TimeUnit.SECONDS)
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }
    assert(e.getMessage.contains("Corrupted RLE data: " + expected), e.getMessage)
  }

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
    // exercising `new int[currentCount]` in readGroup.
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

  test("PACKED: multi-buffer stream with packed run crossing buffer boundary") {
    // Exercises the MultiBufferInputStream.slice() path where the packed bytes span a buffer
    // boundary, causing slice() to return a freshly-allocated contiguous buffer with
    // position() == 0 (the base-0 pos branch in readGroup).
    val n = 256
    val defLevels = Array.tabulate(n)(i => i & 1)
    val bitWidth = 1
    val encoded = encodeRle(defLevels, bitWidth)

    // Split the encoded bytes into small chunks (e.g. 3 bytes each) so the packed data
    // is guaranteed to span at least one buffer boundary within MultiBufferInputStream.
    val chunkSize = 3
    val buffers = encoded.grouped(chunkSize).map { chunk =>
      ByteBuffer.wrap(chunk)
    }.toList.asJava

    val nonNullCount = defLevels.count(_ == 1)
    val plainBytes = plainIntBytes(nonNullCount)(valueAt)

    val reader = new VectorizedRleValuesReader(bitWidth, false)
    reader.initFromPage(n, ByteBufferInputStream.wrap(buffers))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
    val state = ParquetTestAccess.newState(intColumnDescriptor(1), false)
    ParquetTestAccess.resetForNewPage(state, n, 0L)

    val batchSize = 64
    var produced = 0
    var expectedValueIdx = 0
    while (produced < n) {
      val toRead = math.min(batchSize, n - produced)
      val values = new OnHeapColumnVector(toRead, IntegerType)
      ParquetTestAccess.resetForNewBatch(state, toRead)
      ParquetTestAccess.readBatch(reader, state, values, null, valueReader, integerUpdater)

      var i = 0
      while (i < toRead) {
        val absPos = produced + i
        if (defLevels(absPos) == 1) {
          assert(!values.isNullAt(i), s"pos $absPos should be non-null")
          val expected = valueAt(expectedValueIdx)
          assert(
            values.getInt(i) == expected,
            s"pos $absPos value mismatch: got ${values.getInt(i)}, expected $expected")
          expectedValueIdx += 1
        } else {
          assert(values.isNullAt(i), s"pos $absPos should be null")
        }
        i += 1
      }
      produced += toRead
    }
  }

  test("SPARK-59832: truncated dictionary ids fail instead of being partially read") {
    // A dictionary id page with 10 ids, read as if it had 20.
    streams(dictIdPage(Array.fill(10)(3), bitWidth = 4)).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(20, in())
      val c = new OnHeapColumnVector(20, IntegerType)
      interceptCorrupted(PastEnd)(reader.readIntegers(20, c, 0))
    }
    streams(dictIdPage(Array.fill(10)(3), bitWidth = 4)).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(20, in())
      interceptCorrupted(PastEnd)(reader.skipIntegers(20))
    }
    streams(dictIdPage(Array.fill(10)(3), bitWidth = 4)).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(20, in())
      (0 until 10).foreach(_ => assert(reader.readInteger() == 3))
      interceptCorrupted(PastEnd)(reader.readInteger())
    }
  }

  test("SPARK-59832: truncated booleans fail instead of looping forever") {
    val encoded = encodeRle(Array.fill(10)(1), 1)
    streams(BytesUtils.intToBytes(encoded.length) ++ encoded).foreach { in =>
      val reader = new VectorizedRleValuesReader(1)
      reader.initFromPage(20, in())
      val c = new OnHeapColumnVector(20, BooleanType)
      interceptCorrupted(PastEnd)(reader.readBooleans(20, c, 0))
    }
  }

  test("SPARK-59832: truncated definition levels fail in readBatch") {
    // The page declares 100 values but its definition levels only encode 64. Before the fix
    // readBatch returned without progress, so VectorizedColumnReader.readBatch spun forever.
    val n = 100
    val encoded = encodeRle(Array.fill(64)(1), 1)
    val plainBytes = plainIntBytes(n)(valueAt)
    def run(withDefLevels: Boolean, rowIndexes: PrimitiveIterator.OfLong): Unit = {
      val reader = new VectorizedRleValuesReader(1, false)
      reader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encoded)))
      val valueReader = new VectorizedPlainValuesReader
      valueReader.initFromPage(n, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainBytes)))
      val state = ParquetTestAccess.newState(intColumnDescriptor(1), false, rowIndexes)
      ParquetTestAccess.resetForNewPage(state, n, 0L)
      ParquetTestAccess.resetForNewBatch(state, n)
      val values = new OnHeapColumnVector(n, IntegerType)
      val defLevels = if (withDefLevels) new OnHeapColumnVector(n, IntegerType) else null
      interceptCorrupted(PastEnd) {
        ParquetTestAccess.readBatch(reader, state, values, defLevels, valueReader, integerUpdater)
      }
    }
    run(withDefLevels = false, rowIndexes = null)
    run(withDefLevels = true, rowIndexes = null)
    // Skips the truncated levels to reach rows 80 to 90.
    run(withDefLevels = false, rowIndexes = longIterator((80 to 90).toArray))
  }

  test("SPARK-59832: truncated repetition levels fail in readBatchRepeated") {
    // Every row is a top-level row (repetition level 0), but only 64 of 100 are encoded.
    readRepeated(repLevels = Array.fill(64)(0), defLevels = Array.fill(100)(1), n = 100)
  }

  test("SPARK-59832: truncated definition levels fail in readBatchRepeated") {
    // The repetition levels are complete, but only 64 of 100 definition levels are encoded.
    // Before the fix the missing definition levels were silently left as 0.
    readRepeated(repLevels = Array.fill(100)(0), defLevels = Array.fill(64)(1), n = 100)
    // Skips the truncated levels to reach rows 80 to 90.
    readRepeated(repLevels = Array.fill(100)(0), defLevels = Array.fill(64)(1), n = 100,
      rowIndexes = longIterator((80 to 90).toArray))
  }

  private def readRepeated(
      repLevels: Array[Int],
      defLevels: Array[Int],
      n: Int,
      rowIndexes: PrimitiveIterator.OfLong = null): Unit = {
    val prim = Types.primitive(PrimitiveTypeName.INT32, Repetition.REPEATED).named("col")
    val descriptor = new ColumnDescriptor(Array("col"), prim, 1, 1)
    val repReader = new VectorizedRleValuesReader(1, false)
    repReader.initFromPage(
      n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encodeRle(repLevels, 1))))
    val defReader = new VectorizedRleValuesReader(1, false)
    defReader.initFromPage(
      n, ByteBufferInputStream.wrap(ByteBuffer.wrap(encodeRle(defLevels, 1))))
    val valueReader = new VectorizedPlainValuesReader
    valueReader.initFromPage(
      n, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainIntBytes(n)(valueAt))))
    val state = ParquetTestAccess.newState(descriptor, false, rowIndexes)
    ParquetTestAccess.resetForNewPage(state, n, 0L)
    ParquetTestAccess.resetForNewBatch(state, n)
    val repLevelsVec = new OnHeapColumnVector(n, IntegerType)
    val defLevelsVec = new OnHeapColumnVector(n, IntegerType)
    val values = new OnHeapColumnVector(n, IntegerType)
    interceptCorrupted(PastEnd) {
      ParquetTestAccess.readBatchRepeated(repReader, state, repLevelsVec, defReader,
        defLevelsVec, values, valueReader, integerUpdater)
    }
  }

  test("SPARK-59832: reading past a page with bit width 0 fails") {
    // With bit width 0 the page is one implicit run of `valueCount` zeros; the bytes that
    // follow belong to the next section of the page and must not be decoded as levels.
    val trailing = Array[Byte](3, 1, 2, 3)
    val reader = new VectorizedRleValuesReader(0)
    reader.initFromPage(5, ByteBufferInputStream.wrap(ByteBuffer.wrap(trailing)))
    val c = new OnHeapColumnVector(6, IntegerType)
    c.putInts(0, 6, -1)
    reader.readIntegers(5, c, 0)
    assert((0 until 5).forall(c.getInt(_) == 0))
    interceptCorrupted(PastEnd)(reader.readIntegers(1, c, 5))
  }

  test("SPARK-59832: invalid level length is rejected") {
    val payload = Array[Byte](10, 20, 30, 40)
    Seq(-1, -4, -5, payload.length + 1, Int.MaxValue).foreach { length =>
      streams(BytesUtils.intToBytes(length) ++ payload).foreach { in =>
        val reader = new VectorizedRleValuesReader(1)
        val e = intercept[ParquetDecodingException](reader.initFromPage(8, in()))
        assert(e.getMessage.contains(s"Corrupted RLE data: invalid length $length"),
          e.getMessage)
      }
    }
  }

  test("SPARK-59832: invalid bit-packed run is rejected before allocating its buffer") {
    def check(bitWidth: Int, numGroups: Long, data: Array[Byte])(expected: String): Unit = {
      val page = Array(bitWidth.toByte) ++ varint((numGroups << 1) | 1) ++ data
      streams(page).foreach { in =>
        val reader = new VectorizedRleValuesReader()
        reader.initFromPage(10, in())
        val c = new OnHeapColumnVector(10, IntegerType)
        interceptCorrupted(expected)(reader.readIntegers(10, c, 0))
      }
    }
    // Truncated last group. This was already rejected before (by `in.slice`), even when the
    // bytes cover every value read. parquet-java and Arrow accept it, for compatibility with
    // writers that do not pad the last group.
    check(bitWidth = 4, numGroups = 2, data = Array.fill[Byte](5)(0x11))(
      "bit-packed run of 2 groups needs 8 bytes, but only 5 bytes are left")
    // numGroups * 8 and numGroups * bitWidth overflow to 0.
    check(bitWidth = 8, numGroups = 1L << 29, data = encodeRle(Array.fill(10)(3), 8))(
      s"bit-packed run of ${1 << 29} groups is too long")
    // numGroups * 8 and numGroups * bitWidth overflow to negative values.
    check(bitWidth = 2, numGroups = Int.MaxValue, data = Array.fill[Byte](16)(0))(
      s"bit-packed run of ${Int.MaxValue} groups is too long")
    // Would allocate an int[2^30] buffer before noticing that only 3 bytes are left.
    check(bitWidth = 1, numGroups = 1L << 27, data = Array[Byte](1, 2, 3))(
      s"bit-packed run of ${1 << 27} groups needs ${1 << 27} bytes, but only 3 bytes are left")
  }

  test("SPARK-59832: data ending inside a run header or an RLE value fails") {
    // The continuation byte of the run header is missing, and an RLE run of 10 without its
    // value.
    Seq(Array[Byte](4, 0x80.toByte), Array[Byte](4, 0x14)).foreach { page =>
      streams(page).foreach { in =>
        val reader = new VectorizedRleValuesReader()
        reader.initFromPage(10, in())
        val c = new OnHeapColumnVector(10, IntegerType)
        interceptCorrupted("failed to read from input stream")(reader.readIntegers(10, c, 0))
      }
    }
  }

  test("SPARK-59832: cut-off level length is rejected") {
    streams(Array[Byte](3, 0)).foreach { in =>
      val reader = new VectorizedRleValuesReader(1)
      val e = intercept[ParquetDecodingException](reader.initFromPage(8, in()))
      assert(e.getMessage.contains(
        "Corrupted RLE data: the 4-byte length is cut off, only 2 bytes are left in the page"),
        e.getMessage)
    }
  }

  test("SPARK-59832: invalid dictionary id bit width is rejected") {
    Seq(33, 255).foreach { bitWidth =>
      streams(Array[Byte](bitWidth.toByte, 0x14, 0)).foreach { in =>
        val reader = new VectorizedRleValuesReader()
        val e = intercept[ParquetDecodingException](reader.initFromPage(10, in()))
        assert(e.getMessage.contains(s"Corrupted RLE data: invalid bit width $bitWidth"),
          e.getMessage)
      }
    }
  }

  test("SPARK-59832: dictionary ids of an empty page are not decoded as 0") {
    val reader = new VectorizedRleValuesReader()
    reader.initFromPage(5, ByteBufferInputStream.wrap(ByteBuffer.wrap(Array.emptyByteArray)))
    // An all-null page reads no ids.
    val c = new OnHeapColumnVector(5, IntegerType)
    reader.readIntegers(0, c, 0)
    reader.skipIntegers(0)
    interceptCorrupted(PastEnd)(reader.readIntegers(5, c, 0))
  }

  test("SPARK-59832: runs of length 0 are skipped") {
    // [bit width 4][RLE run of 0 x 7][bit-packed run of 0 groups][RLE run of 5 x 3]
    val page = Array[Byte](4, 0, 7, 1, 10, 3)
    val r1 = new VectorizedRleValuesReader()
    r1.initFromPage(5, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    assert((0 until 5).map(_ => r1.readInteger()) == Seq.fill(5)(3))
    val r2 = new VectorizedRleValuesReader()
    r2.initFromPage(5, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    val c = new OnHeapColumnVector(5, IntegerType)
    r2.readIntegers(5, c, 0)
    assert((0 until 5).map(c.getInt) == Seq.fill(5)(3))
  }

  test("SPARK-59832: reads that end exactly at the end of the encoded values succeed") {
    // A run of 20 identical ids (RLE) followed by 17 mixed ids (bit-packed, padded to 24).
    val ids = Array.tabulate(37)(i => if (i < 20) 5 else i % 7)
    streams(dictIdPage(ids, bitWidth = 3)).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(ids.length, in())
      val c = new OnHeapColumnVector(ids.length, IntegerType)
      reader.readIntegers(ids.length, c, 0)
      assert((0 until ids.length).map(c.getInt) == ids.toSeq)
    }
    streams(dictIdPage(ids, bitWidth = 3)).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(ids.length, in())
      reader.skipIntegers(30)
      assert((30 until ids.length).map(_ => reader.readInteger()) == ids.drop(30).toSeq)
    }

    val booleans = Array.tabulate(37)(i => if (i < 20) 1 else i % 2)
    val encoded = encodeRle(booleans, 1)
    // The level length covers exactly the rest of the page.
    streams(BytesUtils.intToBytes(encoded.length) ++ encoded).foreach { in =>
      val reader = new VectorizedRleValuesReader(1)
      reader.initFromPage(booleans.length, in())
      val c = new OnHeapColumnVector(booleans.length, BooleanType)
      reader.readBooleans(booleans.length, c, 0)
      assert((0 until booleans.length).map(c.getBoolean) == booleans.map(_ == 1).toSeq)
    }
  }
}

private object VectorizedRleValuesReaderSuite {

  /**
   * The page as a single buffer and split into 1-byte buffers. The split keeps any part of the
   * page longer than 1 byte, including a slice of it, on a `MultiBufferInputStream`.
   */
  private def streams(bytes: Array[Byte]): Seq[() => ByteBufferInputStream] = {
    assert(bytes.length > 1, "the page must span multiple buffers")
    Seq(
      () => ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)),
      () => ByteBufferInputStream.wrap(bytes.grouped(1).map(ByteBuffer.wrap).toList.asJava))
  }

  /** A dictionary id section: the bit width followed by the RLE/bit-packed ids. */
  private def dictIdPage(ids: Array[Int], bitWidth: Int): Array[Byte] =
    Array(bitWidth.toByte) ++ encodeRle(ids, bitWidth)

  private val PastEnd = "reading past the end of the encoded values"

  private def varint(value: Long): Array[Byte] =
    BytesInput.fromUnsignedVarLong(value).toByteArray

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
    val state = ParquetTestAccess.newState(intColumnDescriptor(maxDef), maxDef == 0)
    ParquetTestAccess.resetForNewPage(state, n, 0L)

    var produced = 0
    var expectedValueIdx = 0
    while (produced < n) {
      val toRead = math.min(batchSize, n - produced)
      val values = new OnHeapColumnVector(toRead, IntegerType)
      val defLevelsVec = new OnHeapColumnVector(toRead, IntegerType)
      ParquetTestAccess.resetForNewBatch(state, toRead)
      val defLevelsArg: WritableColumnVector = if (withDefLevels) defLevelsVec else null
      ParquetTestAccess.readBatch(
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
    val state = ParquetTestAccess.newState(
      intColumnDescriptor(maxDef), maxDef == 0, longIterator(includedPositions))
    ParquetTestAccess.resetForNewPage(state, n, 0L)

    val size = includedPositions.length
    val values = new OnHeapColumnVector(size, IntegerType)
    ParquetTestAccess.resetForNewBatch(state, size)
    ParquetTestAccess.readBatch(reader, state, values, null, valueReader, integerUpdater)

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
      ParquetTestAccess.newState(intColumnDescriptor(maxDef), maxDef == 0)

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
      ParquetTestAccess.resetForNewPage(state, pageN, pageFirstRow)

      var produced = 0
      var expectedValueIdx = 0
      while (produced < pageN) {
        val toRead = math.min(batchSize, pageN - produced)
        val values = new OnHeapColumnVector(toRead, IntegerType)
        ParquetTestAccess.resetForNewBatch(state, toRead)
        ParquetTestAccess.readBatch(
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

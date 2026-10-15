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

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.util.PrimitiveIterator
import java.util.concurrent.{Callable, ExecutionException, TimeoutException, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.parquet.bytes.{ByteBufferInputStream, BytesInput, BytesUtils,
  HeapByteBufferAllocator}
import org.apache.parquet.column.{ColumnDescriptor, Dictionary}
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.util.ThreadUtils

/**
 * Tests that `VectorizedRleValuesReader` fails with a `ParquetDecodingException` on corrupted
 * or truncated input instead of returning wrong values, looping forever or allocating a huge
 * buffer, and that reads ending exactly at the end of the encoded values still work
 * (SPARK-59832).
 */
class VectorizedRleValuesReaderSuite extends SparkFunSuite {

  import VectorizedRleValuesReaderSuite._

  // Runs the reads that may never return before SPARK-59832, a thread per read, so that a read
  // stuck in a loop does not block the following ones. The decode loops do not check for
  // interruption, so a stuck read keeps its thread busy until the JVM exits. After the first
  // timeout, later reads fail without running, so that a regression leaves one busy thread.
  private val executor = ThreadUtils.newDaemonCachedThreadPool("rle-reader-suite")
  @volatile private var timedOut = false

  override def afterAll(): Unit = {
    try {
      executor.shutdownNow()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    } finally {
      super.afterAll()
    }
  }

  /**
   * Checks that `f` fails with a `ParquetDecodingException` whose message contains
   * "Corrupted RLE data: " followed by `expected`.
   */
  private def assertCorrupted(expected: String)(f: => Any): Unit = {
    val e = intercept[ParquetDecodingException](f)
    assert(e.getMessage.contains("Corrupted RLE data: " + expected), e.getMessage)
  }

  /**
   * Like `assertCorrupted`, but runs `f` on another thread with a timeout, so that a read that
   * never returns fails the test.
   */
  private def interceptCorrupted(expected: String)(f: => Any): Unit = {
    assert(!timedOut, "Not run, since an earlier read did not return")
    val future = executor.submit(new Callable[Any] {
      override def call(): Any = f
    })
    assertCorrupted(expected) {
      try {
        future.get(10, TimeUnit.SECONDS)
      } catch {
        case e: ExecutionException => throw e.getCause
        case e: TimeoutException =>
          timedOut = true
          throw e
      }
    }
  }

  /** Checks that reading `n` dictionary ids from `page` fails, on every stream of `page`. */
  private def assertReadIntegersFails(page: Array[Byte], n: Int, expected: String): Unit = {
    streams(page).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(n, in())
      val c = new OnHeapColumnVector(n, IntegerType)
      interceptCorrupted(expected)(reader.readIntegers(n, c, 0))
    }
  }

  test("SPARK-59832: truncated dictionary ids fail instead of being partially read") {
    // A dictionary id page with 10 ids, read as if it had 20.
    val page = dictIdPage(Array.fill(10)(3), bitWidth = 4)
    assertReadIntegersFails(page, 20, PastEnd)
    streams(page).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(20, in())
      interceptCorrupted(PastEnd)(reader.skipIntegers(20))
    }
    streams(page).foreach { in =>
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
      val state = new ParquetReadState(intColumnDescriptor(1), false, rowIndexes)
      state.resetForNewPage(n, 0L)
      state.resetForNewBatch(n)
      val values = new OnHeapColumnVector(n, IntegerType)
      val defLevels = if (withDefLevels) new OnHeapColumnVector(n, IntegerType) else null
      interceptCorrupted(PastEnd) {
        reader.readBatch(state, values, defLevels, valueReader, integerUpdater)
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
    val state = new ParquetReadState(descriptor, false, rowIndexes)
    state.resetForNewPage(n, 0L)
    state.resetForNewBatch(n)
    val repLevelsVec = new OnHeapColumnVector(n, IntegerType)
    val defLevelsVec = new OnHeapColumnVector(n, IntegerType)
    val values = new OnHeapColumnVector(n, IntegerType)
    interceptCorrupted(PastEnd) {
      repReader.readBatchRepeated(state, repLevelsVec, defReader,
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
        assertCorrupted(s"invalid length $length")(reader.initFromPage(8, in()))
      }
    }
  }

  test("SPARK-59832: invalid bit-packed run is rejected before allocating its buffer") {
    def check(bitWidth: Int, numGroups: Long, data: Array[Byte])(expected: String): Unit =
      assertReadIntegersFails(
        Array(bitWidth.toByte) ++ varint((numGroups << 1) | 1) ++ data, 10, expected)
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
      assertReadIntegersFails(page, 10, "failed to read from input stream")
    }
  }

  test("SPARK-59832: cut-off level length is rejected") {
    streams(Array[Byte](3, 0)).foreach { in =>
      val reader = new VectorizedRleValuesReader(1)
      assertCorrupted("the 4-byte length is cut off, only 2 bytes are left in the page") {
        reader.initFromPage(8, in())
      }
    }
  }

  test("SPARK-59832: invalid dictionary id bit width is rejected") {
    Seq(33, 255).foreach { bitWidth =>
      streams(Array[Byte](bitWidth.toByte, 0x14, 0)).foreach { in =>
        val reader = new VectorizedRleValuesReader()
        assertCorrupted(s"invalid bit width $bitWidth")(reader.initFromPage(10, in()))
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
    interceptCorrupted("the dictionary id section is empty, not even the bit width") {
      reader.readIntegers(5, c, 0)
    }
  }

  test("SPARK-59832: runs of length 0 are skipped") {
    // [bit width 4][RLE run of 0 x 7][bit-packed run of 0 groups][RLE run of 5 x 3]
    val page = Array[Byte](4, 0, 7, 1, 10, 3)
    streams(page).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(5, in())
      assert((0 until 5).map(_ => reader.readInteger()) == Seq.fill(5)(3))
    }
    streams(page).foreach { in =>
      val reader = new VectorizedRleValuesReader()
      reader.initFromPage(5, in())
      val c = new OnHeapColumnVector(5, IntegerType)
      reader.readIntegers(5, c, 0)
      assert((0 until 5).map(c.getInt) == Seq.fill(5)(3))
    }
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

  // Non-trivial value formula: off-by-one mismatches won't coincidentally align.
  private def valueAt(idx: Int): Int = idx * 100 + 7

  private def longIterator(values: Array[Int]): PrimitiveIterator.OfLong =
    new PrimitiveIterator.OfLong {
      private var idx = 0
      override def hasNext: Boolean = idx < values.length
      override def nextLong(): Long = { val v = values(idx).toLong; idx += 1; v }
    }

  /** Encodes values via parquet-mr's RLE/bit-packing hybrid encoder (no length prefix). */
  def encodeRle(values: Array[Int], bitWidth: Int): Array[Byte] = {
    val enc = new RunLengthBitPackingHybridEncoder(
      bitWidth, 64 * 1024, 1024 * 1024, new HeapByteBufferAllocator)
    values.foreach(enc.writeInt)
    val out = new ByteArrayOutputStream()
    enc.toBytes.writeAllTo(out)
    out.toByteArray
  }

  /** Little-endian plain INT32 bytes. Layout matches `VectorizedPlainValuesReader.readIntegers`. */
  def plainIntBytes(count: Int)(f: Int => Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < count) {
      buf.putInt(f(i))
      i += 1
    }
    buf.array()
  }

  /** Single-column INT32 descriptor with the given `maxDefinitionLevel`. */
  def intColumnDescriptor(maxDef: Int): ColumnDescriptor = {
    val rep = if (maxDef == 0) Repetition.REQUIRED else Repetition.OPTIONAL
    val prim = Types.primitive(PrimitiveTypeName.INT32, rep).named("col")
    new ColumnDescriptor(Array("col"), prim, 0, maxDef)
  }

  /** Stateless INT32 updater equivalent to the package-private `IntegerUpdater`. */
  val integerUpdater: ParquetVectorUpdater = new ParquetVectorUpdater {
    override def readValues(
        total: Int, offset: Int,
        values: WritableColumnVector, reader: VectorizedValuesReader): Unit =
      reader.readIntegers(total, values, offset)

    override def skipValues(total: Int, reader: VectorizedValuesReader): Unit =
      reader.skipIntegers(total)

    override def readValue(
        offset: Int, values: WritableColumnVector, reader: VectorizedValuesReader): Unit =
      values.putInt(offset, reader.readInteger())

    override def decodeSingleDictionaryId(
        offset: Int,
        values: WritableColumnVector,
        dictionaryIds: WritableColumnVector,
        dictionary: Dictionary): Unit =
      values.putInt(offset, dictionary.decodeToInt(dictionaryIds.getDictId(offset)))
  }
}

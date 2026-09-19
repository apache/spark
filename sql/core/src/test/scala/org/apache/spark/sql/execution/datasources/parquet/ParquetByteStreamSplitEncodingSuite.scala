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
import java.util.Random

import scala.reflect.ClassTag

import org.apache.parquet.bytes.{ByteBufferInputStream, DirectByteBufferAllocator}
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter._
import org.apache.parquet.io.api.Binary

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._

/**
 * Unit tests for [[VectorizedByteStreamSplitValuesReader]].
 *
 * Uses parquet-mr's ByteStreamSplitValuesWriter to encode data, then reads it
 * back with the vectorized reader and verifies correctness. An abstract base
 * covers the shared test matrix (batch reads, single-value reads, skip, direct
 * buffers, extreme values) for all numeric types; concrete sub-classes supply
 * only the type-specific writer/reader/comparison methods. FLBA is tested in a
 * standalone suite because its reader API differs (readBinary vs typed batch).
 */
abstract class ParquetByteStreamSplitEncodingSuite[T: ClassTag] extends SparkFunSuite {

  protected val random = new Random(42)

  // --- type-specific hooks ---

  protected def typeWidth: Int
  protected def sparkType: DataType

  /** Encode values with the parquet-mr ByteStreamSplitValuesWriter. */
  protected def encode(data: Array[T]): Array[Byte]

  /** Batch read from the vectorized reader into a column vector. */
  protected def readBatch(
      reader: VectorizedByteStreamSplitValuesReader,
      total: Int, cv: WritableColumnVector, rowId: Int): Unit

  /** Skip values in the vectorized reader. */
  protected def skipBatch(
      reader: VectorizedByteStreamSplitValuesReader, total: Int): Unit

  /** Read a single value from the vectorized reader. */
  protected def readSingle(reader: VectorizedByteStreamSplitValuesReader): T

  /** Extract a value from the column vector at the given row. */
  protected def getFromVector(cv: WritableColumnVector, rowId: Int): T

  /** Return a new random value (called repeatedly for random-data tests). */
  protected def nextRandom: T

  /** Return a deterministic value for index i (for sequential-data tests). */
  protected def sequentialValue(i: Int): T

  /** Boundary / extreme values to exercise. */
  protected def extremeValues: Array[T]

  /** A single representative value. */
  protected def singleTestValue: T

  /** Override for types that need bitwise comparison (Float, Double). */
  protected def assertEqual(expected: T, actual: T, msg: String): Unit = {
    assert(expected === actual, msg)
  }

  // --- shared helpers ---

  private def newReader(
      page: Array[Byte], count: Int,
      useDirect: Boolean = false): VectorizedByteStreamSplitValuesReader = {
    val reader = new VectorizedByteStreamSplitValuesReader(typeWidth)
    val buf = if (useDirect) {
      val b = ByteBuffer.allocateDirect(page.length)
      b.put(page); b.flip(); b
    } else {
      ByteBuffer.wrap(page)
    }
    reader.initFromPage(count, ByteBufferInputStream.wrap(buf))
    reader
  }

  private def readAndVerify(data: Array[T], useDirect: Boolean = false): Unit = {
    val page = encode(data)
    val reader = newReader(page, data.length, useDirect)
    val cv = new OnHeapColumnVector(data.length, sparkType)
    try {
      readBatch(reader, data.length, cv, 0)
      for (i <- data.indices) {
        assertEqual(data(i), getFromVector(cv, i), s"mismatch at index $i")
      }
    } finally {
      cv.close()
    }
  }

  // --- tests ---

  test("batch read - sequential values") {
    readAndVerify(Array.tabulate(1000)(i => sequentialValue(i)))
  }

  test("batch read - random values") {
    readAndVerify(Array.fill(1000)(nextRandom))
  }

  test("batch read - extreme values") {
    readAndVerify(extremeValues)
  }

  test("batch read - single value") {
    readAndVerify(Array(singleTestValue))
  }

  test("batch read - direct byte buffer") {
    readAndVerify(Array.fill(500)(nextRandom), useDirect = true)
  }

  test("single-value read") {
    val data = Array.fill(100)(nextRandom)
    val reader = newReader(encode(data), data.length)
    for (i <- data.indices) {
      assertEqual(data(i), readSingle(reader), s"mismatch at index $i")
    }
  }

  test("skip then read") {
    val data = Array.tabulate(200)(i => sequentialValue(i))
    val page = encode(data)
    val reader = newReader(page, data.length)
    val cv = new OnHeapColumnVector(data.length, sparkType)
    try {
      // read 10, skip 20, read 10, skip 50, read remaining 110
      readBatch(reader, 10, cv, 0)
      for (i <- 0 until 10) assertEqual(data(i), getFromVector(cv, i), s"mismatch at $i")
      skipBatch(reader, 20)
      readBatch(reader, 10, cv, 10)
      for (i <- 0 until 10) {
        assertEqual(data(30 + i), getFromVector(cv, 10 + i), s"mismatch at ${30 + i}")
      }
      skipBatch(reader, 50)
      val remaining = data.length - 90
      readBatch(reader, remaining, cv, 20)
      for (i <- 0 until remaining) {
        assertEqual(data(90 + i), getFromVector(cv, 20 + i), s"mismatch at ${90 + i}")
      }
    } finally {
      cv.close()
    }
  }
}

// --- Concrete suites ---

/** Helper to create a parquet-mr BSS writer, write values, and return the encoded bytes. */
private object BssWriterHelper {
  def encode(writer: ValuesWriter)(writeAll: ValuesWriter => Unit): Array[Byte] = {
    writeAll(writer)
    val bytes = writer.getBytes.toByteArray
    writer.close()
    bytes
  }
}

class ParquetByteStreamSplitEncodingIntegerSuite
    extends ParquetByteStreamSplitEncodingSuite[Int] {

  override protected def typeWidth: Int = 4
  override protected def sparkType: DataType = IntegerType

  override protected def encode(data: Array[Int]): Array[Byte] =
    BssWriterHelper.encode(
      new IntegerByteStreamSplitValuesWriter(
        data.length, data.length * 4, new DirectByteBufferAllocator())
    )(w => data.foreach(w.writeInteger))

  override protected def readBatch(
      r: VectorizedByteStreamSplitValuesReader,
      total: Int, cv: WritableColumnVector, rowId: Int): Unit =
    r.readIntegers(total, cv, rowId)

  override protected def skipBatch(
      r: VectorizedByteStreamSplitValuesReader, total: Int): Unit =
    r.skipIntegers(total)

  override protected def readSingle(r: VectorizedByteStreamSplitValuesReader): Int =
    r.readInteger()

  override protected def getFromVector(cv: WritableColumnVector, rowId: Int): Int =
    cv.getInt(rowId)

  override protected def nextRandom: Int = random.nextInt()
  override protected def sequentialValue(i: Int): Int = i * 7
  override protected def extremeValues: Array[Int] =
    Array(Int.MinValue, Int.MaxValue, 0, -1, 1)
  override protected def singleTestValue: Int = 42
}

class ParquetByteStreamSplitEncodingLongSuite
    extends ParquetByteStreamSplitEncodingSuite[Long] {

  override protected def typeWidth: Int = 8
  override protected def sparkType: DataType = LongType

  override protected def encode(data: Array[Long]): Array[Byte] =
    BssWriterHelper.encode(
      new LongByteStreamSplitValuesWriter(
        data.length, data.length * 8, new DirectByteBufferAllocator())
    )(w => data.foreach(w.writeLong))

  override protected def readBatch(
      r: VectorizedByteStreamSplitValuesReader,
      total: Int, cv: WritableColumnVector, rowId: Int): Unit =
    r.readLongs(total, cv, rowId)

  override protected def skipBatch(
      r: VectorizedByteStreamSplitValuesReader, total: Int): Unit =
    r.skipLongs(total)

  override protected def readSingle(r: VectorizedByteStreamSplitValuesReader): Long =
    r.readLong()

  override protected def getFromVector(cv: WritableColumnVector, rowId: Int): Long =
    cv.getLong(rowId)

  override protected def nextRandom: Long = random.nextLong()
  override protected def sequentialValue(i: Int): Long = i.toLong * 7
  override protected def extremeValues: Array[Long] =
    Array(Long.MinValue, Long.MaxValue, 0L, -1L, 1L)
  override protected def singleTestValue: Long = 42L
}

class ParquetByteStreamSplitEncodingFloatSuite
    extends ParquetByteStreamSplitEncodingSuite[Float] {

  override protected def typeWidth: Int = 4
  override protected def sparkType: DataType = FloatType

  override protected def encode(data: Array[Float]): Array[Byte] =
    BssWriterHelper.encode(
      new FloatByteStreamSplitValuesWriter(
        data.length, data.length * 4, new DirectByteBufferAllocator())
    )(w => data.foreach(w.writeFloat))

  override protected def readBatch(
      r: VectorizedByteStreamSplitValuesReader,
      total: Int, cv: WritableColumnVector, rowId: Int): Unit =
    r.readFloats(total, cv, rowId)

  override protected def skipBatch(
      r: VectorizedByteStreamSplitValuesReader, total: Int): Unit =
    r.skipFloats(total)

  override protected def readSingle(r: VectorizedByteStreamSplitValuesReader): Float =
    r.readFloat()

  override protected def getFromVector(cv: WritableColumnVector, rowId: Int): Float =
    cv.getFloat(rowId)

  override protected def nextRandom: Float = random.nextFloat()
  override protected def sequentialValue(i: Int): Float = i * 0.1f
  override protected def extremeValues: Array[Float] = Array(
    0.0f, -0.0f, Float.MinValue, Float.MaxValue,
    Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity,
    1.0f, -1.0f, Float.MinPositiveValue)
  override protected def singleTestValue: Float = 3.14f

  override protected def assertEqual(expected: Float, actual: Float, msg: String): Unit = {
    assert(java.lang.Float.floatToRawIntBits(expected) ===
      java.lang.Float.floatToRawIntBits(actual), msg)
  }
}

class ParquetByteStreamSplitEncodingDoubleSuite
    extends ParquetByteStreamSplitEncodingSuite[Double] {

  override protected def typeWidth: Int = 8
  override protected def sparkType: DataType = DoubleType

  override protected def encode(data: Array[Double]): Array[Byte] =
    BssWriterHelper.encode(
      new DoubleByteStreamSplitValuesWriter(
        data.length, data.length * 8, new DirectByteBufferAllocator())
    )(w => data.foreach(w.writeDouble))

  override protected def readBatch(
      r: VectorizedByteStreamSplitValuesReader,
      total: Int, cv: WritableColumnVector, rowId: Int): Unit =
    r.readDoubles(total, cv, rowId)

  override protected def skipBatch(
      r: VectorizedByteStreamSplitValuesReader, total: Int): Unit =
    r.skipDoubles(total)

  override protected def readSingle(r: VectorizedByteStreamSplitValuesReader): Double =
    r.readDouble()

  override protected def getFromVector(cv: WritableColumnVector, rowId: Int): Double =
    cv.getDouble(rowId)

  override protected def nextRandom: Double = random.nextDouble()
  override protected def sequentialValue(i: Int): Double = i * 0.1
  override protected def extremeValues: Array[Double] = Array(
    0.0, -0.0, Double.MinValue, Double.MaxValue,
    Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity,
    1.0, -1.0, Double.MinPositiveValue)
  override protected def singleTestValue: Double = 3.14159265358979

  override protected def assertEqual(expected: Double, actual: Double, msg: String): Unit = {
    assert(java.lang.Double.doubleToRawLongBits(expected) ===
      java.lang.Double.doubleToRawLongBits(actual), msg)
  }
}

class ParquetByteStreamSplitEncodingFLBASuite extends SparkFunSuite {
  private val random = new Random(42)

  private def writeFLBA(data: Array[Array[Byte]], typeWidth: Int): Array[Byte] =
    BssWriterHelper.encode(
      new FixedLenByteArrayByteStreamSplitValuesWriter(
        typeWidth, data.length, data.length * typeWidth, new DirectByteBufferAllocator())
    )(w => data.foreach(b => w.writeBytes(Binary.fromConstantByteArray(b))))

  private def readAndVerifyFLBA(data: Array[Array[Byte]], typeWidth: Int): Unit = {
    val page = writeFLBA(data, typeWidth)
    val reader = new VectorizedByteStreamSplitValuesReader(typeWidth)
    reader.initFromPage(data.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    for (i <- data.indices) {
      val actual = reader.readBinary(typeWidth)
      assert(actual.getBytes.toSeq === data(i).toSeq, s"mismatch at index $i")
    }
  }

  test("read FLBA - width 4") {
    readAndVerifyFLBA(
      Array.fill(200)(Array.fill(4)(random.nextInt(256).toByte)), typeWidth = 4)
  }

  test("read FLBA - width 16") {
    readAndVerifyFLBA(
      Array.fill(100)(Array.fill(16)(random.nextInt(256).toByte)), typeWidth = 16)
  }

  // Odd widths exercise the generic assembly loop (not aligned to int/long boundaries)
  test("read FLBA - width 2 (float16-sized)") {
    readAndVerifyFLBA(
      Array.fill(300)(Array.fill(2)(random.nextInt(256).toByte)), typeWidth = 2)
  }

  test("read FLBA - width 3") {
    readAndVerifyFLBA(
      Array.fill(200)(Array.fill(3)(random.nextInt(256).toByte)), typeWidth = 3)
  }

  test("read FLBA - width 5") {
    readAndVerifyFLBA(
      Array.fill(150)(Array.fill(5)(random.nextInt(256).toByte)), typeWidth = 5)
  }

  test("read FLBA - width 7") {
    readAndVerifyFLBA(
      Array.fill(120)(Array.fill(7)(random.nextInt(256).toByte)), typeWidth = 7)
  }

  test("read FLBA - width 12 (decimal-sized)") {
    readAndVerifyFLBA(
      Array.fill(100)(Array.fill(12)(random.nextInt(256).toByte)), typeWidth = 12)
  }

  test("skip FLBA") {
    val typeWidth = 4
    val data = Array.fill(100)(Array.fill(typeWidth)(random.nextInt(256).toByte))
    val page = writeFLBA(data, typeWidth)
    val reader = new VectorizedByteStreamSplitValuesReader(typeWidth)
    reader.initFromPage(data.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    reader.skipFixedLenByteArray(10, typeWidth)
    for (i <- 10 until data.length) {
      val actual = reader.readBinary(typeWidth)
      assert(actual.getBytes.toSeq === data(i).toSeq, s"mismatch at index $i")
    }
  }

  test("skip + read interleaving FLBA - odd width 5") {
    val typeWidth = 5
    val data = Array.fill(200)(Array.fill(typeWidth)(random.nextInt(256).toByte))
    val page = writeFLBA(data, typeWidth)
    val reader = new VectorizedByteStreamSplitValuesReader(typeWidth)
    reader.initFromPage(data.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    // read 10, skip 30, read 50, skip 60, read remaining
    for (i <- 0 until 10) {
      val actual = reader.readBinary(typeWidth)
      assert(actual.getBytes.toSeq === data(i).toSeq, s"mismatch at index $i")
    }
    reader.skipFixedLenByteArray(30, typeWidth)
    for (i <- 40 until 90) {
      val actual = reader.readBinary(typeWidth)
      assert(actual.getBytes.toSeq === data(i).toSeq, s"mismatch at index $i")
    }
    reader.skipFixedLenByteArray(60, typeWidth)
    for (i <- 150 until data.length) {
      val actual = reader.readBinary(typeWidth)
      assert(actual.getBytes.toSeq === data(i).toSeq, s"mismatch at index $i")
    }
  }

  test("batch readBinary into WritableColumnVector - width 4") {
    val typeWidth = 4
    val data = Array.fill(200)(Array.fill(typeWidth)(random.nextInt(256).toByte))
    val page = writeFLBA(data, typeWidth)
    val reader = new VectorizedByteStreamSplitValuesReader(typeWidth)
    reader.initFromPage(data.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    val cv = new OnHeapColumnVector(data.length, BinaryType)
    try {
      reader.readBinary(data.length, cv, 0)
      for (i <- data.indices) {
        assert(cv.getBinary(i).toSeq === data(i).toSeq, s"mismatch at index $i")
      }
    } finally {
      cv.close()
    }
  }

  test("batch readBinary into WritableColumnVector - odd width 7") {
    val typeWidth = 7
    val data = Array.fill(120)(Array.fill(typeWidth)(random.nextInt(256).toByte))
    val page = writeFLBA(data, typeWidth)
    val reader = new VectorizedByteStreamSplitValuesReader(typeWidth)
    reader.initFromPage(data.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    val cv = new OnHeapColumnVector(data.length, BinaryType)
    try {
      reader.readBinary(data.length, cv, 0)
      for (i <- data.indices) {
        assert(cv.getBinary(i).toSeq === data(i).toSeq, s"mismatch at index $i")
      }
    } finally {
      cv.close()
    }
  }

  test("batch readBinary with skip interleaving - width 12") {
    val typeWidth = 12
    val data = Array.fill(100)(Array.fill(typeWidth)(random.nextInt(256).toByte))
    val page = writeFLBA(data, typeWidth)
    val reader = new VectorizedByteStreamSplitValuesReader(typeWidth)
    reader.initFromPage(data.length, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    val cv = new OnHeapColumnVector(data.length, BinaryType)
    try {
      // read first 20 in batch
      reader.readBinary(20, cv, 0)
      for (i <- 0 until 20) {
        assert(cv.getBinary(i).toSeq === data(i).toSeq, s"mismatch at index $i")
      }
      // skip 30
      reader.skipFixedLenByteArray(30, typeWidth)
      // read next 50 in batch
      reader.readBinary(50, cv, 20)
      for (i <- 0 until 50) {
        assert(cv.getBinary(20 + i).toSeq === data(50 + i).toSeq, s"mismatch at index ${50 + i}")
      }
    } finally {
      cv.close()
    }
  }

  test("single value FLBA - width 1") {
    readAndVerifyFLBA(
      Array(Array(0x42.toByte)), typeWidth = 1)
  }
}

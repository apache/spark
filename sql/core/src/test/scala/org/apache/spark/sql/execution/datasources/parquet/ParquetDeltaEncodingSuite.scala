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

import java.io.IOException
import java.nio.ByteBuffer
import java.util.Random

import org.apache.parquet.bytes.{ByteBufferInputStream, DirectByteBufferAllocator}
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.delta.{DeltaBinaryPackingValuesWriterForInteger, DeltaBinaryPackingValuesWriterForLong}
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, IntegralType, LongType}

/**
 * Read tests for vectorized Delta binary packed reader.
 * Translated from
 *  org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForIntegerTest
 *  org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLongTest
 */
abstract class ParquetDeltaEncodingSuite[T] extends ParquetCompatibilityTest
  with SharedSparkSession {
  protected var blockSize = 128
  protected var miniBlockNum = 4
  protected var reader: VectorizedDeltaBinaryPackedReader = _
  protected var writableColumnVector: WritableColumnVector = _
  protected var writer: ValuesWriter = _
  protected var random: Random = _

  protected def getSparkSqlType: IntegralType

  protected def writeData(data: Array[T]): Unit

  protected def writeData(data: Array[T], length: Int): Unit

  protected def initValuesWriter (): Unit

  protected def allocDataArray(size: Int): Array[T]

  protected def getNextRandom: T

  protected def getTypeMinValue: T

  protected def getTypeMaxValue: T

  protected def readData(total: Int, columnVector : WritableColumnVector, rowId: Int): Unit

  protected def skip(total: Int): Unit

  protected def readDataFromVector(columnVector : WritableColumnVector, rowId: Int): T

  protected def estimatedSize(length: Int) : Double

  protected def setValue(arr: Array[T], index: Int, value: Int): Unit
  protected def compareValues(expected: Int, actual: T) : Boolean


  protected override def beforeEach(): Unit = {
    random = new Random(0)
    initValuesWriter()
    super.beforeAll()
  }

  test("read when data is aligned with block") {
    val data = allocDataArray(5 * blockSize)
    for (i <- 0 until blockSize * 5) {
      data(i) = getNextRandom
    }
    shouldWriteAndRead(data)
  }

  test("read when block is not fully written") {
    val data = allocDataArray(blockSize - 3)
    for (i <- data.indices) {
      data(i) = getNextRandom
    }
    shouldWriteAndRead(data)
  }

  test("read when mini block is not fully written") {
    val miniBlockSize = blockSize / miniBlockNum
    val data = allocDataArray(miniBlockSize - 3)
    for (i <- data.indices) {
      data(i) = getNextRandom
    }
    shouldWriteAndRead(data)
  }

  test("read with negative deltas") {
    val data = allocDataArray(blockSize)
    for (i <- data.indices) {
      setValue(data, i, 10 - (i * 32 - random.nextInt(6)))
    }
    shouldWriteAndRead(data)
  }

  test("read when deltas are same") {
    val data = allocDataArray(2 * blockSize)
    for (i <- 0 until blockSize) {
      setValue(data, i, i * 32)
    }
    for (i <- blockSize until 2 * blockSize) {
      setValue(data, i, 0)
    }
    shouldWriteAndRead(data)
  }

  test("read when values are same") {
    val data = allocDataArray(2 * blockSize)
    for (i <- 0 until blockSize) {
      setValue(data, i, 3)
    }
    for (i <- blockSize until 2 * blockSize) {
      setValue(data, i, 0)
    }
    shouldWriteAndRead(data)
  }

  test("read when delta is 0 for each block") {
    val data = allocDataArray(5 * blockSize + 1)
    for (i <- data.indices) {
      setValue(data, i, (i - 1) / blockSize)
    }
    shouldWriteAndRead(data)
  }

  test("read when data is not aligned with block") {
    val data = allocDataArray(5 * blockSize + 3)
    for (i <- data.indices) {
      setValue(data, i, random.nextInt(20) - 10)
    }
    shouldWriteAndRead(data)
  }

  test("read max min value") {
    val data = allocDataArray(10)
    for (i <- data.indices) {
      if (i % 2 == 0) data(i) = getTypeMinValue
      else data(i) = getTypeMaxValue
    }
    shouldWriteAndRead(data)
  }

  test("throw exception when read more than written") {
    val data = allocDataArray(5 * blockSize + 1)
    for (i <- data.indices) {
      setValue(data, i, i * 32)
    }
    shouldWriteAndRead(data)
    try readData(1, writableColumnVector, data.length)
    catch {
      case e: ParquetDecodingException =>
        // No more values to read. Total values read:  641, total count: 641, trying to read 1 more.
        assert(e.getMessage.startsWith("No more values to read."))
    }
  }

  test("skip()") {
    val data = allocDataArray(5 * blockSize + 1)
    for (i <- data.indices) {
      setValue(data, i, i * 32)
    }
    writeData(data)
    reader = new VectorizedDeltaBinaryPackedReader
    reader.initFromPage(100, writer.getBytes.toInputStream)
    writableColumnVector = new OnHeapColumnVector(data.length, getSparkSqlType)
    for (i <- data.indices) {
      if (i % 3 == 0) {
        skip(1)
      } else {
        readData(1, writableColumnVector, i)
        assert(compareValues(i * 32, readDataFromVector(writableColumnVector, i)))
      }
    }
  }

  test("SkipN()") {
    val data = allocDataArray(5 * blockSize + 1)
    for (i <- data.indices) {
      setValue(data, i, i * 32)
    }
    writeData(data)
    reader = new VectorizedDeltaBinaryPackedReader
    reader.initFromPage(100, writer.getBytes.toInputStream)
    writableColumnVector = new OnHeapColumnVector(data.length, getSparkSqlType)
    var skipCount = 0
    var i = 0
    while (i < data.length) {
      skipCount = (data.length - i) / 2
      readData(1, writableColumnVector, i)
      assert(compareValues(i * 32, readDataFromVector(writableColumnVector, i)))
      skip(skipCount)

      i += skipCount + 1
    }
  }

  test("random data test") {
    val maxSize = 1000
    val data = allocDataArray(maxSize)
    for (round <- 0 until 100000) {
      val size = random.nextInt(maxSize)
      for (i <- 0 until size) {
        data(i) = getNextRandom
      }
      shouldReadAndWrite(data, size)
    }
  }

  @throws[IOException]
  private def shouldWriteAndRead(data: Array[T]): Unit = {
    shouldReadAndWrite(data, data.length)
  }

  private def shouldReadAndWrite(data: Array[T], length: Int): Unit = {
    // SPARK-40052: Check that we can handle direct and non-direct byte buffers depending on the
    // implementation of ByteBufferInputStream.
    for (useDirect <- Seq(true, false)) {
      writeData(data, length)
      reader = new VectorizedDeltaBinaryPackedReader
      val page = writer.getBytes.toByteArray

      assert(estimatedSize(length) >= page.length)
      writableColumnVector = new OnHeapColumnVector(data.length, getSparkSqlType)

      val buf = if (useDirect) {
        ByteBuffer.allocateDirect(page.length)
      } else {
        ByteBuffer.allocate(page.length)
      }
      buf.put(page)
      buf.flip()

      reader.initFromPage(100, ByteBufferInputStream.wrap(buf))
      readData(length, writableColumnVector, 0)
      for (i <- 0 until length) {
        assert(data(i) == readDataFromVector(writableColumnVector, i))
      }

      writer.reset()
    }
  }
}

class ParquetDeltaEncodingInteger extends ParquetDeltaEncodingSuite[Int] {

  override protected def getSparkSqlType: IntegralType = IntegerType
  override protected def writeData(data: Array[Int]): Unit = writeData(data, data.length)

  override protected def writeData(data: Array[Int], length: Int): Unit = {
    for (i <- 0 until length) {
      writer.writeInteger(data(i))
    }
  }

  override protected def initValuesWriter (): Unit = {
    writer = new DeltaBinaryPackingValuesWriterForInteger(
      blockSize,
      miniBlockNum,
      100,
      200,
      new DirectByteBufferAllocator())
  }

  override protected def allocDataArray(size: Int): Array[Int] = new Array[Int](size)

  override protected def getNextRandom: Int = random.nextInt
  override protected def getTypeMinValue: Int = Int.MinValue
  override protected def getTypeMaxValue: Int = Int.MaxValue

  override protected def readData(total: Int, columnVector : WritableColumnVector, rowId: Int): Unit
  = reader.readIntegers(total, columnVector, rowId)

  override protected def skip(total: Int): Unit = reader.skipIntegers(total)

  override protected def readDataFromVector(columnVector: WritableColumnVector, rowId: Int): Int =
    columnVector.getInt(rowId)

  override protected def estimatedSize(length: Int) : Double = {
    val miniBlockSize = blockSize / miniBlockNum
    val miniBlockFlushed = Math.ceil((length.toDouble - 1) / miniBlockSize)
    val blockFlushed = Math.ceil((length.toDouble - 1) / blockSize)
    4 * 5 /* blockHeader */ +
      4 * miniBlockFlushed * miniBlockSize /* data(aligned to miniBlock) */ +
      blockFlushed * miniBlockNum /* bitWidth of mini blocks */ +
      (5.0 * blockFlushed) /* min delta for each block */
  }

  override protected def setValue(arr: Array[Int], index: Int, value: Int): Unit =
    arr(index) = value

  override protected def compareValues(expected: Int, actual: Int) : Boolean =
    expected == actual

}

class ParquetDeltaEncodingLong extends ParquetDeltaEncodingSuite[Long] {

  override protected def getSparkSqlType: IntegralType = LongType
  override protected def writeData(data: Array[Long]): Unit = writeData(data, data.length)

  override protected def writeData(data: Array[Long], length: Int): Unit = {
    for (i <- 0 until length) {
      writer.writeLong(data(i))
    }
  }

  override protected def initValuesWriter (): Unit = {
    writer = new DeltaBinaryPackingValuesWriterForLong(
      blockSize,
      miniBlockNum,
      100,
      200,
      new DirectByteBufferAllocator())
  }

  override protected def allocDataArray(size: Int): Array[Long] = new Array[Long](size)

  override protected def getNextRandom: Long = random.nextLong
  override protected def getTypeMinValue: Long = Long.MinValue
  override protected def getTypeMaxValue: Long = Long.MaxValue

  override protected def readData(total: Int, columnVector: WritableColumnVector, rowId: Int): Unit
    = reader.readLongs(total, columnVector, rowId)

  override protected def skip(total: Int): Unit = reader.skipLongs(total)

  override protected def readDataFromVector(columnVector: WritableColumnVector, rowId: Int): Long =
    columnVector.getLong(rowId)

  override protected def estimatedSize(length: Int) : Double = {
    val miniBlockSize = blockSize / miniBlockNum
    val miniBlockFlushed = Math.ceil((length.toDouble - 1) / miniBlockSize)
    val blockFlushed = Math.ceil((length.toDouble - 1) / blockSize)
    3 * 5 + 1 * 10 /* blockHeader + 3 * int + 1 * long */ +
      8 * miniBlockFlushed * miniBlockSize /* data(aligned to miniBlock) */ +
      blockFlushed * miniBlockNum /* bitWidth of mini blocks */ +
      (10.0 * blockFlushed) /* min delta for each block */
  }

  override protected def setValue(arr: Array[Long], index: Int, value: Int): Unit = {
    arr(index) = value.toLong
  }

  override protected def compareValues(expected: Int, actual: Long) : Boolean = {
    expected.toLong == actual
  }

}

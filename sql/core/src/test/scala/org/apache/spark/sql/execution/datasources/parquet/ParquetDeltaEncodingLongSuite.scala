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
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Read tests for vectorized Delta binary packed Long reader.
 * Translated from
 * org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForIntegerTest
 */
class ParquetDeltaEncodingLongSuite extends ParquetCompatibilityTest with SharedSparkSession {
  private var blockSize = 128
  private var miniBlockNum = 4
  private var reader: VectorizedDeltaBinaryPackedReader = _
  private var writableColumnVector: WritableColumnVector = _
  private var writer: ValuesWriter = _
  private var random: Random = _

  protected override def beforeEach(): Unit = {
    blockSize = 128
    miniBlockNum = 4
    writer = new DeltaBinaryPackingValuesWriterForLong(
      blockSize,
      miniBlockNum,
      100,
      200,
      new DirectByteBufferAllocator())
    random = new Random(0)
    super.beforeAll()
  }

  test("read long when data is aligned with block") {
    val data = new Array[Long](5 * blockSize)
    for (i <- 0 until blockSize * 5) {
      data(i) = random.nextLong
    }
    shouldWriteAndRead(data)
  }

  test("read long when block is not fully written") {
    val data = new Array[Long](blockSize - 3)
    for (i <- data.indices) {
      data(i) = random.nextLong
    }
    shouldWriteAndRead(data)
  }

  test("read long when mini block is not fully written") {
    val miniBlockSize = blockSize / miniBlockNum
    val data = new Array[Long](miniBlockSize - 3)
    for (i <- data.indices) {
      data(i) = random.nextLong
    }
    shouldWriteAndRead(data)
  }

  test("read long with negative deltas") {
    val data = new Array[Long](blockSize)
    for (i <- data.indices) {
      data(i) = 10 - (i * 32 - random.nextInt(6))
    }
    shouldWriteAndRead(data)
  }

  test("read long when deltas are same") {
    val data = new Array[Long](2 * blockSize)
    for (i <- 0 until blockSize) {
      data(i) = i * 32
    }
    shouldWriteAndRead(data)
  }

  test("read long when values are same") {
    val data = new Array[Long](2 * blockSize)
    for (i <- 0 until blockSize) {
      data(i) = 3
    }
    shouldWriteAndRead(data)
  }

  test("read long when delta is 0 for each block") {
    val data = new Array[Long](5 * blockSize + 1)
    for (i <- data.indices) {
      data(i) = (i - 1) / blockSize
    }
    shouldWriteAndRead(data)
  }

  test("read long when data is not aligned with block") {
    val data = new Array[Long](5 * blockSize + 3)
    for (i <- data.indices) {
      data(i) = random.nextInt(20) - 10
    }
    shouldWriteAndRead(data)
  }

  test("read long max min value") {
    val data = new Array[Long](10)
    for (i <- data.indices) {
      if (i % 2 == 0) data(i) = Long.MinValue
      else data(i) = Long.MaxValue
    }
    shouldWriteAndRead(data)
  }

  test("read long throw exception when read more than written") {
    val data = new Array[Long](5 * blockSize + 1)
    for (i <- data.indices) {
      data(i) = i * 32
    }
    shouldWriteAndRead(data)
    try reader.readLongs(1, writableColumnVector, data.length)
    catch {
      case e: ParquetDecodingException =>
        assert("no more values to read, total value count is " + data.length == e.getMessage)
    }
  }

  test("long skip()") {
    val data = new Array[Long](5 * blockSize + 1)
    for (i <- data.indices) {
      data(i) = i * 32
    }
    writeData(data)
    reader = new VectorizedDeltaBinaryPackedReader
    reader.initFromPage(100, writer.getBytes.toInputStream)
    writableColumnVector = new OnHeapColumnVector(data.length, LongType)
    for (i <- data.indices) {
      if (i % 3 == 0) {
        reader.skipLongs(1)
      } else {
        reader.readLongs(1, writableColumnVector, i)
        assert(i * 32 == writableColumnVector.getLong(i))
      }
    }
  }

  test("long SkipN()") {
    val data = new Array[Long](5 * blockSize + 1)
    for (i <- data.indices) {
      data(i) = i * 32
    }
    writeData(data)
    reader = new VectorizedDeltaBinaryPackedReader
    reader.initFromPage(100, writer.getBytes.toInputStream)
    writableColumnVector = new OnHeapColumnVector(data.length, LongType)
    var skipCount = 0
    var i = 0
    while (i < data.length) {
      skipCount = (data.length - i) / 2
      reader.readLongs(1, writableColumnVector, i)
      assert(i * 32 == writableColumnVector.getLong(i))
      reader.skipLongs(skipCount)

      i += skipCount + 1
    }
  }

  test("long randomDataTest") {
    val maxSize = 1000
    val data = new Array[Long](maxSize)
    for (round <- 0 until 100000) {
      val size = random.nextInt(maxSize)
      for (i <- 0 until size) {
        data(i) = random.nextLong
      }
      shouldReadAndWrite(data, size)
      writer.reset()
    }
  }

  @throws[IOException]
  private def shouldWriteAndRead(data: Array[Long]): Unit = {
    shouldReadAndWrite(data, data.length)
  }

  private def shouldReadAndWrite(data: Array[Long], length: Int): Unit = {
    writeData(data, length)
    reader = new VectorizedDeltaBinaryPackedReader
    val page = writer.getBytes.toByteArray
    val miniBlockSize = blockSize / miniBlockNum
    val miniBlockFlushed = Math.ceil((length.toDouble - 1) / miniBlockSize)
    val blockFlushed = Math.ceil((length.toDouble - 1) / blockSize)
    val estimatedSize = 3 * 5 + 1 * 10 /* blockHeader + 3 * int + 1 * long */ +
      8 * miniBlockFlushed * miniBlockSize /* data(aligned to miniBlock) */ +
      blockFlushed * miniBlockNum /* bitWidth of mini blocks */ +
      (10.0 * blockFlushed) /* min delta for each block */
    assert(estimatedSize >= page.length)
    writableColumnVector = new OnHeapColumnVector(data.length, LongType)
    reader.initFromPage(100, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
    reader.readLongs(length, writableColumnVector, 0)
    for (i <- 0 until length) {
      assert(data(i) == writableColumnVector.getLong(i))
    }
  }

  private def writeData(data: Array[Long]): Unit = {
    writeData(data, data.length)
  }

  private def writeData(data: Array[Long], length: Int): Unit = {
    for (i <- 0 until length) {
      writer.writeLong(data(i))
    }
  }

}

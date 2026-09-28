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
import java.nio.charset.StandardCharsets

import org.apache.parquet.bytes.{ByteBufferInputStream, BytesInput, DirectByteBufferAllocator}
import org.apache.parquet.column.values.Utils
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Read tests for vectorized Delta byte array  reader.
 * Translated from * org.apache.parquet.column.values.delta.TestDeltaByteArray
 */
class ParquetDeltaByteArrayEncodingSuite extends ParquetCompatibilityTest with SharedSparkSession {
  val values: Array[String] = Array("parquet-mr", "parquet", "parquet-format");
  val randvalues: Array[String] = Utils.getRandomStringSamples(10000, 32)

  var writer: DeltaByteArrayWriter = _
  var reader: VectorizedDeltaByteArrayReader = _
  private var writableColumnVector: WritableColumnVector = _

  protected override def beforeEach(): Unit = {
    writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator)
    reader = new VectorizedDeltaByteArrayReader()
    super.beforeAll()
  }

  test("test Serialization") {
    assertReadWrite(writer, reader, values)
  }

  test("random strings") {
    assertReadWrite(writer, reader, randvalues)
  }

  test("random strings with skip") {
    assertReadWriteWithSkip(writer, reader, randvalues)
  }

  test("random strings with skipN") {
    assertReadWriteWithSkipN(writer, reader, randvalues)
  }

  test("prefix length larger than the previous value is rejected") {
    // Craft a page by hand: a benign 2-byte first value, then a value whose prefix length
    // claims 65536 bytes of a 2-byte previous value.
    val is = craftPage(prefixLengths = Array(0, 65536), suffixes = Array("ab", ""))
    reader.initFromPage(2, is)
    writableColumnVector = new OnHeapColumnVector(2, StringType)
    val e = intercept[ParquetDecodingException] {
      reader.readBinary(2, writableColumnVector, 0)
    }
    assert(e.getMessage.contains("prefix length 65536"))
  }

  test("negative prefix length is rejected") {
    val is = craftPage(prefixLengths = Array(0, -1), suffixes = Array("ab", "cd"))
    reader.initFromPage(2, is)
    writableColumnVector = new OnHeapColumnVector(2, StringType)
    val e = intercept[ParquetDecodingException] {
      reader.readBinary(2, writableColumnVector, 0)
    }
    assert(e.getMessage.contains("negative prefix length"))
  }

  test("prefix length larger than the previous value is rejected when skipping") {
    val is = craftPage(prefixLengths = Array(0, 65536), suffixes = Array("ab", ""))
    reader.initFromPage(2, is)
    val e = intercept[ParquetDecodingException] {
      reader.skipBinary(2)
    }
    assert(e.getMessage.contains("prefix length 65536"))
  }

  test("prefix length one byte past the previous value is rejected") {
    // With on-heap vectors previous.array() is the whole backing buffer, so without the
    // validation this off-by-one prefix silently copied a stale byte instead of failing.
    val is = craftPage(prefixLengths = Array(0, 3), suffixes = Array("ab", ""))
    reader.initFromPage(2, is)
    writableColumnVector = new OnHeapColumnVector(2, StringType)
    val e = intercept[ParquetDecodingException] {
      reader.readBinary(2, writableColumnVector, 0)
    }
    assert(e.getMessage.contains("prefix length 3"))
  }

  test("more prefix lengths than values in the page are rejected") {
    val is = craftPage(prefixLengths = Array(0, 0, 0), suffixes = Array("a", "b", "c"))
    val e = intercept[ParquetDecodingException] {
      reader.initFromPage(2, is)
    }
    assert(e.getMessage.contains("3 prefix lengths in a page of 2 values"))
  }

  test("prefix length and suffix counts must match") {
    // Without the check, the second row silently read a zero prefix length.
    val e1 = intercept[ParquetDecodingException] {
      reader.initFromPage(2, craftPage(prefixLengths = Array(0), suffixes = Array("ab", "cd")))
    }
    assert(e1.getMessage.contains("1 prefix lengths but 2 suffixes"))

    reader = new VectorizedDeltaByteArrayReader()
    val e2 = intercept[ParquetDecodingException] {
      reader.initFromPage(2, craftPage(prefixLengths = Array(0, 0), suffixes = Array("ab")))
    }
    assert(e2.getMessage.contains("2 prefix lengths but 1 suffixes"))
  }

  test("rows without a decoded value report the row count, not the prefix length") {
    // Both DELTA_BINARY_PACKED headers claim 0 values, but the prefix length header's first value
    // is 5. Decoding 0 prefix lengths still writes that first value into the first slot, so
    // reading a row before checking that it was decoded failed with a misleading
    // "prefix length 5 is larger than the previous value's length 0".
    def header(firstValueZigZag: Int): Array[Byte] =
      // block size 128, 4 mini blocks, 0 values, first value
      Array(0x80, 0x01, 0x04, 0x00, firstValueZigZag).map(_.toByte)
    val bytes = header(firstValueZigZag = 10) ++ header(firstValueZigZag = 0)
    val expected = "reading 1 values from row 0, but only 0 value lengths were decoded"

    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    val e1 = intercept[ParquetDecodingException] {
      reader.readBinary(1, new OnHeapColumnVector(1, StringType), 0)
    }
    assert(e1.getMessage.contains(expected))

    reader = new VectorizedDeltaByteArrayReader()
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    val e2 = intercept[ParquetDecodingException] {
      reader.skipBinary(1)
    }
    assert(e2.getMessage.contains(expected))
  }

  test("prefix length equal to the previous value's length is accepted") {
    val is = craftPage(prefixLengths = Array(0, 2), suffixes = Array("ab", ""))
    reader.initFromPage(2, is)
    writableColumnVector = new OnHeapColumnVector(2, StringType)
    reader.readBinary(2, writableColumnVector, 0)
    assert(writableColumnVector.getBinary(0) sameElements "ab".getBytes)
    assert(writableColumnVector.getBinary(1) sameElements "ab".getBytes)
  }

  test("test lengths") {
    var reader = new VectorizedDeltaBinaryPackedReader
    Utils.writeData(writer, values)
    val data = writer.getBytes.toInputStream
    val length = values.length
    writableColumnVector = new OnHeapColumnVector(length, IntegerType)
    reader.initFromPage(length, data)
    reader.readIntegers(length, writableColumnVector, 0)
    // test prefix lengths
    assert(0 == writableColumnVector.getInt(0))
    assert(7 == writableColumnVector.getInt(1))
    assert(7 == writableColumnVector.getInt(2))

    reader = new VectorizedDeltaBinaryPackedReader
    writableColumnVector = new OnHeapColumnVector(length, IntegerType)
    reader.initFromPage(length, data)
    reader.readIntegers(length, writableColumnVector, 0)
    // test suffix lengths
    assert(10 == writableColumnVector.getInt(0))
    assert(0 == writableColumnVector.getInt(1))
    assert(7 == writableColumnVector.getInt(2))
  }

  /** Builds a raw DELTA_BYTE_ARRAY page from explicit prefix lengths and suffixes. */
  private def craftPage(
      prefixLengths: Array[Int],
      suffixes: Array[String]): ByteBufferInputStream = {
    craftPage(prefixLengths, suffixes.map(_.getBytes(StandardCharsets.UTF_8)))
  }

  private def craftPage(
      prefixLengths: Array[Int],
      suffixes: Array[Array[Byte]]): ByteBufferInputStream = {
    val allocator = new DirectByteBufferAllocator
    val prefixWriter =
      new DeltaBinaryPackingValuesWriterForInteger(128, 4, 64 * 1024, 64 * 1024, allocator)
    val suffixWriter = new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024, allocator)
    prefixLengths.foreach(prefixWriter.writeInteger)
    suffixes.foreach(s => suffixWriter.writeBytes(Binary.fromConstantByteArray(s)))
    BytesInput.concat(prefixWriter.getBytes, suffixWriter.getBytes).toInputStream
  }

  private def assertReadWrite(
      writer: DeltaByteArrayWriter,
      reader: VectorizedDeltaByteArrayReader,
      vals: Array[String]): Unit = {
    Utils.writeData(writer, vals)
    val length = vals.length
    val is = writer.getBytes.toInputStream

    writableColumnVector = new OnHeapColumnVector(length, StringType)

    reader.initFromPage(length, is)
    reader.readBinary(length, writableColumnVector, 0)

    for (i <- 0 until length) {
      assert(vals(i).getBytes() sameElements writableColumnVector.getBinary(i))
    }
  }

  private def assertReadWriteWithSkip(
      writer: DeltaByteArrayWriter,
      reader: VectorizedDeltaByteArrayReader,
      vals: Array[String]): Unit = {
    Utils.writeData(writer, vals)
    val length = vals.length
    val is = writer.getBytes.toInputStream
    writableColumnVector = new OnHeapColumnVector(length, StringType)
    reader.initFromPage(length, is)
    var i = 0
    while ( {
      i < vals.length
    }) {
      reader.readBinary(1, writableColumnVector, i)
      assert(vals(i).getBytes() sameElements writableColumnVector.getBinary(i))
      reader.skipBinary(1)
      i += 2
    }
  }

  private def assertReadWriteWithSkipN(
      writer: DeltaByteArrayWriter,
      reader: VectorizedDeltaByteArrayReader,
      vals: Array[String]): Unit = {
    Utils.writeData(writer, vals)
    val length = vals.length
    val is = writer.getBytes.toInputStream
    writableColumnVector = new OnHeapColumnVector(length, StringType)
    reader.initFromPage(length, is)
    var skipCount = 0
    var i = 0
    while ( {
      i < vals.length
    }) {
      skipCount = (vals.length - i) / 2
      reader.readBinary(1, writableColumnVector, i)
      assert(vals(i).getBytes() sameElements writableColumnVector.getBinary(i))
      reader.skipBinary(skipCount)
      i += skipCount + 1
    }
  }
}

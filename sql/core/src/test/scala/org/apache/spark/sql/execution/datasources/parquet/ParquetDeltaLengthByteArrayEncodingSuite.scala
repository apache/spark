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

import java.nio.charset.StandardCharsets
import java.util.Random

import org.apache.commons.lang3.RandomStringUtils
import org.apache.parquet.bytes.{ByteBufferInputStream, BytesInput, DirectByteBufferAllocator}
import org.apache.parquet.column.values.Utils
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Read tests for vectorized Delta length byte array  reader.
 * Translated from
 * org.apache.parquet.column.values.delta.TestDeltaLengthByteArray
 */
class ParquetDeltaLengthByteArrayEncodingSuite
    extends ParquetCompatibilityTest
    with SharedSparkSession {
  val values: Array[String] = Array("parquet", "hadoop", "mapreduce")
  var writer: DeltaLengthByteArrayValuesWriter = _
  var reader: VectorizedDeltaLengthByteArrayReader = _
  private var writableColumnVector: WritableColumnVector = _

  protected override def beforeEach(): Unit = {
    writer =
      new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator)
    reader = new VectorizedDeltaLengthByteArrayReader()
    super.beforeAll()
  }

  test("test serialization") {
    writeData(writer, values)
    readAndValidate(reader, writer.getBytes.toInputStream, values.length, values)
  }

  test("random strings") {
    val values = Utils.getRandomStringSamples(1000, 32)
    writeData(writer, values)
    readAndValidate(reader, writer.getBytes.toInputStream, values.length, values)
  }

  test("random strings with empty strings") {
    val values = getRandomStringSamplesWithEmptyStrings(1000, 32)
    writeData(writer, values)
    readAndValidate(reader, writer.getBytes.toInputStream, values.length, values)
  }

  test("skip with random strings") {
    val values = Utils.getRandomStringSamples(1000, 32)
    writeData(writer, values)
    reader.initFromPage(values.length, writer.getBytes.toInputStream)
    writableColumnVector = new OnHeapColumnVector(values.length, StringType)
    var i = 0
    while (i < values.length) {
      reader.readBinary(1, writableColumnVector, i)
      assert(values(i).getBytes() sameElements writableColumnVector.getBinary(i))
      reader.skipBinary(1)
      i += 2
    }
    reader = new VectorizedDeltaLengthByteArrayReader()
    reader.initFromPage(values.length, writer.getBytes.toInputStream)
    writableColumnVector = new OnHeapColumnVector(values.length, StringType)
    var skipCount = 0
    i = 0
    while (i < values.length) {
      skipCount = (values.length - i) / 2
      reader.readBinary(1, writableColumnVector, i)
      assert(values(i).getBytes() sameElements writableColumnVector.getBinary(i))
      reader.skipBinary(skipCount)
      i += skipCount + 1
    }
  }

  // Read the lengths from the beginning of the buffer and compare with the lengths of the values
  test("test lengths") {
    val reader = new VectorizedDeltaBinaryPackedReader
    writeData(writer, values)
    val length = values.length
    writableColumnVector = new OnHeapColumnVector(length, IntegerType)
    reader.initFromPage(length, writer.getBytes.toInputStream)
    reader.readIntegers(length, writableColumnVector, 0)
    for (i <- 0 until length) {
      assert(values(i).length == writableColumnVector.getInt(i))
    }
  }

  testWithStreams("value lengths larger than the rest of the page are rejected") { craftPage =>
    // The second value claims more bytes than are left in the page. Int.MaxValue checks that the
    // lengths are rejected before anything is allocated for them.
    Seq(100, Int.MaxValue).foreach { len =>
      val e = intercept[ParquetDecodingException] {
        new VectorizedDeltaLengthByteArrayReader().initFromPage(2, craftPage(Array(2, len), "abcd"))
      }
      assert(e.getMessage.contains(
        s"value lengths add up to ${2L + len} bytes, but only 4 are left"))
    }
  }

  testWithStreams("negative value length is rejected") { craftPage =>
    val e = intercept[ParquetDecodingException] {
      reader.initFromPage(3, craftPage(Array(2, -1, 3), "abcd"))
    }
    assert(e.getMessage.contains("negative value length: -1"))
  }

  testWithStreams("more value lengths than values in the page are rejected") { craftPage =>
    val e = intercept[ParquetDecodingException] {
      reader.initFromPage(2, craftPage(Array(1, 1, 1), "abc"))
    }
    assert(e.getMessage.contains("3 value lengths in a page of 2 values"))
  }

  testWithStreams("rows without a decoded value length are rejected") { craftPage =>
    // The page has 3 values but only 2 value lengths, so the third row has no length.
    def newReader(): VectorizedDeltaLengthByteArrayReader = {
      val r = new VectorizedDeltaLengthByteArrayReader()
      r.initFromPage(3, craftPage(Array(2, 2), "abcd"))
      r
    }
    val expected = "reading 3 values from row 0, but only 2 value lengths were decoded"
    val e1 = intercept[ParquetDecodingException] {
      newReader().readBinary(3, new OnHeapColumnVector(3, StringType), 0)
    }
    assert(e1.getMessage.contains(expected))
    val e2 = intercept[ParquetDecodingException] {
      newReader().skipBinary(3)
    }
    assert(e2.getMessage.contains(expected))

    // getBytes is the path used by the DELTA_BYTE_ARRAY reader to read suffixes.
    val r = newReader()
    r.getBytes(0)
    r.getBytes(1)
    val e3 = intercept[ParquetDecodingException] {
      r.getBytes(2)
    }
    assert(e3.getMessage.contains(
      "reading 1 values from row 2, but only 2 value lengths were decoded"))
  }

  /**
   * Runs `testFun` with a function that builds a raw DELTA_LENGTH_BYTE_ARRAY page from explicit
   * value lengths and value bytes, once held in a single buffer and once split across multiple
   * buffers, since `SingleBufferInputStream` and `MultiBufferInputStream` handle short and
   * negative lengths differently.
   */
  private def testWithStreams(testName: String)(
      testFun: ((Array[Int], String) => ByteBufferInputStream) => Unit): Unit = {
    Seq("single buffer" -> false, "multiple buffers" -> true).foreach { case (name, split) =>
      test(s"$testName ($name)") {
        testFun { (lengths, data) =>
          val lengthWriter = new DeltaBinaryPackingValuesWriterForInteger(
            128, 4, 64 * 1024, 64 * 1024, new DirectByteBufferAllocator)
          lengths.foreach(lengthWriter.writeInteger)
          val bytes = BytesInput.concat(
            lengthWriter.getBytes,
            BytesInput.from(data.getBytes(StandardCharsets.UTF_8))).toByteArray
          VectorizedPlainValuesReaderSuite.toStream(bytes, split)
        }
      }
    }
  }

  private def writeData(writer: DeltaLengthByteArrayValuesWriter, values: Array[String]): Unit = {
    for (i <- values.indices) {
      writer.writeBytes(Binary.fromString(values(i)))
    }
  }

  private def readAndValidate(
      reader: VectorizedDeltaLengthByteArrayReader,
      is: ByteBufferInputStream,
      length: Int,
      expectedValues: Array[String]): Unit = {

    writableColumnVector = new OnHeapColumnVector(length, StringType)

    reader.initFromPage(length, is)
    reader.readBinary(length, writableColumnVector, 0)

    for (i <- 0 until length) {
      assert(expectedValues(i).getBytes() sameElements writableColumnVector.getBinary(i))
    }
  }

  def getRandomStringSamplesWithEmptyStrings(numSamples: Int, maxLength: Int): Array[String] = {
    val randomLen = new Random
    val randomEmpty = new Random
    val samples: Array[String] = new Array[String](numSamples)
    for (i <- 0 until numSamples) {
      var maxLen: Int = randomLen.nextInt(maxLength)
      if(randomEmpty.nextInt() % 11 != 0) {
        maxLen = 0;
      }
      samples(i) = RandomStringUtils.randomAlphanumeric(0, maxLen)
    }
    samples
  }
}

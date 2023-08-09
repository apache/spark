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

import java.util.Random

import org.apache.commons.lang3.RandomStringUtils
import org.apache.parquet.bytes.{ByteBufferInputStream, DirectByteBufferAllocator}
import org.apache.parquet.column.values.Utils
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
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

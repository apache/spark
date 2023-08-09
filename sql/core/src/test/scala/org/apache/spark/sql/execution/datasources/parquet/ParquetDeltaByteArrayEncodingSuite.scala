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

import org.apache.parquet.bytes.DirectByteBufferAllocator
import org.apache.parquet.column.values.Utils
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter

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

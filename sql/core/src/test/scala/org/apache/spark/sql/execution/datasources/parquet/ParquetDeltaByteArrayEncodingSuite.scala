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

import org.apache.parquet.bytes.{ByteBufferInputStream, BytesInput, DirectByteBufferAllocator}
import org.apache.parquet.column.values.Utils
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary

import org.apache.spark.sql.catalyst.util.STUtils
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType, IntegerType, StringType}

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

  test("readBinary rejects a negative decoded length (prefix and suffix)") {
    // A DELTA_BYTE_ARRAY page is concat(prefixLengthHeader, suffixSection), where the suffix
    // section is itself a DELTA_LENGTH_BYTE_ARRAY page concat(suffixLengthHeader, suffixData).
    // Spark's writer never emits a negative length, but a corrupt/third-party file can carry one
    // in either the prefix or the suffix lengths. Both must be rejected: the suffix through
    // suffixReader.getBytes, the prefix through the reader's own checkLength guard.
    val alloc = new DirectByteBufferAllocator()
    def deltaBinaryPacked(values: Int*): BytesInput = {
      val w = new DeltaBinaryPackingValuesWriterForInteger(128, 4, 100, 200, alloc)
      values.foreach(w.writeInteger)
      w.getBytes
    }
    def deltaLengthByteArray(values: String*): BytesInput = {
      val w = new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024, alloc)
      values.foreach(s => w.writeBytes(Binary.fromString(s)))
      w.getBytes
    }
    def firstRowThenNegative(page: Array[Byte]): Unit = {
      reader = new VectorizedDeltaByteArrayReader()
      reader.initFromPage(2, ByteBufferInputStream.wrap(ByteBuffer.wrap(page)))
      val vector = new OnHeapColumnVector(2, StringType)
      reader.readBinary(1, vector, 0)
      assert("abc".getBytes() sameElements vector.getBinary(0))
      val error = intercept[ParquetDecodingException] {
        reader.readBinary(1, vector, 1)
      }
      assert(error.getMessage.contains("negative length"))
    }

    // Negative prefix length: value 0 has prefix 0 (valid), value 1 has prefix -6.
    firstRowThenNegative(
      BytesInput.concat(deltaBinaryPacked(0, -6), deltaLengthByteArray("abc", "def")).toByteArray)

    // Negative suffix length: prefixes are 0, suffix lengths are [3, -6] over data "abc".
    firstRowThenNegative(
      BytesInput.concat(
        deltaBinaryPacked(0, 0),
        deltaBinaryPacked(3, -6),
        BytesInput.from("abc".getBytes())).toByteArray)
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

  testGeo("geo types single point") { geoType =>
    assertGeoReadWrite(writer, reader, Array(makePointWkb(1, 1)), geoType)
  }

  testGeo("geo types multiple identical points") { geoType =>
    assertGeoReadWrite(writer, reader,
      Array(makePointWkb(1, 1), makePointWkb(1, 1), makePointWkb(1, 1)), geoType)
  }

  testGeo("geo types polygons with shared prefix") { geoType =>
    // These polygons share a WKB prefix, exercising delta encoding.
    assertGeoReadWrite(writer, reader, Array(
      makePolygonWkb((3, 3), (4, 4), (5, 5.1), (3, 3)),
      makePolygonWkb((3, 3), (4, 4), (5, 5.2), (3, 3)),
      makePolygonWkb((3, 3), (4, 4), (5, 5.3), (3, 3))),
      geoType)
  }

  private def assertGeoReadWrite(
      writer: DeltaByteArrayWriter,
      reader: VectorizedDeltaByteArrayReader,
      wkbValues: Array[Array[Byte]],
      dataType: DataType): Unit = {

    val (isGeometry, srid) = dataType match {
      case geom: GeometryType => (true, geom.srid)
      case geog: GeographyType => (false, geog.srid)
    }

    val length = wkbValues.length

    writeBinaryData(writer, wkbValues)
    writableColumnVector = new OnHeapColumnVector(length, dataType)

    reader.initFromPage(length, writer.getBytes.toInputStream)
    if (isGeometry) {
      reader.readGeometry(length, writableColumnVector, 0)
    } else {
      reader.readGeography(length, writableColumnVector, 0)
    }

    for (i <- 0 until length) {
      val actualWkb = if (isGeometry) {
        val geom = writableColumnVector.getBinaryView(i)
        assert(srid === STUtils.stGeomSrid(geom))
        STUtils.stGeomAsBinary(geom)
      } else {
        val geog = writableColumnVector.getBinaryView(i)
        assert(srid === STUtils.stGeogSrid(geog))
        STUtils.stGeogAsBinary(geog)
      }
      assert(wkbValues(i) sameElements actualWkb)
    }
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

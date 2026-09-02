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

  // Values whose total length exceeds prevBuf's initial 64-byte capacity and that
  // share long (> 64 byte) prefixes. These exercise the prevBuf grow branch in
  // readBinary/skipBinary, where the already-decoded prefix must be preserved
  // across the reallocation (System.arraycopy(prevBuf, 0, newBuf, 0, prefixLength)).
  // An even count is required so assertReadWriteWithSkip (read even, skip odd)
  // does not run past the last value. The skipped (odd) values also trigger the
  // grow branch, and the following read validates that skipBinary kept prevBuf
  // intact across the reallocation.
  private val longPrefixBase = "a" * 70
  val longPrefixValues: Array[String] = Array(
    // len  70: read, grow, prefix 0
    longPrefixBase,
    // len 160: skip, grow, prefix 70
    longPrefixBase + "b" * 90,
    // len 165: read, shares 160 prefix
    longPrefixBase + "b" * 90 + "c" * 5,
    // len 265: skip, grow, prefix 165
    longPrefixBase + "b" * 90 + "c" * 5 + "d" * 100,
    // len 270: read, shares 265 prefix
    longPrefixBase + "b" * 90 + "c" * 5 + "d" * 100 + "e" * 5,
    // len 271: skip, shares 270 prefix
    longPrefixBase + "b" * 90 + "c" * 5 + "d" * 100 + "e" * 5 + "f")



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

  test("buffer grows preserving long shared prefixes") {
    assertReadWrite(writer, reader, longPrefixValues)
  }

  test("buffer grows preserving long shared prefixes with skip") {
    assertReadWriteWithSkip(writer, reader, longPrefixValues)
  }

  test("setPreviousReader recovers a long previous value across pages (PARQUET-246)") {
    // Reproduces PARQUET-246: the first value of a page is a delta from the
    // previous page's last value. The recovered value exceeds the 64-byte prevBuf
    // capacity, exercising the grow + deep-copy in setPreviousReader.
    val prefix = "a" * 100
    val firstPageVals = Array(prefix, prefix + "0")        // last value len 101
    val secondPageVals = Array(prefix + "1", prefix + "2") // first value deltas off prev page

    val firstWriter =
      new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator)
    Utils.writeData(firstWriter, firstPageVals)

    // Corrupt the second writer so its first value shares the first page's last
    // value prefix (simulating the DeltaByteArrayWriter.reset() bug).
    val secondWriter =
      new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator)
    corruptWriter(secondWriter, firstPageVals.last)
    Utils.writeData(secondWriter, secondPageVals)

    val firstReader = new VectorizedDeltaByteArrayReader()
    var vec: WritableColumnVector = new OnHeapColumnVector(firstPageVals.length, StringType)
    firstReader.initFromPage(firstPageVals.length, firstWriter.getBytes.toInputStream)
    firstReader.readBinary(firstPageVals.length, vec, 0)
    for (i <- firstPageVals.indices) {
      assert(firstPageVals(i).getBytes() sameElements vec.getBinary(i))
    }

    val secondReader = new VectorizedDeltaByteArrayReader()
    vec = new OnHeapColumnVector(secondPageVals.length, StringType)
    secondReader.initFromPage(secondPageVals.length, secondWriter.getBytes.toInputStream)
    secondReader.setPreviousReader(firstReader)
    secondReader.readBinary(secondPageVals.length, vec, 0)
    for (i <- secondPageVals.indices) {
      assert(secondPageVals(i).getBytes() sameElements vec.getBinary(i))
    }
  }

  private def corruptWriter(writer: DeltaByteArrayWriter, data: String): Unit = {
    corruptWriter(writer, Binary.fromString(data).getBytesUnsafe())
  }

  private def corruptWriter(writer: DeltaByteArrayWriter, data: Array[Byte]): Unit = {
    val previous = writer.getClass.getDeclaredField("previous")
    previous.setAccessible(true)
    previous.set(writer, data)
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

  testGeo("geo interleaves skipBinary with readGeoData (null/skipped rows)") { geoType =>
    // A geometry column with null or skipped rows alternates skipBinary (used for the
    // skipped rows) with readGeometry/readGeography on the same reader. Both paths now
    // share the reusable prevBuf, so the shared prefix carried across a skipped value must
    // still be honored by the following read. The values also exceed prevBuf's initial
    // 64-byte capacity, exercising the grow-and-preserve branch under interleaving.
    assertGeoReadWriteWithSkip(writer, reader, sharedPrefixPolygons(6), geoType)
  }

  testGeo("geo setPreviousReader recovers a long value across pages (PARQUET-246)") { geoType =>
    // PARQUET-246 on the geo path: the first value of the second page is a delta from the
    // previous page's last value. The recovered value exceeds the 64-byte prevBuf capacity,
    // exercising the grow + deep-copy in setPreviousReader before readGeoData reuses it.
    val (isGeometry, srid) = geoType match {
      case geom: GeometryType => (true, geom.srid)
      case geog: GeographyType => (false, geog.srid)
    }
    val firstPageVals = sharedPrefixPolygons(2)
    val secondPageVals = sharedPrefixPolygons(2, base = 100)

    val firstWriter =
      new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator)
    writeBinaryData(firstWriter, firstPageVals)

    // Corrupt the second writer so its first value deltas off the first page's last value
    // (simulating the DeltaByteArrayWriter.reset() bug).
    val secondWriter =
      new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator)
    corruptWriter(secondWriter, firstPageVals.last)
    writeBinaryData(secondWriter, secondPageVals)

    val firstReader = new VectorizedDeltaByteArrayReader()
    var vec: WritableColumnVector = new OnHeapColumnVector(firstPageVals.length, geoType)
    firstReader.initFromPage(firstPageVals.length, firstWriter.getBytes.toInputStream)
    readGeoInto(firstReader, firstPageVals.length, vec, isGeometry)
    for (i <- firstPageVals.indices) {
      assert(firstPageVals(i) sameElements extractGeoWkb(vec, i, isGeometry, srid))
    }

    val secondReader = new VectorizedDeltaByteArrayReader()
    vec = new OnHeapColumnVector(secondPageVals.length, geoType)
    secondReader.initFromPage(secondPageVals.length, secondWriter.getBytes.toInputStream)
    secondReader.setPreviousReader(firstReader)
    readGeoInto(secondReader, secondPageVals.length, vec, isGeometry)
    for (i <- secondPageVals.indices) {
      assert(secondPageVals(i) sameElements extractGeoWkb(vec, i, isGeometry, srid))
    }
  }

  /**
   * Generates `count` polygons that share a long (> 64 byte) WKB prefix, differing only in a
   * single trailing coordinate. Exercises the prevBuf grow-and-preserve path where the shared
   * prefix must survive both reads and skips.
   */
  private def sharedPrefixPolygons(count: Int, base: Int = 0): Array[Array[Byte]] = {
    val head = (0 until 12).map(i => (i.toDouble, i.toDouble))
    (0 until count).map { k =>
      // Vary one interior coordinate near the end; keep coords within geography bounds
      // (lon in [-180, 180], lat in [-90, 90]) so the same data is valid for all geo types.
      val ring = head.dropRight(1) ++ Seq((100.0, 40.0 + (base + k) * 0.01), head.head)
      makePolygonWkb(ring: _*)
    }.toArray
  }

  private def readGeoInto(
      reader: VectorizedDeltaByteArrayReader,
      total: Int,
      vec: WritableColumnVector,
      isGeometry: Boolean): Unit = {
    if (isGeometry) {
      reader.readGeometry(total, vec, 0)
    } else {
      reader.readGeography(total, vec, 0)
    }
  }

  private def extractGeoWkb(
      vec: WritableColumnVector,
      i: Int,
      isGeometry: Boolean,
      srid: Int): Array[Byte] = {
    if (isGeometry) {
      val geom = vec.getBinaryView(i)
      assert(srid === STUtils.stGeomSrid(geom))
      STUtils.stGeomAsBinary(geom)
    } else {
      val geog = vec.getBinaryView(i)
      assert(srid === STUtils.stGeogSrid(geog))
      STUtils.stGeogAsBinary(geog)
    }
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
    readGeoInto(reader, length, writableColumnVector, isGeometry)

    for (i <- 0 until length) {
      assert(wkbValues(i) sameElements
        extractGeoWkb(writableColumnVector, i, isGeometry, srid))
    }
  }

  /**
   * Reads even-indexed geo values and skips odd-indexed ones, validating that a value which
   * shares its prefix with a skipped predecessor is still decoded correctly. This mimics a geo
   * column with interleaved null/skipped rows. An even count keeps the read/skip pattern from
   * running past the last value.
   */
  private def assertGeoReadWriteWithSkip(
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

    var i = 0
    while (i < length) {
      if (isGeometry) {
        reader.readGeometry(1, writableColumnVector, i)
      } else {
        reader.readGeography(1, writableColumnVector, i)
      }
      assert(wkbValues(i) sameElements
        extractGeoWkb(writableColumnVector, i, isGeometry, srid))
      if (i + 1 < length) {
        reader.skipBinary(1)
      }
      i += 2
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

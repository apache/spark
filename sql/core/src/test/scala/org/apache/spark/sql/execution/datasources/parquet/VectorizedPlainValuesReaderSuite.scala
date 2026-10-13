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

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{GeographyType, GeometryType, StringType}

/**
 * Tests that the vectorized PLAIN reader rejects corrupt pages on both the read and the skip
 * paths, instead of silently skipping too few bytes or moving the stream position backwards.
 */
class VectorizedPlainValuesReaderSuite extends SparkFunSuite {

  import VectorizedPlainValuesReaderSuite._

  testWithStreams("negative binary length is rejected when reading") { newReader =>
    val reader = newReader(page(4, "abcd", -1, "zzzz"))
    val v = new OnHeapColumnVector(2, StringType)
    val e = intercept[ParquetDecodingException] {
      reader.readBinary(2, v, 0)
    }
    assert(e.getMessage.contains("negative binary length: -1"))
  }

  testWithStreams("negative binary length is rejected when skipping") { newReader =>
    // Without the check, on a single buffer skipping the second value moved the position back
    // to the start of the page, so the next read returned "abcd" again.
    val reader = newReader(page(4, "abcd", -12, "zzzz"))
    val v = new OnHeapColumnVector(1, StringType)
    reader.readBinary(1, v, 0)
    val e = intercept[ParquetDecodingException] {
      reader.skipBinary(1)
    }
    assert(e.getMessage.contains("negative binary length: -12"))
  }

  testWithStreams("binary length larger than the rest of the page is rejected") { newReader =>
    // Int.MaxValue checks that the length is rejected before anything is allocated for it: on
    // multiple buffers, slice(len) allocates len bytes before it detects the end of the page.
    Seq(100, Int.MaxValue).foreach { len =>
      val expected = s"binary length $len is larger than the 2 bytes left in the page"
      val e1 = intercept[ParquetDecodingException] {
        newReader(page(len, "ab")).readBinary(1, new OnHeapColumnVector(1, StringType), 0)
      }
      assert(e1.getMessage.contains(expected))
      val e2 = intercept[ParquetDecodingException] {
        newReader(page(len, "ab")).skipBinary(1)
      }
      assert(e2.getMessage.contains(expected))
    }
  }

  testWithStreams("geo paths reject invalid binary lengths") { newReader =>
    // The lengths are validated before the WKB is parsed, so the payload does not matter.
    Seq(GeometryType(0), GeographyType(4326)).foreach { geoType =>
      def read(bytes: Array[Byte]): Unit = {
        val reader = newReader(bytes)
        val v = new OnHeapColumnVector(1, geoType)
        geoType match {
          case _: GeometryType => reader.readGeometry(1, v, 0)
          case _: GeographyType => reader.readGeography(1, v, 0)
        }
      }
      val e1 = intercept[ParquetDecodingException](read(page(-1, "abcd")))
      assert(e1.getMessage.contains("negative binary length: -1"), geoType)
      // Without the check, readNBytes returned a 2-byte array instead of failing.
      val e2 = intercept[ParquetDecodingException](read(page(100, "ab")))
      assert(e2.getMessage.contains("binary length 100 is larger than the 2 bytes left"), geoType)
    }
  }

  testWithStreams("fixed-width skips past the end of the page are rejected") { newReader =>
    // An 8-byte page holds two 4-byte values or one 8-byte value.
    val eightBytes = page(1, 2)
    val cases: Seq[(String, VectorizedPlainValuesReader => Unit, Long)] = Seq(
      ("skipBytes", _.skipBytes(3), 12L),
      ("skipShorts", _.skipShorts(3), 12L),
      ("skipIntegers", _.skipIntegers(3), 12L),
      ("skipFloats", _.skipFloats(3), 12L),
      ("skipLongs", _.skipLongs(2), 16L),
      ("skipDoubles", _.skipDoubles(2), 16L),
      ("skipFixedLenByteArray", _.skipFixedLenByteArray(3, 4), 12L),
      // 72 booleans are 9 bytes.
      ("skipBooleans", _.skipBooleans(72), 9L))
    cases.foreach { case (name, skip, bytes) =>
      val e = intercept[ParquetDecodingException] {
        skip(newReader(eightBytes))
      }
      assert(e.getMessage.contains(s"Failed to skip $bytes bytes"), name)
    }
  }

  testWithStreams("skips within the page land on the next value") { newReader =>
    // Three binary values (including an empty one) followed by a sentinel int.
    val binaryReader = newReader(page(2, "ab", 0, 3, "cde", 7))
    binaryReader.skipBinary(3)
    assert(binaryReader.readInteger() === 7)

    val reader = newReader(page(1, 2, 3))
    reader.skipIntegers(2)
    assert(reader.readInteger() === 3)
  }

  /**
   * Runs `testFun` on a page held in a single buffer and on the same page split across multiple
   * buffers, since `SingleBufferInputStream` and `MultiBufferInputStream` handle short and
   * negative lengths differently.
   */
  private def testWithStreams(testName: String)(
      testFun: (Array[Byte] => VectorizedPlainValuesReader) => Unit): Unit = {
    Seq("single buffer" -> false, "multiple buffers" -> true).foreach { case (name, split) =>
      test(s"$testName ($name)") {
        testFun { bytes =>
          val reader = new VectorizedPlainValuesReader
          reader.initFromPage(0, toStream(bytes, split))
          reader
        }
      }
    }
  }

  /** Builds a PLAIN page from little-endian ints and raw UTF-8 strings. */
  private def page(chunks: Any*): Array[Byte] = {
    val buf = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)
    chunks.foreach {
      case i: Int => buf.putInt(i)
      case s: String => buf.put(s.getBytes(StandardCharsets.UTF_8))
    }
    java.util.Arrays.copyOf(buf.array(), buf.position())
  }
}

object VectorizedPlainValuesReaderSuite {
  /**
   * Wraps `bytes` in a `ByteBufferInputStream`. With `split`, the bytes are split into 3-byte
   * buffers, which are not aligned with the 4-byte length prefixes, so the stream is a
   * `MultiBufferInputStream` and reads and skips cross buffer boundaries.
   */
  def toStream(bytes: Array[Byte], split: Boolean): ByteBufferInputStream = {
    if (split) {
      val buffers = bytes.grouped(3).map(ByteBuffer.wrap).toList
      assert(buffers.length > 1, "the page must span multiple buffers")
      ByteBufferInputStream.wrap(buffers.asJava)
    } else {
      ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))
    }
  }
}

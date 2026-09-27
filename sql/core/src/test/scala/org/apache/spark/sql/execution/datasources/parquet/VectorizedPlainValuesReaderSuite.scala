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

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.StringType

/**
 * Tests that the vectorized PLAIN reader rejects corrupt pages on both the read and the skip
 * paths, instead of silently skipping too few bytes or moving the stream position backwards.
 */
class VectorizedPlainValuesReaderSuite extends SparkFunSuite {

  test("negative binary length is rejected when reading") {
    val reader = newReader(page(4, "abcd", -1, "zzzz"))
    val v = new OnHeapColumnVector(2, StringType)
    val e = intercept[ParquetDecodingException] {
      reader.readBinary(2, v, 0)
    }
    assert(e.getMessage.contains("negative binary length: -1"))
  }

  test("negative binary length is rejected when skipping") {
    // Without the check, skipping the second value moved the position back to the start of
    // the page, so the next read returned "abcd" again.
    val reader = newReader(page(4, "abcd", -12, "zzzz"))
    val v = new OnHeapColumnVector(1, StringType)
    reader.readBinary(1, v, 0)
    val e = intercept[ParquetDecodingException] {
      reader.skipBinary(1)
    }
    assert(e.getMessage.contains("negative binary length: -12"))
  }

  test("binary length larger than the rest of the page is rejected when skipping") {
    val reader = newReader(page(100, "ab"))
    val e = intercept[ParquetDecodingException] {
      reader.skipBinary(1)
    }
    assert(e.getMessage.contains("Failed to skip 100 bytes"))
  }

  test("fixed-width skips past the end of the page are rejected") {
    // An 8-byte page holds two 4-byte values or one 8-byte value.
    val eightBytes = page(1, 2)
    val cases: Seq[(String, VectorizedPlainValuesReader => Unit, Long)] = Seq(
      ("skipBytes", _.skipBytes(3), 12L),
      ("skipShorts", _.skipShorts(3), 12L),
      ("skipIntegers", _.skipIntegers(3), 12L),
      ("skipFloats", _.skipFloats(3), 12L),
      ("skipLongs", _.skipLongs(2), 16L),
      ("skipDoubles", _.skipDoubles(2), 16L),
      ("skipFixedLenByteArray", _.skipFixedLenByteArray(3, 4), 12L))
    cases.foreach { case (name, skip, bytes) =>
      val e = intercept[ParquetDecodingException] {
        skip(newReader(eightBytes))
      }
      assert(e.getMessage.contains(s"Failed to skip $bytes bytes"), name)
    }
  }

  test("skips within the page land on the next value") {
    // Three binary values (including an empty one) followed by a sentinel int.
    val binaryReader = newReader(page(2, "ab", 0, 3, "cde", 7))
    binaryReader.skipBinary(3)
    assert(binaryReader.readInteger() === 7)

    val reader = newReader(page(1, 2, 3))
    reader.skipIntegers(2)
    assert(reader.readInteger() === 3)
  }

  private def newReader(bytes: Array[Byte]): VectorizedPlainValuesReader = {
    val reader = new VectorizedPlainValuesReader
    reader.initFromPage(0, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    reader
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

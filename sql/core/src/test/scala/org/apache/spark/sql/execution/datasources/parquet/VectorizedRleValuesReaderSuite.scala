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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.SparkFunSuite

/**
 * Group headers in a Parquet page body are attacker-controlled if the file is, so
 * `readNextGroup` validates them against the page's declared value count and remaining
 * byte budget before a count can size an allocation or drive loop control.
 */
class VectorizedRleValuesReaderSuite extends SparkFunSuite {

  import VectorizedRleValuesReaderSuite._

  test("invalid PACKED header: overflowing value count is rejected") {
    // numGroups = 2^28 overflows `numGroups * 8` to a negative Int currentCount, which
    // would send readBatch's leftInPage accounting backwards. Assert on the message:
    // without the validation this also throws, but as a wrapped end-of-stream failure.
    val e = intercept[ParquetDecodingException] {
      packedPageReader(numGroups = 1 << 28, pageValueCount = 1024).readInteger()
    }
    assert(e.getMessage.contains("Invalid bit-packed run"))
  }

  test("invalid PACKED header: count beyond the page byte budget is rejected pre-allocation") {
    // numGroups = 2^27 would allocate a ~4GB int buffer for a page with only a handful
    // of bytes left.
    val e = intercept[ParquetDecodingException] {
      packedPageReader(numGroups = 1 << 27, pageValueCount = 1024).readInteger()
    }
    assert(e.getMessage.contains("Invalid bit-packed run"))
  }

  test("invalid RLE header: run longer than the page's value count is rejected") {
    val out = new ByteArrayOutputStream()
    writeUnsignedVarInt(out, 1L << 21) // RLE header (LSB 0): run of 2^20 values
    out.write(1) // the repeated value (bitWidth 1 -> one byte)
    val reader = new VectorizedRleValuesReader(1, false)
    reader.initFromPage(1024, ByteBufferInputStream.wrap(ByteBuffer.wrap(out.toByteArray)))
    val e = intercept[ParquetDecodingException] {
      reader.readInteger()
    }
    assert(e.getMessage.contains("Invalid RLE run"))
  }

  test("truncated varint header is rejected") {
    // A lone continuation byte: read() returns -1 at end of input, whose set 0x80 bit
    // would otherwise keep the varint continuation loop alive.
    val out = new ByteArrayOutputStream()
    out.write(0x80)
    val reader = new VectorizedRleValuesReader(1, false)
    reader.initFromPage(1024, ByteBufferInputStream.wrap(ByteBuffer.wrap(out.toByteArray)))
    intercept[ParquetDecodingException] {
      reader.readInteger()
    }
  }

  test("PACKED: value count not a multiple of 8 is accepted (final group is zero-padded)") {
    // The last bit-packed group is padded to a multiple of 8 values, so a page of 1023
    // values legitimately carries a run counting 1024 -- the validation's +7 allowance.
    // Guards against the bound being too tight to read valid files.
    val numGroups = 128
    val out = new ByteArrayOutputStream()
    writeUnsignedVarInt(out, (numGroups.toLong << 1) | 1L)
    out.write(Array.fill[Byte](numGroups)(0x55)) // bitWidth 1 -> one byte per group
    val reader = new VectorizedRleValuesReader(1, false)
    reader.initFromPage(1023, ByteBufferInputStream.wrap(ByteBuffer.wrap(out.toByteArray)))
    reader.readInteger()
  }
}

private object VectorizedRleValuesReaderSuite {

  /**
   * Builds a reader over a page whose body starts with a PACKED header (LSB 1) declaring
   * `numGroups` bit-packed groups of 8 values each, followed by far fewer bytes than the
   * numGroups * bitWidth the run requires.
   */
  private def packedPageReader(
      numGroups: Int,
      pageValueCount: Int): VectorizedRleValuesReader = {
    val out = new ByteArrayOutputStream()
    writeUnsignedVarInt(out, (numGroups.toLong << 1) | 1L)
    out.write(Array[Byte](0, 0, 0, 0))
    val reader = new VectorizedRleValuesReader(1, false)
    reader.initFromPage(
      pageValueCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(out.toByteArray)))
    reader
  }

  private def writeUnsignedVarInt(out: ByteArrayOutputStream, value: Long): Unit = {
    var v = value
    while ((v & ~0x7fL) != 0) {
      out.write(((v & 0x7f) | 0x80).toInt)
      v >>>= 7
    }
    out.write(v.toInt)
  }
}

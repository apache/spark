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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.io.{ByteArrayInputStream, FilterInputStream, InputStream}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types._

class UnsafeRowWriterSuite extends SparkFunSuite {

  test("writeNullable matches the setNullAt/write split for null and non-null inputs") {
    // Primitive: a null write sets the null bit, a non-null write stores the value.
    val intWriter = new UnsafeRowWriter(2)
    intWriter.resetRowWriter()
    intWriter.writeNullable(0, 42, false)
    intWriter.writeNullable(1, -1, true)
    val intRow = intWriter.getRow
    assert(!intRow.isNullAt(0) && intRow.getInt(0) == 42)
    assert(intRow.isNullAt(1))

    // Reference type: the staged value is ignored when isNull is set.
    val strWriter = new UnsafeRowWriter(1)
    strWriter.resetRowWriter()
    strWriter.writeNullable(0, UTF8String.fromString("ignored"), true)
    assert(strWriter.getRow.isNullAt(0))

    // Wide decimal (precision > 18): a null write must still reserve the fixed-width slot, exactly
    // like the write(ordinal, null, precision, scale) call the codegen setNull branch used.
    val decWriter = new UnsafeRowWriter(2)
    decWriter.resetRowWriter()
    decWriter.writeNullable(0, null, true, 38, 18)
    val dec = Decimal(123456789.123456789)
    assert(dec.changePrecision(38, 18))
    decWriter.writeNullable(1, dec, false, 38, 18)
    val decRow = decWriter.getRow
    assert(decRow.isNullAt(0))
    assert(decRow.getDecimal(1, 38, 18) == dec)

    // Compact decimal (precision <= 18): the null bit must be set through
    // write(ordinal, null, precision, scale)'s internal null check rather than an explicit
    // setNullAt branch, so it lands on the same setNullAt the codegen previously emitted.
    val compactDecWriter = new UnsafeRowWriter(2)
    compactDecWriter.resetRowWriter()
    compactDecWriter.writeNullable(0, null, true, 18, 2)
    val compactDec = Decimal(123456789.12)
    assert(compactDec.changePrecision(18, 2))
    compactDecWriter.writeNullable(1, compactDec, false, 18, 2)
    val compactDecRow = compactDecWriter.getRow
    assert(compactDecRow.isNullAt(0))
    assert(compactDecRow.getDecimal(1, 18, 2) == compactDec)

    // CalendarInterval: a null write reserves the variable-length slot via write(ordinal, null).
    val intervalWriter = new UnsafeRowWriter(2)
    intervalWriter.resetRowWriter()
    intervalWriter.writeNullable(0, null.asInstanceOf[CalendarInterval], true)
    val interval = new CalendarInterval(1, 2, 3L)
    intervalWriter.writeNullable(1, interval, false)
    val intervalRow = intervalWriter.getRow
    assert(intervalRow.isNullAt(0))
    assert(intervalRow.getInterval(1) == interval)

    // TimestampNanosVal: like CalendarInterval, a null write must reserve the fixed 16-byte slot
    // via write(ordinal, null), which is the reserved-slot contract the codegen setNull relied on.
    val tsNanosWriter = new UnsafeRowWriter(2)
    tsNanosWriter.resetRowWriter()
    tsNanosWriter.writeNullable(0, null.asInstanceOf[TimestampNanosVal], true)
    val tsNanos = new TimestampNanosVal(1234567L, 89.toShort)
    tsNanosWriter.writeNullable(1, tsNanos, false)
    val tsNanosRow = tsNanosWriter.getRow
    assert(tsNanosRow.isNullAt(0))
    assert(tsNanosRow.getTimestampNTZNanos(1) == tsNanos)
  }

  def checkDecimalSizeInBytes(decimal: Decimal, numBytes: Int): Unit = {
    assert(decimal.toJavaBigDecimal.unscaledValue().toByteArray.length == numBytes)
  }

  test("SPARK-25538: zero-out all bits for decimals") {
    val decimal1 = Decimal(0.431)
    decimal1.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal1, 8)

    val decimal2 = Decimal(123456789.1232456789)
    decimal2.changePrecision(38, 18)
    checkDecimalSizeInBytes(decimal2, 11)
    // On an UnsafeRowWriter we write decimal2 first and then decimal1
    val unsafeRowWriter1 = new UnsafeRowWriter(1)
    unsafeRowWriter1.resetRowWriter()
    unsafeRowWriter1.write(0, decimal2, decimal2.precision, decimal2.scale)
    unsafeRowWriter1.reset()
    unsafeRowWriter1.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res1 = unsafeRowWriter1.getRow
    // On a second UnsafeRowWriter we write directly decimal1
    val unsafeRowWriter2 = new UnsafeRowWriter(1)
    unsafeRowWriter2.resetRowWriter()
    unsafeRowWriter2.write(0, decimal1, decimal1.precision, decimal1.scale)
    val res2 = unsafeRowWriter2.getRow
    // The two rows should be the equal
    assert(res1 == res2)
  }

  test("write and get geography through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getBinaryView(0) === null)
    val geography = BinaryView.fromBytes(Array[Byte](1, 2, 3))
    rowWriter.write(1, geography)
    assert(rowWriter.getRow.getBinaryView(1).getBytes sameElements geography.getBytes)
  }

  test("write and get geometry through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getBinaryView(0) === null)
    val geometry = BinaryView.fromBytes(Array[Byte](1, 2, 3))
    rowWriter.write(1, geometry)
    assert(rowWriter.getRow.getBinaryView(1).getBytes sameElements geometry.getBytes)
  }

  test("write and get calendar intervals through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.write(0, null.asInstanceOf[CalendarInterval])
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getInterval(0) === null)
    val interval = new CalendarInterval(0, 1, 0)
    rowWriter.write(1, interval)
    assert(rowWriter.getRow.getInterval(1) === interval)
  }

  test("write and get variant through UnsafeRowWriter") {
    val rowWriter = new UnsafeRowWriter(2)
    rowWriter.resetRowWriter()
    rowWriter.setNullAt(0)
    assert(rowWriter.getRow.isNullAt(0))
    assert(rowWriter.getRow.getVariant(0) === null)
    val variant = new VariantVal(Array[Byte](1, 2, 3), Array[Byte](-1, -2, -3, -4))
    rowWriter.write(1, variant)
    assert(rowWriter.getRow.getVariant(1).debugString() == variant.debugString())
  }

  private val chunkBytes = UnsafeWriter.STREAM_READ_CHUNK_BYTES

  private def shortReads(bytes: Array[Byte]): InputStream =
    new FilterInputStream(new ByteArrayInputStream(bytes)) {
      // Short, unaligned reads, as a remote file system stream may return.
      override def read(b: Array[Byte], off: Int, len: Int): Int =
        super.read(b, off, math.min(len, 4093))
    }

  private def writeBinary(bytes: Array[Byte]): UnsafeRowWriter = {
    val writer = new UnsafeRowWriter(2)
    writer.resetRowWriter()
    writer.write(0, 7L)
    writer.write(1, bytes)
    writer
  }

  private def newWriter(bufferSize: Option[Int]): UnsafeRowWriter = {
    val writer = bufferSize.map(new UnsafeRowWriter(2, _)).getOrElse(new UnsafeRowWriter(2))
    writer.resetRowWriter()
    writer.write(0, 7L)
    writer
  }

  /** Writes the second field from `input`, expecting it to hold `expectedNumBytes` bytes. */
  private def writeStreamed(
      input: InputStream,
      expectedNumBytes: Int,
      bufferSize: Option[Int]): UnsafeRowWriter = {
    val writer = newWriter(bufferSize)
    writer.writeFromStream(1, input, expectedNumBytes)
    writer
  }

  test("SPARK-62256: exactVariableLengthBufferSize sizes a buffer that writes never grow") {
    val path = UTF8String.fromString("file:/a/b")
    val content = Array.tabulate[Byte](1001)(_.toByte)
    val bufferSize = UnsafeRowWriter.exactVariableLengthBufferSize(
      3, path.numBytes().toLong, 0L, content.length.toLong)
    assert(bufferSize === 16 + 0 + 1008)
    val writer = new UnsafeRowWriter(3, bufferSize)
    writer.resetRowWriter()
    writer.write(0, path)
    writer.write(1, Array.emptyByteArray)
    writer.write(2, content)
    val row = writer.getRow
    assert(writer.getBuffer.length === row.getSizeInBytes)
    assert(row.getUTF8String(0) === path)
    assert(row.getBinary(2) === content)
    assert(UnsafeRowWriter.exactVariableLengthBufferSize(3) === 0)
  }

  test("SPARK-62256: exactVariableLengthBufferSize rejects negative and unrepresentable sizes") {
    val max = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
    // A one-field row has 8 bytes of null bits and one 8-byte fixed-length slot.
    assert(UnsafeRowWriter.exactVariableLengthBufferSize(1, max - 16L) === max - 16)
    assert(UnsafeRowWriter.exactVariableLengthBufferSize(1, max - 15L) === -1)
    assert(UnsafeRowWriter.exactVariableLengthBufferSize(1, max / 2L, max / 2L) === -1)
    assert(UnsafeRowWriter.exactVariableLengthBufferSize(1, Long.MaxValue) === -1)
    assert(UnsafeRowWriter.exactVariableLengthBufferSize(2, 8L, -1L) === -1)
  }

  test("SPARK-62256: writeFromStream writes the same bytes as write(byte[])") {
    // Around the padding word and the read chunk.
    val sizes =
      Seq(0, 1, 7, 8, 9, 1001, chunkBytes - 1, chunkBytes, chunkBytes + 1, 2 * chunkBytes + 3)
    sizes.foreach { numBytes =>
      val bytes = Array.tabulate[Byte](numBytes)(i => (i * 31).toByte)
      val expected = writeBinary(bytes).getRow
      val exactSize = UnsafeRowWriter.exactVariableLengthBufferSize(2, numBytes.toLong)
      Seq(None, Some(exactSize)).foreach { bufferSize =>
        val writer = writeStreamed(shortReads(bytes), numBytes, bufferSize)
        assert(writer.getRow === expected, s"$numBytes bytes, buffer size $bufferSize")
        if (bufferSize.isDefined) {
          assert(writer.getBuffer.length === expected.getSizeInBytes,
            s"$numBytes bytes: an exactly sized buffer must not grow")
        }
      }
    }
  }

  test("SPARK-62256: writeFromStream reads into its own chunk, never into the row buffer") {
    val bytes = Array.tabulate[Byte](2 * chunkBytes + 3)(_.toByte)
    var readArrays = List.empty[Array[Byte]]
    val input = new FilterInputStream(new ByteArrayInputStream(bytes)) {
      override def read(b: Array[Byte], off: Int, len: Int): Int = {
        readArrays ::= b
        super.read(b, off, len)
      }
    }
    val writer = writeStreamed(input, bytes.length, bufferSize = None)
    assert(writer.getRow === writeBinary(bytes).getRow)
    assert(readArrays.size >= 3)
    assert(readArrays.forall(_ ne writer.getBuffer), "the stream must not write the row buffer")
    assert(readArrays.forall(_.length <= chunkBytes), "each read must fit in one chunk")
  }

  test("SPARK-62256: writeFromStream asks for at least its minimum chunk") {
    val bytes = Array.tabulate[Byte](2 * chunkBytes + 3)(i => (i % 251 + 1).toByte)
    Seq(0, 1, 7).foreach { expectedNumBytes =>
      var requested = List.empty[Int]
      val input = new FilterInputStream(new ByteArrayInputStream(bytes)) {
        override def read(b: Array[Byte], off: Int, len: Int): Int = {
          requested ::= len
          super.read(b, off, len)
        }
      }
      val writer = writeStreamed(input, expectedNumBytes, bufferSize = None)
      val clue = s"expecting $expectedNumBytes bytes"
      assert(writer.getRow === writeBinary(bytes).getRow, clue)
      assert(requested.forall(_ >= UnsafeWriter.STREAM_READ_MIN_CHUNK_BYTES), clue)
    }
  }

  test("SPARK-62256: writeFromStream writes the whole stream when it is not the expected size") {
    // More than two chunks, so a longer stream grows a buffer sized for less while it is copied.
    val bytes = Array.tabulate[Byte](2 * chunkBytes + 5)(i => (i % 255 + 1).toByte)
    val expected = writeBinary(bytes).getRow
    Seq(0, 1, 8, chunkBytes, bytes.length - 3, bytes.length + 1, bytes.length + 40,
        2 * bytes.length).foreach { expectedNumBytes =>
      val exactSize = UnsafeRowWriter.exactVariableLengthBufferSize(2, expectedNumBytes.toLong)
      Seq(None, Some(exactSize)).foreach { bufferSize =>
        val clue = s"expecting $expectedNumBytes bytes, buffer size $bufferSize"
        val input = shortReads(bytes)
        val writer = writeStreamed(input, expectedNumBytes, bufferSize)
        assert(writer.getRow === expected, clue)
        assert(input.read() === -1, s"$clue: the stream must be read to its end")
      }
    }
  }

  test("SPARK-62256: writeFromStream grows a growable buffer with the stream, toward its size") {
    val bytes = Array.tabulate[Byte](5 * chunkBytes + 3)(i => (i % 251 + 1).toByte)
    val expected = writeBinary(bytes).getRow
    // The stream's size, and one it does not back.
    Seq(bytes.length, 64 * chunkBytes).foreach { expectedNumBytes =>
      val clue = s"expecting $expectedNumBytes bytes"
      val writer = newWriter(bufferSize = None)
      var buffers = List(writer.getBuffer)
      def recordBuffer(): Unit = if (writer.getBuffer ne buffers.head) buffers ::= writer.getBuffer
      val input = new FilterInputStream(shortReads(bytes)) {
        override def read(b: Array[Byte], off: Int, len: Int): Int = {
          recordBuffer()
          super.read(b, off, len)
        }
      }
      writer.writeFromStream(1, input, expectedNumBytes)
      recordBuffer()
      assert(writer.getRow === expected, clue)
      assert(buffers.size <= 16, s"$clue: the buffer must double, not grow per read")
      if (expectedNumBytes == bytes.length) {
        assert(writer.getBuffer.length === expected.getSizeInBytes,
          s"$clue: must end exactly sized")
      } else {
        assert(writer.getBuffer.length <= 2 * expected.getSizeInBytes,
          s"$clue: must follow the data")
      }
    }
  }

  test("SPARK-62256: writeFromStream zeroes the padding of a reused buffer") {
    // A stream of the expected size, a shorter one and a longer one.
    Seq(3 -> 3, 3 -> 16, 11 -> 3).foreach { case (numBytes, expectedNumBytes) =>
      val bytes = Array.tabulate[Byte](numBytes)(i => (i + 1).toByte)
      val writer = new UnsafeRowWriter(1)
      writer.resetRowWriter()
      writer.write(0, Array.fill[Byte](24)(-1))
      writer.reset()
      writer.zeroOutNullBytes()
      writer.writeFromStream(0, new ByteArrayInputStream(bytes), expectedNumBytes)
      val fresh = new UnsafeRowWriter(1)
      fresh.resetRowWriter()
      fresh.write(0, bytes)
      assert(writer.getRow === fresh.getRow, s"$numBytes bytes, expecting $expectedNumBytes")
    }
  }

  test("SPARK-62256: writeFromStream rejects a negative size") {
    val writer = new UnsafeRowWriter(1)
    writer.resetRowWriter()
    intercept[IllegalArgumentException] {
      writer.writeFromStream(0, new ByteArrayInputStream(Array.emptyByteArray), -1)
    }
  }
}

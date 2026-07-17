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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite

class BitmapExpressionUtilsSuite extends SparkFunSuite {

  test("bitmap_bucket_number with positive inputs") {
    Seq((0L, 0L), (1L, 1L), (2L, 1L), (3L, 1L), (65537L, 3L), (65536L, 2L), (3232423L, 99L),
      (4538345L, 139L), (845894934L, 25815L), (2147483647L, 65536L),
      (Long.MaxValue, 281474976710656L), (32768L, 1L), (32769L, 2L), (32770L, 2L)).foreach {
      case (input, expected) =>
        assert(BitmapExpressionUtils.bitmapBucketNumber(input) == expected)
    }
  }

  test("bitmap_bucket_number with negative inputs") {
    Seq((-1L, 0L), (-2L, 0L), (-3L, 0L), (-65536L, -2L), (65537L, 3L), (-65535L, -1L),
      (-3843485L, -117L), (-2147483647L, -65535L), (-2147483648L, -65536L),
      (Long.MinValue, -281474976710656L), (Long.MinValue + 1, -281474976710655L), (-32767L, 0L),
      (-32768L, -1L), (-32769L, -1L)).foreach {
      case (input, expected) =>
        assert(BitmapExpressionUtils.bitmapBucketNumber(input) == expected)
    }
  }

  test("bitmap_bit_position with positive inputs") {
    Seq((0L, 0L), (1L, 0L), (2L, 1L), (3L, 2L), (65537L, 0L), (65536L, 32767L), (3232423L, 21158L),
      (4538345L, 16360L), (845894934L, 21781L), (2147483647L, 32766L), (Long.MaxValue, 32766L),
      (32768L, 32767L), (32769L, 0L), (32770L, 1L)).foreach {
      case (input, expected) =>
        assert(BitmapExpressionUtils.bitmapBitPosition(input) == expected)
    }
  }

  test("bitmap_bit_position with negative inputs") {
    Seq((-1L, 1L), (-2L, 2L), (-3L, 3L), (-65536L, 0L), (-65535L, 32767L), (-3843485L, 9629L),
      (-2147483647L, 32767L), (-2147483648L, 0L), (Long.MinValue, 0L), (Long.MinValue + 1, 32767L),
      (-32767L, 32767L), (-32768L, 0L), (-32769L, 1L)).foreach {
      case (input, expected) =>
        assert(BitmapExpressionUtils.bitmapBitPosition(input) == expected)
    }
  }

  private def createBitmap(): Array[Byte] = {
    Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(0)
  }

  private def clearBitmap(bitmap: Array[Byte]): Unit = {
    for (i <- bitmap.indices) {
      bitmap(i) = 0
    }
  }

  private def setBitmapBits(bitmap: Array[Byte], bytePos: Int, bits: Int): Unit = {
    bitmap.update(bytePos, (bitmap(bytePos) & 0x0ff | bits & 0x0ff).toByte)
  }

  test("bitmap_count empty") {
    val bitmap = createBitmap()
    assert(BitmapExpressionUtils.bitmapCount(bitmap) == 0L)
  }

  test("bitmap_count") {
    val bitmap = createBitmap()
    setBitmapBits(bitmap, 0, 0x01)
    assert(BitmapExpressionUtils.bitmapCount(bitmap) == 1L)

    clearBitmap(bitmap)
    setBitmapBits(bitmap, 0, 0xff)
    assert(BitmapExpressionUtils.bitmapCount(bitmap) == 8L)

    setBitmapBits(bitmap, 1, 0x22)
    assert(BitmapExpressionUtils.bitmapCount(bitmap) == 10L)

    setBitmapBits(bitmap, bitmap.length - 1, 0x67)
    assert(BitmapExpressionUtils.bitmapCount(bitmap) == 15L)
  }

  test("bitmap_contains with bit set") {
    val bitmap = createBitmap()
    bitmap(0) = 0x01.toByte
    assert(BitmapExpressionUtils.bitmapContains(bitmap, 0))
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, 1))
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, 7))
  }

  test("bitmap_contains with multiple bits") {
    val bitmap = createBitmap()
    bitmap(0) = 0x82.toByte // bits 1 and 7 set
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, 0))
    assert(BitmapExpressionUtils.bitmapContains(bitmap, 1))
    assert(BitmapExpressionUtils.bitmapContains(bitmap, 7))
  }

  test("bitmap_contains at boundaries") {
    val bitmap = createBitmap()
    // Last byte, last bit (position = NUM_BITS - 1 = 32767)
    bitmap(bitmap.length - 1) = 0x80.toByte
    assert(BitmapExpressionUtils.bitmapContains(bitmap, BitmapExpressionUtils.NUM_BITS - 1))
    // Last byte, first bit (position = NUM_BITS - 8 = 32760)
    bitmap(bitmap.length - 1) = 0x01.toByte
    assert(BitmapExpressionUtils.bitmapContains(bitmap, BitmapExpressionUtils.NUM_BITS - 8))
  }

  test("bitmap_contains with empty bitmap") {
    val bitmap = createBitmap()
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, 0))
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, 100))
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, BitmapExpressionUtils.NUM_BITS - 1))
  }

  test("bitmap_contains with out-of-range position") {
    val bitmap = createBitmap()
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, BitmapExpressionUtils.NUM_BITS))
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, -1))
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, Long.MaxValue))
  }

  test("bitmap_contains with position exceeding Int.MaxValue") {
    val bitmap = createBitmap()
    // Int.MaxValue + 1 = 2147483648L. Both values far exceed NUM_BITS (32768),
    // so they hit the early-return guard (bitPosition >= NUM_BITS) and never
    // reach the bytePos division. This test documents that out-of-range values
    // in the upper 64-bit space do not throw ArithmeticException.
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, Int.MaxValue.toLong + 1))
    // Verify that the sentinel value does not cause an ArithmeticException or
    // ArrayIndexOutOfBoundsException.
    assert(!BitmapExpressionUtils.bitmapContains(bitmap, Int.MaxValue.toLong * 2))
  }

  test("bitmap_contains with short bitmap") {
    val shortBitmap = Array[Byte](0x01)
    assert(BitmapExpressionUtils.bitmapContains(shortBitmap, 0))
    // Position beyond short bitmap length should return false
    assert(!BitmapExpressionUtils.bitmapContains(shortBitmap, 8))
    assert(!BitmapExpressionUtils.bitmapContains(shortBitmap, BitmapExpressionUtils.NUM_BITS - 1))
  }

  test("bitmap_contains with all-ones bitmap") {
    val bitmap = Array.fill[Byte](BitmapExpressionUtils.NUM_BYTES)(0xFF.toByte)
    assert(BitmapExpressionUtils.bitmapContains(bitmap, 0))
    assert(BitmapExpressionUtils.bitmapContains(bitmap, 1028))
    assert(BitmapExpressionUtils.bitmapContains(bitmap, BitmapExpressionUtils.NUM_BITS - 1))
  }

  test("bitmap_contains with zero-length bitmap") {
    val zeroLengthBitmap = Array.emptyByteArray
    // A zero-length (truly empty) bitmap should return false for any position,
    // without throwing ArrayIndexOutOfBoundsException. This differs from the
    // "empty bitmap" test above which uses a zero-filled NUM_BYTES array.
    assert(!BitmapExpressionUtils.bitmapContains(zeroLengthBitmap, 0))
    assert(!BitmapExpressionUtils.bitmapContains(zeroLengthBitmap, 100))
    assert(!BitmapExpressionUtils.bitmapContains(zeroLengthBitmap, BitmapExpressionUtils.NUM_BITS - 1))
  }
}

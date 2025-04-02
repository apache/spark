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
}

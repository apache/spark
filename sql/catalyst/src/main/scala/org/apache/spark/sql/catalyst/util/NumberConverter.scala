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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.unsafe.types.UTF8String

object NumberConverter {

  /**
   * Decode v into value[].
   *
   * @param v is treated as an unsigned 64-bit integer
   * @param radix must be between MIN_RADIX and MAX_RADIX
   */
  private def decode(v: Long, radix: Int, value: Array[Byte]): Unit = {
    var tmpV = v
    java.util.Arrays.fill(value, 0.asInstanceOf[Byte])
    var i = value.length - 1
    while (tmpV != 0) {
      val q = java.lang.Long.divideUnsigned(tmpV, radix)
      value(i) = (tmpV - q * radix).asInstanceOf[Byte]
      tmpV = q
      i -= 1
    }
  }

  /**
   * Convert value[] into a long. On overflow, return -1 (as mySQL does). If a
   * negative digit is found, ignore the suffix starting there.
   *
   * @param radix  must be between MIN_RADIX and MAX_RADIX
   * @param fromPos is the first element that should be considered
   * @return the result should be treated as an unsigned 64-bit integer.
   */
  private def encode(radix: Int, fromPos: Int, value: Array[Byte]): Long = {
    var v: Long = 0L
    // bound will always be positive since radix >= 2
    // Note that: -1 is equivalent to 11111111...1111 which is the largest unsigned long value
    val bound = java.lang.Long.divideUnsigned(-1 - radix, radix)
    var i = fromPos
    while (i < value.length && value(i) >= 0) {
      // if v < 0, which mean its bit presentation starts with 1, so v * radix will cause
      // overflow since radix is greater than 2
      if (v < 0) {
        return -1
      }
      // check if v greater than bound
      // if v is greater than bound, v * radix + radix will cause overflow.
      if (v >= bound) {
        // However our target is checking whether v * radix + value(i) can cause overflow or not.
        // Because radix >= 2,so (-1 - value(i)) / radix will be positive (its bit presentation
        // will start with 0) and we can easily checking for overflow by checking
        // (-1 - value(i)) / radix < v or not
        if (java.lang.Long.divideUnsigned(-1 - value(i), radix) < v) {
          return -1
        }
      }
      v = v * radix + value(i)
      i += 1
    }
    v
  }

  /**
   * Convert the bytes in value[] to the corresponding chars.
   *
   * @param radix must be between MIN_RADIX and MAX_RADIX
   * @param fromPos is the first nonzero element
   */
  private def byte2char(radix: Int, fromPos: Int, value: Array[Byte]): Unit = {
    var i = fromPos
    while (i < value.length) {
      value(i) = Character.toUpperCase(Character.forDigit(value(i), radix)).asInstanceOf[Byte]
      i += 1
    }
  }

  /**
   * Convert the chars in value[] to the corresponding integers. If invalid
   * character is found, convert it to -1 and ignore the suffix starting there.
   *
   * @param radix must be between MIN_RADIX and MAX_RADIX
   * @param fromPos is the first nonzero element
   */
  private def char2byte(radix: Int, fromPos: Int, value: Array[Byte]): Unit = {
    var i = fromPos
    while (i < value.length) {
      value(i) = Character.digit(value(i), radix).asInstanceOf[Byte]
      // if invalid characters are found, it no need to convert the suffix starting there
      if (value(i) == -1) {
        return
      }
      i += 1
    }
  }

  /**
   * Convert numbers between different number bases. If toBase>0 the result is
   * unsigned, otherwise it is signed.
   * NB: This logic is borrowed from org.apache.hadoop.hive.ql.ud.UDFConv
   */
  def convert(n: Array[Byte], fromBase: Int, toBase: Int ): UTF8String = {
    if (fromBase < Character.MIN_RADIX || fromBase > Character.MAX_RADIX
        || Math.abs(toBase) < Character.MIN_RADIX
        || Math.abs(toBase) > Character.MAX_RADIX) {
      return null
    }

    if (n.length == 0) {
      return null
    }

    var (negative, first) = if (n(0) == '-') (true, 1) else (false, 0)

    // Copy the digits in the right side of the array
    val temp = new Array[Byte](Math.max(n.length, 64))
    var v: Long = -1

    System.arraycopy(n, first, temp, temp.length - n.length + first, n.length - first)
    char2byte(fromBase, temp.length - n.length + first, temp)

    // Do the conversion by going through a 64 bit integer
    v = encode(fromBase, temp.length - n.length + first, temp)

    if (negative && toBase > 0) {
      if (v < 0) {
        v = -1
      } else {
        v = -v
      }
    }
    if (toBase < 0 && v < 0) {
      v = -v
      negative = true
    }
    decode(v, Math.abs(toBase), temp)

    // Find the first non-zero digit or the last digits if all are zero.
    val firstNonZeroPos = {
      val firstNonZero = temp.indexWhere( _ != 0)
      if (firstNonZero != -1) firstNonZero else temp.length - 1
    }
    byte2char(Math.abs(toBase), firstNonZeroPos, temp)

    var resultStartPos = firstNonZeroPos
    if (negative && toBase < 0) {
      resultStartPos = firstNonZeroPos - 1
      temp(resultStartPos) = '-'
    }
    UTF8String.fromBytes(java.util.Arrays.copyOfRange(temp, resultStartPos, temp.length))
  }

  def toBinary(l: Long): Array[Byte] = {
    val result = new Array[Byte](8)
    result(0) = (l >>> 56 & 0xFF).toByte
    result(1) = (l >>> 48 & 0xFF).toByte
    result(2) = (l >>> 40 & 0xFF).toByte
    result(3) = (l >>> 32 & 0xFF).toByte
    result(4) = (l >>> 24 & 0xFF).toByte
    result(5) = (l >>> 16 & 0xFF).toByte
    result(6) = (l >>> 8 & 0xFF).toByte
    result(7) = (l & 0xFF).toByte
    result
  }

  def toBinary(i: Int): Array[Byte] = {
    val result = new Array[Byte](4)
    result(0) = (i >>> 24 & 0xFF).toByte
    result(1) = (i >>> 16 & 0xFF).toByte
    result(2) = (i >>> 8 & 0xFF).toByte
    result(3) = (i & 0xFF).toByte
    result
  }

  def toBinary(s: Short): Array[Byte] = {
    val result = new Array[Byte](2)
    result(0) = (s >>> 8 & 0xFF).toByte
    result(1) = (s & 0xFF).toByte
    result
  }

  def toBinary(s: Byte): Array[Byte] = {
    val result = new Array[Byte](1)
    result(0) = s
    result
  }
}

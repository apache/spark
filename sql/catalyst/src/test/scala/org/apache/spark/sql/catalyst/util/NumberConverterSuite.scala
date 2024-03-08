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

import java.nio.ByteBuffer
import java.nio.ByteOrder.BIG_ENDIAN

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.NumberConverter.{convert, toBinary}
import org.apache.spark.unsafe.types.UTF8String

class NumberConverterSuite extends SparkFunSuite {

  private[this] def checkConv(n: String, fromBase: Int, toBase: Int, expected: String): Unit = {
    Seq(true, false).foreach { ansiEnabled =>
      checkConv(n, fromBase, toBase, expected, ansiEnabled)
    }
  }

  private[this] def checkConv(
      n: String,
      fromBase: Int,
      toBase: Int,
      expected: String,
      ansiEnabled: Boolean): Unit = {
    assert(convert(UTF8String.fromString(n).getBytes, fromBase, toBase, ansiEnabled, null) ===
      UTF8String.fromString(expected))
  }

  test("convert") {
    checkConv("3", 10, 2, "11")
    checkConv("-15", 10, -16, "-F")
    checkConv("-15", 10, 16, "FFFFFFFFFFFFFFF1")
    checkConv("big", 36, 16, "3A48")
    checkConv("9223372036854775807", 36, 16, "FFFFFFFFFFFFFFFF", ansiEnabled = false)
    checkConv("11abc", 10, 16, "B")
  }

  test("SPARK-34909: convert negative to unsigned") {
    checkConv("-10", 11, 7, "45012021522523134134555")
  }

  test("SPARK-44973: conv must allocate enough space for all digits plus negative sign") {
    checkConv(s"${Long.MinValue}", 10, -2, BigInt(Long.MinValue).toString(2))
    checkConv((BigInt(Long.MaxValue) + 1).toString(16), 16, -2, BigInt(Long.MinValue).toString(2))
    checkConv(BigInt(Long.MinValue).toString(16), 16, -2, BigInt(Long.MinValue).toString(2))
  }

  test("byte to binary") {
    checkToBinary(0.toByte)
    checkToBinary(1.toByte)
    checkToBinary(-1.toByte)
    checkToBinary(Byte.MaxValue)
    checkToBinary(Byte.MinValue)
  }

  test("short to binary") {
    checkToBinary(0.toShort)
    checkToBinary(1.toShort)
    checkToBinary(-1.toShort)
    checkToBinary(Short.MaxValue)
    checkToBinary(Short.MinValue)
  }

  test("integer to binary") {
    checkToBinary(0)
    checkToBinary(1)
    checkToBinary(-1)
    checkToBinary(Int.MaxValue)
    checkToBinary(Int.MinValue)
  }

  test("long to binary") {
    checkToBinary(0L)
    checkToBinary(1L)
    checkToBinary(-1L)
    checkToBinary(Long.MaxValue)
    checkToBinary(Long.MinValue)
  }

  def checkToBinary[T](in: T): Unit = in match {
    case b: Byte =>
      assert(toBinary(b) === ByteBuffer.allocate(1).order(BIG_ENDIAN).put(b).array())
    case s: Short =>
      assert(toBinary(s) === ByteBuffer.allocate(2).order(BIG_ENDIAN).putShort(s).array())
    case i: Int =>
      assert(toBinary(i) === ByteBuffer.allocate(4).order(BIG_ENDIAN).putInt(i).array())
    case l: Long =>
      assert(toBinary(l) === ByteBuffer.allocate(8).order(BIG_ENDIAN).putLong(l).array())
  }

}

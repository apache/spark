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
import org.apache.spark.sql.types.{ByteType, IntegerType}

/**
 *  A test suite that tests Z-order expression.
 */
class ZOrderExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("input with same data types") {
    val expectedZOrders = Array(
      Array(0, 1, 4, 5, 16, 17, 20, 21),
      Array(2, 3, 6, 7, 18, 19, 22, 23),
      Array(8, 9, 12, 13, 24, 25, 28, 29),
      Array(10, 11, 14, 15, 26, 27, 30, 31),
      Array(32, 33, 36, 37, 48, 49, 52, 53),
      Array(34, 35, 38, 39, 50, 51, 54, 55),
      Array(40, 41, 44, 45, 56, 57, 60, 61),
      Array(42, 43, 46, 47, 58, 59, 62, 63)
    )
    val resultForByte = Array.fill[Byte](2)(0)
    val resultForShort = Array.fill[Byte](4)(0)
    val resultForInt = Array.fill[Byte](8)(0)
    val resultForLong = Array.fill[Byte](16)(0)
    val bytePositive = Integer.parseInt("11000000", 2).toByte
    val byteNegative = Integer.parseInt("00000000", 2).toByte

    (0 to 7).foreach(x => {
      (0 to 7).foreach(y => {
        val expectedLastByte = expectedZOrders(x)(y).toByte

        // Test positive and negative values
        Seq(
          // Test `ByteType`
          (Literal(x.toByte), Literal(y.toByte), bytePositive, resultForByte),
          (Literal((Byte.MinValue + x).toByte), Literal((Byte.MinValue + y).toByte),
            byteNegative, resultForByte),

          // Test `ShortType`
          (Literal(x.toShort), Literal(y.toShort), bytePositive, resultForShort),
          (Literal((Short.MinValue + x).toShort), Literal((Short.MinValue + y).toShort),
            byteNegative, resultForShort),

          // Test `IntegerType`
          (Literal(x), Literal(y), bytePositive, resultForInt),
          (Literal((Int.MinValue + x)), Literal((Int.MinValue + y)), byteNegative, resultForInt),

          // Test `LongType`
          (Literal(x.toLong), Literal(y.toLong), bytePositive, resultForLong),
          (Literal(Long.MinValue + x), Literal(Long.MinValue + y),
            byteNegative, resultForLong)).foreach {

          case (xValue, yValue, expectedFirstByte, result) =>
            result.update(0, expectedFirstByte)
            result.update(result.length - 1, expectedLastByte)
            checkEvaluation(ZOrder(Seq(xValue, yValue)), result)
        }
      })
    })
  }

  test("input with different data types") {
    checkEvaluation(
      ZOrder(Seq(Literal(-10.toLong), Literal(13.toByte), Literal(0), Literal(Short.MaxValue))),
      Array(121, -103, -35, -99, -74, -37, 109, -86, -86, -86, -86, -1, -1, -1, -10).map(_.toByte))
  }

  test("input with null") {
    checkEvaluation(
      ZOrder(Seq(Literal(null, IntegerType))),
      Array(0, 0, 0, 0).map(_.toByte))

    checkEvaluation(
      ZOrder(Seq(Literal(null, IntegerType), Literal(null, IntegerType))),
      Array(0, 0, 0, 0, 0, 0, 0, 0).map(_.toByte))

    checkEvaluation(
      ZOrder(Seq(Literal(0, IntegerType), Literal(null, IntegerType))),
      Array(-128, 0, 0, 0, 0, 0, 0, 0).map(_.toByte))

    checkEvaluation(
      ZOrder(Seq(Literal(-10.toLong), Literal(null, ByteType), Literal(null, IntegerType),
        Literal(Short.MaxValue))),
      Array(25, -103, -103, -103, -74, -37, 109, -86, -86, -86, -86, -1, -1, -1, -10)
        .map(_.toByte))
  }
}

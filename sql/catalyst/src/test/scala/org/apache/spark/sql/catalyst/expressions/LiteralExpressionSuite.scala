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

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval


class LiteralExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("null") {
    checkEvaluation(Literal.create(null, BooleanType), null)
    checkEvaluation(Literal.create(null, ByteType), null)
    checkEvaluation(Literal.create(null, ShortType), null)
    checkEvaluation(Literal.create(null, IntegerType), null)
    checkEvaluation(Literal.create(null, LongType), null)
    checkEvaluation(Literal.create(null, FloatType), null)
    checkEvaluation(Literal.create(null, DoubleType), null)
    checkEvaluation(Literal.create(null, StringType), null)
    checkEvaluation(Literal.create(null, BinaryType), null)
    checkEvaluation(Literal.create(null, DecimalType.USER_DEFAULT), null)
    checkEvaluation(Literal.create(null, DateType), null)
    checkEvaluation(Literal.create(null, TimestampType), null)
    checkEvaluation(Literal.create(null, CalendarIntervalType), null)
    checkEvaluation(Literal.create(null, ArrayType(ByteType, true)), null)
    checkEvaluation(Literal.create(null, ArrayType(StringType, true)), null)
    checkEvaluation(Literal.create(null, MapType(StringType, IntegerType)), null)
    checkEvaluation(Literal.create(null, StructType(Seq.empty)), null)
  }

  test("default") {
    checkEvaluation(Literal.default(BooleanType), false)
    checkEvaluation(Literal.default(ByteType), 0.toByte)
    checkEvaluation(Literal.default(ShortType), 0.toShort)
    checkEvaluation(Literal.default(IntegerType), 0)
    checkEvaluation(Literal.default(LongType), 0L)
    checkEvaluation(Literal.default(FloatType), 0.0f)
    checkEvaluation(Literal.default(DoubleType), 0.0)
    checkEvaluation(Literal.default(StringType), "")
    checkEvaluation(Literal.default(BinaryType), "".getBytes(StandardCharsets.UTF_8))
    checkEvaluation(Literal.default(DecimalType.USER_DEFAULT), Decimal(0))
    checkEvaluation(Literal.default(DecimalType.SYSTEM_DEFAULT), Decimal(0))
    checkEvaluation(Literal.default(DateType), DateTimeUtils.toJavaDate(0))
    checkEvaluation(Literal.default(TimestampType), DateTimeUtils.toJavaTimestamp(0L))
    checkEvaluation(Literal.default(CalendarIntervalType), new CalendarInterval(0, 0L))
    checkEvaluation(Literal.default(ArrayType(StringType)), Array())
    checkEvaluation(Literal.default(MapType(IntegerType, StringType)), Map())
    checkEvaluation(Literal.default(StructType(StructField("a", StringType) :: Nil)), Row(""))
  }

  test("boolean literals") {
    checkEvaluation(Literal(true), true)
    checkEvaluation(Literal(false), false)
  }

  test("int literals") {
    List(0, 1, Int.MinValue, Int.MaxValue).foreach { d =>
      checkEvaluation(Literal(d), d)
      checkEvaluation(Literal(d.toLong), d.toLong)
      checkEvaluation(Literal(d.toShort), d.toShort)
      checkEvaluation(Literal(d.toByte), d.toByte)
    }
    checkEvaluation(Literal(Long.MinValue), Long.MinValue)
    checkEvaluation(Literal(Long.MaxValue), Long.MaxValue)
  }

  test("double literals") {
    List(0.0, -0.0, Double.NegativeInfinity, Double.PositiveInfinity).foreach { d =>
      checkEvaluation(Literal(d), d)
      checkEvaluation(Literal(d.toFloat), d.toFloat)
    }
    checkEvaluation(Literal(Double.MinValue), Double.MinValue)
    checkEvaluation(Literal(Double.MaxValue), Double.MaxValue)
    checkEvaluation(Literal(Float.MinValue), Float.MinValue)
    checkEvaluation(Literal(Float.MaxValue), Float.MaxValue)

  }

  test("string literals") {
    checkEvaluation(Literal(""), "")
    checkEvaluation(Literal("test"), "test")
    checkEvaluation(Literal("\u0000"), "\u0000")
  }

  test("sum two literals") {
    checkEvaluation(Add(Literal(1), Literal(1)), 2)
  }

  test("binary literals") {
    checkEvaluation(Literal.create(new Array[Byte](0), BinaryType), new Array[Byte](0))
    checkEvaluation(Literal.create(new Array[Byte](2), BinaryType), new Array[Byte](2))
  }

  test("decimal") {
    List(-0.0001, 0.0, 0.001, 1.2, 1.1111, 5).foreach { d =>
      checkEvaluation(Literal(Decimal(d)), Decimal(d))
      checkEvaluation(Literal(Decimal(d.toInt)), Decimal(d.toInt))
      checkEvaluation(Literal(Decimal(d.toLong)), Decimal(d.toLong))
      checkEvaluation(Literal(Decimal((d * 1000L).toLong, 10, 3)),
        Decimal((d * 1000L).toLong, 10, 3))
      checkEvaluation(Literal(BigDecimal(d.toString)), Decimal(d))
      checkEvaluation(Literal(new java.math.BigDecimal(d.toString)), Decimal(d))
    }
  }

  test("array") {
    def toCatalyst(a: Array[_], elementType: DataType): Any = {
      CatalystTypeConverters.createToCatalystConverter(ArrayType(elementType))(a)
    }
    checkEvaluation(Literal(Array(1, 2, 3)), toCatalyst(Array(1, 2, 3), IntegerType))
    checkEvaluation(Literal(Array("a", "b", "c")), toCatalyst(Array("a", "b", "c"), StringType))
    checkEvaluation(Literal(Array(1.0, 4.0)), toCatalyst(Array(1.0, 4.0), DoubleType))
  }

  test("unsupported types (map and struct) in literals") {
    def checkUnsupportedTypeInLiteral(v: Any) = {
      val errMsgMap = intercept[RuntimeException] {
        Literal(v)
      }
      assert(errMsgMap.getMessage.startsWith("Unsupported literal type"))
    }
    checkUnsupportedTypeInLiteral(Map("key1" -> 1, "key2" ->2))
    checkUnsupportedTypeInLiteral(("mike", 29, 1.0))
  }
}

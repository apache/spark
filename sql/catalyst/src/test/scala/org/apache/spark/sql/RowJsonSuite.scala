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
package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.time.LocalDate

import org.json4s.JsonAST.{JArray, JBool, JDecimal, JDouble, JLong, JNull, JObject, JString, JValue}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.encoders.{ExamplePoint, ExamplePointUDT}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

/**
 * Test suite for [[Row]] JSON serialization.
 */
class RowJsonSuite extends SparkFunSuite {
  private val schema = new StructType()
    .add("c1", "string")
    .add("c2", IntegerType)

  private def testJson(name: String, value: Any, dt: DataType, expected: JValue): Unit = {
    test(name) {
      val row = new GenericRowWithSchema(Array(value), new StructType().add("a", dt))
      assert(row.jsonValue === JObject("a" -> expected))
    }
  }

  private def testJson(value: Any, dt: DataType, expected: JValue): Unit = {
    testJson(s"$dt $value", value, dt, expected)
  }

  // Nulls
  private def testJsonNull(dt: DataType, expected: JValue): Unit = {
    testJson(null, dt, JNull)
  }
  testJsonNull(IntegerType, JNull)
  testJsonNull(FloatType, JNull)
  testJsonNull(ArrayType(DoubleType, containsNull = true), JNull)

  // Primitives
  testJson(true, BooleanType, JBool(true))
  testJson(false, BooleanType, JBool(false))
  testJson(23.toByte, ByteType, JLong(23))
  testJson(-126.toByte, ByteType, JLong(-126))
  testJson(20281.toShort, ShortType, JLong(20281))
  testJson(-8752.toShort, ShortType, JLong(-8752))
  testJson(1078231987, IntegerType, JLong(1078231987))
  testJson(-10, IntegerType, JLong(-10))
  testJson(139289832109874199L, LongType, JLong(139289832109874199L))
  testJson(-7873748239973488L, LongType, JLong(-7873748239973488L))
  testJson(10.232e10f, FloatType, JDouble(10.232e10f))
  testJson(9.7e-13f, FloatType, JDouble(9.7e-13f))
  testJson(3.891e98d, DoubleType, JDouble(3.891e98d))
  testJson(-7.8e5d, DoubleType, JDouble(-7.8e5d))
  testJson(BigDecimal("1092.88"), DecimalType(10, 2), JDecimal(BigDecimal("1092.88")))
  testJson(Decimal("782.0003"), DecimalType(7, 4), JDecimal(BigDecimal("782.0003")))
  testJson(new java.math.BigDecimal("-77.89"), DecimalType(4, 2), JDecimal(BigDecimal("-77.89")))
  testJson("hello world", StringType, JString("hello world"))
  testJson("BinaryType", Array('a'.toByte, 'b'.toByte), BinaryType, JString("YWI="))
  testJson(Date.valueOf("2019-04-22"), DateType, JString("2019-04-22"))
  testJson(LocalDate.of(2018, 5, 14), DateType, JString("2018-05-14"))
  testJson(
    Timestamp.valueOf("2017-01-06 10:22:03.00"),
    TimestampType,
    JString("2017-01-06 10:22:03"))
  testJson(
    Timestamp.valueOf("2017-05-30 10:22:03.00").toInstant,
    TimestampType,
    JString("2017-05-30 10:22:03"))

  // Complex types
  testJson(
    "ArrayType(LongType,true)",
    Array(1L, null, 77L),
    ArrayType(LongType, containsNull = true),
    JArray(JLong(1L) :: JNull :: JLong(77L) :: Nil))

  testJson(
    Seq(1, -2, 3),
    ArrayType(IntegerType, containsNull = false),
    JArray(JLong(1) :: JLong(-2) :: JLong(3) :: Nil))

  testJson(
    Map("a" -> "b", "c" -> "d", "e" -> null),
    MapType(StringType, StringType, valueContainsNull = true),
    JObject("a" -> JString("b"), "c" -> JString("d"), "e" -> JNull))

  testJson(
    Map(1 -> "b", 2 -> "d", 3 -> null),
    MapType(IntegerType, StringType, valueContainsNull = true),
    JArray(
      JObject("key" -> JLong(1), "value" -> JString("b")) ::
      JObject("key" -> JLong(2), "value" -> JString("d")) ::
      JObject("key" -> JLong(3), "value" -> JNull) :: Nil))

  testJson(
    new GenericRowWithSchema(Array("1", 2), schema),
    schema,
    JObject("c1" -> JString("1"), "c2" -> JLong(2)))

  testJson(
    "UDT",
    new ExamplePoint(3.4d, 8.98d),
    new ExamplePointUDT,
    JArray(JDouble(3.4d) :: JDouble(8.98d) :: Nil))

  test("no schema") {
    val e = intercept[IllegalArgumentException] {
      Row("a").jsonValue
    }
    assert(e.getMessage.contains("requires a non-null schema"))
  }

  test("unsupported type") {
    val e = intercept[IllegalArgumentException] {
      val row = new GenericRowWithSchema(
        Array((1, 2)),
        new StructType().add("a", ObjectType(classOf[(Int, Int)])))
      row.jsonValue
    }
    assert(e.getMessage.contains("Failed to convert value"))
  }
}

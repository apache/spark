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

package org.apache.spark.sql.types

import org.apache.spark.SparkFunSuite

class DataTypeSuite extends SparkFunSuite {

  test("construct an ArrayType") {
    val array = ArrayType(StringType)

    assert(ArrayType(StringType, true) === array)
  }

  test("construct an MapType") {
    val map = MapType(StringType, IntegerType)

    assert(MapType(StringType, IntegerType, true) === map)
  }

  test("extract fields from a StructType") {
    val struct = StructType(
      StructField("a", IntegerType, true) ::
      StructField("b", LongType, false) ::
      StructField("c", StringType, true) ::
      StructField("d", FloatType, true) :: Nil)

    assert(StructField("b", LongType, false) === struct("b"))

    intercept[IllegalArgumentException] {
      struct("e")
    }

    val expectedStruct = StructType(
      StructField("b", LongType, false) ::
      StructField("d", FloatType, true) :: Nil)

    assert(expectedStruct === struct(Set("b", "d")))
    intercept[IllegalArgumentException] {
      struct(Set("b", "d", "e", "f"))
    }
  }

  test("extract field index from a StructType") {
    val struct = StructType(
      StructField("a", LongType) ::
      StructField("b", FloatType) :: Nil)

    assert(struct.fieldIndex("a") === 0)
    assert(struct.fieldIndex("b") === 1)

    intercept[IllegalArgumentException] {
      struct.fieldIndex("non_existent")
    }
  }

  def checkDataTypeJsonRepr(dataType: DataType): Unit = {
    test(s"JSON - $dataType") {
      assert(DataType.fromJson(dataType.json) === dataType)
    }
  }

  checkDataTypeJsonRepr(NullType)
  checkDataTypeJsonRepr(BooleanType)
  checkDataTypeJsonRepr(ByteType)
  checkDataTypeJsonRepr(ShortType)
  checkDataTypeJsonRepr(IntegerType)
  checkDataTypeJsonRepr(LongType)
  checkDataTypeJsonRepr(FloatType)
  checkDataTypeJsonRepr(DoubleType)
  checkDataTypeJsonRepr(DecimalType(10, 5))
  checkDataTypeJsonRepr(DecimalType.Unlimited)
  checkDataTypeJsonRepr(DateType)
  checkDataTypeJsonRepr(TimestampType)
  checkDataTypeJsonRepr(StringType)
  checkDataTypeJsonRepr(BinaryType)
  checkDataTypeJsonRepr(ArrayType(DoubleType, true))
  checkDataTypeJsonRepr(ArrayType(StringType, false))
  checkDataTypeJsonRepr(MapType(IntegerType, StringType, true))
  checkDataTypeJsonRepr(MapType(IntegerType, ArrayType(DoubleType), false))

  val metadata = new MetadataBuilder()
    .putString("name", "age")
    .build()
  val structType = StructType(Seq(
    StructField("a", IntegerType, nullable = true),
    StructField("b", ArrayType(DoubleType), nullable = false),
    StructField("c", DoubleType, nullable = false, metadata)))
  checkDataTypeJsonRepr(structType)

  def checkDefaultSize(dataType: DataType, expectedDefaultSize: Int): Unit = {
    test(s"Check the default size of ${dataType}") {
      assert(dataType.defaultSize === expectedDefaultSize)
    }
  }

  checkDefaultSize(NullType, 1)
  checkDefaultSize(BooleanType, 1)
  checkDefaultSize(ByteType, 1)
  checkDefaultSize(ShortType, 2)
  checkDefaultSize(IntegerType, 4)
  checkDefaultSize(LongType, 8)
  checkDefaultSize(FloatType, 4)
  checkDefaultSize(DoubleType, 8)
  checkDefaultSize(DecimalType(10, 5), 4096)
  checkDefaultSize(DecimalType.Unlimited, 4096)
  checkDefaultSize(DateType, 4)
  checkDefaultSize(TimestampType, 12)
  checkDefaultSize(StringType, 4096)
  checkDefaultSize(BinaryType, 4096)
  checkDefaultSize(ArrayType(DoubleType, true), 800)
  checkDefaultSize(ArrayType(StringType, false), 409600)
  checkDefaultSize(MapType(IntegerType, StringType, true), 410000)
  checkDefaultSize(MapType(IntegerType, ArrayType(DoubleType), false), 80400)
  checkDefaultSize(structType, 812)

  def checkEqualsIgnoreCompatibleNullability(
      from: DataType,
      to: DataType,
      expected: Boolean): Unit = {
    val testName =
      s"equalsIgnoreCompatibleNullability: (from: ${from}, to: ${to})"
    test(testName) {
      assert(DataType.equalsIgnoreCompatibleNullability(from, to) === expected)
    }
  }

  checkEqualsIgnoreCompatibleNullability(
    from = ArrayType(DoubleType, containsNull = true),
    to = ArrayType(DoubleType, containsNull = true),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = ArrayType(DoubleType, containsNull = false),
    to = ArrayType(DoubleType, containsNull = false),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = ArrayType(DoubleType, containsNull = false),
    to = ArrayType(DoubleType, containsNull = true),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = ArrayType(DoubleType, containsNull = true),
    to = ArrayType(DoubleType, containsNull = false),
    expected = false)
  checkEqualsIgnoreCompatibleNullability(
    from = ArrayType(DoubleType, containsNull = false),
    to = ArrayType(StringType, containsNull = false),
    expected = false)

  checkEqualsIgnoreCompatibleNullability(
    from = MapType(StringType, DoubleType, valueContainsNull = true),
    to = MapType(StringType, DoubleType, valueContainsNull = true),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = MapType(StringType, DoubleType, valueContainsNull = false),
    to = MapType(StringType, DoubleType, valueContainsNull = false),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = MapType(StringType, DoubleType, valueContainsNull = false),
    to = MapType(StringType, DoubleType, valueContainsNull = true),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = MapType(StringType, DoubleType, valueContainsNull = true),
    to = MapType(StringType, DoubleType, valueContainsNull = false),
    expected = false)
  checkEqualsIgnoreCompatibleNullability(
    from = MapType(StringType, ArrayType(IntegerType, true), valueContainsNull = true),
    to = MapType(StringType, ArrayType(IntegerType, false), valueContainsNull = true),
    expected = false)
  checkEqualsIgnoreCompatibleNullability(
    from = MapType(StringType, ArrayType(IntegerType, false), valueContainsNull = true),
    to = MapType(StringType, ArrayType(IntegerType, true), valueContainsNull = true),
    expected = true)


  checkEqualsIgnoreCompatibleNullability(
    from = StructType(StructField("a", StringType, nullable = true) :: Nil),
    to = StructType(StructField("a", StringType, nullable = true) :: Nil),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = StructType(StructField("a", StringType, nullable = false) :: Nil),
    to = StructType(StructField("a", StringType, nullable = false) :: Nil),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = StructType(StructField("a", StringType, nullable = false) :: Nil),
    to = StructType(StructField("a", StringType, nullable = true) :: Nil),
    expected = true)
  checkEqualsIgnoreCompatibleNullability(
    from = StructType(StructField("a", StringType, nullable = true) :: Nil),
    to = StructType(StructField("a", StringType, nullable = false) :: Nil),
    expected = false)
  checkEqualsIgnoreCompatibleNullability(
    from = StructType(
      StructField("a", StringType, nullable = false) ::
      StructField("b", StringType, nullable = true) :: Nil),
    to = StructType(
      StructField("a", StringType, nullable = false) ::
      StructField("b", StringType, nullable = false) :: Nil),
    expected = false)
}

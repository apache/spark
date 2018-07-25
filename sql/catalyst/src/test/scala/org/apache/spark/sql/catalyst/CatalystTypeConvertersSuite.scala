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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class CatalystTypeConvertersSuite extends SparkFunSuite {

  private val simpleTypes: Seq[DataType] = Seq(
    StringType,
    DateType,
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType.SYSTEM_DEFAULT,
    DecimalType.USER_DEFAULT)

  test("null handling in rows") {
    val schema = StructType(simpleTypes.map(t => StructField(t.getClass.getName, t)))
    val convertToCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)
    val convertToScala = CatalystTypeConverters.createToScalaConverter(schema)

    val scalaRow = Row.fromSeq(Seq.fill(simpleTypes.length)(null))
    assert(convertToScala(convertToCatalyst(scalaRow)) === scalaRow)
  }

  test("null handling for individual values") {
    for (dataType <- simpleTypes) {
      assert(CatalystTypeConverters.createToScalaConverter(dataType)(null) === null)
    }
  }

  test("option handling in convertToCatalyst") {
    // convertToCatalyst doesn't handle unboxing from Options. This is inconsistent with
    // createToCatalystConverter but it may not actually matter as this is only called internally
    // in a handful of places where we don't expect to receive Options.
    assert(CatalystTypeConverters.convertToCatalyst(Some(123)) === Some(123))
  }

  test("option handling in createToCatalystConverter") {
    assert(CatalystTypeConverters.createToCatalystConverter(IntegerType)(Some(123)) === 123)
  }

  test("primitive array handling") {
    val intArray = Array(1, 100, 10000)
    val intUnsafeArray = UnsafeArrayData.fromPrimitiveArray(intArray)
    val intArrayType = ArrayType(IntegerType, false)
    assert(CatalystTypeConverters.createToScalaConverter(intArrayType)(intUnsafeArray) === intArray)

    val doubleArray = Array(1.1, 111.1, 11111.1)
    val doubleUnsafeArray = UnsafeArrayData.fromPrimitiveArray(doubleArray)
    val doubleArrayType = ArrayType(DoubleType, false)
    assert(CatalystTypeConverters.createToScalaConverter(doubleArrayType)(doubleUnsafeArray)
      === doubleArray)
  }

  test("An array with null handling") {
    val intArray = Array(1, null, 100, null, 10000)
    val intGenericArray = new GenericArrayData(intArray)
    val intArrayType = ArrayType(IntegerType, true)
    assert(CatalystTypeConverters.createToScalaConverter(intArrayType)(intGenericArray)
      === intArray)
    assert(CatalystTypeConverters.createToCatalystConverter(intArrayType)(intArray)
      == intGenericArray)

    val doubleArray = Array(1.1, null, 111.1, null, 11111.1)
    val doubleGenericArray = new GenericArrayData(doubleArray)
    val doubleArrayType = ArrayType(DoubleType, true)
    assert(CatalystTypeConverters.createToScalaConverter(doubleArrayType)(doubleGenericArray)
      === doubleArray)
    assert(CatalystTypeConverters.createToCatalystConverter(doubleArrayType)(doubleArray)
      == doubleGenericArray)
  }

  test("converting a wrong value to the struct type") {
    val structType = new StructType().add("f1", IntegerType)
    val exception = intercept[IllegalArgumentException] {
      CatalystTypeConverters.createToCatalystConverter(structType)("test")
    }
    assert(exception.getMessage.contains("The value (test) of the type "
      + "(java.lang.String) cannot be converted to struct<f1:int>"))
  }

  test("converting a wrong value to the map type") {
    val mapType = MapType(StringType, IntegerType, false)
    val exception = intercept[IllegalArgumentException] {
      CatalystTypeConverters.createToCatalystConverter(mapType)("test")
    }
    assert(exception.getMessage.contains("The value (test) of the type "
      + "(java.lang.String) cannot be converted to a map type with key "
      + "type (string) and value type (int)"))
  }

  test("converting a wrong value to the array type") {
    val arrayType = ArrayType(IntegerType, true)
    val exception = intercept[IllegalArgumentException] {
      CatalystTypeConverters.createToCatalystConverter(arrayType)("test")
    }
    assert(exception.getMessage.contains("The value (test) of the type "
      + "(java.lang.String) cannot be converted to an array of int"))
  }

  test("converting a wrong value to the decimal type") {
    val decimalType = DecimalType(10, 0)
    val exception = intercept[IllegalArgumentException] {
      CatalystTypeConverters.createToCatalystConverter(decimalType)("test")
    }
    assert(exception.getMessage.contains("The value (test) of the type "
      + "(java.lang.String) cannot be converted to decimal(10,0)"))
  }

  test("converting a wrong value to the string type") {
    val exception = intercept[IllegalArgumentException] {
      CatalystTypeConverters.createToCatalystConverter(StringType)(0.1)
    }
    assert(exception.getMessage.contains("The value (0.1) of the type "
      + "(java.lang.Double) cannot be converted to the string type"))
  }

  test("SPARK-24571: convert Char to String") {
    val chr: Char = 'X'
    val converter = CatalystTypeConverters.createToCatalystConverter(StringType)
    val expected = UTF8String.fromString("X")
    assert(converter(chr) === expected)
  }
}

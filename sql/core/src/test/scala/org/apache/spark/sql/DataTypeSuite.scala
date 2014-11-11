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

import org.scalatest.FunSuite

class DataTypeSuite extends FunSuite {

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

  def checkDataTypeJsonRepr(dataType: DataType): Unit = {
    test(s"JSON - $dataType") {
      assert(DataType.fromJson(dataType.json) === dataType)
    }
  }

  checkDataTypeJsonRepr(BooleanType)
  checkDataTypeJsonRepr(ByteType)
  checkDataTypeJsonRepr(ShortType)
  checkDataTypeJsonRepr(IntegerType)
  checkDataTypeJsonRepr(LongType)
  checkDataTypeJsonRepr(FloatType)
  checkDataTypeJsonRepr(DoubleType)
  checkDataTypeJsonRepr(DecimalType.Unlimited)
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
  checkDataTypeJsonRepr(
    StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", ArrayType(DoubleType), nullable = false),
      StructField("c", DoubleType, nullable = false, metadata))))
}

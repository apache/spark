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

package org.apache.spark.sql.api.java

import org.apache.spark.sql.types.util.DataTypeConversions
import org.scalatest.FunSuite

import org.apache.spark.sql._
import DataTypeConversions._

class ScalaSideDataTypeConversionSuite extends FunSuite {

  def checkDataType(scalaDataType: DataType) {
    val javaDataType = asJavaDataType(scalaDataType)
    val actual = asScalaDataType(javaDataType)
    assert(scalaDataType === actual, s"Converted data type ${actual} " +
      s"does not equal the expected data type ${scalaDataType}")
  }

  test("convert data types") {
    // Simple DataTypes.
    checkDataType(StringType)
    checkDataType(BinaryType)
    checkDataType(BooleanType)
    checkDataType(TimestampType)
    checkDataType(DecimalType)
    checkDataType(DoubleType)
    checkDataType(FloatType)
    checkDataType(ByteType)
    checkDataType(IntegerType)
    checkDataType(LongType)
    checkDataType(ShortType)

    // Simple ArrayType.
    val simpleScalaArrayType = ArrayType(StringType, true)
    checkDataType(simpleScalaArrayType)

    // Simple MapType.
    val simpleScalaMapType = MapType(StringType, LongType)
    checkDataType(simpleScalaMapType)

    // Simple StructType.
    val simpleScalaStructType = StructType(
      StructField("a", DecimalType, false) ::
      StructField("b", BooleanType, true) ::
      StructField("c", LongType, true) ::
      StructField("d", BinaryType, false) :: Nil)
    checkDataType(simpleScalaStructType)

    // Complex StructType.
    val complexScalaStructType = StructType(
      StructField("simpleArray", simpleScalaArrayType, true) ::
      StructField("simpleMap", simpleScalaMapType, true) ::
      StructField("simpleStruct", simpleScalaStructType, true) ::
      StructField("boolean", BooleanType, false) :: Nil)
    checkDataType(complexScalaStructType)

    // Complex ArrayType.
    val complexScalaArrayType = ArrayType(complexScalaStructType, true)
    checkDataType(complexScalaArrayType)

    // Complex MapType.
    val complexScalaMapType = MapType(complexScalaStructType, complexScalaArrayType, false)
    checkDataType(complexScalaMapType)
  }
}

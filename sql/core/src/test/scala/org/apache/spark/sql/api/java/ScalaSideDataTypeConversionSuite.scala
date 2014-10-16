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

import org.apache.spark.sql.{DataType => SDataType, StructField => SStructField}
import org.apache.spark.sql.{StructType => SStructType}
import DataTypeConversions._

class ScalaSideDataTypeConversionSuite extends FunSuite {

  def checkDataType(scalaDataType: SDataType) {
    val javaDataType = asJavaDataType(scalaDataType)
    val actual = asScalaDataType(javaDataType)
    assert(scalaDataType === actual, s"Converted data type ${actual} " +
      s"does not equal the expected data type ${scalaDataType}")
  }

  test("convert data types") {
    // Simple DataTypes.
    checkDataType(org.apache.spark.sql.StringType)
    checkDataType(org.apache.spark.sql.BinaryType)
    checkDataType(org.apache.spark.sql.BooleanType)
    checkDataType(org.apache.spark.sql.TimestampType)
    checkDataType(org.apache.spark.sql.DecimalType)
    checkDataType(org.apache.spark.sql.DoubleType)
    checkDataType(org.apache.spark.sql.FloatType)
    checkDataType(org.apache.spark.sql.ByteType)
    checkDataType(org.apache.spark.sql.IntegerType)
    checkDataType(org.apache.spark.sql.LongType)
    checkDataType(org.apache.spark.sql.ShortType)

    // Simple ArrayType.
    val simpleScalaArrayType =
      org.apache.spark.sql.ArrayType(org.apache.spark.sql.StringType, true)
    checkDataType(simpleScalaArrayType)

    // Simple MapType.
    val simpleScalaMapType =
      org.apache.spark.sql.MapType(org.apache.spark.sql.StringType, org.apache.spark.sql.LongType)
    checkDataType(simpleScalaMapType)

    // Simple StructType.
    val simpleScalaStructType = SStructType(
      SStructField("a", org.apache.spark.sql.DecimalType, false) ::
      SStructField("b", org.apache.spark.sql.BooleanType, true) ::
      SStructField("c", org.apache.spark.sql.LongType, true) ::
      SStructField("d", org.apache.spark.sql.BinaryType, false) :: Nil)
    checkDataType(simpleScalaStructType)

    // Complex StructType.
    val complexScalaStructType = SStructType(
      SStructField("simpleArray", simpleScalaArrayType, true) ::
      SStructField("simpleMap", simpleScalaMapType, true) ::
      SStructField("simpleStruct", simpleScalaStructType, true) ::
      SStructField("boolean", org.apache.spark.sql.BooleanType, false) :: Nil)
    checkDataType(complexScalaStructType)

    // Complex ArrayType.
    val complexScalaArrayType =
      org.apache.spark.sql.ArrayType(complexScalaStructType, true)
    checkDataType(complexScalaArrayType)

    // Complex MapType.
    val complexScalaMapType =
      org.apache.spark.sql.MapType(complexScalaStructType, complexScalaArrayType, false)
    checkDataType(complexScalaMapType)
  }
}

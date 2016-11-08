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

package org.apache.spark.sql.execution.datasources.csv

import scala.util.Random

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class CSVInferSchemaSuite extends SparkFunSuite {

  test("String fields types are inferred correctly from null types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(NullType, "", options) == NullType)
    assert(CSVInferSchema.inferField(NullType, null, options) == NullType)
    assert(CSVInferSchema.inferField(NullType, "100000000000", options) == LongType)
    assert(CSVInferSchema.inferField(NullType, "60", options) == IntegerType)
    assert(CSVInferSchema.inferField(NullType, "3.5", options) == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(NullType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(NullType, "True", options) == BooleanType)
    assert(CSVInferSchema.inferField(NullType, "FAlSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(CSVInferSchema.inferField(NullType, textValueOne, options) == expectedTypeOne)
  }

  test("String fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(LongType, "1.0", options) == DoubleType)
    assert(CSVInferSchema.inferField(LongType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(IntegerType, "1.0", options) == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, null, options) == DoubleType)
    assert(CSVInferSchema.inferField(DoubleType, "test", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08-20 14:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(CSVInferSchema.inferField(LongType, "True", options) == BooleanType)
    assert(CSVInferSchema.inferField(IntegerType, "FALSE", options) == BooleanType)
    assert(CSVInferSchema.inferField(TimestampType, "FALSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(CSVInferSchema.inferField(IntegerType, textValueOne, options) == expectedTypeOne)
  }

  test("Timestamp field types are inferred correctly via custom data format") {
    var options = new CSVOptions(Map("timestampFormat" -> "yyyy-mm"), "GMT")
    assert(CSVInferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)
    options = new CSVOptions(Map("timestampFormat" -> "yyyy"), "GMT")
    assert(CSVInferSchema.inferField(TimestampType, "2015", options) == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(IntegerType, "2015-08-20 14", options) == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "2015-08-20 14:10", options) == StringType)
    assert(CSVInferSchema.inferField(LongType, "2015-08 14:49:00", options) == StringType)
  }

  test("Boolean fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")
    assert(CSVInferSchema.inferField(LongType, "Fale", options) == StringType)
    assert(CSVInferSchema.inferField(DoubleType, "TRUEe", options) == StringType)
  }

  test("Type arrays are merged to highest common type") {
    assert(
      CSVInferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).deep == Array(LongType).deep)
    assert(
      CSVInferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).deep == Array(DoubleType).deep)
  }

  test("Null fields are handled properly when a nullValue is specified") {
    val types = Seq(NullType, StringType, LongType, IntegerType,
      DoubleType, TimestampType, BooleanType, DecimalType(1, 1))
    types.foreach { t =>
      Seq("null", "\\N").foreach { v =>
        val options = new CSVOptions(Map("nullValue" -> v), "GMT")
        assert(CSVInferSchema.inferField(t, v, options) == t)
      }
    }

    // nullable field with multiple nullValue option.
    val nullValues = Seq("abc", "", "123", "null")
    val nullValuesStr = compact(render(nullValues))
    types.foreach { t =>
      val options = new CSVOptions(Map("nullValue" -> nullValuesStr), "GMT")
      val nullVal = nullValues(Random.nextInt(nullValues.length))
      assert(CSVInferSchema.inferField(t, nullVal, options) == t)
    }
  }

  test("Merging Nulltypes should yield Nulltype.") {
    val mergedNullTypes = CSVInferSchema.mergeRowTypes(Array(NullType), Array(NullType))
    assert(mergedNullTypes.deep == Array(NullType).deep)
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new CSVOptions(Map("TiMeStampFormat" -> "yyyy-mm"), "GMT")
    assert(CSVInferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)
  }

  test("SPARK-18877: `inferField` on DecimalType should find a common type with `typeSoFar`") {
    val options = new CSVOptions(Map.empty[String, String], "GMT")

    // 9.03E+12 is Decimal(3, -10) and 1.19E+11 is Decimal(3, -9).
    assert(CSVInferSchema.inferField(DecimalType(3, -10), "1.19E+11", options) ==
      DecimalType(4, -9))

    // BigDecimal("12345678901234567890.01234567890123456789") is precision 40 and scale 20.
    val value = "12345678901234567890.01234567890123456789"
    assert(CSVInferSchema.inferField(DecimalType(3, -10), value, options) == DoubleType)

    // Seq(s"${Long.MaxValue}1", "2015-12-01 00:00:00") should be StringType
    assert(CSVInferSchema.inferField(NullType, s"${Long.MaxValue}1", options) == DecimalType(20, 0))
    assert(CSVInferSchema.inferField(DecimalType(20, 0), "2015-12-01 00:00:00", options)
      == StringType)
  }

  test("DoubleType should be infered when user defined nan/inf are provided") {
    val options = new CSVOptions(Map("nanValue" -> "nan", "negativeInf" -> "-inf",
      "positiveInf" -> "inf"), "GMT")
    assert(CSVInferSchema.inferField(NullType, "nan", options) == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "inf", options) == DoubleType)
    assert(CSVInferSchema.inferField(NullType, "-inf", options) == DoubleType)
  }
}

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

package org.apache.spark.sql.catalyst.csv

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.types._

class CSVInferSchemaSuite extends SparkFunSuite with SQLHelper {

  test("String fields types are inferred correctly from null types") {
    val options = new CSVOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "", options) == NullType)
    assert(inferSchema.inferField(NullType, null, options) == NullType)
    assert(inferSchema.inferField(NullType, "100000000000", options) == LongType)
    assert(inferSchema.inferField(NullType, "60", options) == IntegerType)
    assert(inferSchema.inferField(NullType, "3.5", options) == DoubleType)
    assert(inferSchema.inferField(NullType, "test", options) == StringType)
    assert(inferSchema.inferField(NullType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(inferSchema.inferField(NullType, "True", options) == BooleanType)
    assert(inferSchema.inferField(NullType, "FAlSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(inferSchema.inferField(NullType, textValueOne, options) == expectedTypeOne)
  }

  test("String fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(LongType, "1.0", options) == DoubleType)
    assert(inferSchema.inferField(LongType, "test", options) == StringType)
    assert(inferSchema.inferField(IntegerType, "1.0", options) == DoubleType)
    assert(inferSchema.inferField(DoubleType, null, options) == DoubleType)
    assert(inferSchema.inferField(DoubleType, "test", options) == StringType)
    assert(inferSchema.inferField(LongType, "2015-08-20 14:57:00", options) == TimestampType)
    assert(inferSchema.inferField(DoubleType, "2015-08-20 15:57:00", options) == TimestampType)
    assert(inferSchema.inferField(LongType, "True", options) == BooleanType)
    assert(inferSchema.inferField(IntegerType, "FALSE", options) == BooleanType)
    assert(inferSchema.inferField(TimestampType, "FALSE", options) == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(inferSchema.inferField(IntegerType, textValueOne, options) == expectedTypeOne)
  }

  test("Timestamp field types are inferred correctly via custom data format") {
    var options = new CSVOptions(Map("timestampFormat" -> "yyyy-MM"), false, "GMT")
    var inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)

    options = new CSVOptions(Map("timestampFormat" -> "yyyy"), false, "GMT")
    inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(TimestampType, "2015", options) == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(IntegerType, "2015-08-20 14", options) == StringType)
    assert(inferSchema.inferField(DoubleType, "2015-08-20 14:10", options) == StringType)
    assert(inferSchema.inferField(LongType, "2015-08 14:49:00", options) == StringType)
  }

  test("Boolean fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(LongType, "Fale", options) == StringType)
    assert(inferSchema.inferField(DoubleType, "TRUEe", options) == StringType)
  }

  test("Type arrays are merged to highest common type") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(
      inferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      inferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).deep == Array(LongType).deep)
    assert(
      inferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).deep == Array(DoubleType).deep)
  }

  test("Null fields are handled properly when a nullValue is specified") {
    var options = new CSVOptions(Map("nullValue" -> "null"), false, "GMT")
    var inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "null", options) == NullType)
    assert(inferSchema.inferField(StringType, "null", options) == StringType)
    assert(inferSchema.inferField(LongType, "null", options) == LongType)

    options = new CSVOptions(Map("nullValue" -> "\\N"), false, "GMT")
    inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(IntegerType, "\\N", options) == IntegerType)
    assert(inferSchema.inferField(DoubleType, "\\N", options) == DoubleType)
    assert(inferSchema.inferField(TimestampType, "\\N", options) == TimestampType)
    assert(inferSchema.inferField(BooleanType, "\\N", options) == BooleanType)
    assert(inferSchema.inferField(DecimalType(1, 1), "\\N", options) == DecimalType(1, 1))
  }

  test("Merging Nulltypes should yield Nulltype.") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)
    val mergedNullTypes = inferSchema.mergeRowTypes(Array(NullType), Array(NullType))
    assert(mergedNullTypes.deep == Array(NullType).deep)
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new CSVOptions(Map("TiMeStampFormat" -> "yyyy-mm"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(TimestampType, "2015-08", options) == TimestampType)
  }

  test("SPARK-18877: `inferField` on DecimalType should find a common type with `typeSoFar`") {
    val options = new CSVOptions(Map.empty[String, String], false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    // 9.03E+12 is Decimal(3, -10) and 1.19E+11 is Decimal(3, -9).
    assert(inferSchema.inferField(DecimalType(3, -10), "1.19E+11", options) ==
      DecimalType(4, -9))

    // BigDecimal("12345678901234567890.01234567890123456789") is precision 40 and scale 20.
    val value = "12345678901234567890.01234567890123456789"
    assert(inferSchema.inferField(DecimalType(3, -10), value, options) == DoubleType)

    // Seq(s"${Long.MaxValue}1", "2015-12-01 00:00:00") should be StringType
    assert(inferSchema.inferField(NullType, s"${Long.MaxValue}1", options) == DecimalType(20, 0))
    assert(inferSchema.inferField(DecimalType(20, 0), "2015-12-01 00:00:00", options)
      == StringType)
  }

  test("DoubleType should be inferred when user defined nan/inf are provided") {
    val options = new CSVOptions(Map("nanValue" -> "nan", "negativeInf" -> "-inf",
      "positiveInf" -> "inf"), false, "GMT")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "nan", options) == DoubleType)
    assert(inferSchema.inferField(NullType, "inf", options) == DoubleType)
    assert(inferSchema.inferField(NullType, "-inf", options) == DoubleType)
  }
}

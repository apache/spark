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

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class CSVInferSchemaSuite extends SparkFunSuite with SQLHelper {

  test("String fields types are inferred correctly from null types") {
    val options = new CSVOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"), false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "") == NullType)
    assert(inferSchema.inferField(NullType, null) == NullType)
    assert(inferSchema.inferField(NullType, "100000000000") == LongType)
    assert(inferSchema.inferField(NullType, "60") == IntegerType)
    assert(inferSchema.inferField(NullType, "3.5") == DoubleType)
    assert(inferSchema.inferField(NullType, "test") == StringType)
    assert(inferSchema.inferField(NullType, "2015-08-20 15:57:00") == TimestampType)
    assert(inferSchema.inferField(NullType, "True") == BooleanType)
    assert(inferSchema.inferField(NullType, "FAlSE") == BooleanType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(inferSchema.inferField(NullType, textValueOne) == expectedTypeOne)
  }

  test("String fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss"), false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(LongType, "1.0") == DoubleType)
    assert(inferSchema.inferField(LongType, "test") == StringType)
    assert(inferSchema.inferField(IntegerType, "1.0") == DoubleType)
    assert(inferSchema.inferField(DoubleType, null) == DoubleType)
    assert(inferSchema.inferField(DoubleType, "test") == StringType)
    assert(inferSchema.inferField(LongType, "2015-08-20 14:57:00") == StringType)
    assert(inferSchema.inferField(DoubleType, "2015-08-20 15:57:00") == StringType)
    assert(inferSchema.inferField(LongType, "True") == StringType)
    assert(inferSchema.inferField(IntegerType, "FALSE") == StringType)
    assert(inferSchema.inferField(TimestampType, "FALSE") == StringType)

    val textValueOne = Long.MaxValue.toString + "0"
    val decimalValueOne = new java.math.BigDecimal(textValueOne)
    val expectedTypeOne = DecimalType(decimalValueOne.precision, decimalValueOne.scale)
    assert(inferSchema.inferField(IntegerType, textValueOne) == expectedTypeOne)
  }

  test("Timestamp field types are inferred correctly via custom data format") {
    var options = new CSVOptions(Map("timestampFormat" -> "yyyy-mm"), false, "UTC")
    var inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(TimestampType, "2015-08") == TimestampType)

    options = new CSVOptions(Map("timestampFormat" -> "yyyy"), false, "UTC")
    inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(TimestampType, "2015") == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(IntegerType, "2015-08-20 14") == StringType)
    assert(inferSchema.inferField(DoubleType, "2015-08-20 14:10") == StringType)
    assert(inferSchema.inferField(LongType, "2015-08 14:49:00") == StringType)
  }

  test("Boolean fields types are inferred correctly from other types") {
    val options = new CSVOptions(Map.empty[String, String], false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(LongType, "Fale") == StringType)
    assert(inferSchema.inferField(DoubleType, "TRUEe") == StringType)
  }

  test("Type arrays are merged to highest common type") {
    var options = new CSVOptions(Map.empty[String, String], false, "UTC")
    var inferSchema = new CSVInferSchema(options)

    assert(
      inferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).sameElements(Array(StringType)))
    assert(
      inferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).sameElements(Array(LongType)))
    assert(
      inferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).sameElements(Array(DoubleType)))

    // Can merge DateType and TimestampType into TimestampType when no timestamp format specified
    assert(
      inferSchema.mergeRowTypes(Array(DateType),
        Array(TimestampNTZType)).sameElements(Array(TimestampNTZType)))
    assert(
      inferSchema.mergeRowTypes(Array(DateType),
        Array(TimestampType)).sameElements(Array(TimestampType)))

    // Merge DateType and TimestampType into StringType when there are timestamp formats specified
    options = new CSVOptions(
      Map("timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
        "timestampNTZFormat" -> "yyyy/MM/dd HH:mm:ss"),
      false,
      "UTC")
    inferSchema = new CSVInferSchema(options)
    assert(
      inferSchema.mergeRowTypes(Array(DateType),
        Array(TimestampNTZType)).sameElements(Array(StringType)))
    assert(
      inferSchema.mergeRowTypes(Array(DateType),
        Array(TimestampType)).sameElements(Array(StringType)))
  }

  test("Null fields are handled properly when a nullValue is specified") {
    var options = new CSVOptions(Map("nullValue" -> "null"), false, "UTC")
    var inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "null") == NullType)
    assert(inferSchema.inferField(StringType, "null") == StringType)
    assert(inferSchema.inferField(LongType, "null") == LongType)

    options = new CSVOptions(Map("nullValue" -> "\\N"), false, "UTC")
    inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(IntegerType, "\\N") == IntegerType)
    assert(inferSchema.inferField(DoubleType, "\\N") == DoubleType)
    assert(inferSchema.inferField(TimestampType, "\\N") == TimestampType)
    assert(inferSchema.inferField(BooleanType, "\\N") == BooleanType)
    assert(inferSchema.inferField(DecimalType(1, 1), "\\N") == DecimalType(1, 1))
  }

  test("Merging Nulltypes should yield Nulltype.") {
    val options = new CSVOptions(Map.empty[String, String], false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    val mergedNullTypes = inferSchema.mergeRowTypes(Array(NullType), Array(NullType))
    assert(mergedNullTypes.sameElements(Array(NullType)))
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new CSVOptions(Map("TiMeStampFormat" -> "yyyy-mm"), false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(TimestampType, "2015-08") == TimestampType)
  }

  test("SPARK-18877: `inferField` on DecimalType should find a common type with `typeSoFar`") {
    val options = new CSVOptions(Map.empty[String, String], false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    withSQLConf(SQLConf.LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL_ENABLED.key -> "true") {
      // 9.03E+12 is Decimal(3, -10) and 1.19E+11 is Decimal(3, -9).
      assert(inferSchema.inferField(DecimalType(3, -10), "1.19E11") ==
        DecimalType(4, -9))
    }

    // BigDecimal("12345678901234567890.01234567890123456789") is precision 40 and scale 20.
    val value = "12345678901234567890.01234567890123456789"
    assert(inferSchema.inferField(DecimalType(3, 0), value) == DoubleType)

    // Seq(s"${Long.MaxValue}1", "2015-12-01 00:00:00") should be StringType
    assert(inferSchema.inferField(NullType, s"${Long.MaxValue}1") == DecimalType(20, 0))
    assert(inferSchema.inferField(DecimalType(20, 0), "2015-12-01 00:00:00")
      == StringType)
  }

  test("DoubleType should be inferred when user defined nan/inf are provided") {
    val options = new CSVOptions(Map("nanValue" -> "nan", "negativeInf" -> "-inf",
      "positiveInf" -> "inf"), false, "UTC")
    val inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(NullType, "nan") == DoubleType)
    assert(inferSchema.inferField(NullType, "inf") == DoubleType)
    assert(inferSchema.inferField(NullType, "-inf") == DoubleType)
  }

  test("inferring the decimal type using locale") {
    def checkDecimalInfer(langTag: String, expectedType: DataType): Unit = {
      val options = new CSVOptions(
        parameters = Map("locale" -> langTag, "inferSchema" -> "true", "sep" -> "|"),
        columnPruning = false,
        defaultTimeZoneId = "UTC")
      val inferSchema = new CSVInferSchema(options)

      val df = new DecimalFormat("", new DecimalFormatSymbols(Locale.forLanguageTag(langTag)))
      val input = df.format(Decimal(1000001).toBigDecimal)

      assert(inferSchema.inferField(NullType, input) == expectedType)
    }

    // input like '1,0' is inferred as strings for backward compatibility.
    Seq("en-US").foreach(checkDecimalInfer(_, StringType))
    Seq("ko-KR", "ru-RU", "de-DE").foreach(checkDecimalInfer(_, DecimalType(7, 0)))
  }

  test("SPARK-39469: inferring date type") {
    // "yyyy/MM/dd" format
    var options = new CSVOptions(Map("dateFormat" -> "yyyy/MM/dd"),
      false, "UTC")
    var inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(NullType, "2018/12/02") == DateType)
    // "MMM yyyy" format
    options = new CSVOptions(Map("dateFormat" -> "MMM yyyy"),
      false, "GMT")
    inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(NullType, "Dec 2018") == DateType)
    // Field should strictly match date format to infer as date
    options = new CSVOptions(
      Map("dateFormat" -> "yyyy-MM-dd", "timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss"),
      columnPruning = false,
      defaultTimeZoneId = "GMT")
    inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(NullType, "2018-12-03T11:00:00") == TimestampType)
    assert(inferSchema.inferField(NullType, "2018-12-03") == DateType)
  }

  test("SPARK-39469: inferring the schema of columns with mixing dates and timestamps properly") {
    var options = new CSVOptions(
      Map("dateFormat" -> "yyyy_MM_dd", "timestampFormat" -> "yyyy|MM|dd",
        "timestampNTZFormat" -> "yyyy/MM/dd"),
      columnPruning = false,
      defaultTimeZoneId = "UTC")
    var inferSchema = new CSVInferSchema(options)

    assert(inferSchema.inferField(DateType, "2012_12_12") == DateType)

    // inferField should infer a column as string type if it contains mixing dates and timestamps
    assert(inferSchema.inferField(DateType, "2003|01|01") == StringType)
    // SQL configuration must be set to default to TimestampNTZ
    withSQLConf(SQLConf.TIMESTAMP_TYPE.key -> SQLConf.TimestampTypes.TIMESTAMP_NTZ.toString) {
      assert(inferSchema.inferField(DateType, "2003/02/05") == StringType)
    }
    assert(inferSchema.inferField(TimestampNTZType, "2012_12_12") == StringType)
    assert(inferSchema.inferField(TimestampType, "2018_12_03") == StringType)

    // No errors when Date and Timestamp have the same format. Inference defaults to date
    options = new CSVOptions(
      Map("dateFormat" -> "yyyy_MM_dd", "timestampFormat" -> "yyyy_MM_dd"),
      columnPruning = false,
      defaultTimeZoneId = "UTC")
    inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(DateType, "2012_12_12") == DateType)
  }

  test("SPARK-45433: inferring the schema when timestamps do not match specified timestampFormat" +
    " with only one row") {
    val options = new CSVOptions(
      Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss"),
      columnPruning = false,
      defaultTimeZoneId = "UTC")
    val inferSchema = new CSVInferSchema(options)
    assert(inferSchema.inferField(NullType, "2884-06-24T02:45:51.138") == StringType)
  }
}

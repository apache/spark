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

import java.time.LocalTime

import org.apache.spark.{SparkConf, SparkDateTimeException}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

abstract class TimeFunctionsSuiteBase extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("SPARK-52881: make_time function") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("hour", IntegerType, nullable = false),
      StructField("minute", IntegerType, nullable = false),
      StructField("second", DecimalType(16, 6), nullable = false)
    ))
    val data = Seq(
      Row(0, 0, BigDecimal(0.0)),
      Row(1, 2, BigDecimal(3.4)),
      Row(23, 59, BigDecimal(59.999999))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "make_time(hour, minute, second)"
    )
    val result2 = df.select(
      make_time(col("hour"), col("minute"), col("second"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      "00:00:00",
      "01:02:03.4",
      "23:59:59.999999"
    ).toDF("timeString").select(col("timeString").cast("time"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }

  test("SPARK-52885: hour function") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("time", TimeType(), nullable = false)
    ))
    val data = Seq(
      Row(LocalTime.parse("00:00:00")),
      Row(LocalTime.parse("01:02:03.4")),
      Row(LocalTime.parse("23:59:59.999999"))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "hour(time)"
    )
    val result2 = df.select(
      hour(col("time"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      0,
      1,
      23
    ).toDF("hour").select(col("hour"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }

  test("SPARK-52886: minute function") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("time", TimeType(), nullable = false)
    ))
    val data = Seq(
      Row(LocalTime.parse("00:00:00")),
      Row(LocalTime.parse("01:02:03.4")),
      Row(LocalTime.parse("23:59:59.999999"))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "minute(time)"
    )
    val result2 = df.select(
      minute(col("time"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      0,
      2,
      59
    ).toDF("minute").select(col("minute"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }

  test("SPARK-52887: second function") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("time", TimeType(), nullable = false)
    ))
    val data = Seq(
      Row(LocalTime.parse("00:00:00")),
      Row(LocalTime.parse("01:02:03.4")),
      Row(LocalTime.parse("23:59:59.999999"))
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "second(time)"
    )
    val result2 = df.select(
      second(col("time"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      0,
      3,
      59
    ).toDF("second").select(col("second"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }

  test("SPARK-52883: to_time function without format") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("str", StringType, nullable = false)
    ))
    val data = Seq(
      Row("00:00:00"),
      Row("01:02:03.4"),
      Row("23:59:59.999999")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "to_time(str)"
    )
    val result2 = df.select(
      to_time(col("str"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      "00:00:00",
      "01:02:03.4",
      "23:59:59.999999"
    ).toDF("timeString").select(col("timeString").cast("time"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)

    // Error is thrown for malformed input.
    val invalidTimeDF = Seq("invalid_time").toDF("str")
    checkError(
      exception = intercept[SparkDateTimeException] {
        invalidTimeDF.select(to_time(col("str"))).collect()
      },
      condition = "CANNOT_PARSE_TIME",
      parameters = Map("input" -> "'invalid_time'", "format" -> "'HH:mm:ss.SSSSSS'")
    )
  }

  test("SPARK-52883: to_time function with format") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("str", StringType, nullable = false),
      StructField("format", StringType, nullable = false)
    ))
    val data = Seq(
      Row("00.00.00", "HH.mm.ss"),
      Row("01.02.03.4", "HH.mm.ss.S"),
      Row("23.59.59.999999", "HH.mm.ss.SSSSSS")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "to_time(str, format)"
    )
    val result2 = df.select(
      to_time(col("str"), col("format"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      "00:00:00",
      "01:02:03.4",
      "23:59:59.999999"
    ).toDF("timeString").select(col("timeString").cast("time"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)

    // Error is thrown for malformed input.
    val invalidTimeDF = Seq(("invalid_time", "HH.mm.ss")).toDF("str", "format")
    checkError(
      exception = intercept[SparkDateTimeException] {
        invalidTimeDF.select(to_time(col("str"), col("format"))).collect()
      },
      condition = "CANNOT_PARSE_TIME",
      parameters = Map("input" -> "'invalid_time'", "format" -> "'HH.mm.ss'")
    )
  }

  test("SPARK-52884: try_to_time function without format") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("str", StringType)
    ))
    val data = Seq(
      Row("00:00:00"),
      Row("01:02:03.4"),
      Row("23:59:59.999999"),
      Row("invalid_time"),
      Row(null)
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "try_to_time(str)"
    )
    val result2 = df.select(
      try_to_time(col("str"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      "00:00:00",
      "01:02:03.4",
      "23:59:59.999999",
      null,
      null
    ).toDF("timeString").select(col("timeString").cast("time"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }

  test("SPARK-52884: try_to_time function with format") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("str", StringType),
      StructField("format", StringType)
    ))
    val data = Seq(
      Row("00.00.00", "HH.mm.ss"),
      Row("01.02.03.4", "HH.mm.ss.SSS"),
      Row("23.59.59.999999", "HH.mm.ss.SSSSSS"),
      Row("invalid_time", "HH.mm.ss"),
      Row("00.00.00", "invalid_format"),
      Row("invalid_time", "invalid_format"),
      Row("00:00:00", "HH.mm.ss"),
      Row("abc", "HH.mm.ss"),
      Row("00:00:00", null),
      Row(null, "HH.mm.ss")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "try_to_time(str, format)"
    )
    val result2 = df.select(
      try_to_time(col("str"), col("format"))
    )
    // Check that both methods produce the same result.
    checkAnswer(result1, result2)

    // Expected output of the function.
    val expected = Seq(
      "00:00:00",
      "01:02:03.4",
      "23:59:59.999999",
      null,
      null,
      null,
      null,
      null,
      null,
      null
    ).toDF("timeString").select(col("timeString").cast("time"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }
}

// This class is used to run the same tests with ANSI mode enabled explicitly.
class TimeFunctionsAnsiOnSuite extends TimeFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// This class is used to run the same tests with ANSI mode disabled explicitly.
class TimeFunctionsAnsiOffSuite extends TimeFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}

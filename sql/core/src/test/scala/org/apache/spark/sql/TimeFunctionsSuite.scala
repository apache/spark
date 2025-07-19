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

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class TimeFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")

  test("to_time function without format") {
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
  }

  test("to_time function with format") {
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
  }

  test("try_to_time function without format") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("str", StringType)
    ))
    val data = Seq(
      Row("00:00:00"),
      Row("01:02:03.4"),
      Row("23:59:59.999999"),
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
      null
    ).toDF("timeString").select(col("timeString").cast("time"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }

  test("try_to_time function with format") {
    // Input data for the function.
    val schema = StructType(Seq(
      StructField("str", StringType),
      StructField("format", StringType)
    ))
    val data = Seq(
      Row("00.00.00", "HH.mm.ss"),
      Row("01.02.03.4", "HH.mm.ss.SSS"),
      Row("23.59.59.999999", "HH.mm.ss.SSSSSS"),
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
      null
    ).toDF("timeString").select(col("timeString").cast("time"))
    // Check that the results match the expected output.
    checkAnswer(result1, expected)
    checkAnswer(result2, expected)
  }
}

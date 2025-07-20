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

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

abstract class TimeFunctionsSuiteBase extends QueryTest with SharedSparkSession {
  import testImplicits._

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
}

// This class is used to run the same tests with ANSI mode enabled explicitly.
class TimeFunctionsAnsiOnSuite extends TimeFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// This class is used to run the same tests with ANSI mode disabled explicitly.
class TimeFunctionsAnsiOffSuite extends TimeFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}

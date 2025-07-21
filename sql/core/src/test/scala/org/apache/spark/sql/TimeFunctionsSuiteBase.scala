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

  // Helper method to assert that two DataFrames with TimeType values are approximately equal.
  // This method assumes that the two dataframes (df1 and df2) have the same schemas and sizes.
  // Also, only 1 column is expected in each DataFrame, and that column must be of TimeType.
  private def assertTwoTimesAreApproximatelyEqual(df1: DataFrame, df2: DataFrame) = {
    // Check that both DataFrames have the same schema.
    val schema1 = df1.schema
    val schema2 = df2.schema
    require(schema1 == schema2, s"Both DataFrames must have the same schema, but got " +
      s"$schema1 and $schema2 for the two given DataFrames df1 and df2, respectively.")
    // Check that both DataFrames have the same number of rows.
    val numRows1 = df1.count()
    val numRows2 = df2.count()
    require(numRows1 == numRows2, s"Both DataFrames must have the same number of rows, but got" +
      s"$numRows1 and $numRows2 rows in the two given DataFrames df1 and df2, respectively.")
    // Check that both DataFrames have only 1 column.
    val fields1 = schema1.fields.length
    require(fields1 == 1, s"The first DataFrame must have only one column, but got $fields1.")
    val fields2 = schema2.fields.length
    require(fields2 == 1, s"The second DataFrame must have only one column, but got $fields2.")
    // Check that the column type is TimeType.
    val columnType1 = schema1.fields.head.dataType
    require(columnType1.isInstanceOf[TimeType], s"The column type of the first DataFrame " +
      s"must be TimeType, but got $columnType1.")
    val columnType2 = schema2.fields.head.dataType
    require(columnType2.isInstanceOf[TimeType], s"The column type of the second DataFrame " +
      s"must be TimeType, but got $columnType2.")

    // Extract the LocalTime values from the input DataFrames.
    val time1: LocalTime = df1.collect().head.get(0).asInstanceOf[LocalTime]
    val time2: LocalTime = df2.collect().head.get(0).asInstanceOf[LocalTime]

    // Parse the results as times in milliseconds.
    val timeValue1 = java.sql.Time.valueOf(time1).getTime
    val timeValue2 = java.sql.Time.valueOf(time2).getTime

    // Check that the time difference is within a set number of minutes.
    val maxTimeDiffInMinutes = 15 // This should be enough time to ensure correctness.
    val timeDiffInMillis = Math.abs(timeValue1 - timeValue2)
    assert(
      timeDiffInMillis <= maxTimeDiffInMinutes * 60 * 1000,
      s"Time difference exceeds $maxTimeDiffInMinutes minutes: $timeDiffInMillis ms."
    )
  }

  test("SPARK-52882: current_time function with default precision") {
    // Create a dummy DataFrame with a single row to test the current_time() function.
    val df = spark.range(1)

    // Test the function using both `selectExpr` and `select`.
    val result1 = df.selectExpr(
      "current_time()"
    )
    val result2 = df.select(
      current_time()
    )

    // Check that both methods produce approximately the same result.
    assertTwoTimesAreApproximatelyEqual(result1, result2)
  }

  test("SPARK-52882: current_time function with specified precision") {
    (0 to 6).foreach { precision: Int =>
      // Create a dummy DataFrame with a single row to test the current_time(precision) function.
      val df = spark.range(1)

      // Test the function using both `selectExpr` and `select`.
      val result1 = df.selectExpr(
        s"current_time($precision)"
      )
      val result2 = df.select(
        current_time(precision)
      )

      // Confirm that the precision is correctly set.
      assert(result1.schema.fields.head.dataType == TimeType(precision))
      assert(result2.schema.fields.head.dataType == TimeType(precision))

      // Check that both methods produce approximately the same result.
      assertTwoTimesAreApproximatelyEqual(result1, result2)
    }
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
}

// This class is used to run the same tests with ANSI mode enabled explicitly.
class TimeFunctionsAnsiOnSuite extends TimeFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")
}

// This class is used to run the same tests with ANSI mode disabled explicitly.
class TimeFunctionsAnsiOffSuite extends TimeFunctionsSuiteBase {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "false")
}

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
}

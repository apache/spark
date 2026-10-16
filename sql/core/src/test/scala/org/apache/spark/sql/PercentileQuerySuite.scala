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

import java.time.{Duration, Period}

import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for percentile aggregate function.
 */
class PercentileQuerySuite extends SharedSparkSession {
  import testImplicits._

  private val table = "percentile_test"

  test("SPARK-39567: Support Ansi Interval type in Percentile") {
    withTempView(table) {
      Seq((Period.ofMonths(100), Duration.ofSeconds(100L)),
        (Period.ofMonths(200), Duration.ofSeconds(200L)),
        (Period.ofMonths(300), Duration.ofSeconds(300L)))
        .toDF("col1", "col2").createOrReplaceTempView(table)
      checkAnswer(
        spark.sql(
          s"""SELECT
            |  CAST(percentile(col1, 0.5) AS STRING),
            |  SUM(null),
            |  CAST(percentile(col2, 0.5) AS STRING)
            |FROM $table
          """.stripMargin),
        Row("INTERVAL '16-8' YEAR TO MONTH", null, "INTERVAL '0 00:03:20' DAY TO SECOND"))
    }
  }

  test("SPARK-57982: exact percentile is monotonically non-decreasing in the percentage") {
    withTempView(table) {
      // Values near 1e18, where consecutive doubles are spaced ~128 apart.
      spark.range(0, 20).selectExpr("1e18 + id * 128.0 AS x")
        .createOrReplaceTempView(table)

      // percentile(x, 0.04) must not exceed percentile(x, 0.05).
      checkAnswer(
        spark.sql(s"SELECT percentile(x, 0.04) <= percentile(x, 0.05) FROM $table"),
        Row(true))

      // percentile over increasing percentages must come back non-decreasing.
      val percentages = (0 to 100).map(_ / 100.0)
      val arrayLit = percentages.mkString("array(", ", ", ")")
      val result = spark.sql(s"SELECT percentile(x, $arrayLit) FROM $table")
        .head().getSeq[Double](0)
      assert(result.sliding(2).forall(w => w.head <= w.last),
        s"percentile results are not monotonically non-decreasing: $result")
    }
  }
}

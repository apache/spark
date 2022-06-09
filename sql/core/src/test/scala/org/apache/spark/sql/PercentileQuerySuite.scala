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
class PercentileQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val table = "percentile_test"

  test("SPARK-37138, SPARK-39427: Disable Ansi Interval type in Percentile") {
    withTempView(table) {
      Seq((Period.ofMonths(100), Duration.ofSeconds(100L)),
        (Period.ofMonths(200), Duration.ofSeconds(200L)),
        (Period.ofMonths(300), Duration.ofSeconds(300L)))
        .toDF("col1", "col2").createOrReplaceTempView(table)
      val e = intercept[AnalysisException] {
        spark.sql(
          s"""SELECT
            |  CAST(percentile(col1, 0.5) AS STRING),
            |  SUM(null),
            |  CAST(percentile(col2, 0.5) AS STRING)
            |FROM $table""".stripMargin).collect()
      }
      assert(e.getMessage.contains("data type mismatch"))
    }
  }
}

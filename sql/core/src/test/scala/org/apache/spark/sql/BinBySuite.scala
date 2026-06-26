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

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class BinBySuite extends QueryTest with SharedSparkSession {

  private def createMetricsView(): Unit = {
    spark.sql(
      """SELECT TIMESTAMP '2024-01-01 00:00:00' AS ts_start,
        |       TIMESTAMP '2024-01-01 01:00:00' AS ts_end,
        |       CAST(1 AS DOUBLE) AS value""".stripMargin).createOrReplaceTempView("metrics")
  }

  private val binByQuery =
    """SELECT * FROM metrics BIN BY (
      |  RANGE ts_start TO ts_end
      |  BIN WIDTH INTERVAL '5' MINUTE
      |  DISTRIBUTE UNIFORM (value)
      |)""".stripMargin

  test("BIN BY analyzes but physical execution is not yet implemented") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      withTempView("metrics") {
        createMetricsView()
        val df = spark.sql(binByQuery)

        // Analysis is fully functional when the operator is enabled.
        df.queryExecution.assertAnalyzed()

        // Physical execution is stubbed until the follow-up PR; planning surfaces a clean
        // UNSUPPORTED_FEATURE error rather than an internal error.
        checkError(
          exception = intercept[SparkUnsupportedOperationException] {
            df.collect()
          },
          condition = "UNSUPPORTED_FEATURE.BIN_BY",
          parameters = Map.empty[String, String])
      }
    }
  }

  test("BIN BY is gated off by default") {
    withTempView("metrics") {
      createMetricsView()
      // Gated off, the operator is rejected at analysis with the same UNSUPPORTED_FEATURE.BIN_BY
      // condition the execution stub raises when enabled.
      checkError(
        exception = intercept[SparkThrowable] {
          spark.sql(binByQuery).queryExecution.assertAnalyzed()
        },
        condition = "UNSUPPORTED_FEATURE.BIN_BY",
        parameters = Map.empty[String, String])
    }
  }

  test("BIN BY analyzes NTZ inputs and a custom ALIGN TO with renamed outputs") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      // NTZ inputs default the origin to epoch (LTZ defaults to the session-zone epoch).
      spark.sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP_NTZ'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 01:00:00', 1.0D)
          |  AS t(ts_start, ts_end, value)
          |BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value))
          |""".stripMargin).queryExecution.assertAnalyzed()

      // Custom ALIGN TO origin with renamed output columns.
      spark.sql(
        """SELECT * FROM VALUES
          |  (TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 02:00:00', 10.0D, 5.0D)
          |  AS t(ts_start, ts_end, a, b)
          |BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '1' HOUR
          |  ALIGN TO TIMESTAMP'2024-01-01 00:30:00'
          |  DISTRIBUTE UNIFORM (a, b)
          |  BIN_START AS w_start BIN_END AS w_end
          |  BIN_DISTRIBUTE_RATIO AS frac)
          |""".stripMargin).queryExecution.assertAnalyzed()
    }
  }
}

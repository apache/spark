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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.test.SharedSparkSession

class BinBySuite extends QueryTest with SharedSparkSession {

  test("BIN BY analyzes but physical execution is not yet implemented") {
    withTempView("metrics") {
      spark.sql(
        """SELECT TIMESTAMP '2024-01-01 00:00:00' AS ts_start,
          |       TIMESTAMP '2024-01-01 01:00:00' AS ts_end,
          |       CAST(1 AS DOUBLE) AS value""".stripMargin).createOrReplaceTempView("metrics")
      val df = spark.sql(
        """SELECT * FROM metrics BIN BY (
          |  RANGE ts_start TO ts_end
          |  BIN WIDTH INTERVAL '5' MINUTE
          |  DISTRIBUTE UNIFORM (value)
          |)""".stripMargin)

      // Analysis is fully functional in this PR.
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

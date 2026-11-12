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

import org.apache.spark.sql.test.SharedSparkSession

class GapFillSuite extends QueryTest with SharedSparkSession {

  test("GAP FILL in SQL text is rejected as not yet supported") {
    withTempView("metrics") {
      spark.sql("SELECT TIMESTAMP_NTZ '2024-01-01 00:00:00' AS ts, 1 AS v")
        .createOrReplaceTempView("metrics")
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM metrics GAP FILL (ORDER BY ts BIN WIDTH INTERVAL '5' MINUTE)")
        },
        condition = "UNSUPPORTED_FEATURE.GAP_FILL",
        parameters = Map.empty[String, String])
    }
  }
}

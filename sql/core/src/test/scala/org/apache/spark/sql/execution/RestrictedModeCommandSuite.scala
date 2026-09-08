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

package org.apache.spark.sql.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for the opt-in restricted SQL execution mode that need a running session -- in particular
 * that the check reaches the body of an analysis-only command (whose body moves into
 * `innerChildren` once analyzed), and that the static config cannot be turned off at runtime.
 */
class RestrictedModeCommandSuite extends QueryTest with SharedSparkSession {

  private val restricted = StaticSQLConf.RESTRICTED_MODE_ENABLED.key
  // The config is rendered double-quoted by `toSQLConf` in the error message.
  private val configName = "\"" + restricted + "\""

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(restricted, "true")

  private def checkRestricted(sqlText: String, feature: String): Unit = {
    checkError(
      exception = intercept[AnalysisException](sql(sqlText)),
      condition = "UNSUPPORTED_FEATURE.SQL_RESTRICTED_MODE",
      parameters = Map("feature" -> feature, "config" -> configName))
  }

  test("restricted mode rejects reflect inside a CREATE VIEW body") {
    withView("v") {
      // CreateViewCommand is an analysis-only command: once analyzed, its body (which contains the
      // reflect call) moves out of `children` into `innerChildren`.
      checkRestricted(
        "CREATE VIEW v AS SELECT reflect('java.lang.Math', 'abs', -1) AS c",
        "The `reflect` function")
    }
  }

  test("restricted mode rejects TRANSFORM inside a CACHE TABLE ... AS SELECT body") {
    // CacheTableAsSelect is an analysis-only command whose query runs at command execution, so the
    // gate must reach the body; the query is never executed because analysis rejects it first.
    checkRestricted(
      "CACHE TABLE c AS SELECT TRANSFORM(id) USING 'cat' AS (x) FROM range(1)",
      "The TRANSFORM ... USING clause")
  }

  test("the restricted mode config cannot be turned off at runtime") {
    val e = intercept[AnalysisException](sql(s"SET $restricted=false"))
    assert(e.getCondition == "CANNOT_MODIFY_STATIC_CONFIG")
  }
}

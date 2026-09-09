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
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
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

  test("restricted mode is enforced with the single-pass resolver enabled") {
    // The restricted-mode gate runs in `CheckAnalysis`, which only the fixed-point analyzer
    // invokes. The single-pass resolver would otherwise resolve `reflect` and mark the plan
    // analyzed without the gate; a restricted-mode session must fall back to the fixed-point
    // analyzer in every single-pass mode (fully enabled, which skips the ResolverGuard, and
    // tentative, which consults it) so the gate cannot be bypassed.
    Seq(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key,
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key).foreach { key =>
      withSQLConf(key -> "true") {
        checkRestricted("SELECT reflect('java.lang.Math', 'abs', -1)", "The `reflect` function")
      }
    }
  }

  test("restricted mode reaches a feature nested inside a scalar subquery") {
    checkRestricted(
      "SELECT (SELECT reflect('java.lang.Math', 'abs', -1)) AS c",
      "The `reflect` function")
  }

  test("restricted mode allows deeply nested subqueries with no restricted feature") {
    // `checkRestrictedMode` descends into subquery plans, which are also part of `innerChildren`.
    // Visiting them through both paths doubles the traversal at every nesting level, so a modest
    // chain of nested scalar subqueries would take exponential time. Analyzing this query must
    // stay feasible (it is linear once each subquery is traversed only once).
    val depth = 40
    val nested = (1 to depth).foldLeft("SELECT 1 AS c") { (inner, _) => s"SELECT ($inner) AS c" }
    checkAnswer(sql(nested), Row(1))
  }
}

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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.resolver.{
  AnalyzerBridgeState,
  ExplicitlyUnsupportedResolverFeature,
  Resolver,
  ResolverGuard
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.test.SharedSparkSession

class ResolverGuardSuite extends QueryTest with SharedSparkSession {

  // Queries that should pass the OperatorResolverGuard

  test("Select * from an inline table") {
    checkResolverGuard("SELECT * FROM VALUES(1,2,3)", shouldPass = true)
  }

  test("Select the named parameters from an inline table") {
    checkResolverGuard("SELECT col1,col2,col3 FROM VALUES(1,2,3)", shouldPass = true)
  }

  test("Inline table as a top level operator") {
    checkResolverGuard("VALUES(1,2,3)", shouldPass = true)
  }

  test("Select one row") {
    checkResolverGuard("SELECT 'Hello world!'", shouldPass = true)
  }

  test("Where clause with a literal") {
    checkResolverGuard(
      "SELECT * FROM VALUES(1, 2, false), (3, 4, true) WHERE true",
      shouldPass = true
    )
  }

  test("Where clause with an attribute") {
    checkResolverGuard(
      "SELECT * FROM VALUES(1, 2, false), (3, 4, true) WHERE col3",
      shouldPass = true
    )
  }

  test("Explicit cast with auto-alias") {
    checkResolverGuard(
      "SELECT CAST(1 AS DECIMAL(3,2))",
      shouldPass = true
    )
  }

  test("Multipart attribute name") {
    checkResolverGuard("SELECT table.col1 FROM VALUES(1) AS table", shouldPass = true)
  }

  test("Predicates") {
    checkResolverGuard("SELECT true and false", shouldPass = true)
    checkResolverGuard("SELECT true or false", shouldPass = true)
    checkResolverGuard(
      "SELECT col1 from VALUES(1,2) where true and false or true",
      shouldPass = true
    )
    checkResolverGuard("SELECT 1 = 2", shouldPass = true)
    checkResolverGuard("SELECT 1 != 2", shouldPass = true)
    checkResolverGuard("SELECT 1 IN (1,2,3)", shouldPass = true)
    checkResolverGuard("SELECT 1 NOT IN (1,2,3)", shouldPass = true)
    checkResolverGuard("SELECT 1 IS NULL", shouldPass = true)
    checkResolverGuard("SELECT 1 IS NOT NULL", shouldPass = true)
    checkResolverGuard("SELECT INTERVAL '1' DAY > INTERVAL '1' HOUR", shouldPass = true)
  }

  test("Star target") {
    checkResolverGuard("SELECT table.* FROM VALUES(1) as table", shouldPass = true)
  }

  test("Binary arithmetic") {
    checkResolverGuard("SELECT col1+col2 FROM VALUES(1,2)", shouldPass = true)
    checkResolverGuard("SELECT 1 + 2.3 / 2 - 3 DIV 2 + 3.0 * 10.0", shouldPass = true)
    checkResolverGuard(
      "SELECT TIMESTAMP'2011-11-11 11:11:11' - TIMESTAMP'2011-11-11 11:11:10'",
      shouldPass = true
    )
    checkResolverGuard(
      "SELECT DATE'2020-01-01' - TIMESTAMP'2019-10-06 10:11:12.345678'",
      shouldPass = true
    )
    checkResolverGuard("SELECT DATE'2012-01-01' - INTERVAL 3 HOURS", shouldPass = true)
    checkResolverGuard(
      "SELECT DATE'2012-01-01' + INTERVAL '12:12:12' HOUR TO SECOND",
      shouldPass = true
    )
    checkResolverGuard("SELECT DATE'2012-01-01' + 1", shouldPass = true)
    checkResolverGuard("SELECT 2 * INTERVAL 2 YEAR", shouldPass = true)
  }

  test("Supported recursive types") {
    Seq("ARRAY", "MAP", "STRUCT").foreach { typeName =>
      checkResolverGuard(
        s"SELECT col1 FROM VALUES($typeName(1,2),3)",
        shouldPass = true
      )
    }
  }

  test("Recursive types related functions") {
    checkResolverGuard("SELECT NAMED_STRUCT('a', 1)", shouldPass = true)
    checkResolverGuard("SELECT MAP_CONTAINS_KEY(MAP(1, 'a', 2, 'b'), 2)", shouldPass = true)
    checkResolverGuard("SELECT ARRAY_CONTAINS(ARRAY(1, 2, 3), 2);", shouldPass = true)
  }

  test("Conditional expressions") {
    checkResolverGuard("SELECT COALESCE(NULL, 1)", shouldPass = true)
    checkResolverGuard("SELECT col1, IF(col1 > 1, 1, 0) FROM VALUES(1,2),(2,3)", shouldPass = true)
    checkResolverGuard(
      "SELECT col1, CASE WHEN col1 > 1 THEN 1 ELSE 0 END FROM VALUES(1,2),(2,3)",
      shouldPass = true
    )
  }

  test("User specified alias") {
    checkResolverGuard("SELECT 1 AS alias", shouldPass = true)
  }

  test("Select from table") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (col1 INT, col2 INT)")
      checkResolverGuard("SELECT * FROM test_table", shouldPass = true)
    }
  }

  test("Single-layer subquery") {
    checkResolverGuard("SELECT * FROM (SELECT * FROM VALUES(1))", shouldPass = true)
  }

  test("Multi-layer subquery") {
    checkResolverGuard("SELECT * FROM (SELECT * FROM (SELECT * FROM VALUES(1)))", shouldPass = true)
  }

  test("Union all") {
    checkResolverGuard(
      "SELECT * FROM VALUES(1) UNION ALL SELECT * FROM VALUES(2)",
      shouldPass = true
    )
  }

  test("CTE") {
    checkResolverGuard(
      """
      WITH cte1 AS (
        SELECT * FROM VALUES (1)
      ),
      cte2 AS (
        SELECT * FROM VALUES (2)
      )
      SELECT * FROM cte1
      UNION ALL
      SELECT * FROM cte2
      """,
      shouldPass = true
    )
  }

  test("Subquery column aliases") {
    checkResolverGuard(
      "SELECT t.a, t.b FROM VALUES (1, 2) t (a, b)",
      shouldPass = true
    )
  }

  test("Function") {
    checkResolverGuard("SELECT assert_true(true)", shouldPass = true)
  }

  // Queries that shouldn't pass the OperatorResolverGuard

  test("Scalar subquery") {
    checkResolverGuard("SELECT (SELECT * FROM VALUES(1))", shouldPass = false)
  }

  test("EXISTS subquery") {
    checkResolverGuard(
      "SELECT * FROM VALUES (1) WHERE EXISTS (SELECT * FROM VALUES(1))",
      shouldPass = false
    )
  }

  test("IN subquery") {
    checkResolverGuard(
      "SELECT * FROM VALUES (1) WHERE col1 IN (SELECT * FROM VALUES(1))",
      shouldPass = false
    )
  }

  test("Function without the braces") {
    checkResolverGuard("SELECT current_date", shouldPass = false)
  }

  test("Session variables") {
    withSessionVariable {
      checkResolverGuard("SELECT session_variable", shouldPass = false)
    }
  }

  test("Case sensitive analysis") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      checkResolverGuard("SELECT 1", shouldPass = false)
    }
  }

  test("Union distinct") {
    checkResolverGuard(
      "SELECT * FROM VALUES (1) UNION DISTINCT SELECT * FROM VALUES (2)",
      shouldPass = true
    )
  }

  test("UDFs") {
    sql("CREATE FUNCTION supermario(x INT) RETURNS INT RETURN x + 3")
    checkResolverGuard("SELECT supermario(2)", shouldPass = false)
  }

  test("PLAN_ID_TAG") {
    val plan = spark.sessionState.sqlParser.parsePlan("SELECT col1 FROM VALUES (1)")

    val planId: Long = 0
    plan.asInstanceOf[Project].projectList.head.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)

    checkResolverGuard(plan, shouldPass = false)
  }

  test("Catch ExplicitlyUnsupportedResolverFeature exceptions") {

    class ThrowsExplicitlyUnsupportedFeatureResolver
        extends Resolver(spark.sessionState.catalogManager) {
      override def lookupMetadataAndResolve(
          plan: LogicalPlan,
          analyzerBridgeState: Option[AnalyzerBridgeState] = None): LogicalPlan =
        throw new ExplicitlyUnsupportedResolverFeature("ResolverGuardSuite dummy exception")
    }

    val dummyPlan = Project(Seq.empty, new OneRowRelation)

    checkError(
      exception = intercept[SparkException](
        checkResolverGuard(
          plan = dummyPlan,
          shouldPass = true,
          mockResolver = Some(new ThrowsExplicitlyUnsupportedFeatureResolver)
        )
      ),
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> "Resolver failed to resolve a feature supported in resolver guard."
      )
    )
  }

  private def checkResolverGuard(query: String, shouldPass: Boolean): Unit = {
    checkResolverGuard(spark.sql(query).queryExecution.logical, shouldPass)
  }

  private def checkResolverGuard(
      plan: LogicalPlan,
      shouldPass: Boolean,
      mockResolver: Option[Resolver] = None): Unit = {
    val resolverGuard = new ResolverGuard(spark.sessionState.catalogManager)
    assert(resolverGuard.apply(plan) == shouldPass)

    if (shouldPass) {
      checkSuccessfulResolution(plan, mockResolver)
    }
  }

  private def checkSuccessfulResolution(
      plan: LogicalPlan,
      mockResolver: Option[Resolver] = None) = {
    val resolver = mockResolver match {
      case None =>
        new Resolver(
          catalogManager = spark.sessionState.catalogManager,
          extensions = spark.sessionState.analyzer.singlePassResolverExtensions
        )
      case Some(mock) => mock
    }

    try {
      resolver.lookupMetadataAndResolve(plan)
    } catch {
      case throwable: Throwable =>
        throw SparkException.internalError(
          msg = s"Resolver failed to resolve a feature supported in resolver guard.",
          cause = throwable
        )
    }
  }

  private def withSessionVariable(body: => Unit): Unit = {
    sql("DECLARE session_variable = 1;")
    try {
      body
    } finally {
      sql("DROP TEMPORARY VARIABLE session_variable;")
    }
  }
}

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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.test.SharedSparkSession

class ResolverGuardSuite extends ResolverGuardSuiteBase {
  test("Select * from an inline table") {
    checkResolverGuard("SELECT * FROM VALUES(1,2,3)")
  }

  test("Select the named parameters from an inline table") {
    checkResolverGuard("SELECT col1,col2,col3 FROM VALUES(1,2,3)")
  }

  test("Inline table as a top level operator") {
    checkResolverGuard("VALUES(1,2,3)")
  }

  test("Select one row") {
    checkResolverGuard("SELECT 'Hello world!'")
  }

  test("Where clause with a literal") {
    checkResolverGuard(
      "SELECT * FROM VALUES(1, 2, false), (3, 4, true) WHERE true"
    )
  }

  test("Limit clause with a literal") {
    checkResolverGuard(
      "SELECT * FROM VALUES(1, 2, false), (3, 4, true) LIMIT 1"
    )
  }

  test("Offset clause with a literal") {
    checkResolverGuard(
      "SELECT * FROM VALUES(1, 2, false), (3, 4, true) OFFSET 1"
    )
  }

  test("Tail clause with a literal") {
    checkResolverGuard(
      Tail(
        Literal(1),
        sql("SELECT * FROM VALUES(1, 2, false), (3, 4, true)").queryExecution.logical
      ),
      None
    )
  }

  test("Where clause with an attribute") {
    checkResolverGuard(
      "SELECT * FROM VALUES(1, 2, false), (3, 4, true) WHERE col3"
    )
  }

  test("Explicit cast with auto-alias") {
    checkResolverGuard(
      "SELECT CAST(1 AS DECIMAL(3,2))"
    )
  }

  test("Multipart attribute name") {
    checkResolverGuard("SELECT table.col1 FROM VALUES(1) AS table")
  }

  test("Predicates") {
    checkResolverGuard("SELECT true and false")
    checkResolverGuard("SELECT true or false")
    checkResolverGuard(
      "SELECT col1 from VALUES(1,2) where true and false or true"
    )
    checkResolverGuard("SELECT 1 = 2")
    checkResolverGuard("SELECT 1 != 2")
    checkResolverGuard("SELECT 1 IN (1,2,3)")
    checkResolverGuard("SELECT 1 NOT IN (1,2,3)")
    checkResolverGuard("SELECT 1 IS NULL")
    checkResolverGuard("SELECT 1 IS NOT NULL")
    checkResolverGuard("SELECT INTERVAL '1' DAY > INTERVAL '1' HOUR")
  }

  test("Star target") {
    checkResolverGuard("SELECT table.* FROM VALUES(1) as table")
  }

  test("Binary arithmetic") {
    checkResolverGuard("SELECT col1+col2 FROM VALUES(1,2)")
    checkResolverGuard("SELECT 1 + 2.3 / 2 - 3 DIV 2 + 3.0 * 10.0")
    checkResolverGuard(
      "SELECT TIMESTAMP'2011-11-11 11:11:11' - TIMESTAMP'2011-11-11 11:11:10'"
    )
    checkResolverGuard(
      "SELECT DATE'2020-01-01' - TIMESTAMP'2019-10-06 10:11:12.345678'"
    )
    checkResolverGuard("SELECT DATE'2012-01-01' - INTERVAL 3 HOURS")
    checkResolverGuard(
      "SELECT DATE'2012-01-01' + INTERVAL '12:12:12' HOUR TO SECOND"
    )
    checkResolverGuard("SELECT DATE'2012-01-01' + 1")
    checkResolverGuard("SELECT 2 * INTERVAL 2 YEAR")
  }

  test("Supported recursive types") {
    Seq("ARRAY", "MAP", "STRUCT").foreach { typeName =>
      checkResolverGuard(
        s"SELECT col1 FROM VALUES($typeName(1,2),3)"
      )
    }
  }

  test("Recursive types related functions") {
    checkResolverGuard("SELECT NAMED_STRUCT('a', 1)")
    checkResolverGuard("SELECT MAP_CONTAINS_KEY(MAP(1, 'a', 2, 'b'), 2)")
    checkResolverGuard("SELECT ARRAY_CONTAINS(ARRAY(1, 2, 3), 2);")
  }

  test("Conditional expressions") {
    checkResolverGuard("SELECT COALESCE(NULL, 1)")
    checkResolverGuard("SELECT col1, IF(col1 > 1, 1, 0) FROM VALUES(1,2),(2,3)")
    checkResolverGuard(
      "SELECT col1, CASE WHEN col1 > 1 THEN 1 ELSE 0 END FROM VALUES(1,2),(2,3)"
    )
  }

  test("User specified alias") {
    checkResolverGuard("SELECT 1 AS alias")
  }

  test("Select from table") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (col1 INT, col2 INT)")
      checkResolverGuard("SELECT * FROM test_table")
    }
  }

  test("Single-layer subquery") {
    checkResolverGuard("SELECT * FROM (SELECT * FROM VALUES(1))")
  }

  test("Multi-layer subquery") {
    checkResolverGuard("SELECT * FROM (SELECT * FROM (SELECT * FROM VALUES(1)))")
  }

  for (setOperation <- Seq("UNION", "INTERSECT", "EXCEPT")) {
    test(s"$setOperation ALL") {
      checkResolverGuard(
        s"SELECT * FROM VALUES(1) $setOperation ALL SELECT * FROM VALUES(2)"
      )
    }

    test(s"$setOperation DISTINCT") {
      checkResolverGuard(
        s"SELECT * FROM VALUES (1) $setOperation DISTINCT SELECT * FROM VALUES (2)"
      )
    }

    test(s"CTE with $setOperation") {
      checkResolverGuard(
        s"""
            WITH cte1 AS (
              SELECT * FROM VALUES (1)
            ),
            cte2 AS (
              SELECT * FROM VALUES (2)
            )
            SELECT * FROM cte1
            $setOperation ALL
            SELECT * FROM cte2
      """
      )
    }
  }

  test("Subquery column aliases") {
    checkResolverGuard(
      "SELECT t.a, t.b FROM VALUES (1, 2) t (a, b)"
    )
  }

  test("Function") {
    checkResolverGuard("SELECT assert_true(true)")
  }

  test("Supported literal functions") {
    checkResolverGuard("SELECT current_date")
    checkResolverGuard("SELECT current_timestamp")
  }

  test("Group by") {
    checkResolverGuard("SELECT col1, count(col1) FROM VALUES(1) GROUP BY ALL")
    checkResolverGuard("SELECT * FROM VALUES(1,2,3) GROUP BY ALL")
    checkResolverGuard("SELECT col1 FROM VALUES(1) GROUP BY 1")
    checkResolverGuard("SELECT col1, col1 + 1 FROM VALUES(1) GROUP BY 1, col1")
  }

  test("Order by") {
    checkResolverGuard("SELECT col1 FROM VALUES(1) ORDER BY ALL")
    checkResolverGuard("SELECT col1 FROM VALUES(1) ORDER BY 1")
    checkResolverGuard("SELECT col1, col1 + 1 FROM VALUES(1) ORDER BY 1, col1")
  }

  test("Scalar subquery") {
    checkResolverGuard(
      "SELECT (SELECT col1 FROM VALUES (1)) + (SELECT col1 FROM VALUES (2))"
    )
  }

  test("IN subquery") {
    checkResolverGuard(
      "SELECT * FROM VALUES (1, 2) WHERE col1 IN (SELECT col1 FROM VALUES (3))"
    )
  }

  test("EXISTS subquery") {
    checkResolverGuard(
      """
      SELECT * FROM
        VALUES (1, 2) AS t1
      WHERE
        EXISTS (
          SELECT * FROM VALUES (1, 3) AS t2 WHERE t1.col1 = t2.col1
        )
      """
    )
  }

  test("EXPLAIN") {
    checkResolverGuard("EXPLAIN EXTENDED SELECT * FROM VALUES (1)")
    checkResolverGuard("EXPLAIN EXPLAIN SELECT * FROM VALUES (1)")
  }

  test("DESCRIBE") {
    checkResolverGuard("DESCRIBE QUERY SELECT * FROM VALUES (1)")
  }

  test("HAVING") {
    checkResolverGuard(
      "SELECT col1 FROM VALUES(1) GROUP BY col1 HAVING col1 > 1"
    )
  }


  test("TABLESAMPLE") {
    checkResolverGuard(
      "SELECT * FROM (VALUES (1), (2), (3)) TABLESAMPLE (40 PERCENT)"
    )
  }

  test("Semi-structured extract") {
    checkResolverGuard("SELECT PARSE_JSON('{\"a\":1}'):a")
  }

  test("Named parameters") {
    checkResolverGuard(
      plan = spark.sql("SELECT :first", Map("first" -> 1)).queryExecution.logical,
      unsupportedReason = None
    )
    checkResolverGuard(
      plan = spark.sql("EXPLAIN SELECT :first", Map("first" -> 1)).queryExecution.logical,
      unsupportedReason = None
    )
  }

  test("Unsupported literal functions") {
    checkResolverGuard(
      "SELECT current_user",
      unsupportedReason = Some("unsupported attribute name 'current_user'")
    )
    checkResolverGuard(
      "SELECT session_user",
      unsupportedReason = Some("unsupported attribute name 'session_user'")
    )
    checkResolverGuard("SELECT user", unsupportedReason = Some("unsupported attribute name 'user'"))
  }

  test("Case sensitive analysis") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      checkResolverGuard(
        "SELECT 1",
        unsupportedReason = Some("configuration: caseSensitiveAnalysis")
      )
    }
  }

  test("UDF") {
    withSqlFunction("supermario") {
      sql("CREATE FUNCTION supermario(x INT) RETURNS INT RETURN x + 3")

      checkResolverGuard("SELECT supermario(2)", unsupportedReason = Some("non-builtin function"))
    }
  }

  test("UDF in a database with the same name as a built-in function") {
    withDatabase("upper") {
      sql("CREATE DATABASE IF NOT EXISTS upper")

      withSqlFunction("supermario") {
        sql("USE DATABASE upper")

        sql("CREATE FUNCTION supermario(x INT) RETURNS INT RETURN x + 3")

        checkResolverGuard(
          "SELECT upper.supermario(2)",
          unsupportedReason = Some("multi-part function name")
        )
      }
    }
  }

  test("Multi-part persistent function name is still rejected") {
    checkResolverGuard(
      "SELECT spark_catalog.default.some_func(1)",
      unsupportedReason = Some("multi-part function name"))
  }

  test("PLAN_ID_TAG") {
    val plan = spark.sessionState.sqlParser.parsePlan("SELECT col1 FROM VALUES (1)")

    val planId: Long = 0
    plan.asInstanceOf[Project].projectList.head.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)

    checkResolverGuard(plan, unsupportedReason = Some("PLAN_ID_TAG"))
  }

  test("Star outside of Project list") {
    checkResolverGuard("SELECT * FROM VALUES (1, 2) WHERE 3 IN (*)")
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
          unsupportedReason = None,
          mockResolver = Some(new ThrowsExplicitlyUnsupportedFeatureResolver)
        )
      ),
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> "Resolver failed to resolve a feature supported in resolver guard."
      )
    )
  }


  test("Star target with columns renames") {
    val df = sql("SELECT * FROM VALUES(1,2)")
    val newDf = df.withColumnsRenamed(Seq("col2"), Seq("newCol2"))
    checkResolverGuard(plan = newDf.queryExecution.logical, None)
  }


  test("Regex query with column reference pattern") {
    withSQLConf("spark.sql.parser.quotedRegexColumnNames" -> "true") {
      checkResolverGuard(
        """SELECT `[a-z].{3}(1|3)` FROM VALUES('a', 'b', 'c')"""
      )
    }
  }

}

trait ResolverGuardSuiteBase extends QueryTest with SharedSparkSession {
  protected def checkResolverGuard(
      query: String,
      unsupportedReason: Option[String] = None): Unit = {
    checkResolverGuard(
      spark.sessionState.sqlParser.parsePlan(query),
      unsupportedReason,
      mockResolver = None
    )
  }

  protected def checkResolverGuard(plan: LogicalPlan, unsupportedReason: Option[String]): Unit = {
    checkResolverGuard(plan, unsupportedReason, mockResolver = None)
  }

  protected def checkResolverGuard(
      plan: LogicalPlan,
      unsupportedReason: Option[String],
      mockResolver: Option[Resolver]): Unit = {
    val resolverGuard = new ResolverGuard(
      catalogManager = spark.sessionState.catalogManager
    )

    val result = resolverGuard.apply(plan)
    assert(result.planUnsupportedReason == unsupportedReason)

    if (unsupportedReason.isEmpty) {
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
          extensions = spark.sessionState.analyzer.singlePassResolverExtensions,
          metadataResolverExtensions =
            spark.sessionState.analyzer.singlePassMetadataResolverExtensions
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

  protected def withSqlFunction[R](name: String)(body: => R): R = {
    try {
      body
    } finally {
      spark.sql(s"DROP FUNCTION IF EXISTS $name")
    }
  }
}

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

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Ascending,
  Expression,
  GreaterThan,
  Literal,
  NamedExpression,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, Sort}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.internal.SQLConf

/**
 * Parity tests between the single-pass resolver and the fixed-point Analyzer. In dual run, a query
 * that both analyzers resolve must produce the same plan, and a query that the fixed-point
 * Analyzer rejects must not be resolved by the single-pass resolver.
 */
class SinglePassResolverDualRunSuite extends HybridAnalyzerSuiteBase {

  private def parsePlan(sqlText: String): LogicalPlan =
    spark.sessionState.sqlParser.parsePlan(sqlText)

  private def parseExpression(sqlText: String): Expression =
    spark.sessionState.sqlParser.parseExpression(sqlText)

  /**
   * Analyzes `query` in dual run and checks that it fails with the fixed-point Analyzer error,
   * which means that the single-pass resolver failed too.
   */
  private def checkFixedPointError(query: => DataFrame, conditionPrefix: String): Unit = {
    val exception = intercept[AnalysisException] {
      query.queryExecution.assertAnalyzed()
    }
    assert(exception.getCondition.startsWith(conditionPrefix), exception.getMessage)
  }

  /** Runs `analyze` with the single-pass resolver only and returns the error condition. */
  private def singlePassErrorCondition(analyze: => Unit): String = {
    withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
      intercept[AnalysisException] {
        analyze
      }.getCondition
    }
  }

  testDualRun("Correlated subqueries cannot reference hidden output of the outer operator") {
    // An unresolved plan, as sent by Spark Connect for
    // `df.select("a").filter(exists(... WHERE s.k = b))`.
    checkFixedPointError(
      Dataset.ofRows(
        spark,
        Filter(
          parseExpression("EXISTS (SELECT 1 FROM VALUES (2) AS s(k) WHERE s.k = b)"),
          Project(
            Seq(UnresolvedAttribute("a")),
            parsePlan("SELECT * FROM VALUES (1, 2) AS t(a, b)")
          )
        )
      ),
      conditionPrefix = "UNRESOLVED_COLUMN"
    )
    assert(
      singlePassErrorCondition(
        sql(
          """
            |SELECT a
            |FROM VALUES (1, 2) AS t(a, b)
            |ORDER BY (SELECT max(v) FROM VALUES (2, 3) AS s(k, v) WHERE s.k = t.b)
            |""".stripMargin
        ).queryExecution.assertAnalyzed()
      ).startsWith("UNRESOLVED_COLUMN")
    )
  }

  testDualRun("Project above Filter cannot reference hidden output") {
    // An unresolved plan, as sent by Spark Connect for
    // `df.select("col2").filter($"col1" > 1).select("col1")`.
    val plan = Project(
      Seq(UnresolvedAttribute("col1")),
      Filter(
        GreaterThan(UnresolvedAttribute("col1"), Literal(1)),
        Project(Seq(UnresolvedAttribute("col2")), parsePlan("SELECT * FROM VALUES (1, 2)"))
      )
    )
    assert(
      singlePassErrorCondition(spark.sessionState.executePlan(plan).assertAnalyzed())
        .startsWith("UNRESOLVED_COLUMN")
    )
  }

  testDualRun("Correlated subquery in HAVING references a grouping column") {
    checkAnswer(
      sql(
        """
          |SELECT col1
          |FROM VALUES (1), (2) AS t(col1)
          |GROUP BY col1
          |HAVING EXISTS (SELECT 1 FROM VALUES (1) AS s(c) WHERE s.c = col1)
          |""".stripMargin
      ),
      Row(1)
    )
  }

  testDualRun("Aggregate over a grouping analytics derived table") {
    checkAnswer(
      sql("SELECT sum(a) FROM (SELECT a FROM VALUES (1), (2) AS t(a) GROUP BY CUBE(a)) sq"),
      Row(3)
    )
  }

  testDualRun("HAVING and ORDER BY over grouping analytics") {
    checkAnswer(
      sql(
        """
          |SELECT a, sum(b) AS s
          |FROM VALUES (1, 10), (2, 20) AS t(a, b)
          |GROUP BY ROLLUP(a)
          |HAVING sum(b) > 10
          |ORDER BY a
          |""".stripMargin
      ),
      Seq(Row(null, 30), Row(2, 20))
    )
  }

  testDualRun("ORDER BY above a DataFrame select over an aggregate") {
    // Unresolved plans, as sent by Spark Connect for
    // `df.groupBy("a").agg(...).select("a").orderBy(...)`.
    def sortOverSelectOverAggregate(
        aggregateExpression: NamedExpression,
        sortKey: Expression): DataFrame = {
      Dataset.ofRows(
        spark,
        Sort(
          Seq(SortOrder(sortKey, Ascending)),
          global = true,
          Project(
            Seq(UnresolvedAttribute("a")),
            Aggregate(
              Seq(UnresolvedAttribute("a")),
              Seq(UnresolvedAttribute("a"), aggregateExpression),
              parsePlan("SELECT * FROM VALUES (1, 2, 3) AS t(a, b, c)")
            )
          )
        )
      )
    }

    checkFixedPointError(
      sortOverSelectOverAggregate(
        aggregateExpression = UnresolvedAlias(parseExpression("max(c)")),
        sortKey = parseExpression("sum(b)")
      ),
      conditionPrefix = "UNRESOLVED_COLUMN"
    )
    checkAnswer(
      sortOverSelectOverAggregate(
        aggregateExpression = Alias(parseExpression("sum(b)"), "s")(),
        sortKey = UnresolvedAttribute("s")
      ),
      Row(1)
    )
  }

  testDualRun("ORDER BY and HAVING at the top of a derived table") {
    checkAnswer(
      sql(
        """
          |SELECT *
          |FROM (SELECT col1 AS a, a + 1 AS b FROM VALUES (1) GROUP BY col1 ORDER BY col1) sq
          |""".stripMargin
      ),
      Row(1, 2)
    )
    checkAnswer(
      sql(
        """
          |SELECT *
          |FROM (
          |  SELECT col1 AS alias FROM VALUES ('a') GROUP BY col1 HAVING (SELECT col1 = 'a')
          |) sq
          |""".stripMargin
      ),
      Row("a")
    )
  }

  testDualRun("Set operation casts of struct columns") {
    checkAnswer(
      sql("SELECT named_struct('A', 1) AS s UNION ALL SELECT named_struct('a', 2L) AS s"),
      Seq(Row(Row(1L)), Row(Row(2L)))
    )
    checkAnswer(
      sql(
        """
          |SELECT 1 AS x, named_struct('a', 1) AS s
          |UNION ALL
          |SELECT 2L AS x, named_struct('b', CAST(NULL AS INT)) AS s
          |""".stripMargin
      ),
      Seq(Row(1L, Row(1)), Row(2L, Row(null)))
    )
  }

  testDualRun("LIMIT ALL does not apply to CTE references in subquery expressions") {
    checkAnswer(
      sql(
        """
          |WITH t AS (SELECT 1 AS n)
          |SELECT * FROM VALUES (1) AS v(c) WHERE c IN (SELECT n FROM t) LIMIT ALL
          |""".stripMargin
      ),
      Row(1)
    )
    sql(
      """
        |WITH RECURSIVE t MAX RECURSION LEVEL 100 AS (
        |  SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 60
        |)
        |SELECT (SELECT count(*) FROM t) LIMIT ALL
        |""".stripMargin
    ).queryExecution.assertAnalyzed()
  }

  testDualRun("Positional parameters with legacy parameter substitution") {
    withSQLConf(SQLConf.LEGACY_PARAMETER_SUBSTITUTION_CONSTANTS_ONLY.key -> "true") {
      checkAnswer(spark.sql("SELECT 1", Array(1)), Row(1))
      val explainOutput = spark.sql("EXPLAIN SELECT ?", Array(1)).collect()
      assert(explainOutput.head.getString(0).contains("Project [1 AS"))

      // The guard is skipped here, so only analyze: collecting adds operators that the
      // single-pass resolver does not support.
      withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
        spark.sql("SELECT 1", Array(1)).queryExecution.assertAnalyzed()
        spark.sql("EXPLAIN SELECT ?", Array(1)).queryExecution.assertAnalyzed()
      }
    }
  }

  testDualRun("Recursive CTE self-reference with a different case") {
    checkAnswer(
      sql(
        """
          |WITH RECURSIVE R(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5)
          |SELECT * FROM R
          |""".stripMargin
      ),
      (1 to 5).map(Row(_))
    )
    checkAnswer(
      sql(
        """
          |WITH r AS (SELECT 100 AS n)
          |SELECT * FROM (
          |  WITH RECURSIVE R(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5)
          |  SELECT * FROM R
          |)
          |""".stripMargin
      ),
      (1 to 5).map(Row(_))
    )
  }
}

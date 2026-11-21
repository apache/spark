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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.resolver.{
  ExplicitlyUnsupportedResolverFeature,
  Resolver
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ExplicitlyUnsupportedResolverFeatureSuite extends SharedSparkSession {
  test("Unsupported table types") {
    withTable("csv_table") {
      spark.sql("CREATE TABLE csv_table (col1 INT) USING CSV;").collect()
      checkResolution("SELECT * FROM csv_table;", shouldPass = true)
    }
    withTable("json_table") {
      spark.sql("CREATE TABLE json_table (col1 INT) USING JSON;").collect()
      checkResolution("SELECT * FROM json_table;", shouldPass = true)
    }
    withTable("parquet_table") {
      spark.sql("CREATE TABLE parquet_table (col1 INT) USING PARQUET;").collect()
      checkResolution("SELECT * FROM parquet_table;", shouldPass = true)
    }
    withTable("orc_table") {
      spark.sql("CREATE TABLE orc_table (col1 INT) USING ORC;").collect()
      checkResolution("SELECT * FROM orc_table;", shouldPass = true)
    }
  }

  test("SPARK-57353: HAVING with grouping analytics is unsupported (SPARK-57346)") {
    checkResolution(
      """SELECT a, SUM(b) FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY ROLLUP(a) HAVING SUM(b) > 30""".stripMargin,
      shouldPass = false,
      expectedMessage = Some("HAVING with grouping analytics (SPARK-57346)")
    )
  }

  test("SPARK-57353: ORDER BY with grouping analytics is unsupported (SPARK-57346)") {
    checkResolution(
      """SELECT a, SUM(b) as s FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY CUBE(a) ORDER BY s""".stripMargin,
      shouldPass = false,
      expectedMessage = Some("ORDER BY with grouping analytics (SPARK-57346)")
    )
  }

  test("SPARK-57353: LCA with grouping analytics is unsupported") {
    checkResolution(
      """SELECT a, SUM(b) as total, total + 1
        |FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY CUBE(a)""".stripMargin,
      shouldPass = false,
      expectedMessage = Some("lateral column alias with grouping analytics")
    )
  }

  test("ASOF JOIN MATCH_CONDITION accepts ARRAY operands") {
    withSQLConf(SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true") {
      checkResolution(
        """SELECT t.a, r.a FROM VALUES (ARRAY(1, 3)) AS t(a)
          |ASOF JOIN VALUES (ARRAY(1, 2)) AS r(a) MATCH_CONDITION (t.a >= r.a);""".stripMargin,
        shouldPass = true
      )
      checkResolution(
        """SELECT t.a, r.a FROM VALUES (ARRAY(named_struct('seq', 1))) AS t(a)
          |ASOF JOIN VALUES (ARRAY(named_struct('seq', 2))) AS r(a)
          |MATCH_CONDITION (t.a >= r.a);""".stripMargin,
        shouldPass = true
      )
      checkResolution(
        """SELECT t.k, r.k FROM VALUES (10) AS t(k)
          |ASOF JOIN VALUES (5) AS r(k) MATCH_CONDITION (t.k >= r.k);""".stripMargin,
        shouldPass = true
      )
    }
  }

  test("SPARK-57353: ORDER BY with grouping analytics in subquery does not reject outer sort") {
    // Grouping analytics inside a scalar subquery must not leak hasGroupingAnalytics to the
    // outer query. The outer ORDER BY is unrelated and should resolve successfully.
    checkResolution(
      """SELECT (SELECT SUM(x) FROM VALUES (1),(2) t(x)
        |  GROUP BY GROUPING SETS (())) AS s ORDER BY s""".stripMargin,
      shouldPass = true
    )
  }

  test("SPARK-57353: HAVING with grouping analytics in subquery does not reject outer having") {
    // Same boundary isolation for HAVING: grouping analytics confined to a subquery must not
    // cause the outer HAVING to be rejected.
    checkResolution(
      """SELECT a, (SELECT SUM(x) FROM VALUES (1),(2) t(x)
        |  GROUP BY GROUPING SETS (())) AS s
        |FROM VALUES (1),(2),(3) v(a)
        |GROUP BY a
        |HAVING a > 1""".stripMargin,
      shouldPass = true
    )
  }

  test("SPARK-57353: HAVING with grouping analytics in derived table does not reject outer") {
    // Grouping analytics inside a derived table (SubqueryAlias) must not leak
    // hasGroupingAnalytics to the outer query. The outer HAVING is unrelated.
    checkResolution(
      """SELECT a FROM (SELECT a FROM VALUES (1) t(a) GROUP BY CUBE(a)) s
        |GROUP BY a HAVING a > 0""".stripMargin,
      shouldPass = true
    )
  }

  test("SPARK-57353: ORDER BY with grouping analytics in derived table does not reject outer") {
    // Same boundary isolation for ORDER BY: grouping analytics confined to a derived table
    // must not cause the outer ORDER BY to be rejected.
    checkResolution(
      """SELECT a FROM (SELECT a FROM VALUES (1) t(a) GROUP BY CUBE(a)) s
        |ORDER BY a""".stripMargin,
      shouldPass = true
    )
  }

  test("Implicitly named subquery whose outer references the post-resolution rules change") {
    withTable("n1", "n2") {
      spark.sql("CREATE TABLE n1(c CHAR(2)) USING PARQUET")
      spark.sql("CREATE TABLE n2(c5 CHAR(5), s STRING) USING PARQUET")
      spark.sql("INSERT INTO n1 VALUES ('a')")
      spark.sql("INSERT INTO n2 VALUES ('a', 'a')")
      // `ApplyCharTypePadding` splits the IN and references the outer `n1.c` once per split IN,
      // so the outer references of the subqueries change from (c) to (c, c).
      val condition = "n1.c IN (n2.c5, n2.s)"
      val exists = s"EXISTS (SELECT 1 FROM n2 WHERE $condition)"
      val scalar = s"(SELECT count(*) FROM n2 WHERE $condition)"
      val query = s"SELECT $exists, $scalar FROM n1"
      checkResolution(
        query,
        shouldPass = false,
        expectedMessage = Some("an implicitly named column whose subquery has its outer references")
      )
      checkResolution(s"SELECT $exists AS e, $scalar AS `exists(c)` FROM n1", shouldPass = true)
      checkResolution(s"SELECT c FROM n1 WHERE $exists", shouldPass = true)
      // The post-resolution rules change the plans of these subqueries, but not their outer
      // references or their SQL, so they are kept.
      checkResolution("SELECT EXISTS (SELECT 1 FROM n2 WHERE n1.c = n2.c5) FROM n1",
        shouldPass = true)
      checkResolution("SELECT EXISTS (SELECT 1 FROM n2) FROM n1", shouldPass = true)

      // Dual run falls back to the fixed-point Analyzer, which names the columns after the
      // changed outer references.
      withSQLConf(
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "true",
        SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE.key -> "1.0") {
        val df = spark.sql(query)
        assert(df.columns.toSeq == Seq("exists(c, c)", "scalarsubquery(c, c)"))
        assert(df.collect().toSeq == Seq(Row(true, 1)))

        // An unaliased column of a DataFrame is named after its SQL too.
        val n1 = spark.createDataFrame(spark.table("n1").collectAsList(), spark.table("n1").schema)
        val existsColumn = expr("EXISTS (SELECT 1 FROM n2 WHERE c IN (n2.c5, n2.s))")
        val dfExists = n1.select(existsColumn)
        assert(dfExists.columns.toSeq == Seq("exists(c, c)"))
        assert(dfExists.collect().toSeq == Seq(Row(true)))
        assert(n1.select(existsColumn.as("e")).columns.toSeq == Seq("e"))
      }
    }
  }

  private def checkResolution(
      sqlText: String,
      shouldPass: Boolean = false,
      expectedMessage: Option[String] = None): Unit = {
    val unresolvedPlan = spark.sessionState.sqlParser.parsePlan(sqlText)
    checkPlanResolution(unresolvedPlan, shouldPass, expectedMessage)
  }

  private def checkPlanResolution(
      plan: LogicalPlan,
      shouldPass: Boolean,
      expectedMessage: Option[String]): Unit = {
    val resolver = new Resolver(
      spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions,
      metadataResolverExtensions = spark.sessionState.analyzer.singlePassMetadataResolverExtensions,
      extendedRewriteRules = spark.sessionState.analyzer.singlePassPostHocResolutionRules
    )

    if (shouldPass) {
      resolver.lookupMetadataAndResolve(plan)
    } else {
      val exception = intercept[ExplicitlyUnsupportedResolverFeature] {
        resolver.lookupMetadataAndResolve(plan)
      }
      assert(exception.getMessage.contains(expectedMessage.get))
    }
  }
}

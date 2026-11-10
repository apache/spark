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

import org.apache.spark.sql.catalyst.analysis.resolver.{
  ExplicitlyUnsupportedResolverFeature,
  Resolver
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ExplicitlyUnsupportedResolverFeatureSuite extends SharedSparkSession {
  import testImplicits._

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

  test("SPARK-57353: HAVING with grouping analytics is supported") {
    checkResolution(
      """SELECT a, SUM(b) FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY ROLLUP(a) HAVING SUM(b) > 30""".stripMargin,
      shouldPass = true
    )
  }

  test("SPARK-57353: ORDER BY with grouping analytics is supported") {
    checkResolution(
      """SELECT a, SUM(b) as s FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY CUBE(a) ORDER BY s""".stripMargin,
      shouldPass = true
    )
  }

  test("Aggregate functions in ORDER BY with grouping analytics") {
    checkResolution(
      """SELECT a, b, SUM(b) AS sb FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY GROUPING SETS ((a, b), (a)) ORDER BY SUM(b)""".stripMargin,
      expectedMessage = Some("aggregate functions in ORDER BY over grouping analytics")
    )
    checkResolution(
      """SELECT a, SUM(b) AS s FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY ROLLUP(a) ORDER BY a, s""".stripMargin,
      shouldPass = true
    )
  }

  test("Metadata columns referenced by both an operator and its ancestor") {
    withTable("t") {
      spark.sql("CREATE TABLE t (id INT) USING parquet")
      checkResolution(
        "SELECT _metadata.file_name FROM t WHERE _metadata.row_index < 100",
        expectedMessage =
          Some("metadata columns referenced by both an operator and its ancestor")
      )
      checkResolution(
        "SELECT id FROM t WHERE _metadata.row_index < 100",
        shouldPass = true
      )
    }
  }

  test("Hidden columns referenced across a pipe operator") {
    checkResolution(
      """VALUES (0), (1) lhs(a)
        ||> INNER JOIN VALUES (1), (2) rhs(a) USING (a)
        ||> EXTEND rhs.a AS z
        ||> DROP z
        ||> ORDER BY rhs.a""".stripMargin,
      expectedMessage = Some("hidden columns referenced across a pipe operator")
    )
  }

  test("Nested fields of grouping expressions in HAVING with grouping analytics") {
    checkResolution(
      """SELECT s.a FROM VALUES (named_struct('a', 1)) AS t(s)
        |GROUP BY ROLLUP(s.a) HAVING s.a > 0""".stripMargin,
      expectedMessage = Some("unresolved columns in HAVING over grouping analytics")
    )
  }

  test("NaN pivot values") {
    checkResolution(
      """SELECT * FROM VALUES (1.0D, 10) AS t(k, v)
        |PIVOT (SUM(v) FOR k IN (DOUBLE('NaN') AS x, 1.0D AS y))""".stripMargin,
      expectedMessage = Some("NaN pivot values")
    )
  }

  test("Hidden columns referenced both inside and outside lambda functions") {
    val df = Seq((Seq(1, 2), 1, 2)).toDF("i", "x", "d")
    checkPlanResolution(
      df.select("x").filter("exists(i, x -> x % d == 0)").queryExecution.logical,
      shouldPass = false,
      expectedMessage =
        Some("hidden columns referenced both inside and outside lambda functions")
    )
    checkPlanResolution(
      df.select("x", "d").filter("exists(i, x -> x % d == 0)").queryExecution.logical,
      shouldPass = true,
      expectedMessage = None
    )
  }

  test("SPARK-57353: LCA with grouping analytics is supported") {
    checkResolution(
      """SELECT a, SUM(b) as total, total + 1
        |FROM VALUES (1,10),(1,20),(2,30) AS t(a,b)
        |GROUP BY CUBE(a)""".stripMargin,
      shouldPass = true
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

  gridTest("SET VARIABLE rejects unmatched DEFAULT during resolution")(
    Seq(
      "SET VAR default_target = DEFAULT",
      "SET VAR (default_target) = (VALUES(DEFAULT))",
      "SET VAR (default_target) = (SELECT (SELECT DEFAULT))")) { statement =>
    withSessionVariable("default_target") {
      sql("DECLARE VARIABLE default_target INT DEFAULT 7")
      checkResolution(
        statement,
        expectedMessage = Some("DEFAULT references in SET VARIABLE")
      )
    }
  }

  gridTest("SET VARIABLE resolves real default names")(
    Seq(
      "SET VAR (default_target) = (SELECT default FROM VALUES(11) AS t(default))",
      "SET VAR (default_target) = " +
        "(SELECT (SELECT default) FROM VALUES(12) AS t(default))",
      "SET VAR default_target = default")) { statement =>
    withSessionVariable("default_target", "default") {
      sql("DECLARE VARIABLE default_target INT DEFAULT 7")
      sql("DECLARE VARIABLE default INT DEFAULT 13")
      checkResolution(statement, shouldPass = true)
    }
  }

  test("non-SQL user-defined function falls back") {
    spark.udf.register("scala_udf", (x: Int) => x + 1)
    try {
      checkResolution(
        "SELECT scala_udf(1)",
        expectedMessage = Some("non-SQL function (source: 'scala_udf')")
      )
    } finally {
      spark.sessionState.catalog.dropTempFunction("scala_udf", ignoreIfNotExists = true)
    }
  }

  test("SQL table function falls back") {
    withUserDefinedFunction("sql_tvf" -> false) {
      sql("CREATE FUNCTION sql_tvf(n INT) RETURNS TABLE(id INT) RETURN SELECT n")
      checkResolution(
        "SELECT * FROM sql_tvf(1)",
        expectedMessage = Some("SQL table function")
      )
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
      metadataResolverExtensions = spark.sessionState.analyzer.singlePassMetadataResolverExtensions
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

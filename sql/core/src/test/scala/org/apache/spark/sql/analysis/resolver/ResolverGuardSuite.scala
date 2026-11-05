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
import org.apache.spark.sql.catalyst.SqlScriptingContextManager
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.analysis.resolver.{
  AnalyzerBridgeState,
  ExplicitlyUnsupportedResolverFeature,
  PlanPreprocessor,
  ResolutionGap,
  Resolver,
  ResolverGuard
}
import org.apache.spark.sql.catalyst.catalog.VariableDefinition
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  CreateArray,
  GetMapValue,
  GetStructField,
  JsonArray,
  JsonObjectExpr,
  Literal
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.scripting.SqlScriptingTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class ResolverGuardSuite extends ResolverGuardSuiteBase with SqlScriptingTestUtils {
  import testImplicits._

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

  test("SQL JSON constructor and path functions") {
    checkResolverGuard("""SELECT JSON_ARRAY(1, 'x', true)""")
    checkResolverGuard("""SELECT JSON_ARRAY('[1]' FORMAT JSON)""")
    // Nested clause-free JSON_ARRAY routes as UnresolvedRoutedJsonArray before resolution; the
    // guard gives it the flat call's owner check and admits the unshadowed built-in.
    checkResolverGuard("""SELECT JSON_ARRAY(JSON_ARRAY(1))""")
    checkResolverGuard("""SELECT JSON_VALUE('{"a":1}', '$.a')""")
    checkResolverGuard("""SELECT JSON_VALUE('{"a":1}', '$.a' RETURNING INT)""")
    checkResolverGuard("""SELECT JSON_QUERY('{"a":[1]}', '$.a')""")
    checkResolverGuard("""SELECT JSON_QUERY('{"a":[1]}', '$.a' WITH ARRAY WRAPPER)""")
    checkResolverGuard("""SELECT JSON_EXISTS('{"a":1}', '$.a')""")
    checkResolverGuard("""SELECT JSON_EXISTS('{"a":1}', '$.a' TRUE ON ERROR)""")
    // Clause-bearing and explicit FORMAT JSON forms parse to a stock-owned UnresolvedJsonObject;
    // assert the guard admits it.
    checkResolverGuard("""SELECT JSON_OBJECT('a' VALUE 1 RETURNING STRING)""")
    checkResolverGuard("""SELECT JSON_OBJECT('a' VALUE '[1]' FORMAT JSON)""")
    // Both levels of a value-nested JSON_OBJECT parse to UnresolvedRoutedJsonObject; the guard
    // checks them like the flat call before admitting the built-in.
    checkResolverGuard("""SELECT JSON_OBJECT('a' VALUE JSON_OBJECT('b' VALUE 1))""")
    // A plan built on an analyzed query holds the resolved constructors; assert the guard admits
    // them. They are time-zone aware, so the TimeZoneAwareExpression arm already covers them.
    Seq(
      "SELECT JSON_ARRAY(1 NULL ON NULL) AS j" -> classOf[JsonArray],
      "SELECT JSON_OBJECT('a' VALUE 1 NULL ON NULL) AS j" -> classOf[JsonObjectExpr]
    ).foreach { case (query, cls) =>
      val analyzed = sql(query).queryExecution.analyzed
      assert(analyzed.expressions.exists(_.exists(cls.isInstance)), query)
      checkResolverGuard(Project(analyzed.output, analyzed), unsupportedReason = None)
    }
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

    checkResolverGuard(
      plan,
      unsupportedReason = Some("PLAN_ID_TAG with strict DataFrame column resolution")
    )
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
        throw new ExplicitlyUnsupportedResolverFeature(
          ResolutionGap.ViewBody,
          "ResolverGuardSuite dummy exception"
        )
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

  test("Session-qualified function is rejected") {
    checkResolverGuard(
      "SELECT session.some_func(1)",
      unsupportedReason = Some("session-qualified user-defined function")
    )
  }

  test("Generator functions are unsupported") {
    Seq(
      "explode(array(1, 2, 3))",
      "posexplode(array(1, 2, 3))",
      "inline(array(struct(1, 'a')))"
    ).foreach { call =>
      val name = call.takeWhile(_ != '(')
      checkResolverGuard(s"SELECT $call", unsupportedReason = Some(s"unsupported function $name"))
    }
  }

  test("Higher-order functions") {
    checkResolverGuard("SELECT transform(array(1, 2), x -> x + 1)")
    checkResolverGuard("SELECT filter(array(1, 2), x -> x > 1)")
    checkResolverGuard("SELECT aggregate(array(1, 2), 0, (acc, x) -> acc + x)")
  }

  test("Metadata column star target") {
    withTable("t") {
      sql("CREATE TABLE t (col INT) USING parquet")
      checkResolverGuard("SELECT _metadata.* FROM t")
    }
  }

  test("COALESCE with star expression") {
    checkResolverGuard(
      "SELECT COALESCE(TA.*) FROM VALUES(1, 2) AS TA(c1, c2), VALUES('a', 'b') AS TB(ca, cb)"
    )
  }

  test("CreateArray expression") {
    checkResolverGuard("SELECT array(1, 2, 3)")
    checkResolverGuard("SELECT array()")
    checkResolverGuard("SELECT array(1, 2.0, '3')")

    val plan = Project(
      Seq(UnresolvedAlias(CreateArray(Seq(Literal(1), Literal(2), Literal(3))))),
      OneRowRelation()
    )
    checkResolverGuard(plan = plan, unsupportedReason = None)
  }

  test("DISTRIBUTE BY") {
    checkResolverGuard(
      plan = spark
        .sql("SELECT col1, col2 FROM values(1,2) DISTRIBUTE BY col1")
        .queryExecution
        .logical,
      unsupportedReason = None
    )
  }

  test("GetMapValue") {
    val df = Seq(Map("key" -> 1)).toDF("m").select($"m")
    val mapAttribute = df.queryExecution.analyzed.output.head
    val plan = Project(
      projectList = Seq(Alias(GetMapValue(mapAttribute, Literal("key")), "value")()),
      child = df.queryExecution.logical
    )

    checkResolverGuard(plan = plan, unsupportedReason = None)
  }

  test("GetStructField") {
    val df = Seq((1, 2)).toDF("a", "b").select(struct($"a", $"b").as("s"))
    val structAttribute = df.queryExecution.analyzed.output.head
    val plan = Project(
      projectList = Seq(Alias(GetStructField(structAttribute, 0, Some("a")), "field")()),
      child = df.queryExecution.logical
    )

    checkResolverGuard(plan = plan, unsupportedReason = None)
  }

  test("Grouping analytics") {
    val testCases = Seq(
      "SELECT col1 FROM VALUES(1) GROUP BY CUBE(col1)",
      "SELECT col1 FROM VALUES(1) GROUP BY ROLLUP(col1)",
      "SELECT col1 FROM VALUES(1) GROUP BY GROUPING SETS((col1), ())"
    )

    testCases.foreach(query => checkResolverGuard(query))
  }

  test("IDENTIFIER clause") {
    checkResolverGuard("SELECT IDENTIFIER('c' || '1') FROM VALUES(1) AS T(c1)")
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (c1 INT) USING parquet")
      checkResolverGuard("SELECT * FROM IDENTIFIER('t' || '1')")
    }
  }

  test("IDENTIFIER clause with functions") {
    checkResolverGuard("SELECT IDENTIFIER(concat('c', '1')) FROM VALUES(1) AS T(c1)")
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (c1 INT) USING parquet")
      checkResolverGuard("SELECT * FROM IDENTIFIER(concat('t', '1'))")
    }
  }

  test("IDENTIFIER clause with variable is supported") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 (c1 INT) USING csv")
      withSessionVariable("tbl_name") {
        spark.sql("DECLARE tbl_name STRING DEFAULT 't1'")
        checkResolverGuard("SELECT * FROM IDENTIFIER(tbl_name)")
      }
    }
    withSessionVariable("col_name") {
      spark.sql("DECLARE col_name STRING DEFAULT 'c1'")
      checkResolverGuard("SELECT IDENTIFIER(col_name) FROM VALUES(1) AS T(c1)")
    }
  }

  test("IDENTIFIER expressions use standard function support checks") {
    checkResolverGuardOnly("SELECT IDENTIFIER(max('c1')) FROM VALUES(1) AS T(c1)")
    checkResolverGuardOnly("SELECT IDENTIFIER((SELECT 'c1')) FROM VALUES(1) AS T(c1)")
    checkResolverGuardOnly(
      "SELECT IDENTIFIER(WINDOW('c1', '1 minute')) FROM VALUES(1) AS T(c1)",
      unsupportedReason = Some("unsupported function window")
    )
    withSqlFunction("identifier_name") {
      sql("CREATE FUNCTION identifier_name() RETURNS STRING RETURN 'c1'")
      checkResolverGuardOnly(
        "SELECT IDENTIFIER(identifier_name()) FROM VALUES(1) AS T(c1)",
        unsupportedReason = Some("non-builtin function")
      )
    }
  }

  test("IDENTIFIER subquery remains guard-supported") {
    checkResolverGuardOnly("SELECT * FROM IDENTIFIER((SELECT 't'))")
  }

  test("JSON_TABLE") {
    // JSON_TABLE is parsed into a [[Generate]] operator, which is not supported yet.
    checkResolverGuard(
      "SELECT * FROM JSON_TABLE('{\"items\":[1]}', '$.items[*]' " +
      "COLUMNS (value INT PATH '$'))",
      Some("class org.apache.spark.sql.catalyst.plans.logical.Generate operator resolution")
    )
  }

  test("LATERAL JOIN") {
    val lateralJoinQuery =
      "SELECT * FROM (VALUES (1, 2)) AS t1(c1, c2) JOIN LATERAL (SELECT c1 + c2 AS c3) ON c2 = c3"
    checkResolverGuard(lateralJoinQuery)
  }

  test("LIMIT ALL") {
    checkResolverGuard(
      """
        WITH RECURSIVE r AS (
          SELECT 0 AS level
          UNION ALL
          SELECT level + 1 FROM r WHERE level < 9
        )
        SELECT * FROM r LIMIT ALL;
        """
    )
  }

  test("Lambda variable") {
    val testCases = Seq(
      "SELECT array_sort(array(2, 1), (p1, p2) -> IF(p1 > p2, 1, 0))",
      "SELECT transform(array(2, 1), x -> x * 2)",
      "SELECT filter(array(2, 1), x -> x > 0)"
    )

    testCases.foreach(query => checkResolverGuard(query))
  }

  test("Metadata column resolution") {
    withTable("t") {
      spark.sql("CREATE TABLE t (col INT) USING csv")
      checkResolverGuard("SELECT _metadata FROM t")
    }
  }

  test("NamePlaceholder") {
    val query = "SELECT struct(*) FROM VALUES (1)"
    checkResolverGuard(query)
  }

  test("Named argument expression") {
    val query = "SELECT mask('ABCabc123', lowerChar => 'q')"
    checkResolverGuard(query)
  }

  test("Natural join with multiple resolution passes") {
    val df1 = Seq(1 -> "a").toDF("k", "v").as("left")
    val df2 = Seq(1 -> "b").toDF("k", "v").as("right")
    val dfQuery = df1
      .join(df2, "k", "outer")
      .withColumn("left_all", struct($"left.k"))
      .withColumn("right_all", struct($"right.k"))
    checkResolverGuard(
      plan = dfQuery.queryExecution.logical,
      unsupportedReason = Some("NaturalJoin second resolution")
    )
  }

  test("PLAN_ID_TAG with set operation") {
    val child = spark.sessionState.sqlParser.parsePlan(
      "SELECT col1 FROM VALUES (1) UNION ALL SELECT col1 FROM VALUES (2)"
    )
    val planId: Long = 0
    child.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)
    val attribute = UnresolvedAttribute("col1")
    attribute.setTagValue(LogicalPlan.PLAN_ID_TAG, planId)
    val plan = Project(Seq(attribute), child)

    withSQLConf(SQLConf.STRICT_DATAFRAME_COLUMN_RESOLUTION.key -> "false") {
      checkResolverGuard(plan, unsupportedReason = None)
    }
  }

  test("Pivot") {
    checkResolverGuard(
      plan = spark
        .sql("SELECT * FROM VALUES (2024,1) PIVOT (sum(col1) FOR col2 IN (1 AS q1))")
        .queryExecution
        .logical,
      unsupportedReason = None
    )
  }

  test("Unpivot") {
    checkResolverGuard(
      plan = spark
        .sql("SELECT * FROM VALUES (2024,1) UNPIVOT (col1 FOR col2 IN (col1 AS `Jan-Mar`))")
        .queryExecution
        .logical,
      unsupportedReason = None
    )
  }

  test("Recursive CTE") {
    val query =
      """
        WITH RECURSIVE r AS (
          SELECT 0 AS level
          UNION ALL
          SELECT level + 1 FROM r WHERE level < 9
        )
        SELECT * FROM r;
      """

    checkResolverGuard(query)
  }

  test("SQL pipe aggregate with ordinals") {
    checkResolverGuard("values (3, 4) as tab(x, y) |> aggregate sum(y) group by 1")
  }

  test("SQL pipe syntax") {
    withTable("t") {
      spark.sql("CREATE TABLE t (x INT, y STRING) USING CSV")
      checkResolverGuard(
        """
          | FROM t AS t_alias
          | |> SELECT t_alias.x as tx, t_alias.y as ty
          | |> WHERE ty = 'def'
          | |> SELECT tx
          |""".stripMargin
      )
    }
  }

  test("Star target with columns") {
    val df1 = Seq(1 -> "a").toDF("k", "v").as("left")
    val df2 = df1.withColumn("left_duplicates", struct($"left.*"))
    checkResolverGuard(plan = df2.queryExecution.logical, None)
  }

  test("Star target with except") {
    checkResolverGuard("SELECT * EXCEPT(col2) FROM VALUES(1,2)")
  }

  test("Star with metadata target in aggregate") {
    withTable("t") {
      spark.sql("CREATE TABLE t (col INT) USING csv")
      checkResolverGuard("SELECT _metadata.* FROM t GROUP BY ALL")
    }
  }

  test("Streaming relation") {
    withTable("t1") {
      spark.sql("CREATE TABLE t1 USING parquet AS SELECT * FROM VALUES (1)")

      checkResolverGuard(
        "SELECT * FROM STREAM t1",
        unsupportedReason = Some(
          "class org.apache.spark.sql.catalyst.analysis.NamedStreamingRelation operator resolution"
        )
      )
    }
  }

  test("TVFs resolution") {
    checkResolverGuard("SELECT * FROM range(10)")
    checkResolverGuard(
      "SELECT * FROM python_worker_logs()",
      unsupportedReason = Some("unsupported table valued function python_worker_logs")
    )

    val generatorTableValuedFunctions = Seq(
      "collations" -> "SELECT * FROM collations()",
      "explode" -> "SELECT * FROM explode(array(1,2,3))",
      "explode_outer" -> "SELECT * FROM explode_outer(array())",
      "inline" -> "SELECT * FROM inline(array(struct(1, 'a')))",
      "inline_outer" -> "SELECT * FROM inline_outer(array(struct(1, 'a')))",
      "json_tuple" -> "SELECT * FROM json_tuple('{\"a\":1}', 'a')",
      "posexplode" -> "SELECT * FROM posexplode(array(1,2,3))",
      "posexplode_outer" -> "SELECT * FROM posexplode_outer(array())",
      "sql_keywords" -> "SELECT * FROM sql_keywords()",
      "stack" -> "SELECT * FROM stack(2, 1, 2, 3)",
      "variant_explode" -> "SELECT * FROM variant_explode(parse_json('[1,2,3]'))",
      "variant_explode_outer" -> "SELECT * FROM variant_explode_outer(parse_json('[]'))"
    )
    generatorTableValuedFunctions.foreach { case (name, query) =>
      checkResolverGuard(
        query,
        unsupportedReason = Some(s"generator table valued function $name")
      )
    }
  }

  test("table valued function with alias") {
    checkResolverGuard("SELECT * FROM range(10) AS my_table(r)")
  }

  test("multi-part table valued function name") {
    checkResolverGuard(
      "SELECT * FROM catalog.schema.range(10)",
      unsupportedReason = Some("multi-part table valued function name")
    )
  }

  test("streaming table valued function") {
    // Streaming TVFs are wrapped in NamedStreamingRelation for source naming support,
    // so the resolver guard hits that wrapper first before the underlying TVF.
    checkResolverGuard(
      "SELECT * FROM STREAM range(10)",
      unsupportedReason = Some(
        "class org.apache.spark.sql.catalyst.analysis.NamedStreamingRelation operator resolution"
      )
    )
  }

  test("user-defined table valued function") {
    withSqlFunction("custom_tvf") {
      sql("CREATE FUNCTION custom_tvf() RETURNS TABLE(x INT) RETURN SELECT 1 AS x")
      checkResolverGuard(
        "SELECT * FROM custom_tvf()",
        unsupportedReason = Some("UDTF")
      )
    }
  }

  test("Temp variables") {
    checkResolverGuard("SELECT 1")
    withTempVariable("temp_variable") {
      checkResolverGuard("SELECT temp_variable")
      checkResolverGuard("SELECT 1")
    }
  }

  test("Scripting variables") {
    val compoundBody = spark.sessionState.sqlParser.parsePlan("""
      BEGIN
      SELECT 1;
      END
      """).asInstanceOf[CompoundBody]
    withSqlScriptingContextManager(spark, compoundBody) { _ =>
      checkResolverGuard("SELECT 1")
      withScriptingVariable("scripting_variable", "value") {
        checkResolverGuard("SELECT scripting_variable")
        checkResolverGuard("SELECT 1")
      }
    }
  }

  test("SET VARIABLE") {
    withSessionVariable("set_variable_target") {
      sql("DECLARE VARIABLE set_variable_target INT DEFAULT 0")
      checkResolverGuard("SET VAR set_variable_target = 1")
      checkResolverGuard("SET VAR (set_variable_target) = (SELECT col1 FROM VALUES (2))")
    }
  }

  test("SET VARIABLE scripting targets") {
    val compoundBody = spark.sessionState.sqlParser.parsePlan("""
      BEGIN
      SELECT 1;
      END
      """).asInstanceOf[CompoundBody]
    withSqlScriptingContextManager(spark, compoundBody) { _ =>
      withScriptingVariable("set_variable_target", "value") {
        checkResolverGuardOnly("SET VAR set_variable_target = 'other_value'")
      }
    }
  }

  gridTest("SET VARIABLE with an unknown target")(
    Seq(
      "SET VAR missing_set_variable_target = 1",
      "SET VAR missing_set_variable_target = source_variable")) { statement =>
    checkResolverGuardOnly(statement)
  }

  test("SET VARIABLE rejects nested raw and preprocessed CTE sources") {
    withSessionVariable("set_variable_target") {
      sql("DECLARE VARIABLE set_variable_target INT")
      val parsedPlan = spark.sessionState.sqlParser.parsePlan(
        "SET VAR (set_variable_target) = " +
          "(SELECT value FROM " +
          "(WITH source(value) AS (VALUES(1)) SELECT value FROM source) nested_source)")
      val preprocessedPlan = new PlanPreprocessor(
        resolver = new Resolver(spark.sessionState.catalogManager),
        resolveExpressionIdentifiersInRootScope = true).substitutePlan(parsedPlan)
      val unsupportedReason = Some("SET VARIABLE with a CTE source")

      checkResolverGuard(parsedPlan, unsupportedReason)
      checkResolverGuard(preprocessedPlan, unsupportedReason)
    }
  }

  test("EXECUTE IMMEDIATE is unsupported") {
    val unsupportedReason = Some(
      "class org.apache.spark.sql.catalyst.analysis.UnresolvedExecuteImmediate " +
      "operator resolution"
    )
    checkResolverGuard("EXECUTE IMMEDIATE 'SELECT 1'", unsupportedReason)
    withSessionVariable("execute_immediate_target") {
      sql("DECLARE VARIABLE execute_immediate_target INT")
      checkResolverGuard(
        "EXECUTE IMMEDIATE 'SELECT 1' INTO execute_immediate_target",
        unsupportedReason
      )
    }
  }

  test("Positional parameters") {
    checkResolverGuard(
      plan = spark.sql("SELECT ?", Array(1)).queryExecution.logical,
      unsupportedReason = None
    )
    withSQLConf(SQLConf.LEGACY_PARAMETER_SUBSTITUTION_CONSTANTS_ONLY.key -> "true") {
      checkResolverGuard(
        plan = spark.sql("SELECT ?", Array(1)).queryExecution.logical,
        unsupportedReason = Some(
          "class org.apache.spark.sql.catalyst.analysis.PosParameter expression resolution"
        )
      )
    }
  }

  test("BIN BY") {
    withSQLConf(SQLConf.BIN_BY_ENABLED.key -> "true") {
      checkResolverGuard(
        plan = spark
          .sql(
            """SELECT * FROM VALUES
              |  (TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 01:00:00', 1.0D)
              |  AS t(ts_start, ts_end, value)
              |BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE
              |  DISTRIBUTE UNIFORM (value))
              |""".stripMargin)
          .queryExecution
          .logical,
        unsupportedReason = None
      )
    }
  }

  test("UnresolvedExtractValue") {
    val query = "SELECT 1 AS a FROM VALUES(ARRAY(1)) GROUP BY col1 ORDER BY col1[0]"
    checkResolverGuard(query)
  }

  test("UnresolvedHint") {
    ResolverGuard.BUILTIN_HINT_NAMES.foreach { hintName =>
      checkResolverGuardOnly(s"SELECT /*+ $hintName */ col1 FROM VALUES(1, 2)")
    }
    checkResolverGuardOnly(
      "SELECT /*+ UNKNOWN_HINT */ col1 FROM VALUES(1, 2)",
      unsupportedReason = Some("user defined hint")
    )
  }

  test("prioritizeOrdinalResolutionInSort disabled is unsupported") {
    withSQLConf("spark.sql.prioritizeOrdinalResolutionInSort.enabled" -> "false") {
      checkResolverGuard(
        "SELECT 1",
        unsupportedReason = Some("configuration: prioritizeOrdinalResolutionInSortDisabled")
      )
    }
  }

  test("persistentCatalogFirst is unsupported") {
    withSQLConf("spark.sql.legacy.persistentCatalogFirst" -> "true") {
      checkResolverGuard(
        "SELECT 1",
        unsupportedReason = Some("configuration: persistentCatalogFirst")
      )
    }
  }

  gridTest("ASOF JOIN")(Seq(true, false)) { enabled =>
    val asOfJoinQuery =
      "SELECT t.symbol, q.bid_price FROM " +
      "VALUES (TIMESTAMP '2026-06-29 10:00:05', 'AAPL') AS t(trade_time, symbol) " +
      "ASOF JOIN VALUES (TIMESTAMP '2026-06-29 10:00:00', 'AAPL', 180.10) " +
      "AS q(quote_time, symbol, bid_price) " +
      "MATCH_CONDITION (t.trade_time >= q.quote_time) ON t.symbol = q.symbol"
    val expectedReason = if (enabled) {
      None
    } else {
      Some("class org.apache.spark.sql.catalyst.plans.logical.AsOfJoin operator resolution")
    }
    withSQLConf(
      SQLConf.SQL_ASOF_JOIN_ENABLED.key -> "true",
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key ->
      enabled.toString
    ) {
      checkResolverGuard(asOfJoinQuery, expectedReason)
    }
  }

}

trait ResolverGuardSuiteBase extends SharedSparkSession {
  /**
   * Verifies the [[ResolverGuard]] result without attempting full resolution. Use when full
   * resolution requires environment setup not present in this suite.
   */
  protected def checkResolverGuardOnly(
      query: String,
      unsupportedReason: Option[String] = None): Unit = {
    val plan = spark.sessionState.sqlParser.parsePlan(query)
    val resolverGuard = new ResolverGuard(catalogManager = spark.sessionState.catalogManager)
    assert(resolverGuard.apply(plan).planUnsupportedReason == unsupportedReason)
  }

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

  protected def withScriptingVariable[R](name: String, value: String)(body: => R): R = {
    val variableManager = SqlScriptingContextManager.get().get.getVariableManager.get

    variableManager.create(
      nameParts = Seq(name),
      varDef = VariableDefinition(Identifier.of(Array(name), name), value, Literal(value)),
      overrideIfExists = false
    )

    body
  }

  protected def withTempVariable[R](name: String)(body: => R): R = {
    spark.sql(s"DECLARE $name = 1;")
    try {
      body
    } finally {
      spark.sql(s"DROP TEMPORARY VARIABLE $name;")
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

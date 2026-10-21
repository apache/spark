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

import java.util.UUID

import org.apache.spark.{SparkRuntimeException, TaskContext}
import org.apache.spark.sql.IntegratedUDFTestUtils.{registerTestUDF, shouldTestPythonUDFs, TestPythonUDF}
import org.apache.spark.sql.execution.{ExpandExec, LocalTableScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

/**
 * End-to-end tests for VALUES lists with expressions that are not foldable, which are evaluated
 * by an `Expand` over a `OneRowRelation` when the query runs.
 */
class InlineTableNonFoldableExpressionsSuite
  extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private def expandExecs(df: DataFrame): Seq[ExpandExec] =
    collect(df.queryExecution.executedPlan) { case e: ExpandExec => e }

  private def contextOf(query: String, fragment: String): ExpectedContext = {
    val start = query.indexOf(fragment)
    assert(start >= 0, s"'$fragment' is not part of '$query'")
    ExpectedContext(fragment, start, start + fragment.length - 1)
  }

  test("non-foldable expressions are evaluated on executors") {
    withUserDefinedFunction("in_task" -> true) {
      spark.udf.register("in_task", udf(() => TaskContext.get() != null).asNondeterministic())
      val df = sql("SELECT * FROM VALUES (1, in_task()), (2, in_task()) AS t(id, in_task)")
      checkAnswer(df, Seq(Row(1, true), Row(2, true)))
      val execs = expandExecs(df)
      assert(execs.size == 1)
      // All the rows are evaluated in a single task.
      assert(execs.head.execute().getNumPartitions == 1)
    }
  }

  test("VALUES lists with only foldable expressions still become local relations") {
    val df = sql("SELECT * FROM VALUES (1, 'a'), (2 + 1, upper('b')) AS t(id, s)")
    checkAnswer(df, Seq(Row(1, "a"), Row(3, "B")))
    assert(expandExecs(df).isEmpty)
    assert(collect(df.queryExecution.executedPlan) { case l: LocalTableScanExec => l }.size == 1)
  }

  test("VALUES lists that were supported before are evaluated during analysis as before") {
    withUserDefinedFunction("on_driver" -> true) {
      spark.udf.register("on_driver", udf((s: String) => s != null && TaskContext.get() == null))
      val queries = Seq(
        "SELECT * FROM VALUES (1, 'a'), (2 + 1, upper('b')) AS t(a, b)",
        "SELECT * FROM VALUES (current_date(), 1 + 1) AS t(a, b)",
        // The UDF is not foldable, but it was supported because its input is a CURRENT_LIKE
        // expression. It is evaluated on the driver at the end of the analysis phase.
        "SELECT * FROM VALUES (on_driver(current_user())) AS t(a)")
      queries.foreach { query =>
        val df = sql(query)
        val legacy = withSQLConf(
            SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED.key -> "false") {
          sql(query)
        }
        comparePlans(
          df.queryExecution.analyzed, legacy.queryExecution.analyzed, checkAnalysis = false)
        checkAnswer(df, legacy)
        assert(expandExecs(df).isEmpty)
      }
      checkAnswer(sql(queries.last), Row(true))
    }

    // The expressions of inline tables with CURRENT_LIKE expressions are evaluated at the end of
    // the analysis phase, so evaluation errors are reported when the query is optimized.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val df = sql("SELECT * FROM VALUES (current_date(), 1 / 0) AS t(a, b)")
      val e = intercept[AnalysisException](df.collect())
      assert(e.getCondition == "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION")
    }
  }

  test("nondeterministic expressions") {
    // Every expression has its own seed.
    val uuids = sql("SELECT * FROM VALUES (uuid()), (uuid()), (uuid())").collect()
    assert(uuids.map(_.getString(0)).distinct.length == 3)

    // Each row is evaluated like the only row of a one-row relation.
    checkAnswer(
      sql("SELECT * FROM VALUES (rand(5)), (rand(5))"),
      sql("SELECT rand(5) UNION ALL SELECT rand(5)"))

    // The seeds are chosen during analysis, so executing the plan again returns the same values.
    val df = sql("SELECT * FROM VALUES (rand(), 1), (rand(), 2)")
    checkAnswer(df, df.collect().toSeq)
  }

  test("equal nondeterministic expressions are evaluated separately for every row") {
    withUserDefinedFunction("random_uuid" -> true) {
      spark.udf.register("random_uuid", udf(() => UUID.randomUUID().toString).asNondeterministic())
      Seq(true, false).foreach { codegen =>
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
          val rows = sql("SELECT * FROM VALUES (random_uuid()), (random_uuid())").collect()
          assert(rows.map(_.getString(0)).distinct.length == 2)
        }
      }
    }
  }

  test("CURRENT_LIKE expressions are evaluated once per query") {
    val df = sql(
      """SELECT count(DISTINCT ts), count(DISTINCT r)
        |FROM VALUES (current_timestamp(), rand()), (current_timestamp(), rand()) AS t(ts, r)
        |""".stripMargin)
    checkAnswer(df, Row(1, 2))
  }

  test("type coercion and nullability") {
    val df = sql("SELECT * FROM VALUES (1, rand()), (2L, NULL) AS t(a, b)")
    assert(df.schema == StructType(Seq(
      StructField("a", LongType, nullable = false),
      StructField("b", DoubleType, nullable = true))))
    checkAnswer(df.select($"a", $"b".isNull), Seq(Row(1L, false), Row(2L, true)))
  }

  test("uncorrelated scalar subqueries") {
    checkAnswer(sql("SELECT * FROM VALUES ((SELECT 1) + (SELECT 2), 'a')"), Row(3, "a"))
    withTable("t") {
      sql("CREATE TABLE t(id INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2)")
      sql("INSERT INTO t VALUES ((SELECT max(id) FROM t) + 1)")
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("uncorrelated scalar subqueries with CURRENT_LIKE expressions") {
    // The CURRENT_LIKE expressions inside and next to the subqueries share the value of the rest
    // of the query.
    checkAnswer(
      sql("SELECT d = current_date() FROM VALUES ((SELECT current_date())) AS t(d)"),
      Row(true))
    checkAnswer(
      sql(
        """SELECT d = date_add(current_date(), 1)
          |FROM VALUES (date_add(current_date(), (SELECT 1))) AS t(d)
          |""".stripMargin),
      Row(true))
    withTable("t") {
      sql("CREATE TABLE t(id INT, ts TIMESTAMP) USING parquet")
      sql("INSERT INTO t VALUES (1, TIMESTAMP'2000-01-01 00:00:00')")
      sql(
        """INSERT INTO t VALUES
          |  ((SELECT max(id) FROM t WHERE ts < current_timestamp()) + 1, current_timestamp())
          |""".stripMargin)
      checkAnswer(sql("SELECT id FROM t"), Seq(Row(1), Row(2)))
    }
  }

  test("correlated references and subqueries other than scalar subqueries are rejected") {
    withTempView("t") {
      sql("CREATE TEMPORARY VIEW t AS SELECT * FROM VALUES (1) AS t(a)")
      val query = "SELECT * FROM t, LATERAL (VALUES (t.a, rand())) AS v(b, r)"
      checkError(
        exception = intercept[AnalysisException](sql(query)),
        condition = "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE",
        parameters = Map("expr" -> "\"outer(t.a)\""),
        context = contextOf(query, "t.a"))
    }
    Seq(
      "SELECT * FROM VALUES (EXISTS (SELECT 1), rand())",
      "SELECT * FROM VALUES (1 IN (SELECT 1), rand())",
      // CURRENT_LIKE expressions don't make them evaluable during analysis.
      "SELECT * FROM VALUES (EXISTS (SELECT current_date()))",
      "SELECT * FROM VALUES (current_date() IN (SELECT current_date()))").foreach { query =>
      val e = intercept[AnalysisException](sql(query))
      assert(e.getCondition == "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE")
    }
  }

  test("aggregate, window and generator functions are rejected") {
    Seq(
      "SELECT * FROM VALUES (count(1), rand())",
      "SELECT * FROM VALUES (row_number() OVER (ORDER BY 1), rand())",
      "SELECT * FROM VALUES (explode(array(1, 2)), 1)").foreach { query =>
      val e = intercept[AnalysisException](sql(query))
      assert(e.getCondition == "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE")
    }
  }

  test("SQL UDFs are rejected") {
    withUserDefinedFunction("plus_one" -> true) {
      sql("CREATE TEMPORARY FUNCTION plus_one(x INT) RETURNS INT RETURN x + 1")
      val e = intercept[AnalysisException](sql("SELECT * FROM VALUES (plus_one(1))"))
      assert(e.getCondition == "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE")
    }
  }

  test("Python UDFs are rejected") {
    assume(shouldTestPythonUDFs)
    withUserDefinedFunction("py_udf" -> true) {
      registerTestUDF(TestPythonUDF(name = "py_udf"), spark)
      val e = intercept[AnalysisException](sql("SELECT * FROM VALUES (py_udf(1))"))
      assert(e.getCondition == "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE")
    }
  }

  test("evaluation errors are raised when the query runs") {
    val df = sql("SELECT * FROM VALUES (1, raise_error('boom'))")
    checkError(
      exception = intercept[SparkRuntimeException](df.collect()),
      condition = "USER_RAISED_EXCEPTION",
      parameters = Map("errorMessage" -> "boom"))
  }

  test("the expressions of columns that are not read are not evaluated") {
    // Like the expressions of a SELECT list, the optimizer removes them.
    checkAnswer(sql("SELECT a FROM VALUES (1, raise_error('boom')) AS t(a, b)"), Row(1))
  }

  test("INSERT INTO ... VALUES with non-foldable expressions and DEFAULT") {
    withTable("t") {
      sql("CREATE TABLE t(id INT, s STRING DEFAULT 'x', u STRING) USING parquet")
      sql("INSERT INTO t VALUES (1, DEFAULT, uuid()), (2, 'y', uuid())")
      checkAnswer(sql("SELECT id, s, length(u) FROM t"), Seq(Row(1, "x", 36), Row(2, "y", 36)))
      checkAnswer(sql("SELECT count(DISTINCT u) FROM t"), Row(2))
    }
  }

  test("self-join of an inline table with non-foldable expressions") {
    val df = sql("SELECT * FROM VALUES (1, rand()), (2, rand()) AS t(id, r)")
    val joined = df.as("a").join(df.as("b"), $"a.id" === $"b.id")
    // Both sides are the same plan, so they produce the same values.
    checkAnswer(joined.select($"a.id", $"a.r" === $"b.r"), Seq(Row(1, true), Row(2, true)))
  }

  test("whole-stage codegen and adaptive query execution") {
    Seq(true, false).foreach { codegen =>
      Seq(true, false).foreach { aqe =>
        withSQLConf(
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          val df = sql(
            """SELECT a + 1, b IS NOT NULL, c
              |FROM VALUES (1, rand(), (SELECT 'x')), (2, rand(), 'y') AS t(a, b, c)
              |""".stripMargin)
          checkAnswer(df, Seq(Row(2, true, "x"), Row(3, true, "y")))
          assert(expandExecs(df).size == 1)
        }
      }
    }
  }

  test("SQL metrics") {
    val df = sql("SELECT * FROM VALUES (rand()), (rand()), (rand())")
    df.collect()
    assert(expandExecs(df).head.metrics("numOutputRows").value == 3)
  }

  test("large inline table with non-foldable expressions") {
    val rows = (1 to 1000).map(i => s"($i, rand())").mkString(", ")
    checkAnswer(
      sql(s"SELECT count(*), count(DISTINCT a), count(DISTINCT b) FROM VALUES $rows AS t(a, b)"),
      Row(1000, 1000, 1000))
  }

  test("non-foldable expressions are rejected if the feature is disabled") {
    withSQLConf(SQLConf.INLINE_TABLE_NON_FOLDABLE_EXPRESSIONS_ENABLED.key -> "false") {
      val query = "SELECT * FROM VALUES (1, rand(5))"
      checkError(
        exception = intercept[AnalysisException](sql(query)),
        condition = "INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE",
        parameters = Map("expr" -> "\"rand(5)\""),
        context = contextOf(query, "rand(5)"))

      Seq("(SELECT 1)", "(SELECT current_date())").foreach { subquery =>
        val subqueryQuery = s"SELECT * FROM VALUES ($subquery)"
        checkError(
          exception = intercept[AnalysisException](sql(subqueryQuery)),
          condition = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES",
          parameters = Map.empty,
          context = contextOf(subqueryQuery, s"VALUES ($subquery)"))
      }
    }
  }
}

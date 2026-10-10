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

import org.scalatest.Tag

import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Differential tests for `RewriteSlidingFramesAsPrefixDifferences`: the original query (config
 * off) and the rewritten query (config on) must return identical results, and the rewrite must
 * not add exchanges or sorts.
 *
 * The rule only applies with `spark.sql.ansi.enabled = false`, which these tests set together
 * with `spark.sql.optimizer.windowPrefixRewrite.enabled`.
 */
class WindowPrefixRewriteQuerySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private val windowPrefixRewrite = SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key

  private def run(enabled: Boolean)(sqlText: String): DataFrame =
    withSQLConf(SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> enabled.toString) {
      spark.sql(sqlText)
    }

  /** Runs the query in both modes and compares the full projection, ignoring row order. */
  private def assertIdentical(sqlText: String): Unit = {
    val original = run(false)(sqlText)
    val rewritten = run(true)(sqlText)
    checkAnswer(original, rewritten.collect())
    assert(rewritten.schema === original.schema)
  }

  private def collectWindows(plan: SparkPlan): Seq[WindowExec] = plan.collect {
    case w: WindowExec => w
  }

  /** The query must actually be rewritten when the config is on. */
  private def assertRewritten(sqlText: String): Unit =
    withSQLConf(
      SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      assert(collectWindows(spark.sql(sqlText).queryExecution.executedPlan).size === 2)
    }

  /** The query must not be rewritten when the config is on. */
  private def assertNotRewritten(sqlText: String): Unit =
    withSQLConf(
      SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      assert(collectWindows(spark.sql(sqlText).queryExecution.executedPlan).size === 1)
    }

  private def setupView(sqlText: String): Unit = spark.sql(sqlText).createOrReplaceTempView("t")

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
      implicit pos: org.scalactic.source.Position): Unit =
    super.test(testName, testTags: _*)(
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false")(testFun))(pos)

  test("rewrite produces identical results for multiple widths, measures and nullability") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 10 AS k, id AS o,
          |  CASE WHEN id % 5 = 0 THEN CAST(NULL AS BIGINT) ELSE (id * 37) % 1000 END AS vn,
          |  (id * 13) % 500 AS v
          |FROM range(0, 500)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, vn, v,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s2,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS s6,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) AS s179
          |FROM t
        """.stripMargin)
      // The query is rewritten: one window for the running prefixes, one for the differences.
      assertRewritten(
        "SELECT sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)" +
          " FROM t")
    }
  }

  test("duplicate (key, order) rows") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 5 AS k, id % 3 AS o, (id * 7) % 100 AS v
          |FROM range(0, 100)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s2,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s4
          |FROM t
        """.stripMargin)
    }
  }

  test("short partitions: fewer rows than the frame width") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 20 AS k, id % 20 AS o, id AS v
          |FROM range(0, 200)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29
          |FROM t
        """.stripMargin)
    }
  }

  test("all-NULL measures within a partition return NULL, not 0") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 2 AS k, id AS o,
          |  CASE WHEN id % 10 = 9 THEN id ELSE CAST(NULL AS BIGINT) END AS vn
          |FROM range(0, 50)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, vn,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS s3,
          |  sum(vn) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS s0
          |FROM t
        """.stripMargin)
    }
  }

  test("narrow integral measures (tinyint, smallint, int) are rewritten identically") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 10 AS k, id AS o,
          |  CAST(id % 7 AS TINYINT) AS b,
          |  CASE WHEN id % 5 = 0 THEN CAST(NULL AS SMALLINT) ELSE CAST(id % 13 AS SMALLINT) END
          |    AS ns,
          |  CAST(id % 101 AS INT) AS i
          |FROM range(0, 300)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, b, ns, i,
          |  sum(b) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sb,
          |  sum(ns) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS sns,
          |  sum(i) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS si
          |FROM t
        """.stripMargin)
      assertRewritten(
        "SELECT sum(i) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 29 PRECEDING AND CURRENT" +
          " ROW) FROM t")
    }
  }

  test("window without ORDER BY is not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS v FROM range(0, 60)")
      val sql = """
        |SELECT k, v,
        |  sum(v) OVER (PARTITION BY k ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS s49
        |FROM t
      """.stripMargin
      assertIdentical(sql)
      assertNotRewritten(sql)
    }
  }

  test("min, max, avg, float sums, counts and try_sum are not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, id AS v, id % 7 AS f FROM range(0, 90)")
      assertNotRewritten(
        """
          |SELECT k, o, v, f,
          |  min(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS mn,
          |  max(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS mx
          |FROM t
        """.stripMargin)
      assertNotRewritten(
        """
          |SELECT k, o, v,
          |  sum(v * 0.5) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS ds
          |FROM t
        """.stripMargin)
      assertNotRewritten(
        """
          |SELECT k, o, v,
          |  avg(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS av
          |FROM t
        """.stripMargin)
      assertNotRewritten(
        """
          |SELECT k, o, v,
          |  count(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS cn
          |FROM t
        """.stripMargin)
      assertNotRewritten(
        """
          |SELECT k, o, v,
          |  try_sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS ts
          |FROM t
        """.stripMargin)
      // Mixed group: sum qualifies, the other members keep their frames.
      val mixed =
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o
          |    ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS ss,
          |  count(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW)
          |    AS cn
          |FROM t
        """.stripMargin
      assertIdentical(mixed)
      assertRewritten(mixed)
    }
  }

  test("decimal sums are not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, CAST(id AS DECIMAL(10, 2)) AS v FROM range(0, 90)")
      assertIdentical(
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS ds
          |FROM t
        """.stripMargin)
      assertNotRewritten(
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS ds
          |FROM t
        """.stripMargin)
    }
  }

  test("prefix overflow with wrapping sums is bit-identical") {
    withTempView("t") {
      // The prefix passes 2^63 while every frame fits; a frame sum itself overflows too.
      setupView(
        """
          |SELECT 1 AS k, id AS o, element_at(
          |  array(4611686018427387904L, 4611686018427387904L, -4611686018427387904L,
          |        9223372036854775807L, 1L), CAST(id + 1 AS INT)) AS v
          |FROM range(0, 5)
        """.stripMargin)
      assertIdentical(
        """
          |SELECT k, o, v,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s1,
          |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s2
          |FROM t
        """.stripMargin)
    }
  }

  test("sums in an ANSI session are not rewritten") {
    withTempView("t") {
      setupView("SELECT id % 3 AS k, id AS o, id AS v FROM range(0, 90)")
      val sql = """
        |SELECT k, o, v,
        |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 31 PRECEDING AND CURRENT ROW) AS ss
        |FROM t
      """.stripMargin
      withSQLConf(
        SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        checkAnswer(spark.sql(sql), spark.sql(sql).collect())
        assert(collectWindows(spark.sql(sql).queryExecution.executedPlan).size === 1)
      }
    }
  }

  test("the rewrite adds no exchange or sort") {
    withTempView("t") {
      setupView(
        """
          |SELECT id % 10 AS k, id AS o,
          |  CASE WHEN id % 5 = 0 THEN CAST(NULL AS BIGINT) ELSE id END AS vn,
          |  id AS v
          |FROM range(0, 500)
        """.stripMargin)
      val sql = """
        |SELECT k, o, vn, v,
        |  sum(vn) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS s6,
        |  sum(v) OVER (PARTITION BY k ORDER BY o ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS s29
        |FROM t
      """.stripMargin
      val before = withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        run(false)(sql).queryExecution.executedPlan
      }
      val after = withSQLConf(
        SQLConf.WINDOW_PREFIX_REWRITE_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.sql(sql).queryExecution.executedPlan
      }
      def counts(plan: SparkPlan): (Int, Int, Int) = (
        plan.collect { case e: ShuffleExchangeLike => e }.size,
        plan.collect { case s: SortExec => s }.size,
        collectWindows(plan).size)
      val (beforeExchanges, beforeSorts, _) = counts(before)
      val (afterExchanges, afterSorts, _) = counts(after)
      assert(afterExchanges === beforeExchanges)
      assert(afterSorts === beforeSorts)
      // The two WindowExec operators are served by a single sort.
      assert(afterSorts === 1)
    }
  }

  test("randomised differential test") {
    withTempView("t") {
      val rand = new scala.util.Random(42)
      val widths = Seq(1, 2, 3, 7, 29, 90, 179)
      for (_ <- 0 until 20) {
        val partMod = 1 + rand.nextInt(20)
        val numRows = partMod * (1 + rand.nextInt(40))
        val nullEvery = 1 + rand.nextInt(6)
        spark.range(0, numRows).select(
          ($"id" % partMod).as("k"),
          $"id".as("o"),
          functions.when($"id" % nullEvery === 0, functions.lit(null).cast("bigint"))
            .otherwise(($"id" * (1 + rand.nextInt(97))) % 100000)
            .as("vn"),
          $"id".as("v")).createOrReplaceTempView("t")
        var selectedWidths = rand.shuffle(widths).take(1 + rand.nextInt(4))
        while (selectedWidths.sum < 16) {
          selectedWidths = selectedWidths :+ widths(rand.nextInt(widths.length))
        }
        val sumClauses = selectedWidths
          .map { w =>
            s"sum(${if (rand.nextBoolean()) "vn" else "v"}) OVER " +
              s"(PARTITION BY k ORDER BY o ROWS BETWEEN ${w - 1} PRECEDING AND CURRENT ROW) AS s$w"
          }
          .mkString(",\n  ")
        assertIdentical(s"SELECT k, o, vn, v, $sumClauses FROM t")
      }
    }
  }
}

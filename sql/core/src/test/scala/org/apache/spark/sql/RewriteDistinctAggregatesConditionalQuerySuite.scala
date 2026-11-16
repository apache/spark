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

import org.apache.spark.sql.catalyst.expressions.{Expression, If, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Join, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RewriteDistinctAggregatesConditionalQuerySuite extends QueryTest with SharedSparkSession {

  private def checkRewriteAndResult(
      conditionalSql: String,
      filterSql: String): Unit = {
    val expectedRows = spark.sql(filterSql).collect()

    // Verify the rewrite produces the same result as the explicit FILTER form.
    withSQLConf(
      SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      checkAnswer(spark.sql(conditionalSql), expectedRows)
    }

    // Verify the non-rewritten form also matches the explicit FILTER form.
    withSQLConf(
      SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
      checkAnswer(spark.sql(conditionalSql), expectedRows)
    }
  }

  private def hasUserConditionFilter(plan: LogicalPlan): Boolean = {
    val aggregateExpressions = plan.collect {
      case a: Aggregate => a.aggregateExpressions
    }.flatten
    val aggregateExprs = aggregateExpressions.flatMap {
      _.collect { case ae: AggregateExpression => ae }
    }
    // The Expand rewrite adds gid-routing filters to every distinct aggregate; we only
    // care about user-condition filters introduced by the IF/CASE canonicalization.
    aggregateExprs.flatMap(_.filter).exists(_.references.exists(_.name != "gid"))
  }

  private def orLeaves(expr: Expression): Seq[Expression] = expr match {
    case Or(left, right) => orLeaves(left) ++ orLeaves(right)
    case other => Seq(other)
  }

  private def existenceJoinCount(plan: LogicalPlan): Int = {
    plan.collect { case Join(_, _, _: ExistenceJoin, _, _) => () }.size
  }

  /**
   * Conf on must keep the conf-off ExistenceJoin count. The union guard copies each
   * filter into the Expand key, and RewritePredicateSubquery builds one join per copy.
   * When `stayUnnormalized` is set, the IF/CASE condition must not become a FILTER.
   */
  private def assertSameExistenceJoinCount(
      sqlText: String,
      expectedJoins: Int,
      stayUnnormalized: Boolean = false): Unit = {
    val (expectedRows, joinsOff) = withSQLConf(
      SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
      val df = spark.sql(sqlText)
      (df.collect(), existenceJoinCount(df.queryExecution.optimizedPlan))
    }
    assert(joinsOff == expectedJoins,
      s"conf off produced $joinsOff existence joins, expected $expectedJoins")
    withSQLConf(
      SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
      val df = spark.sql(sqlText)
      checkAnswer(df, expectedRows)
      val plan = df.queryExecution.optimizedPlan
      val joinsOn = existenceJoinCount(plan)
      assert(joinsOn == expectedJoins,
        s"conf on produced $joinsOn existence joins, expected $expectedJoins:\n$plan")
      if (stayUnnormalized) {
        assert(!hasUserConditionFilter(plan),
          "predicate-subquery condition should not become a FILTER clause")
      }
    }
  }

  test("rewrite COUNT(DISTINCT IF(cond, col, NULL)) correctness") {
    withTempView("t") {
      spark.range(7)
        .selectExpr(
          "cast(id % 3 + 1 as int) as key",
          "cast(id * 10 as int) as col1",
          "case when id % 4 = 0 then null else cast(id * 100 as int) end as col2")
        .createOrReplaceTempView("t")

      // Two conditional distinct counts on the same base so the rewrite fires (size > 1).
      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT IF(col1 > 10, col2, NULL)),
          |  COUNT(DISTINCT IF(col1 > 20, col2, NULL))
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 10),
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 20)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  test("rewrite COUNT(DISTINCT CASE WHEN cond THEN col END) correctness") {
    withTempView("t") {
      spark.range(7)
        .selectExpr(
          "cast(id % 3 + 1 as int) as key",
          "cast(id * 10 as int) as col1",
          "case when id % 4 = 0 then null else cast(id * 100 as string) end as col2")
        .createOrReplaceTempView("t")

      // Two CASE WHEN conditional distinct counts on the same base so the rewrite fires.
      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT CASE WHEN col1 > 10 THEN col2 END),
          |  COUNT(DISTINCT CASE WHEN col1 > 20 THEN col2 END)
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 10),
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 20)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  test("rewrite COUNT(DISTINCT CASE WHEN cond THEN col ELSE NULL END) correctness") {
    withTempView("t") {
      spark.range(6)
        .selectExpr(
          "cast(id % 2 + 1 as int) as key",
          "cast(id * 10 as int) as col1",
          "case when id % 4 = 0 then null else cast(id * 1.0 as double) end as col2")
        .createOrReplaceTempView("t")

      // Two CASE WHEN ... ELSE NULL counts on the same base so the rewrite fires.
      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT CASE WHEN col1 > 10 THEN col2 ELSE NULL END),
          |  COUNT(DISTINCT CASE WHEN col1 > 20 THEN col2 ELSE NULL END)
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 10),
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 20)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  test("rewrite with no GROUP BY") {
    withTempView("t") {
      spark.range(5)
        .selectExpr(
          "cast(id * 10 as int) as col1",
          "case when id % 3 = 0 then null else cast(id * 100 as int) end as col2")
        .createOrReplaceTempView("t")

      // Two counts so the rewrite fires without GROUP BY.
      checkRewriteAndResult(
        """SELECT
          |  COUNT(DISTINCT IF(col1 > 10, col2, NULL)),
          |  COUNT(DISTINCT IF(col1 > 20, col2, NULL))
          |FROM t""".stripMargin,
        """SELECT
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 10),
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 20)
          |FROM t""".stripMargin)
    }
  }

  test("rewrite with all NULLs in conditional branch") {
    withTempView("t") {
      spark.range(3)
        .selectExpr(
          "cast(id % 2 + 1 as int) as key",
          "cast(id * 5 as int) as col1",
          "cast(id * 100 as int) as col2")
        .createOrReplaceTempView("t")

      // col1 values are 0, 5, 10 - both thresholds (> 10 and > 20) yield zero matches,
      // so both counts return 0. Two counts so the rewrite fires.
      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT IF(col1 > 10, col2, NULL)),
          |  COUNT(DISTINCT IF(col1 > 20, col2, NULL))
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 10),
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 20)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  test("rewrite with duplicates in base column") {
    withTempView("t") {
      spark.range(6)
        .selectExpr(
          "cast(id % 2 + 1 as int) as key",
          "cast(id * 10 as int) as col1",
          "case when id % 3 = 0 then 100 when id % 3 = 1 then 100 else 200 end as col2")
        .createOrReplaceTempView("t")

      // Two counts on the same base (with duplicates) so the rewrite fires.
      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT IF(col1 > 10, col2, NULL)),
          |  COUNT(DISTINCT IF(col1 > 20, col2, NULL))
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 10),
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 20)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  test("different bases skip the conditional rewrite and keep correct results") {
    withTempView("t") {
      spark.range(5)
        .selectExpr(
          "cast(id % 2 + 1 as int) as key",
          "cast(id * 10 as int) as col1",
          "case when id % 3 = 0 then null else cast(id * 100 as int) end as col2",
          "case when id % 4 = 0 then null else cast(id * 10 as string) end as col3")
        .createOrReplaceTempView("t")

      val conditionalSql =
        """SELECT key,
          |  COUNT(DISTINCT IF(col1 > 10, col2, NULL)) as cnt1,
          |  COUNT(DISTINCT IF(col1 > 5, col3, NULL)) as cnt2
          |FROM t GROUP BY key""".stripMargin

      val filterSql =
        """SELECT key,
          |  COUNT(DISTINCT col2) FILTER (WHERE col1 > 10) as cnt1,
          |  COUNT(DISTINCT col3) FILTER (WHERE col1 > 5) as cnt2
          |FROM t GROUP BY key""".stripMargin

      checkRewriteAndResult(conditionalSql, filterSql)

      // col2 and col3 are different distinct children, so normalization does not
      // reduce the group count and the gate keeps the original IFs.
      withSQLConf(SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        val plan = spark.sql(conditionalSql).queryExecution.optimizedPlan
        assert(!hasUserConditionFilter(plan),
          "different bases should not produce a user-condition FILTER clause")
      }
    }
  }

  test("rewrite does not affect COUNT(DISTINCT IF(cond, col, non_null))") {
    withTempView("t") {
      spark.range(3)
        .selectExpr(
          "cast(id % 2 + 1 as int) as key",
          "cast(id * 10 as int) as col1",
          "cast(id * 100 as int) as col2")
        .createOrReplaceTempView("t")

      // Two counts so mayNeedtoRewrite fires and the non-null-else guard is exercised.
      val sqlText =
        """SELECT key,
          |  COUNT(DISTINCT IF(col1 > 10, col2, 0)),
          |  COUNT(DISTINCT IF(col1 > 20, col2, 0))
          |FROM t GROUP BY key""".stripMargin

      // Collect the conf-off result as the semantic baseline; the conf-on query must match it.
      val withoutRewriteRows = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
        spark.sql(sqlText).collect()
      }
      withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        checkAnswer(spark.sql(sqlText), withoutRewriteRows)
      }

      // When the switch is enabled, the non-null ELSE branch must not be canonicalized
      // into a user-condition FILTER; only the internal gid-routing filters introduced
      // by the Expand rewrite may be present.
      val hasFilter = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        hasUserConditionFilter(spark.sql(sqlText).queryExecution.optimizedPlan)
      }
      assert(!hasFilter,
        "Non-null ELSE branch should not produce a user-condition FILTER clause")
    }
  }

  test("rewrite is not applied to error-prone base expressions (ANSI divide by zero)") {
    withTempView("t") {
      // Rows with x = 0 make both conditions false, so the IF branches short-circuit
      // and 10 / x is never evaluated for them.
      spark.range(5)
        .selectExpr(
          "cast(id % 2 + 1 as int) as key",
          "cast(id % 2 as int) as x")
        .createOrReplaceTempView("t")

      // Two conditional distinct counts on the same error-prone base so that rewrite()
      // is reached; the canonicalization must NOT fire for such a base.
      val sqlText =
        """SELECT key,
          |  COUNT(DISTINCT IF(x <> 0, 10 / x, NULL)),
          |  COUNT(DISTINCT IF(x > 0, 10 / x, NULL))
          |FROM t GROUP BY key""".stripMargin

      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        // Must not raise DIVIDE_BY_ZERO: the canonicalization must leave the IFs intact
        // so that their branch short-circuiting is preserved.
        checkAnswer(spark.sql(sqlText), Seq(Row(1, 0L, 0L), Row(2, 1L, 1L)))
        assert(!hasUserConditionFilter(spark.sql(sqlText).queryExecution.optimizedPlan),
          "error-prone base should not produce a user-condition FILTER clause")
      }
    }
  }

  test("guard: high-cardinality base with low-selectivity disjoint predicates") {
    withTempView("t") {
      // ord is unique (high-cardinality base); each predicate matches ~1% of rows and
      // they are disjoint. This is the workload shape that regressed before the
      // union-of-filters guard: the guarded key must stay bounded by the pre-rewrite
      // plan's cardinality while the Expand stays collapsed.
      spark.range(1000)
        .selectExpr(
          "cast(id % 7 as int) as key",
          "cast(id as bigint) as ord",
          "cast(id % 100 as int) as bucket")
        .createOrReplaceTempView("t")

      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT IF(bucket = 0, ord, NULL)),
          |  COUNT(DISTINCT IF(bucket = 3, ord, NULL))
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT ord) FILTER (WHERE bucket = 0),
          |  COUNT(DISTINCT ord) FILTER (WHERE bucket = 3)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  test("SPARK-61819: guard: the aggregate above the Expand outputs no more rows than with " +
      "the config off") {
    withTempView("t") {
      // The shape of the test above, with more rows: ord is unique, and each condition matches
      // 1% of the rows. Without the guard, the distinct key of the normalized group would be ord
      // on every row, so the aggregate above the Expand would output one row per input row.
      spark.range(10000)
        .selectExpr(
          "cast(id % 7 as int) as key",
          "cast(id as bigint) as ord",
          "cast(id % 100 as int) as bucket")
        .createOrReplaceTempView("t")
      val query =
        """SELECT key,
          |  COUNT(DISTINCT IF(bucket = 0, ord, NULL)),
          |  COUNT(DISTINCT IF(bucket = 3, ord, NULL))
          |FROM t GROUP BY key""".stripMargin
      // The number of rows that the aggregate directly above the Expand outputs.
      def rowsAboveExpand(plan: LogicalPlan): Long = {
        val first = plan.collectFirst {
          case a: Aggregate if a.child.isInstanceOf[Expand] => a
        }.getOrElse(fail(s"expected an aggregate above an Expand:\n$plan"))
        classic.Dataset.ofRows(spark, first).count()
      }
      val rowsWithConfigOff = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
        rowsAboveExpand(spark.sql(query).queryExecution.optimizedPlan)
      }
      withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        val plan = spark.sql(query).queryExecution.optimizedPlan
        val expand = plan.collectFirst { case e: Expand => e }
          .getOrElse(fail(s"expected an Expand:\n$plan"))
        assert(expand.projections.size == 1, s"expected one distinct group:\n$plan")
        // The same plan without the guard on the Expand key.
        val unguarded = plan.transformUp {
          case e: Expand => e.copy(projections = e.projections.map(_.map {
            case If(_, base, Literal(null, _)) => base
            case other => other
          }))
        }
        // Each condition matches 100 rows, and the rows that match no condition add one NULL
        // row per grouping value and distinct group: 2 * (100 + 7) rows with the config off,
        // 200 + 7 with the guard, and one row per input row without it.
        assert(rowsWithConfigOff == 214)
        assert(rowsAboveExpand(plan) == 207)
        assert(rowsAboveExpand(unguarded) == 10000)
      }
    }
  }

  test("guard: mixed overlapping windows and disjoint flags") {
    withTempView("t") {
      spark.range(1000)
        .selectExpr(
          "cast(id % 7 as int) as key",
          "cast(id as bigint) as ord",
          "cast(id % 30 as int) as day",
          "cast(id % 100 as int) as bucket")
        .createOrReplaceTempView("t")

      // Overlapping windows (day < 10 is a subset of day < 30) plus disjoint selective flags in
      // the same query: the union guard must keep every count correct.
      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT IF(day < 10, ord, NULL)),
          |  COUNT(DISTINCT IF(day < 30, ord, NULL)),
          |  COUNT(DISTINCT IF(bucket = 0, ord, NULL)),
          |  COUNT(DISTINCT IF(bucket = 7, ord, NULL))
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT ord) FILTER (WHERE day < 10),
          |  COUNT(DISTINCT ord) FILTER (WHERE day < 30),
          |  COUNT(DISTINCT ord) FILTER (WHERE bucket = 0),
          |  COUNT(DISTINCT ord) FILTER (WHERE bucket = 7)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  test("guard: nullable condition that is NULL for some rows") {
    withTempView("t") {
      // Literal IF(NULL, ...) is folded to CAST(NULL AS ...) by SimplifyConditionals
      // before Distinct Aggregate Rewrite, so it never reaches the union guard. A
      // predicate over a nullable column that is NULL for some rows does.
      spark.range(50)
        .selectExpr(
          "cast(id % 7 as int) as key",
          "cast(id as bigint) as ord",
          "cast(id % 100 as int) as bucket",
          "case when id % 3 = 0 then cast(null as int) else cast(id % 5 as int) end as flag")
        .createOrReplaceTempView("t")

      val sqlText =
        """SELECT key,
          |  COUNT(DISTINCT IF(flag > 0, ord, NULL)),
          |  COUNT(DISTINCT IF(bucket = 0, ord, NULL))
          |FROM t GROUP BY key""".stripMargin
      val withoutRewriteRows = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
        spark.sql(sqlText).collect()
      }
      withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        checkAnswer(spark.sql(sqlText), withoutRewriteRows)

        val plan = spark.sql(sqlText).queryExecution.optimizedPlan
        val expand = plan.collectFirst { case e: Expand => e }.getOrElse(
          fail(s"expected Expand in optimized plan:\n$plan"))
        assert(expand.projections.size == 1,
          s"expected 1 distinct group, got ${expand.projections.size}")
        val guard = expand.projections.head.collectFirst {
          case i @ If(_, trueValue, falseValue)
              if falseValue.foldable && falseValue.eval() == null &&
                trueValue.references.exists(_.name == "ord") => i
        }.getOrElse(fail(s"no union guard over ord in Expand:\n$plan"))
        val leaves = orLeaves(guard.predicate)
        assert(leaves.exists(_.references.exists(_.name == "flag")),
          s"guard misses flag predicate: ${guard.predicate}")
        assert(leaves.exists(_.references.exists(_.name == "bucket")),
          s"guard misses bucket predicate: ${guard.predicate}")
      }
    }
  }

  test("guard: mixed filtered and unfiltered counts on the same base") {
    withTempView("t") {
      spark.range(1000)
        .selectExpr(
          "cast(id % 7 as int) as key",
          "cast(id as bigint) as ord",
          "cast(id % 100 as int) as bucket")
        .createOrReplaceTempView("t")

      // The unfiltered count needs ord over ALL rows, so the shared Expand key must
      // stay unguarded; the filtered counts ride along via their flag columns.
      checkRewriteAndResult(
        """SELECT key,
          |  COUNT(DISTINCT ord),
          |  COUNT(DISTINCT IF(bucket = 0, ord, NULL))
          |FROM t GROUP BY key""".stripMargin,
        """SELECT key,
          |  COUNT(DISTINCT ord),
          |  COUNT(DISTINCT ord) FILTER (WHERE bucket = 0)
          |FROM t GROUP BY key""".stripMargin)
    }
  }

  gridTest("SPARK-61819: guard: conditions on a column computed by a non-deterministic " +
      "expression keep the guard")(Seq("condition-in-subquery", "column-in-subquery")) { form =>
    withTable("t_files") {
      spark.range(200)
        .selectExpr(
          "cast(id % 3 as int) as key",
          "id as ord",
          "cast(id % 10 as int) as bucket")
        .write.saveAsTable("t_files")
      // Analysis rejects a non-deterministic expression in an aggregate argument, so
      // input_file_name() can reach the counts only through a column computed in a subquery.
      // The condition that the rule sees reads that column, so it is deterministic, and the
      // three counts share one guarded distinct group.
      val (subqueryColumn, condition) = form match {
        case "condition-in-subquery" =>
          ("input_file_name() LIKE '%part%' AS from_file", "from_file")
        case "column-in-subquery" => ("input_file_name() AS file", "file LIKE '%part%'")
      }
      val query =
        s"""SELECT key,
           |  COUNT(DISTINCT IF($condition, ord, NULL)),
           |  COUNT(DISTINCT IF(bucket = 3, ord, NULL)),
           |  COUNT(DISTINCT IF(bucket = 5, ord, NULL))
           |FROM (SELECT key, ord, bucket, $subqueryColumn FROM t_files)
           |GROUP BY key""".stripMargin
      val expected = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
        spark.sql(query).collect().toSeq
      }
      withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        val df = spark.sql(query)
        checkAnswer(df, expected)
        val plan = df.queryExecution.optimizedPlan
        val expand = plan.collectFirst { case e: Expand => e }
          .getOrElse(fail(s"expected an Expand:\n$plan"))
        val ord = expand.child.output.find(_.name == "ord").get
        assert(!expand.projections.exists(_.exists(_.semanticEquals(ord))),
          s"a distinct group is keyed by the raw base:\n$plan")
        assert(expand.projections.size == 1, s"expected one distinct group:\n$plan")
        val guard = expand.projections.head.collectFirst {
          case i @ If(_, trueValue, Literal(null, _)) if trueValue.semanticEquals(ord) => i
        }.getOrElse(fail(s"no guard over ord:\n$plan"))
        assert(orLeaves(guard.predicate).size == 3,
          s"the guard does not cover the three conditions: ${guard.predicate}")
      }
    }
  }

  test("rewrite is present in optimized plan") {
    withTempView("t") {
      spark.range(2)
        .selectExpr(
          "cast(id + 1 as int) as key",
          "cast(id * 10 as int) as col1",
          "cast(id * 100 as int) as col2")
        .createOrReplaceTempView("t")

      // Two conditional distinct counts on the same base column trigger canonicalization,
      // so the optimized plan must contain user-condition FILTER clauses.
      val hasFilter = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        val df = spark.sql(
          """SELECT key,
            |  COUNT(DISTINCT IF(col1 > 10, col2, NULL)) as cnt1,
            |  COUNT(DISTINCT IF(col1 > 5, col2, NULL)) as cnt2
            |FROM t GROUP BY key""".stripMargin)
        hasUserConditionFilter(df.queryExecution.optimizedPlan)
      }

      assert(hasFilter, "Optimized plan should contain user-condition FILTER clause")
    }
  }

  private def servingLogFixture(f: => Unit): Unit = {
    withTempView("serving_log", "models") {
      spark.range(20).selectExpr(
        "id % 3 as key",
        "id as request_id",
        "case when id % 11 = 0 then null else id % 5 end as user_id",
        "id % 4 as model",
        "id % 3 as tier",
        "id % 7 as priority",
        "id * 10 as tokens").createOrReplaceTempView("serving_log")
      spark.range(2).selectExpr(
        "id as model",
        "id % 2 as tier").createOrReplaceTempView("models")
      f
    }
  }

  test("guard: EXISTS, IN, and NOT IN filters are not joined twice") {
    servingLogFixture {
      // Another distinct aggregate forces the Expand path. Each predicate subquery
      // is its own filtered group; guarding it would join the dimension table twice.
      val sqlText =
        """SELECT key,
          |  SUM(DISTINCT tokens),
          |  COUNT(DISTINCT request_id) FILTER (WHERE EXISTS (
          |    SELECT 1 FROM models WHERE serving_log.model = models.model)),
          |  COUNT(DISTINCT user_id) FILTER (WHERE tier IN (
          |    SELECT tier FROM models WHERE models.model = serving_log.model)),
          |  COUNT(DISTINCT priority) FILTER (WHERE user_id NOT IN (
          |    SELECT tier FROM models WHERE models.model = serving_log.model))
          |FROM serving_log GROUP BY key""".stripMargin
      assertSameExistenceJoinCount(sqlText, expectedJoins = 3)
    }
  }

  test("guard: IF and CASE conditions with predicate subqueries stay un-normalized") {
    servingLogFixture {
      // Same base, so a subquery-blind normalize would collapse these into one group.
      // That group cannot be guarded, and Expand would project the raw request_id.
      val sqlText =
        """SELECT key,
          |  COUNT(DISTINCT IF(EXISTS (
          |    SELECT 1 FROM models WHERE serving_log.model = models.model),
          |    request_id, NULL)),
          |  COUNT(DISTINCT CASE WHEN tier IN (
          |    SELECT tier FROM models WHERE models.model = serving_log.model)
          |    THEN request_id END),
          |  COUNT(DISTINCT CASE WHEN user_id NOT IN (
          |    SELECT tier FROM models WHERE models.model = serving_log.model)
          |    THEN request_id ELSE NULL END)
          |FROM serving_log GROUP BY key""".stripMargin
      assertSameExistenceJoinCount(sqlText, expectedJoins = 3, stayUnnormalized = true)
    }
  }
}

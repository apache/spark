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

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
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

  test("multiple conditional distinct counts collapse and produce correct results") {
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
}

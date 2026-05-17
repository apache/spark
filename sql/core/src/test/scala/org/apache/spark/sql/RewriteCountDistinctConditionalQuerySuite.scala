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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RewriteCountDistinctConditionalQuerySuite extends QueryTest with SharedSparkSession {

  private def checkRewriteAndResult(
      conditionalSql: String,
      filterSql: String): Unit = {
    withTempView("t") {
      // Verify the rewrite produces the same result as the explicit FILTER form.
      val withRewrite = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        spark.sql(conditionalSql).collect()
      }
      val withoutRewrite = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
        spark.sql(conditionalSql).collect()
      }
      val explicitFilter = spark.sql(filterSql).collect()

      assert(withRewrite.sameElements(explicitFilter),
        "Rewritten query should match explicit FILTER query")
      assert(withoutRewrite.sameElements(explicitFilter),
        "Non-rewritten query should also match explicit FILTER query")
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

      checkRewriteAndResult(
        "SELECT key, COUNT(DISTINCT IF(col1 > 10, col2, NULL)) FROM t GROUP BY key",
        "SELECT key, COUNT(DISTINCT col2) FILTER (WHERE col1 > 10) FROM t GROUP BY key")
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

      checkRewriteAndResult(
        "SELECT key, COUNT(DISTINCT CASE WHEN col1 > 10 THEN col2 END) FROM t GROUP BY key",
        "SELECT key, COUNT(DISTINCT col2) FILTER (WHERE col1 > 10) FROM t GROUP BY key")
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

      checkRewriteAndResult(
        """SELECT key, COUNT(DISTINCT CASE WHEN col1 > 10 THEN col2 ELSE NULL END)
          |FROM t GROUP BY key""".stripMargin,
        "SELECT key, COUNT(DISTINCT col2) FILTER (WHERE col1 > 10) FROM t GROUP BY key")
    }
  }

  test("rewrite with no GROUP BY") {
    withTempView("t") {
      spark.range(5)
        .selectExpr(
          "cast(id * 10 as int) as col1",
          "case when id % 3 = 0 then null else cast(id * 100 as int) end as col2")
        .createOrReplaceTempView("t")

      checkRewriteAndResult(
        "SELECT COUNT(DISTINCT IF(col1 > 10, col2, NULL)) FROM t",
        "SELECT COUNT(DISTINCT col2) FILTER (WHERE col1 > 10) FROM t")
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

      checkRewriteAndResult(
        "SELECT key, COUNT(DISTINCT IF(col1 > 10, col2, NULL)) FROM t GROUP BY key",
        "SELECT key, COUNT(DISTINCT col2) FILTER (WHERE col1 > 10) FROM t GROUP BY key")
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

      checkRewriteAndResult(
        "SELECT key, COUNT(DISTINCT IF(col1 > 10, col2, NULL)) FROM t GROUP BY key",
        "SELECT key, COUNT(DISTINCT col2) FILTER (WHERE col1 > 10) FROM t GROUP BY key")
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

      val sqlText = "SELECT key, COUNT(DISTINCT IF(col1 > 10, col2, 0)) FROM t GROUP BY key"

      val withRewrite = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        spark.sql(sqlText).collect()
      }
      val withoutRewrite = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "false") {
        spark.sql(sqlText).collect()
      }

      assert(withRewrite.sameElements(withoutRewrite),
        "Non-null ELSE branch should not be rewritten")
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

      val planStr = withSQLConf(
        SQLConf.REWRITE_COUNT_DISTINCT_CONDITIONAL_ENABLED.key -> "true") {
        val df = spark.sql(
          "SELECT key, COUNT(DISTINCT IF(col1 > 10, col2, NULL)) FROM t GROUP BY key")
        df.queryExecution.optimizedPlan.toString
      }

      assert(planStr.contains("FILTER"),
        s"Optimized plan should contain FILTER clause. Plan:\n$planStr")
    }
  }
}

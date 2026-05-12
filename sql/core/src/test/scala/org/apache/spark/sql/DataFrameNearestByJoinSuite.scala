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

import org.apache.spark.sql.catalyst.plans.{NearestByDirection, NearestByJoinMode, NearestByJoinType}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class DataFrameNearestByJoinSuite extends QueryTest with SharedSparkSession {

  private def prepareForNearestByJoin(): (classic.DataFrame, classic.DataFrame) = {
    val users = spark.createDataFrame(
      Seq((1, 10.0), (2, 20.0), (3, 30.0))).toDF("user_id", "score")
    val products = spark.createDataFrame(
      Seq(("A", 11.0), ("B", 22.0), ("C", 5.0))).toDF("product", "pscore")
    (users, products)
  }

  test("similarity, inner, k=1") {
    val (users, products) = prepareForNearestByJoin()
    val result = users.nearestByJoin(
      products,
      -abs(users("score") - products("pscore")),
      numResults = 1,
      mode = "exact",
      direction = "similarity")

    checkAnswer(
      result.select("user_id", "product").orderBy("user_id"),
      Seq(Row(1, "A"), Row(2, "B"), Row(3, "B"))
    )
  }

  test("distance, inner, k=2") {
    val (users, products) = prepareForNearestByJoin()
    val result = users.nearestByJoin(
      products,
      abs(users("score") - products("pscore")),
      numResults = 2,
      mode = "exact",
      direction = "distance")

    // For each user_id, closest 2 by |score - pscore|:
    //   user 1 (10): A (|10-11|=1), C (|10-5|=5)
    //   user 2 (20): B (|20-22|=2), A (|20-11|=9)
    //   user 3 (30): B (|30-22|=8), A (|30-11|=19)
    checkAnswer(
      result.select("user_id", "product").orderBy("user_id", "product"),
      Seq(
        Row(1, "A"), Row(1, "C"),
        Row(2, "A"), Row(2, "B"),
        Row(3, "A"), Row(3, "B"))
    )
  }

  test("left outer when right side is empty") {
    val (users, products) = prepareForNearestByJoin()
    val emptyProducts = products.filter(lit(false))
    val result = users.nearestByJoin(
      emptyProducts,
      -abs(users("score") - emptyProducts("pscore")),
      numResults = 1,
      joinType = "leftouter",
      mode = "approx",
      direction = "similarity")

    checkAnswer(
      result.select("user_id", "product").orderBy("user_id"),
      Seq(Row(1, null), Row(2, null), Row(3, null))
    )
  }

  test("inner drops left rows with no matches") {
    val (users, products) = prepareForNearestByJoin()
    val emptyProducts = products.filter(lit(false))
    val result = users.nearestByJoin(
      emptyProducts,
      -abs(users("score") - emptyProducts("pscore")),
      numResults = 1,
      mode = "exact",
      direction = "similarity")

    assert(result.count() === 0)
  }

  test("self-join: each row finds nearest other rows in the same DataFrame") {
    val (users, _) = prepareForNearestByJoin()
    // We pass `users` as both sides; DeduplicateRelations rewrites the right side to
    // generate fresh ExprIds, so the join resolves. Both `users("score")` references in
    // the ranking expression bind to the original (left) attribute, so the rank is
    // identically 0 for every candidate -- this test exercises self-join resolution,
    // not nearest-row selection.
    val result = users.nearestByJoin(
      users,
      -abs(users("score") - users("score")),
      numResults = 2,
      mode = "exact",
      direction = "similarity")

    // 3 users x 2 nearest = 6 rows; output schema has user_id and score from both sides.
    assert(result.count() === 6)
    assert(result.columns.length === 4)
  }

  test("inner: NULL ranking values for all candidates drops the left row") {
    // Construct a left side where every comparison yields NULL: a NULL score on the left makes
    // `abs(left.score - right.pscore)` evaluate to NULL for every right row, so MaxMinByK skips
    // every candidate (its `ord == null` early-return path) and the heap stays empty. With INNER,
    // the left row is dropped entirely.
    val users = spark.createDataFrame(
      Seq[(Int, java.lang.Double)]((1, null), (2, 20.0d))).toDF("user_id", "score")
    val products = spark.createDataFrame(
      Seq(("A", 11.0), ("B", 22.0))).toDF("product", "pscore")

    val result = users.nearestByJoin(
      products,
      abs(users("score") - products("pscore")),
      numResults = 1,
      mode = "exact",
      direction = "distance")

    // Only user 2 should appear; user 1 (NULL score) drops because no candidate has a
    // non-null ranking value.
    checkAnswer(
      result.select("user_id", "product"),
      Seq(Row(2, "B"))
    )
  }

  test("left outer: NULL ranking values for all candidates preserves left with NULLs") {
    // Same shape as the previous test, but LEFT OUTER preserves user 1 with NULL right-side
    // columns instead of dropping it.
    val users = spark.createDataFrame(
      Seq[(Int, java.lang.Double)]((1, null), (2, 20.0d))).toDF("user_id", "score")
    val products = spark.createDataFrame(
      Seq(("A", 11.0), ("B", 22.0))).toDF("product", "pscore")

    val result = users.nearestByJoin(
      products,
      abs(users("score") - products("pscore")),
      numResults = 1,
      joinType = "leftouter",
      mode = "exact",
      direction = "distance")

    checkAnswer(
      result.select("user_id", "product").orderBy("user_id"),
      Seq(Row(1, null), Row(2, "B"))
    )
  }

  test("numResults larger than right side returns min(k, available) per left row") {
    // Right side has 3 rows; ask for 5. Each left row should get exactly 3 matches, not 5
    // padded with NULLs.
    val (users, products) = prepareForNearestByJoin()
    val result = users.nearestByJoin(
      products,
      abs(users("score") - products("pscore")),
      numResults = 5,
      mode = "exact",
      direction = "distance")

    // 3 users x min(5, 3) = 9 rows.
    assert(result.count() === 9)
    // No NULL padding: every left row pairs with every product exactly once.
    val perUser = result.groupBy("user_id").count().collect().map(r => r.getInt(0) -> r.getLong(1))
    assert(perUser.toMap === Map(1 -> 3L, 2 -> 3L, 3 -> 3L))
  }

  test("duplicate left rows each get an independent top-K") {
    // Two identical user rows must not be collapsed into a single group: each must independently
    // produce its own top-K. This proves the per-row __qid tagging in the rewrite works.
    val users = spark.createDataFrame(
      Seq((1, 10.0), (1, 10.0))).toDF("user_id", "score")
    val products = spark.createDataFrame(
      Seq(("A", 11.0), ("B", 22.0), ("C", 5.0))).toDF("product", "pscore")

    val result = users.nearestByJoin(
      products,
      abs(users("score") - products("pscore")),
      numResults = 1,
      mode = "exact",
      direction = "distance")

    // Two identical left rows -> two output rows, both pairing with product A (closest to 10.0).
    checkAnswer(
      result.select("user_id", "product"),
      Seq(Row(1, "A"), Row(1, "A"))
    )
  }

  test("conflicting column names between sides resolve via DataFrame qualifiers") {
    // Both sides have a column named `score`; the ranking expression disambiguates via
    // DataFrame-qualified accessors.
    val left = spark.createDataFrame(Seq((1, 10.0), (2, 20.0))).toDF("id", "score")
    val right = spark.createDataFrame(
      Seq(("A", 11.0), ("B", 22.0), ("C", 5.0))).toDF("name", "score")

    val result = left.nearestByJoin(
      right,
      -abs(left("score") - right("score")),
      numResults = 1,
      mode = "exact",
      direction = "similarity")

    checkAnswer(
      result.select("id", "name").orderBy("id"),
      Seq(Row(1, "A"), Row(2, "B"))
    )
    // Output schema should carry both `score` columns through (4 columns total).
    assert(result.columns.length === 4)
  }

  test("streaming inputs are rejected at analysis time") {
    // Build a streaming left side and a static right side; NearestByJoin must be rejected
    // at analysis before the optimizer rewrite (an unconditioned cross-product fed into a
    // global Aggregate keyed by a per-row identifier) ever runs.
    import testImplicits._
    implicit val ctx = spark.sqlContext
    val streamingUsers = MemoryStream[(Int, Double)].toDF().toDF("user_id", "score")
    val products = spark.createDataFrame(
      Seq(("A", 11.0), ("B", 22.0), ("C", 5.0))).toDF("product", "pscore")

    checkError(
      exception = intercept[AnalysisException] {
        streamingUsers.nearestByJoin(
          products,
          -abs(streamingUsers("score") - products("pscore")),
          numResults = 1,
          mode = "exact",
          direction = "similarity").queryExecution.analyzed
      },
      condition = "NEAREST_BY_JOIN.STREAMING_NOT_SUPPORTED",
      parameters = Map.empty)
  }

  test("rejected when spark.sql.crossJoin.enabled is false") {
    // The rewrite produces an unconditioned cross-product internally, so when the user has
    // opted out of cross-products via `spark.sql.crossJoin.enabled = false`, NEAREST BY
    // queries are rejected at analysis time with `NEAREST_BY_JOIN.CROSS_JOIN_NOT_ENABLED` --
    // a NEAREST BY-specific error class added so the user does not see internal rewrite
    // attributes in the error message.
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      val (users, products) = prepareForNearestByJoin()
      checkError(
        exception = intercept[AnalysisException] {
          users.nearestByJoin(
            products,
            -abs(users("score") - products("pscore")),
            numResults = 1,
            mode = "exact",
            direction = "similarity").queryExecution.analyzed
        },
        condition = "NEAREST_BY_JOIN.CROSS_JOIN_NOT_ENABLED",
        parameters = Map.empty)
    }
  }

  test("exact + left outer: empty right side preserves all left rows with NULLs") {
    // Exercises the EXACT + LEFT OUTER combination, which no other test covers together.
    val (users, products) = prepareForNearestByJoin()
    val emptyProducts = products.filter(lit(false))
    val result = users.nearestByJoin(
      emptyProducts,
      -abs(users("score") - emptyProducts("pscore")),
      numResults = 1,
      joinType = "leftouter",
      mode = "exact",
      direction = "similarity")

    checkAnswer(
      result.select("user_id", "product").orderBy("user_id"),
      Seq(Row(1, null), Row(2, null), Row(3, null))
    )
  }

  test("SQL: APPROX NEAREST SIMILARITY") {
    val (users, products) = prepareForNearestByJoin()
    users.createOrReplaceTempView("t_users")
    products.createOrReplaceTempView("t_products")
    try {
      val result = spark.sql(
        """
          |SELECT u.user_id, p.product
          |FROM t_users u JOIN t_products p
          |  APPROX NEAREST 1 BY SIMILARITY -abs(u.score - p.pscore)
          |""".stripMargin)
      checkAnswer(
        result.orderBy("user_id"),
        Seq(Row(1, "A"), Row(2, "B"), Row(3, "B"))
      )
    } finally {
      spark.catalog.dropTempView("t_users")
      spark.catalog.dropTempView("t_products")
    }
  }

  test("SQL: EXACT NEAREST DISTANCE") {
    val (users, products) = prepareForNearestByJoin()
    users.createOrReplaceTempView("t_users")
    products.createOrReplaceTempView("t_products")
    try {
      val result = spark.sql(
        """
          |SELECT u.user_id, p.product
          |FROM t_users u JOIN t_products p
          |  EXACT NEAREST 1 BY DISTANCE abs(u.score - p.pscore)
          |""".stripMargin)
      checkAnswer(
        result.orderBy("user_id"),
        Seq(Row(1, "A"), Row(2, "B"), Row(3, "B"))
      )
    } finally {
      spark.catalog.dropTempView("t_users")
      spark.catalog.dropTempView("t_products")
    }
  }

  test("invalid numResults is rejected") {
    val (users, products) = prepareForNearestByJoin()
    Seq(0, 100001).foreach { k =>
      checkError(
        exception = intercept[AnalysisException] {
          users.nearestByJoin(
            products,
            -abs(users("score") - products("pscore")),
            numResults = k,
            mode = "exact",
            direction = "similarity")
        },
        condition = "NEAREST_BY_JOIN.NUM_RESULTS_OUT_OF_RANGE",
        parameters = Map(
          "numResults" -> k.toString,
          "min" -> "1",
          "max" -> "100000"))
    }
  }

  test("invalid joinType is rejected") {
    val (users, products) = prepareForNearestByJoin()
    checkError(
      exception = intercept[AnalysisException] {
        users.nearestByJoin(
          products,
          -abs(users("score") - products("pscore")),
          numResults = 1,
          joinType = "rightouter",
          mode = "approx",
          direction = "similarity")
      },
      condition = "NEAREST_BY_JOIN.UNSUPPORTED_JOIN_TYPE",
      parameters = Map(
        "joinType" -> "rightouter",
        "supported" -> NearestByJoinType.supportedDisplay))
  }

  test("invalid mode is rejected") {
    val (users, products) = prepareForNearestByJoin()
    checkError(
      exception = intercept[AnalysisException] {
        users.nearestByJoin(
          products,
          -abs(users("score") - products("pscore")),
          numResults = 1,
          joinType = "inner",
          mode = "bogus",
          direction = "similarity")
      },
      condition = "NEAREST_BY_JOIN.UNSUPPORTED_MODE",
      parameters = Map(
        "mode" -> "bogus",
        "supported" -> NearestByJoinMode.supported.mkString("'", "', '", "'")))
  }

  test("invalid direction is rejected") {
    val (users, products) = prepareForNearestByJoin()
    checkError(
      exception = intercept[AnalysisException] {
        users.nearestByJoin(
          products,
          -abs(users("score") - products("pscore")),
          numResults = 1,
          mode = "exact",
          direction = "bogus")
      },
      condition = "NEAREST_BY_JOIN.UNSUPPORTED_DIRECTION",
      parameters = Map(
        "direction" -> "bogus",
        "supported" -> NearestByDirection.supported.mkString("'", "', '", "'")))
  }

  test("non-orderable ranking expression is rejected") {
    val (users, products) = prepareForNearestByJoin()
    checkError(
      exception = intercept[AnalysisException] {
        users.nearestByJoin(
          products,
          map(users("score"), products("pscore")),
          numResults = 1,
          mode = "exact",
          direction = "similarity")
      },
      condition = "NEAREST_BY_JOIN.NON_ORDERABLE_RANKING_EXPRESSION",
      parameters = Map(
        "expression" -> "\"map(score, pscore)\"",
        "type" -> "\"MAP<DOUBLE, DOUBLE>\""))
  }

  test("EXACT mode rejects nondeterministic ranking expression") {
    val (users, products) = prepareForNearestByJoin()
    checkError(
      exception = intercept[AnalysisException] {
        users.nearestByJoin(
          products,
          rand() + products("pscore"),
          numResults = 1,
          joinType = "inner",
          mode = "exact",
          direction = "similarity")
      },
      condition = "NEAREST_BY_JOIN.EXACT_WITH_NONDETERMINISTIC_EXPRESSION",
      matchPVals = true,
      parameters = Map("expression" -> ".*rand.*pscore.*"))
  }
}

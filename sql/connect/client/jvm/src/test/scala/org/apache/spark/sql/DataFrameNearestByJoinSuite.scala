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

import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.functions._

/**
 * End-to-end Connect-side coverage for `Dataset.nearestByJoin`. Mirrors the
 * `DataFrameNearestByJoinSuite` in `sql/core` for the classic path; this suite ensures the same
 * API behaves correctly when invoked through the Connect client (proto serialization, server-side
 * proto-to-catalyst translation in `SparkConnectPlanner.transformNearestByJoin`, and result
 * roundtrip).
 */
class DataFrameNearestByJoinSuite extends QueryTest with RemoteSparkSession {
  import testImplicits._

  private lazy val users = Seq((1, 10.0), (2, 20.0), (3, 30.0)).toDF("user_id", "score")

  private lazy val products = Seq(("A", 11.0), ("B", 22.0), ("C", 5.0)).toDF("product", "pscore")

  test("inner approx similarity k=1") {
    checkAnswer(
      users
        .nearestByJoin(
          right = products,
          rankingExpression = -abs(users("score") - products("pscore")),
          numResults = 1,
          mode = "approx",
          direction = "similarity")
        .select("user_id", "product")
        .orderBy("user_id"),
      Seq(Row(1, "A"), Row(2, "B"), Row(3, "B")))
  }

  test("inner approx distance k=2") {
    checkAnswer(
      users
        .nearestByJoin(
          right = products,
          rankingExpression = abs(users("score") - products("pscore")),
          numResults = 2,
          mode = "approx",
          direction = "distance")
        .select("user_id", "product")
        .orderBy("user_id", "product"),
      Seq(Row(1, "A"), Row(1, "C"), Row(2, "A"), Row(2, "B"), Row(3, "A"), Row(3, "B")))
  }

  test("left outer with empty right preserves left rows with NULLs") {
    val emptyProducts = products.filter(lit(false))
    checkAnswer(
      users
        .nearestByJoin(
          right = emptyProducts,
          rankingExpression = -abs(users("score") - emptyProducts("pscore")),
          numResults = 1,
          mode = "exact",
          direction = "similarity",
          joinType = "leftouter")
        .select("user_id", "product")
        .orderBy("user_id"),
      Seq(Row(1, null), Row(2, null), Row(3, null)))
  }

  test("output schema has no rewrite-internal columns") {
    val result = users.nearestByJoin(
      right = products,
      rankingExpression = -abs(users("score") - products("pscore")),
      numResults = 1,
      mode = "exact",
      direction = "similarity")
    // Only the user-visible columns flow through; no `__qid`, `__nearest_matches__`, etc.
    assert(result.columns.toSet === Set("user_id", "score", "product", "pscore"))
  }

  test("invalid mode is rejected") {
    val ex = intercept[AnalysisException] {
      users.nearestByJoin(
        right = products,
        rankingExpression = -abs(users("score") - products("pscore")),
        numResults = 1,
        mode = "bogus",
        direction = "similarity")
    }
    assert(ex.getCondition === "NEAREST_BY_JOIN.UNSUPPORTED_MODE")
  }
}

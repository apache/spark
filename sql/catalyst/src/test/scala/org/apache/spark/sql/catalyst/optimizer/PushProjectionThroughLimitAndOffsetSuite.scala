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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Add, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class PushProjectionThroughLimitAndOffsetSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Optimizer Batch",
      FixedPoint(100),
      PushProjectionThroughLimitAndOffset,
      EliminateLimits,
      LimitPushDown) :: Nil
  }

  test("SPARK-40501: push projection through limit") {
    val testRelation = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))

    val query1 = testRelation
      .limit(10)
      .select(Symbol("a"), Symbol("b"), 'c')
      .limit(15).analyze
    val optimized1 = Optimize.execute(query1)
    val expected1 = testRelation
      .select(Symbol("a"), Symbol("b"), 'c')
      .limit(10).analyze
    comparePlans(optimized1, expected1)

    val query2 = testRelation
      .sortBy($"a".asc)
      .limit(10)
      .select(Symbol("a"), Symbol("b"), 'c')
      .limit(15).analyze
    val optimized2 = Optimize.execute(query2)
    val expected2 = testRelation
      .sortBy($"a".asc)
      .select(Symbol("a"), Symbol("b"), 'c')
      .limit(10).analyze
    comparePlans(optimized2, expected2)

    val query3 = testRelation
      .limit(10)
      .select(Symbol("a"), Symbol("b"), 'c')
      .limit(20)
      .select(Symbol("a"))
      .limit(15).analyze
    val optimized3 = Optimize.execute(query3)
    val expected3 = testRelation
      .select(Symbol("a"), Symbol("b"), 'c')
      .select(Symbol("a"))
      .limit(10).analyze
    comparePlans(optimized3, expected3)

    val query4 = testRelation
      .sortBy($"a".asc)
      .limit(10)
      .select(Symbol("a"), Symbol("b"), 'c')
      .limit(20)
      .select(Symbol("a"))
      .limit(15).analyze
    val optimized4 = Optimize.execute(query4)
    val expected4 = testRelation
      .sortBy($"a".asc)
      .select(Symbol("a"), Symbol("b"), 'c')
      .select(Symbol("a"))
      .limit(10).analyze
    comparePlans(optimized4, expected4)
  }

  test("push projection through offset") {
    val testRelation = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(30).map(_ => Row(1, 2, 3)))

    val query1 = testRelation
      .offset(5)
      .select($"a", $"b", $"c")
      .analyze
    val optimized1 = Optimize.execute(query1)
    val expected1 = testRelation
      .select($"a", $"b", $"c")
      .offset(5).analyze
    comparePlans(optimized1, expected1)

    val query2 = testRelation
      .limit(15).offset(5)
      .select($"a", $"b", $"c")
      .analyze
    val optimized2 = Optimize.execute(query2)
    val expected2 = testRelation
      .select($"a", $"b", $"c")
      .limit(15).offset(5).analyze
    comparePlans(optimized2, expected2)

    val query3 = testRelation
      .offset(5).limit(15)
      .select($"a", $"b", $"c")
      .analyze
    val optimized3 = Optimize.execute(query3)
    val expected3 = testRelation
      .select($"a", $"b", $"c")
      .localLimit(Add(15, 5)).offset(5).globalLimit(15)
      .analyze
    comparePlans(optimized3, expected3)

    val query4 = testRelation
      .offset(5).limit(15)
      .select($"a", $"b", $"c")
      .limit(10).analyze
    val optimized4 = Optimize.execute(query4)
    val expected4 = testRelation
      .select($"a", $"b", $"c")
      .localLimit(Add(10, 5)).offset(5).globalLimit(10)
      .analyze
    comparePlans(optimized4, expected4)

    val query5 = testRelation
      .localLimit(10)
      .select($"a", $"b", $"c")
      .offset(5).limit(10).analyze
    val optimized5 = Optimize.execute(query5)
    val expected5 = testRelation
      .select($"a", $"b", $"c")
      .localLimit(10).offset(5).globalLimit(10)
      .analyze
    comparePlans(optimized5, expected5)

    val query6 = testRelation
      .localLimit(20)
      .select($"a", $"b", $"c")
      .offset(5).limit(10).analyze
    val optimized6 = Optimize.execute(query6)
    val expected6 = testRelation
      .select($"a", $"b", $"c")
      .localLimit(15).offset(5).globalLimit(10)
      .analyze
    comparePlans(optimized6, expected6)
  }

  test("SPARK-59438: do not push projection with scalar subquery through limit") {
    // A scalar subquery is deterministic, but later rewrites transform it into a join
    // that can destroy the sort order Limit depends on.
    val testRelation = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))
    val subqueryRelation = LocalRelation.fromExternalRows(
      Seq("x".attr.int, "y".attr.int),
      Seq(Row(1, 10)))
    val scalarSubquery = ScalarSubquery(
      subqueryRelation.groupBy($"x")(sum($"y").as("total")).analyze)

    // Project with scalar subquery over GlobalLimit + LocalLimit: should NOT be pushed down
    val query1 = testRelation
      .sortBy($"a".asc)
      .limit(3)
      .select($"a", scalarSubquery.as("sub"))
      .analyze
    val optimized1 = Optimize.execute(query1)
    // The Project should remain above the Limit, not be pushed below it
    comparePlans(optimized1, query1)

    // Project with scalar subquery over LocalLimit only: should NOT be pushed down
    val query2 = testRelation
      .limit(10)
      .select($"a", scalarSubquery.as("sub"))
      .limit(5).analyze
    val optimized2 = Optimize.execute(query2)
    // After EliminateLimits merges the limits to 5, the subquery Project should stay on top
    val expected2 = testRelation
      .limit(5)
      .select($"a", scalarSubquery.as("sub"))
      .analyze
    comparePlans(optimized2, expected2)
  }

  test("SPARK-59438: do not push projection with scalar subquery through offset") {
    val testRelation = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(30).map(_ => Row(1, 2, 3)))
    val subqueryRelation = LocalRelation.fromExternalRows(
      Seq("x".attr.int, "y".attr.int),
      Seq(Row(1, 10)))
    val scalarSubquery = ScalarSubquery(
      subqueryRelation.groupBy($"x")(sum($"y").as("total")).analyze)

    // Project with scalar subquery over Offset: should NOT be pushed down
    val query1 = testRelation
      .offset(5)
      .select($"a", scalarSubquery.as("sub"))
      .analyze
    val optimized1 = Optimize.execute(query1)
    comparePlans(optimized1, query1)

    // Project with scalar subquery over Limit + Offset: should NOT be pushed down
    val query2 = testRelation
      .limit(15).offset(5)
      .select($"a", scalarSubquery.as("sub"))
      .analyze
    val optimized2 = Optimize.execute(query2)
    comparePlans(optimized2, query2)
  }

  test("SPARK-59438: still push simple deterministic projection through limit") {
    // Regression guard: simple column projections without subqueries should still be pushed.
    val testRelation = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))

    val query = testRelation
      .sortBy($"a".asc)
      .limit(3)
      .select($"a", $"b")
      .analyze
    val optimized = Optimize.execute(query)
    val expected = testRelation
      .sortBy($"a".asc)
      .select($"a", $"b")
      .limit(3).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-59438: still push deterministic expression (non-subquery) through limit") {
    // Deterministic expressions like arithmetic should still be pushed.
    val testRelation = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))

    val query = testRelation
      .limit(10)
      .select($"a", ($"b" + $"c").as("sum"))
      .limit(5).analyze
    val optimized = Optimize.execute(query)
    val expected = testRelation
      .select($"a", ($"b" + $"c").as("sum"))
      .limit(5).analyze
    comparePlans(optimized, expected)
  }
}

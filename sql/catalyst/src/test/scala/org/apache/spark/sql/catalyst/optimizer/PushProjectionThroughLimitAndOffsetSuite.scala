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
import org.apache.spark.sql.catalyst.expressions.{Add, Exists, ListQuery, OuterReference, ScalarSubquery}
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

  // ---- SPARK-59438 tests: correlated subqueries must NOT be pushed ----

  test("SPARK-59438: do not push correlated scalar subquery through GlobalLimit + LocalLimit") {
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))
    val subRel = LocalRelation.fromExternalRows(
      Seq("x".attr.int, "y".attr.int),
      Seq(Row(1, 10)))

    // Correlated scalar subquery: references outer column 'a' via OuterReference
    val correlatedSub = ScalarSubquery(
      subRel.where($"x" === OuterReference($"a")).select(sum($"y").as("total")).analyze,
      outerAttrs = Seq($"a"))

    // Project with correlated scalar subquery over GlobalLimit + LocalLimit
    val query = rel
      .sortBy($"a".asc)
      .limit(3)
      .select($"a", correlatedSub.as("sub"))
      .analyze
    val optimized = Optimize.execute(query)
    // Project should stay above the Limit
    comparePlans(optimized, query)
  }

  test("SPARK-59438: do not push correlated scalar subquery through bare LocalLimit") {
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))
    val subRel = LocalRelation.fromExternalRows(
      Seq("x".attr.int, "y".attr.int),
      Seq(Row(1, 10)))

    val correlatedSub = ScalarSubquery(
      subRel.where($"x" === OuterReference($"a")).select(sum($"y").as("total")).analyze,
      outerAttrs = Seq($"a"))

    // Bare LocalLimit (not wrapped in GlobalLimit) -- covers the first match arm
    val query = rel
      .localLimit(5)
      .select($"a", correlatedSub.as("sub"))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-59438: do not push correlated scalar subquery through Offset") {
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(30).map(_ => Row(1, 2, 3)))
    val subRel = LocalRelation.fromExternalRows(
      Seq("x".attr.int, "y".attr.int),
      Seq(Row(1, 10)))

    val correlatedSub = ScalarSubquery(
      subRel.where($"x" === OuterReference($"a")).select(sum($"y").as("total")).analyze,
      outerAttrs = Seq($"a"))

    // Project with correlated scalar subquery over Offset
    val query1 = rel
      .offset(5)
      .select($"a", correlatedSub.as("sub"))
      .analyze
    val optimized1 = Optimize.execute(query1)
    comparePlans(optimized1, query1)

    // Project with correlated scalar subquery over Limit + Offset
    val query2 = rel
      .limit(15).offset(5)
      .select($"a", correlatedSub.as("sub"))
      .analyze
    val optimized2 = Optimize.execute(query2)
    comparePlans(optimized2, query2)
  }

  test("SPARK-59438: do not push correlated EXISTS through Limit") {
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int),
      1.to(10).map(_ => Row(1, 2)))
    val subRel = LocalRelation.fromExternalRows(
      Seq("x".attr.int),
      Seq(Row(1)))

    val correlatedExists = Exists(
      subRel.where($"x" === OuterReference($"a")).analyze,
      outerAttrs = Seq($"a"))

    val query = rel
      .sortBy($"a".asc)
      .limit(3)
      .select($"a", correlatedExists.as("has_match"))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("SPARK-59438: do not push IN (ListQuery) through Limit") {
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int),
      1.to(10).map(_ => Row(1, 2)))
    val subRel = LocalRelation.fromExternalRows(
      Seq("x".attr.int),
      Seq(Row(1)))

    val inSubquery = ListQuery(
      subRel.select($"x").analyze)

    val query = rel
      .sortBy($"a".asc)
      .limit(3)
      .select($"a", $"a".in(inSubquery).as("in_sub"))
      .analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  // ---- SPARK-59438 regression guards: safe projections must still be pushed ----

  test("SPARK-59438: uncorrelated scalar subquery IS still pushed through Limit") {
    // Uncorrelated subqueries are evaluated once and never become joins,
    // so pushing them preserves the limit-at-root plan shape (SPARK-40501).
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))
    val subRel = LocalRelation.fromExternalRows(
      Seq("x".attr.int, "y".attr.int),
      Seq(Row(1, 10)))

    // Uncorrelated: no outerAttrs, no OuterReference
    val uncorrelatedSub = ScalarSubquery(
      subRel.groupBy($"x")(sum($"y").as("total")).analyze)

    val query = rel
      .sortBy($"a".asc)
      .limit(3)
      .select($"a", uncorrelatedSub.as("sub"))
      .analyze
    val optimized = Optimize.execute(query)
    // The uncorrelated subquery Project SHOULD be pushed below the Limit
    val expected = rel
      .sortBy($"a".asc)
      .select($"a", uncorrelatedSub.as("sub"))
      .limit(3).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-59438: simple column projection still pushed through Limit") {
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))

    val query = rel
      .sortBy($"a".asc)
      .limit(3)
      .select($"a", $"b")
      .analyze
    val optimized = Optimize.execute(query)
    val expected = rel
      .sortBy($"a".asc)
      .select($"a", $"b")
      .limit(3).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-59438: deterministic arithmetic expression still pushed through Limit") {
    val rel = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))

    val query = rel
      .limit(10)
      .select($"a", ($"b" + $"c").as("sum"))
      .limit(5).analyze
    val optimized = Optimize.execute(query)
    val expected = rel
      .select($"a", ($"b" + $"c").as("sum"))
      .limit(5).analyze
    comparePlans(optimized, expected)
  }
}

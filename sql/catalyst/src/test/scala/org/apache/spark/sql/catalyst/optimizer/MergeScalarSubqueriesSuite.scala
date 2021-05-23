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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, GetStructField, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class MergeScalarSubqueriesSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("MergeScalarSubqueries", Once, MergeScalarSubqueries) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.string)

  test("Merging subqueries with projects") {
    val subquery1 = testRelation.select(('a + 1).as("a_plus1"))
    val subquery2 = testRelation.select(('a + 2).as("a_plus2"))
    val subquery3 = testRelation.select('b)
    val subquery4 = testRelation.select(('a + 1).as("a_plus1_2"))
    val subquery5 = testRelation.select(('a + 2).as("a_plus2_2"))
    val subquery6 = testRelation.select('b.as("b_2"))
    val originalQuery = testRelation
      .select(
        ScalarSubquery(subquery1),
        ScalarSubquery(subquery2),
        ScalarSubquery(subquery3),
        ScalarSubquery(subquery4),
        ScalarSubquery(subquery5),
        ScalarSubquery(subquery6))

    val mergedSubquery = testRelation
      .select(
        ('a + 1).as("a_plus1"),
        ('a + 2).as("a_plus2"),
        'b)
      .select(CreateNamedStruct(Seq(
        Literal("a_plus1"), 'a_plus1,
        Literal("a_plus2"), 'a_plus2,
        Literal("b"), 'b
      )).as("mergedValue"))
    val correctAnswer = testRelation
      .select(
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 2).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 2).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with aggregates") {
    val subquery1 = testRelation.groupBy('b)(max('a).as("max_a"))
    val subquery2 = testRelation.groupBy('b)(sum('a).as("sum_a"))
    val subquery3 = testRelation.groupBy('b)('b)
    val subquery4 = testRelation.groupBy('b)(max('a).as("max_a_2"))
    val subquery5 = testRelation.groupBy('b)(sum('a).as("sum_a_2"))
    val subquery6 = testRelation.groupBy('b)('b.as("b_2"))
    val originalQuery = testRelation
      .select(
        ScalarSubquery(subquery1),
        ScalarSubquery(subquery2),
        ScalarSubquery(subquery3),
        ScalarSubquery(subquery4),
        ScalarSubquery(subquery5),
        ScalarSubquery(subquery6))

    val mergedSubquery = testRelation
      .groupBy('b)(
        max('a).as("max_a"),
        sum('a).as("sum_a"),
        'b)
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), 'max_a,
        Literal("sum_a"), 'sum_a,
        Literal("b"), 'b
      )).as("mergedValue"))
    val correctAnswer = testRelation
      .select(
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 2).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 2).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with filters") {
    val subquery1 = testRelation.where('a > 1).select('a)
    val subquery2 = testRelation.where('a > 1).select('b)
    val subquery3 = testRelation.select('a.as("a_2")).where('a_2 > 1).select('a_2)
    val subquery4 = testRelation.select('a.as("a_2"), 'b).where('a_2 > 1).select('b.as("b_2"))
    val originalQuery = testRelation
      .select(
        ScalarSubquery(subquery1),
        ScalarSubquery(subquery2),
        ScalarSubquery(subquery3),
        ScalarSubquery(subquery4))

    val mergedSubquery = testRelation
      .where('a > 1)
      .select('a, 'b)
      .select(CreateNamedStruct(Seq(
        Literal("a"), 'a,
        Literal("b"), 'b
      )).as("mergedValue"))
    val correctAnswer = testRelation
      .select(
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with aggregate filters") {
    val subquery1 = testRelation.having('b)(max('a).as("max_a"))(max('a) > 1)
    val subquery2 = testRelation.having('b)(sum('a).as("sum_a"))(max('a) > 1)
    val originalQuery = testRelation.select(
      ScalarSubquery(subquery1),
      ScalarSubquery(subquery2))

    val mergedSubquery = testRelation
      .having('b)(
        max('a).as("max_a"),
        sum('a).as("sum_a"))('max_a > 1)
      .select(
        'max_a,
        'sum_a)
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), 'max_a,
        Literal("sum_a"), 'sum_a
      )).as("mergedValue"))
    val correctAnswer = testRelation
      .select(
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with joins") {
    val subquery1 = testRelation.as("t1")
      .join(
        testRelation.as("t2"),
        Inner,
        Some($"t1.b" === $"t2.b"))
      .select($"t1.a")
    val subquery2 = testRelation.as("t1")
      .select('a.as("a_1"), 'b.as("b_1"), 'c.as("c_1"))
      .join(
        testRelation.as("t2").select('a.as("a_2"), 'b.as("b_2"), 'c.as("c_2")),
        Inner,
        Some('b_1 === 'b_2))
      .select('c_2)
    val originalQuery = testRelation.select(
      ScalarSubquery(subquery1),
      ScalarSubquery(subquery2))

    val mergedSubquery = testRelation.as("t1")
      .select('a, 'b, 'c)
      .join(
        testRelation.as("t2").select('a, 'b, 'c),
        Inner,
        Some($"t1.b" === $"t2.b"))
      .select($"t1.a", $"t2.c")
      .select(CreateNamedStruct(Seq(
        Literal("a"), 'a,
        Literal("c_2"), 'c
      )).as("mergedValue"))
    val correctAnswer = testRelation
      .select(
        GetStructField(ScalarSubquery(mergedSubquery), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(mergedSubquery), 1).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Do not merge different aggregate implementations") {
    // supports HashAggregate
    val subquery1 = testRelation.groupBy('b)(max('a).as("max_a"))
    val subquery2 = testRelation.groupBy('b)(min('a).as("min_a"))

    // supports ObjectHashAggregate
    val subquery3 = testRelation
      .groupBy('b)(CollectList('a).toAggregateExpression(isDistinct = false).as("collectlist_a"))
    val subquery4 = testRelation
      .groupBy('b)(CollectSet('a).toAggregateExpression(isDistinct = false).as("collectset_a"))

    // supports SortAggregate
    val subquery5 = testRelation.groupBy('b)(max('c).as("max_c"))
    val subquery6 = testRelation.groupBy('b)(min('c).as("min_c"))

    val originalQuery = testRelation
      .select(
        ScalarSubquery(subquery1),
        ScalarSubquery(subquery2),
        ScalarSubquery(subquery3),
        ScalarSubquery(subquery4),
        ScalarSubquery(subquery5),
        ScalarSubquery(subquery6))

    val hashAggregates = testRelation
      .groupBy('b)(
        max('a).as("max_a"),
        min('a).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), 'max_a,
        Literal("min_a"), 'min_a
      )).as("mergedValue"))
    val objectHashAggregates = testRelation
      .groupBy('b)(
        CollectList('a).toAggregateExpression(isDistinct = false).as("collectlist_a"),
        CollectSet('a).toAggregateExpression(isDistinct = false).as("collectset_a"))
      .select(CreateNamedStruct(Seq(
        Literal("collectlist_a"), 'collectlist_a,
        Literal("collectset_a"), 'collectset_a
      )).as("mergedValue"))
    val sortAggregates = testRelation
      .groupBy('b)(
        max('c).as("max_c"),
        min('c).as("min_c"))
      .select(CreateNamedStruct(Seq(
        Literal("max_c"), 'max_c,
        Literal("min_c"), 'min_c
      )).as("mergedValue"))
    val correctAnswer = testRelation
      .select(
        GetStructField(ScalarSubquery(hashAggregates), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(hashAggregates), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(objectHashAggregates), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(objectHashAggregates), 1).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(sortAggregates), 0).as("scalarsubquery()"),
        GetStructField(ScalarSubquery(sortAggregates), 1).as("scalarsubquery()"))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}

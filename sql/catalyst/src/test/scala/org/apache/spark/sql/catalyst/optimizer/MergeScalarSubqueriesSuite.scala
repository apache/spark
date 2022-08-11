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
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct, GetStructField, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectList, CollectSet}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class MergeScalarSubqueriesSuite extends PlanTest {

  override def beforeEach(): Unit = {
    CTERelationDef.curId.set(0)
  }

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("MergeScalarSubqueries", Once, MergeScalarSubqueries) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.string)

  private def definitionNode(plan: LogicalPlan, cteIndex: Int) = {
    CTERelationDef(plan, cteIndex, underSubquery = true)
  }

  private def extractorExpression(cteIndex: Int, output: Seq[Attribute], fieldIndex: Int) = {
    GetStructField(ScalarSubquery(CTERelationRef(cteIndex, _resolved = true, output)), fieldIndex)
      .as("scalarsubquery()")
  }

  test("Merging subqueries with projects") {
    val subquery1 = ScalarSubquery(testRelation.select(('a + 1).as("a_plus1")))
    val subquery2 = ScalarSubquery(testRelation.select(('a + 2).as("a_plus2")))
    val subquery3 = ScalarSubquery(testRelation.select('b))
    val subquery4 = ScalarSubquery(testRelation.select(('a + 1).as("a_plus1_2")))
    val subquery5 = ScalarSubquery(testRelation.select(('a + 2).as("a_plus2_2")))
    val subquery6 = ScalarSubquery(testRelation.select('b.as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4,
        subquery5,
        subquery6)

    val mergedSubquery = testRelation
      .select(
        ('a + 1).as("a_plus1"),
        ('a + 2).as("a_plus2"),
        'b)
      .select(
        CreateNamedStruct(Seq(
          Literal("a_plus1"), 'a_plus1,
          Literal("a_plus2"), 'a_plus2,
          Literal("b"), 'b
        )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1),
          extractorExpression(0, analyzedMergedSubquery.output, 2),
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1),
          extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with aggregates") {
    val subquery1 = ScalarSubquery(testRelation.groupBy('b)(max('a).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy('b)(sum('a).as("sum_a")))
    val subquery3 = ScalarSubquery(testRelation.groupBy('b)('b))
    val subquery4 = ScalarSubquery(testRelation.groupBy('b)(max('a).as("max_a_2")))
    val subquery5 = ScalarSubquery(testRelation.groupBy('b)(sum('a).as("sum_a_2")))
    val subquery6 = ScalarSubquery(testRelation.groupBy('b)('b.as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4,
        subquery5,
        subquery6)

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
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1),
          extractorExpression(0, analyzedMergedSubquery.output, 2),
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1),
          extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with aggregates with complex grouping expressions") {
    val subquery1 = ScalarSubquery(testRelation.groupBy('b > 1 && 'a === 2)(max('a).as("max_a")))
    val subquery2 = ScalarSubquery(
      testRelation
        .select('a, 'b.as("b_2"))
        .groupBy(Literal(2) === 'a && Literal(1) < 'b_2)(sum('a).as("sum_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val mergedSubquery = testRelation
      .select('a, 'b, 'c)
      .groupBy('b > 1 && 'a === 2)(
        max('a).as("max_a"),
        sum('a).as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), 'max_a,
        Literal("sum_a"), 'sum_a
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with aggregates with multiple grouping expressions") {
    // supports HashAggregate
    val subquery1 = ScalarSubquery(testRelation.groupBy('b, 'c)(max('a).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy('b, 'c)(min('a).as("min_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val hashAggregates = testRelation
      .groupBy('b, 'c)(
        max('a).as("max_a"),
        min('a).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), 'max_a,
        Literal("min_a"), 'min_a
      )).as("mergedValue"))
    val analyzedHashAggregates = hashAggregates.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedHashAggregates.output, 0),
          extractorExpression(0, analyzedHashAggregates.output, 1)),
      Seq(definitionNode(analyzedHashAggregates, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with filters") {
    val subquery1 = ScalarSubquery(testRelation.where('a > 1).select('a))
    // Despite having an extra Project node, `subquery2` is mergeable with `subquery1`
    val subquery2 = ScalarSubquery(testRelation.where('a > 1).select('b.as("b_1")).select('b_1))
    // Despite lacking a Project node, `subquery3` is mergeable with the result of merging
    // `subquery1` and `subquery2`
    val subquery3 = ScalarSubquery(testRelation.select('a.as("a_2")).where('a_2 > 1).select('a_2))
    val subquery4 = ScalarSubquery(
      testRelation.select('a.as("a_2"), 'b).where('a_2 > 1).select('b.as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4)

    val mergedSubquery = testRelation
      .select('a, 'b, 'c)
      .where('a > 1)
      .select('a, 'b, 'c)
      .select('a, 'b)
      .select(CreateNamedStruct(Seq(
        Literal("a"), 'a,
        Literal("b"), 'b
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1),
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with complex filter conditions") {
    val subquery1 = ScalarSubquery(testRelation.where('a > 1 && 'b === 2).select('a))
    val subquery2 = ScalarSubquery(
      testRelation
        .select('a.as("a_2"), 'b)
        .where(Literal(2) === 'b && Literal(1) < 'a_2)
        .select('b.as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val mergedSubquery = testRelation
      .select('a, 'b, 'c)
      .where('a > 1 && 'b === 2)
      .select('a, 'b.as("b_2"))
      .select(CreateNamedStruct(Seq(
        Literal("a"), 'a,
        Literal("b_2"), 'b_2
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Do not merge subqueries with different filter conditions") {
    val subquery1 = ScalarSubquery(testRelation.where('a > 1).select('a))
    val subquery2 = ScalarSubquery(testRelation.where('a < 1).select('a))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Merging subqueries with aggregate filters") {
    val subquery1 = ScalarSubquery(
      testRelation.having('b)(max('a).as("max_a"))(max('a) > 1))
    val subquery2 = ScalarSubquery(
      testRelation.having('b)(sum('a).as("sum_a"))(max('a) > 1))
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

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
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merging subqueries with joins") {
    val subquery1 = ScalarSubquery(testRelation.as("t1")
      .join(
        testRelation.as("t2"),
        Inner,
        Some($"t1.b" === $"t2.b"))
      .select($"t1.a").analyze)
    val subquery2 = ScalarSubquery(testRelation.as("t1")
      .select('a.as("a_1"), 'b.as("b_1"), 'c.as("c_1"))
      .join(
        testRelation.as("t2").select('a.as("a_2"), 'b.as("b_2"), 'c.as("c_2")),
        Inner,
        Some('b_1 === 'b_2))
      .select('c_2).analyze)
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation.as("t1")
      .select('a, 'b, 'c)
      .join(
        testRelation.as("t2").select('a, 'b, 'c),
        Inner,
        Some($"t1.b" === $"t2.b"))
      .select($"t1.a", $"t2.c")
      .select(CreateNamedStruct(Seq(
        Literal("a"), 'a,
        Literal("c"), 'c
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merge subqueries with complex join conditions") {
    val subquery1 = ScalarSubquery(testRelation.as("t1")
      .join(
        testRelation.as("t2"),
        Inner,
        Some($"t1.b" < $"t2.b" && $"t1.a" === $"t2.c"))
      .select($"t1.a").analyze)
    val subquery2 = ScalarSubquery(testRelation.as("t1")
      .select('a.as("a_1"), 'b.as("b_1"), 'c.as("c_1"))
      .join(
        testRelation.as("t2").select('a.as("a_2"), 'b.as("b_2"), 'c.as("c_2")),
        Inner,
        Some('c_2 === 'a_1 && 'b_1 < 'b_2))
      .select('c_2).analyze)
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation.as("t1")
      .select('a, 'b, 'c)
      .join(
        testRelation.as("t2").select('a, 'b, 'c),
        Inner,
        Some($"t1.b" < $"t2.b" && $"t1.a" === $"t2.c"))
      .select($"t1.a", $"t2.c")
      .select(CreateNamedStruct(Seq(
        Literal("a"), 'a,
        Literal("c"), 'c
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Do not merge subqueries with different join types") {
    val subquery1 = ScalarSubquery(testRelation.as("t1")
      .join(
        testRelation.as("t2"),
        Inner,
        Some($"t1.b" === $"t2.b"))
      .select($"t1.a"))
    val subquery2 = ScalarSubquery(testRelation.as("t1")
      .join(
        testRelation.as("t2"),
        LeftOuter,
        Some($"t1.b" === $"t2.b"))
      .select($"t1.a"))
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Do not merge subqueries with different join conditions") {
    val subquery1 = ScalarSubquery(testRelation.as("t1")
      .join(
        testRelation.as("t2"),
        Inner,
        Some($"t1.b" < $"t2.b"))
      .select($"t1.a"))
    val subquery2 = ScalarSubquery(testRelation.as("t1")
      .join(
        testRelation.as("t2"),
        Inner,
        Some($"t1.b" > $"t2.b"))
      .select($"t1.a"))
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Do not merge subqueries with nondeterministic elements") {
    val subquery1 = ScalarSubquery(testRelation.select(('a + rand(0)).as("rand_a")))
    val subquery2 = ScalarSubquery(testRelation.select(('b + rand(0)).as("rand_b")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)

    val subquery3 = ScalarSubquery(testRelation.where('a > rand(0)).select('a))
    val subquery4 = ScalarSubquery(testRelation.where('a > rand(0)).select('b))
    val originalQuery2 = testRelation
      .select(
        subquery3,
        subquery4)

    comparePlans(Optimize.execute(originalQuery2.analyze), originalQuery2.analyze)

    val subquery5 = ScalarSubquery(testRelation.groupBy()((max('a) + rand(0)).as("max_a")))
    val subquery6 = ScalarSubquery(testRelation.groupBy()((max('b) + rand(0)).as("max_b")))
    val originalQuery3 = testRelation
      .select(
        subquery5,
        subquery6)

    comparePlans(Optimize.execute(originalQuery3.analyze), originalQuery3.analyze)
  }

  test("Do not merge different aggregate implementations") {
    // supports HashAggregate
    val subquery1 = ScalarSubquery(testRelation.groupBy('b)(max('a).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy('b)(min('a).as("min_a")))

    // supports ObjectHashAggregate
    val subquery3 = ScalarSubquery(testRelation
      .groupBy('b)(CollectList('a).toAggregateExpression(isDistinct = false).as("collectlist_a")))
    val subquery4 = ScalarSubquery(testRelation
      .groupBy('b)(CollectSet('a).toAggregateExpression(isDistinct = false).as("collectset_a")))

    // supports SortAggregate
    val subquery5 = ScalarSubquery(testRelation.groupBy('b)(max('c).as("max_c")))
    val subquery6 = ScalarSubquery(testRelation.groupBy('b)(min('c).as("min_c")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4,
        subquery5,
        subquery6)

    val hashAggregates = testRelation
      .groupBy('b)(
        max('a).as("max_a"),
        min('a).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), 'max_a,
        Literal("min_a"), 'min_a
      )).as("mergedValue"))
    val analyzedHashAggregates = hashAggregates.analyze
    val objectHashAggregates = testRelation
      .groupBy('b)(
        CollectList('a).toAggregateExpression(isDistinct = false).as("collectlist_a"),
        CollectSet('a).toAggregateExpression(isDistinct = false).as("collectset_a"))
      .select(CreateNamedStruct(Seq(
        Literal("collectlist_a"), 'collectlist_a,
        Literal("collectset_a"), 'collectset_a
      )).as("mergedValue"))
    val analyzedObjectHashAggregates = objectHashAggregates.analyze
    val sortAggregates = testRelation
      .groupBy('b)(
        max('c).as("max_c"),
        min('c).as("min_c"))
      .select(CreateNamedStruct(Seq(
        Literal("max_c"), 'max_c,
        Literal("min_c"), 'min_c
      )).as("mergedValue"))
    val analyzedSortAggregates = sortAggregates.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedHashAggregates.output, 0),
          extractorExpression(0, analyzedHashAggregates.output, 1),
          extractorExpression(1, analyzedObjectHashAggregates.output, 0),
          extractorExpression(1, analyzedObjectHashAggregates.output, 1),
          extractorExpression(2, analyzedSortAggregates.output, 0),
          extractorExpression(2, analyzedSortAggregates.output, 1)),
        Seq(
          definitionNode(analyzedHashAggregates, 0),
          definitionNode(analyzedObjectHashAggregates, 1),
          definitionNode(analyzedSortAggregates, 2)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Do not merge subqueries with different aggregate grouping orders") {
    // supports HashAggregate
    val subquery1 = ScalarSubquery(testRelation.groupBy('b, 'c)(max('a).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy('c, 'b)(min('a).as("min_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Merging subqueries from different places") {
    val subquery1 = ScalarSubquery(testRelation.select(('a + 1).as("a_plus1")))
    val subquery2 = ScalarSubquery(testRelation.select(('a + 2).as("a_plus2")))
    val subquery3 = ScalarSubquery(testRelation.select('b))
    val subquery4 = ScalarSubquery(testRelation.select(('a + 1).as("a_plus1_2")))
    val subquery5 = ScalarSubquery(testRelation.select(('a + 2).as("a_plus2_2")))
    val subquery6 = ScalarSubquery(testRelation.select('b.as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3)
      .where(
        subquery4 +
        subquery5 +
        subquery6 === 0)

    val mergedSubquery = testRelation
      .select(
        ('a + 1).as("a_plus1"),
        ('a + 2).as("a_plus2"),
        'b)
      .select(
        CreateNamedStruct(Seq(
          Literal("a_plus1"), 'a_plus1,
          Literal("a_plus2"), 'a_plus2,
          Literal("b"), 'b
        )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1),
          extractorExpression(0, analyzedMergedSubquery.output, 2))
        .where(
          extractorExpression(0, analyzedMergedSubquery.output, 0) +
          extractorExpression(0, analyzedMergedSubquery.output, 1) +
          extractorExpression(0, analyzedMergedSubquery.output, 2) === 0),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}

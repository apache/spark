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

  val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").string)
  val testRelationWithNonBinaryCollation = LocalRelation(
    Symbol("ucs_basic").string("UCS_BASIC"),
    Symbol("ucs_basic_lcase").string("UCS_BASIC_LCASE"))

  private def definitionNode(plan: LogicalPlan, cteIndex: Int) = {
    CTERelationDef(plan, cteIndex, underSubquery = true)
  }

  private def extractorExpression(cteIndex: Int, output: Seq[Attribute], fieldIndex: Int) = {
    GetStructField(ScalarSubquery(
      CTERelationRef(cteIndex, _resolved = true, output, isStreaming = false)), fieldIndex)
      .as("scalarsubquery()")
  }

  test("Merging subqueries with projects") {
    val subquery1 = ScalarSubquery(testRelation.select((Symbol("a") + 1).as("a_plus1")))
    val subquery2 = ScalarSubquery(testRelation.select((Symbol("a") + 2).as("a_plus2")))
    val subquery3 = ScalarSubquery(testRelation.select(Symbol("b")))
    val subquery4 = ScalarSubquery(testRelation.select((Symbol("a") + 1).as("a_plus1_2")))
    val subquery5 = ScalarSubquery(testRelation.select((Symbol("a") + 2).as("a_plus2_2")))
    val subquery6 = ScalarSubquery(testRelation.select(Symbol("b").as("b_2")))
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
        (Symbol("a") + 1).as("a_plus1"),
        (Symbol("a") + 2).as("a_plus2"),
        Symbol("b"))
      .select(
        CreateNamedStruct(Seq(
          Literal("a_plus1"), Symbol("a_plus1"),
          Literal("a_plus2"), Symbol("a_plus2"),
          Literal("b"), Symbol("b")
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
    val subquery1 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(max(Symbol("a")).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(sum(Symbol("a")).as("sum_a")))
    val subquery3 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(Symbol("b")))
    val subquery4 = ScalarSubquery(
      testRelation.groupBy(Symbol("b"))(max(Symbol("a")).as("max_a_2")))
    val subquery5 = ScalarSubquery(
      testRelation.groupBy(Symbol("b"))(sum(Symbol("a")).as("sum_a_2")))
    val subquery6 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(Symbol("b").as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4,
        subquery5,
        subquery6)

    val mergedSubquery = testRelation
      .groupBy(Symbol("b"))(
        max(Symbol("a")).as("max_a"),
        sum(Symbol("a")).as("sum_a"),
        Symbol("b"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), Symbol("max_a"),
        Literal("sum_a"), Symbol("sum_a"),
        Literal("b"), Symbol("b")
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
    val subquery1 = ScalarSubquery(testRelation.groupBy(
      Symbol("b") > 1 && Symbol("a") === 2)(max(Symbol("a")).as("max_a")))
    val subquery2 = ScalarSubquery(
      testRelation
        .select(Symbol("a"), Symbol("b").as("b_2"))
        .groupBy(Literal(2) === Symbol("a") &&
          Literal(1) < Symbol("b_2"))(sum(Symbol("a")).as("sum_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val mergedSubquery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .groupBy(Symbol("b") > 1 && Symbol("a") === 2)(
        max(Symbol("a")).as("max_a"),
        sum(Symbol("a")).as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), Symbol("max_a"),
        Literal("sum_a"), Symbol("sum_a")
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
    val subquery1 = ScalarSubquery(testRelation.groupBy(Symbol("b"),
      Symbol("c"))(max(Symbol("a")).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy(Symbol("b"),
      Symbol("c"))(min(Symbol("a")).as("min_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val hashAggregates = testRelation
      .groupBy(Symbol("b"), Symbol("c"))(
        max(Symbol("a")).as("max_a"),
        min(Symbol("a")).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), Symbol("max_a"),
        Literal("min_a"), Symbol("min_a")
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

  test("Avoid merge when mixing sort and hash aggs") {
    val subquery1 = ScalarSubquery(testRelationWithNonBinaryCollation.groupBy(
      Symbol("ucs_basic"))(max(Symbol("ucs_basic")).as("max_ucs_basic")))
    val subquery2 = ScalarSubquery(testRelationWithNonBinaryCollation.groupBy(
      Symbol("ucs_basic_lcase"))(max(Symbol("ucs_basic_lcase")).as("ucs_basic_lcase")))
    val originalQuery = testRelationWithNonBinaryCollation.select(subquery1, subquery2)
    Optimize.execute(originalQuery.analyze).collect {
      case WithCTE(_, _) => fail("Should not have merged")
    }
  }

  test("Merging subqueries with filters") {
    val subquery1 = ScalarSubquery(testRelation.where(Symbol("a") > 1).select(Symbol("a")))
    // Despite having an extra Project node, `subquery2` is mergeable with `subquery1`
    val subquery2 = ScalarSubquery(testRelation.where(Symbol("a") > 1).select(
      Symbol("b").as("b_1")).select(Symbol("b_1")))
    // Despite lacking a Project node, `subquery3` is mergeable with the result of merging
    // `subquery1` and `subquery2`
    val subquery3 = ScalarSubquery(testRelation.select(
      Symbol("a").as("a_2")).where(Symbol("a_2") > 1).select(Symbol("a_2")))
    val subquery4 = ScalarSubquery(testRelation.select(
      Symbol("a").as("a_2"), Symbol("b")).where(Symbol("a_2") > 1).select(Symbol("b").as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4)

    val mergedSubquery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .where(Symbol("a") > 1)
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .select(Symbol("a"), Symbol("b"))
      .select(CreateNamedStruct(Seq(
        Literal("a"), Symbol("a"),
        Literal("b"), Symbol("b")
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
    val subquery1 = ScalarSubquery(
      testRelation.where(Symbol("a") > 1 && Symbol("b") === 2).select(Symbol("a")))
    val subquery2 = ScalarSubquery(
      testRelation
        .select(Symbol("a").as("a_2"), Symbol("b"))
        .where(Literal(2) === Symbol("b") && Literal(1) < Symbol("a_2"))
        .select(Symbol("b").as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val mergedSubquery = testRelation
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .where(Symbol("a") > 1 && Symbol("b") === 2)
      .select(Symbol("a"), Symbol("b").as("b_2"))
      .select(CreateNamedStruct(Seq(
        Literal("a"), Symbol("a"),
        Literal("b_2"), Symbol("b_2")
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
    val subquery1 = ScalarSubquery(testRelation.where(Symbol("a") > 1).select(Symbol("a")))
    val subquery2 = ScalarSubquery(testRelation.where(Symbol("a") < 1).select(Symbol("a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Merging subqueries with aggregate filters") {
    val subquery1 = ScalarSubquery(
      testRelation.having(Symbol("b"))(max(Symbol("a")).as("max_a"))(max(Symbol("a")) > 1))
    val subquery2 = ScalarSubquery(
      testRelation.having(Symbol("b"))(sum(Symbol("a")).as("sum_a"))(max(Symbol("a")) > 1))
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation
      .having(Symbol("b"))(
        max(Symbol("a")).as("max_a"),
        sum(Symbol("a")).as("sum_a"))(Symbol("max_a") > 1)
      .select(
        Symbol("max_a"),
        Symbol("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), Symbol("max_a"),
        Literal("sum_a"), Symbol("sum_a")
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
      .select(Symbol("a").as("a_1"), Symbol("b").as("b_1"), Symbol("c").as("c_1"))
      .join(
        testRelation.as("t2").select(Symbol("a").as("a_2"), Symbol("b").as("b_2"),
          Symbol("c").as("c_2")),
        Inner,
        Some(Symbol("b_1") === Symbol("b_2")))
      .select(Symbol("c_2")).analyze)
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation.as("t1")
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .join(
        testRelation.as("t2").select(Symbol("a"), Symbol("b"), Symbol("c")),
        Inner,
        Some($"t1.b" === $"t2.b"))
      .select($"t1.a", $"t2.c")
      .select(CreateNamedStruct(Seq(
        Literal("a"), Symbol("a"),
        Literal("c"), Symbol("c")
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
      .select(Symbol("a").as("a_1"), Symbol("b").as("b_1"), Symbol("c").as("c_1"))
      .join(
        testRelation.as("t2").select(Symbol("a").as("a_2"), Symbol("b").as("b_2"),
          Symbol("c").as("c_2")),
        Inner,
        Some(Symbol("c_2") === Symbol("a_1") && Symbol("b_1") < Symbol("b_2")))
      .select(Symbol("c_2")).analyze)
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation.as("t1")
      .select(Symbol("a"), Symbol("b"), Symbol("c"))
      .join(
        testRelation.as("t2").select(Symbol("a"), Symbol("b"), Symbol("c")),
        Inner,
        Some($"t1.b" < $"t2.b" && $"t1.a" === $"t2.c"))
      .select($"t1.a", $"t2.c")
      .select(CreateNamedStruct(Seq(
        Literal("a"), Symbol("a"),
        Literal("c"), Symbol("c")
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
    val subquery1 = ScalarSubquery(
      testRelation.select((Symbol("a") + rand(0)).as("rand_a")))
    val subquery2 = ScalarSubquery(
      testRelation.select((Symbol("b") + rand(0)).as("rand_b")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)

    val subquery3 = ScalarSubquery(
      testRelation.where(Symbol("a") > rand(0)).select(Symbol("a")))
    val subquery4 = ScalarSubquery(
      testRelation.where(Symbol("a") > rand(0)).select(Symbol("b")))
    val originalQuery2 = testRelation
      .select(
        subquery3,
        subquery4)

    comparePlans(Optimize.execute(originalQuery2.analyze), originalQuery2.analyze)

    val subquery5 = ScalarSubquery(
      testRelation.groupBy()((max(Symbol("a")) + rand(0)).as("max_a")))
    val subquery6 = ScalarSubquery(
      testRelation.groupBy()((max(Symbol("b")) + rand(0)).as("max_b")))
    val originalQuery3 = testRelation
      .select(
        subquery5,
        subquery6)

    comparePlans(Optimize.execute(originalQuery3.analyze), originalQuery3.analyze)
  }

  test("Do not merge different aggregate implementations") {
    // supports HashAggregate
    val subquery1 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(max(Symbol("a")).as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(min(Symbol("a")).as("min_a")))

    // supports ObjectHashAggregate
    val subquery3 = ScalarSubquery(testRelation
      .groupBy(Symbol("b"))(CollectList(Symbol("a")).
        toAggregateExpression(isDistinct = false).as("collectlist_a")))
    val subquery4 = ScalarSubquery(testRelation
      .groupBy(Symbol("b"))(CollectSet(Symbol("a")).
        toAggregateExpression(isDistinct = false).as("collectset_a")))

    // supports SortAggregate
    val subquery5 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(max(Symbol("c")).as("max_c")))
    val subquery6 = ScalarSubquery(testRelation.groupBy(Symbol("b"))(min(Symbol("c")).as("min_c")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4,
        subquery5,
        subquery6)

    val hashAggregates = testRelation
      .groupBy(Symbol("b"))(
        max(Symbol("a")).as("max_a"),
        min(Symbol("a")).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), Symbol("max_a"),
        Literal("min_a"), Symbol("min_a")
      )).as("mergedValue"))
    val analyzedHashAggregates = hashAggregates.analyze
    val objectHashAggregates = testRelation
      .groupBy(Symbol("b"))(
        CollectList(Symbol("a")).toAggregateExpression(isDistinct = false).as("collectlist_a"),
        CollectSet(Symbol("a")).toAggregateExpression(isDistinct = false).as("collectset_a"))
      .select(CreateNamedStruct(Seq(
        Literal("collectlist_a"), Symbol("collectlist_a"),
        Literal("collectset_a"), Symbol("collectset_a")
      )).as("mergedValue"))
    val analyzedObjectHashAggregates = objectHashAggregates.analyze
    val sortAggregates = testRelation
      .groupBy(Symbol("b"))(
        max(Symbol("c")).as("max_c"),
        min(Symbol("c")).as("min_c"))
      .select(CreateNamedStruct(Seq(
        Literal("max_c"), Symbol("max_c"),
        Literal("min_c"), Symbol("min_c")
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
    val subquery1 = ScalarSubquery(
      testRelation.groupBy(Symbol("b"), Symbol("c"))(max(Symbol("a")).as("max_a")))
    val subquery2 = ScalarSubquery(
      testRelation.groupBy(Symbol("c"), Symbol("b"))(min(Symbol("a")).as("min_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Merging subqueries from different places") {
    val subquery1 = ScalarSubquery(testRelation.select((Symbol("a") + 1).as("a_plus1")))
    val subquery2 = ScalarSubquery(testRelation.select((Symbol("a") + 2).as("a_plus2")))
    val subquery3 = ScalarSubquery(testRelation.select(Symbol("b")))
    val subquery4 = ScalarSubquery(testRelation.select((Symbol("a") + 1).as("a_plus1_2")))
    val subquery5 = ScalarSubquery(testRelation.select((Symbol("a") + 2).as("a_plus2_2")))
    val subquery6 = ScalarSubquery(testRelation.select(Symbol("b").as("b_2")))
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
        (Symbol("a") + 1).as("a_plus1"),
        (Symbol("a") + 2).as("a_plus2"),
        Symbol("b"))
      .select(
        CreateNamedStruct(Seq(
          Literal("a_plus1"), Symbol("a_plus1"),
          Literal("a_plus2"), Symbol("a_plus2"),
          Literal("b"), Symbol("b")
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

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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, CreateNamedStruct, GetStructField, If, Literal, Or, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

class MergeSubplansSuite extends PlanTest {

  override def beforeEach(): Unit = {
    CTERelationDef.curId.set(0)
    PlanMerger.curId.set(0)
  }

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("MergeSubplans", Once, MergeSubplans) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".string)
  val testRelationWithNonBinaryCollation = LocalRelation(
    $"utf8_binary".string("UTF8_BINARY"),
    $"utf8_lcase".string("UTF8_LCASE"))

  private def definitionNode(plan: LogicalPlan, cteIndex: Int) = {
    CTERelationDef(plan, cteIndex, underSubquery = true)
  }

  private def extractorExpression(
      cteIndex: Int,
      output: Seq[Attribute],
      fieldIndex: Int,
      alias: String = "scalarsubquery()") = {
    GetStructField(
      ScalarSubquery(CTERelationRef(cteIndex, _resolved = true, output, isStreaming = false)),
      fieldIndex)
      .as(alias)
  }

  test("Merging subqueries with projects") {
    val subquery1 = ScalarSubquery(testRelation.select(($"a" + 1).as("a_plus1")))
    val subquery2 = ScalarSubquery(testRelation.select(($"a" + 2).as("a_plus2")))
    val subquery3 = ScalarSubquery(testRelation.select($"b"))
    val subquery4 = ScalarSubquery(testRelation.select(($"a" + 1).as("a_plus1_2")))
    val subquery5 = ScalarSubquery(testRelation.select(($"a" + 2).as("a_plus2_2")))
    val subquery6 = ScalarSubquery(testRelation.select($"b".as("b_2")))
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
        ($"a" + 1).as("a_plus1"),
        ($"a" + 2).as("a_plus2"),
        $"b")
      .select(
        CreateNamedStruct(Seq(
          Literal("a_plus1"), $"a_plus1",
          Literal("a_plus2"), $"a_plus2",
          Literal("b"), $"b"
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
    val subquery1 = ScalarSubquery(testRelation.groupBy($"b")(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy($"b")(sum($"a").as("sum_a")))
    val subquery3 = ScalarSubquery(testRelation.groupBy($"b")($"b"))
    val subquery4 = ScalarSubquery(testRelation.groupBy($"b")(max($"a").as("max_a_2")))
    val subquery5 = ScalarSubquery(testRelation.groupBy($"b")(sum($"a").as("sum_a_2")))
    val subquery6 = ScalarSubquery(testRelation.groupBy($"b")($"b".as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4,
        subquery5,
        subquery6)

    val mergedSubquery = testRelation
      .groupBy($"b")(
        max($"a").as("max_a"),
        sum($"a").as("sum_a"),
        $"b")
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("sum_a"), $"sum_a",
        Literal("b"), $"b"
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
    val subquery1 = ScalarSubquery(
      testRelation.groupBy($"b" > 1 && $"a" === 2)(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation
      .select($"a", $"b".as("b_2"))
      .groupBy(Literal(2) === $"a" && Literal(1) < $"b_2")(sum($"a").as("sum_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val mergedSubquery = testRelation
      .select($"a", $"b", $"c")
      .groupBy($"b" > 1 && $"a" === 2)(
        max($"a").as("max_a"),
        sum($"a").as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("sum_a"), $"sum_a"
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
    val subquery1 = ScalarSubquery(testRelation.groupBy($"b", $"c")(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy($"b", $"c")(min($"a").as("min_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val hashAggregates = testRelation
      .groupBy($"b", $"c")(
        max($"a").as("max_a"),
        min($"a").as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
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
    val subquery1 = ScalarSubquery(testRelationWithNonBinaryCollation
      .groupBy($"utf8_binary")(max($"utf8_binary").as("max_utf8_binary")))
    val subquery2 = ScalarSubquery(testRelationWithNonBinaryCollation
      .groupBy($"utf8_lcase")(max($"utf8_lcase").as("utf8_lcase")))
    val originalQuery = testRelationWithNonBinaryCollation.select(subquery1, subquery2)
    Optimize.execute(originalQuery.analyze).collect {
      case WithCTE(_, _) => fail("Should not have merged")
    }
  }

  test("Merging subqueries with filters") {
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).select($"a"))
    // Despite having an extra Project node, `subquery2` is mergeable with `subquery1`
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).select($"b".as("b_1")).select($"b_1"))
    // Despite lacking a Project node, `subquery3` is mergeable with the result of merging
    // `subquery1` and `subquery2`
    val subquery3 = ScalarSubquery(
      testRelation.select($"a".as("a_2")).where($"a_2" > 1).select($"a_2"))
    val subquery4 = ScalarSubquery(
      testRelation.select($"a".as("a_2"), $"b").where($"a_2" > 1).select($"b".as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4)

    val mergedSubquery = testRelation
      .select($"a", $"b", $"c")
      .where($"a" > 1)
      .select($"a", $"b", $"c")
      .select($"a", $"b")
      .select(CreateNamedStruct(Seq(
        Literal("a"), $"a",
        Literal("b"), $"b"
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
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1 && $"b" === 2).select($"a"))
    val subquery2 = ScalarSubquery(testRelation
      .select($"a".as("a_2"), $"b")
      .where(Literal(2) === $"b" && Literal(1) < $"a_2")
      .select($"b".as("b_2")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    val mergedSubquery = testRelation
      .select($"a", $"b", $"c")
      .where($"a" > 1 && $"b" === 2)
      .select($"a", $"b".as("b_2"))
      .select(CreateNamedStruct(Seq(
        Literal("a"), $"a",
        Literal("b_2"), $"b_2"
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
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).select($"a"))
    val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).select($"a"))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Merging subqueries with aggregate filters") {
    val subquery1 = ScalarSubquery(
      testRelation.having($"b")(max($"a").as("max_a"))(max($"a") > 1))
    val subquery2 = ScalarSubquery(
      testRelation.having($"b")(sum($"a").as("sum_a"))(max($"a") > 1))
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation
      .having($"b")(
        max($"a").as("max_a"),
        sum($"a").as("sum_a"))($"max_a" > 1)
      .select(
        $"max_a",
        $"sum_a")
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("sum_a"), $"sum_a"
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
      .select($"a".as("a_1"), $"b".as("b_1"), $"c".as("c_1"))
      .join(
        testRelation.as("t2").select($"a".as("a_2"), $"b".as("b_2"), $"c".as("c_2")),
        Inner,
        Some($"b_1" === $"b_2"))
      .select($"c_2").analyze)
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation.as("t1")
      .join(testRelation.as("t2"), Inner, Some($"t1.b" === $"t2.b"))
      .select($"t1.a", $"t2.c")
      .select(CreateNamedStruct(Seq(
        Literal("a"), $"a",
        Literal("c"), $"c"
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
      .select($"a".as("a_1"), $"b".as("b_1"), $"c".as("c_1"))
      .join(
        testRelation.as("t2").select($"a".as("a_2"), $"b".as("b_2"), $"c".as("c_2")),
        Inner,
        Some($"c_2" === $"a_1" && $"b_1" < $"b_2"))
      .select($"c_2").analyze)
    val originalQuery = testRelation.select(
      subquery1,
      subquery2)

    val mergedSubquery = testRelation.as("t1")
      .join(testRelation.as("t2"), Inner, Some($"t1.b" < $"t2.b" && $"t1.a" === $"t2.c"))
      .select($"t1.a", $"t2.c")
      .select(CreateNamedStruct(Seq(
        Literal("a"), $"a",
        Literal("c"), $"c"
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
    val subquery1 = ScalarSubquery(testRelation.select(($"a" + rand(0)).as("rand_a")))
    val subquery2 = ScalarSubquery(testRelation.select(($"b" + rand(0)).as("rand_b")))
    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)

    val subquery3 = ScalarSubquery(testRelation.where($"a" > rand(0)).select($"a"))
    val subquery4 = ScalarSubquery(testRelation.where($"a" > rand(0)).select($"b"))
    val originalQuery2 = testRelation
      .select(
        subquery3,
        subquery4)

    comparePlans(Optimize.execute(originalQuery2.analyze), originalQuery2.analyze)

    val subquery5 = ScalarSubquery(testRelation.groupBy()((max($"a") + rand(0)).as("max_a")))
    val subquery6 = ScalarSubquery(testRelation.groupBy()((max($"b") + rand(0)).as("max_b")))
    val originalQuery3 = testRelation
      .select(
        subquery5,
        subquery6)

    comparePlans(Optimize.execute(originalQuery3.analyze), originalQuery3.analyze)
  }

  test("Do not merge different aggregate implementations") {
    // supports HashAggregate
    val subquery1 = ScalarSubquery(testRelation.groupBy($"b")(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy($"b")(min($"a").as("min_a")))

    // supports ObjectHashAggregate
    val subquery3 = ScalarSubquery(testRelation
      .groupBy($"b")(collectList($"a").as("collectlist_a")))
    val subquery4 = ScalarSubquery(testRelation
      .groupBy($"b")(collectSet($"a").as("collectset_a")))

    // supports SortAggregate
    val subquery5 = ScalarSubquery(testRelation.groupBy($"b")(max($"c").as("max_c")))
    val subquery6 = ScalarSubquery(testRelation.groupBy($"b")(min($"c").as("min_c")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2,
        subquery3,
        subquery4,
        subquery5,
        subquery6)

    val hashAggregates = testRelation
      .groupBy($"b")(
        max($"a").as("max_a"),
        min($"a").as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedHashAggregates = hashAggregates.analyze
    val objectHashAggregates = testRelation
      .groupBy($"b")(
        collectList($"a").as("collectlist_a"),
        collectSet($"a").as("collectset_a"))
      .select(CreateNamedStruct(Seq(
        Literal("collectlist_a"), $"collectlist_a",
        Literal("collectset_a"), $"collectset_a"
      )).as("mergedValue"))
    val analyzedObjectHashAggregates = objectHashAggregates.analyze
    val sortAggregates = testRelation
      .groupBy($"b")(
        max($"c").as("max_c"),
        min($"c").as("min_c"))
      .select(CreateNamedStruct(Seq(
        Literal("max_c"), $"max_c",
        Literal("min_c"), $"min_c"
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
    val subquery1 = ScalarSubquery(testRelation.groupBy($"b", $"c")(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy($"c", $"b")(min($"a").as("min_a")))

    val originalQuery = testRelation
      .select(
        subquery1,
        subquery2)

    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("Merging subqueries from different places") {
    val subquery1 = ScalarSubquery(testRelation.select(($"a" + 1).as("a_plus1")))
    val subquery2 = ScalarSubquery(testRelation.select(($"a" + 2).as("a_plus2")))
    val subquery3 = ScalarSubquery(testRelation.select($"b"))
    val subquery4 = ScalarSubquery(testRelation.select(($"a" + 1).as("a_plus1_2")))
    val subquery5 = ScalarSubquery(testRelation.select(($"a" + 2).as("a_plus2_2")))
    val subquery6 = ScalarSubquery(testRelation.select($"b".as("b_2")))
    val originalQuery = testRelation
      .where(
        subquery4 +
        subquery5 +
        subquery6 === 0)
      .select(
        subquery1,
        subquery2,
        subquery3)

    val mergedSubquery = testRelation
      .select(
        ($"a" + 1).as("a_plus1"),
        ($"a" + 2).as("a_plus2"),
        $"b")
      .select(
        CreateNamedStruct(Seq(
          Literal("a_plus1"), $"a_plus1",
          Literal("a_plus2"), $"a_plus2",
          Literal("b"), $"b"
        )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .where(
          extractorExpression(0, analyzedMergedSubquery.output, 0) +
          extractorExpression(0, analyzedMergedSubquery.output, 1) +
          extractorExpression(0, analyzedMergedSubquery.output, 2) === 0)
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1),
          extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merge aggregates") {
    val agg1 = testRelation.groupBy()(min($"a").as("min_a"))
    val agg2 = testRelation.groupBy()(max($"a").as("max_a"))
    val originalQuery = agg1.join(agg2)

    val mergedSubquery = testRelation
      .groupBy()(
        min($"a").as("min_a"),
        max($"a").as("max_a")
      )
      .select(
        CreateNamedStruct(Seq(
          Literal("min_a"), $"min_a",
          Literal("max_a"), $"max_a"
        )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      OneRowRelation().select(extractorExpression(0, analyzedMergedSubquery.output, 0, "min_a"))
        .join(
          OneRowRelation()
            .select(extractorExpression(0, analyzedMergedSubquery.output, 1, "max_a"))),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merge non-siblig aggregates") {
    val agg1 = testRelation.groupBy()(min($"a").as("min_a"))
    val agg2 = testRelation.groupBy()(max($"a").as("max_a"))
    val originalQuery = agg1.join(testRelation).join(agg2)

    val mergedSubquery = testRelation
      .groupBy()(
        min($"a").as("min_a"),
        max($"a").as("max_a")
      )
      .select(
        CreateNamedStruct(Seq(
          Literal("min_a"), $"min_a",
          Literal("max_a"), $"max_a"
        )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      OneRowRelation().select(extractorExpression(0, analyzedMergedSubquery.output, 0, "min_a"))
        .join(testRelation)
        .join(
          OneRowRelation()
            .select(extractorExpression(0, analyzedMergedSubquery.output, 1, "max_a"))),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merge subqueries and aggregates") {
    val subquery1 = ScalarSubquery(testRelation.groupBy()(min($"a").as("min_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy()(max($"a").as("max_a")))
    val agg1 = testRelation.groupBy()(sum($"a").as("sum_a"))
    val agg2 = testRelation.groupBy()(avg($"a").as("avg_a"))
    val originalQuery =
      testRelation
        .select(
          subquery1,
          subquery2)
        .join(agg1)
        .join(agg2)

    val mergedSubquery = testRelation
      .groupBy()(
        min($"a").as("min_a"),
        max($"a").as("max_a"),
        sum($"a").as("sum_a"),
        avg($"a").as("avg_a")
      )
      .select(
        CreateNamedStruct(Seq(
          Literal("min_a"), $"min_a",
          Literal("max_a"), $"max_a",
          Literal("sum_a"), $"sum_a",
          Literal("avg_a"), $"avg_a"
        )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1))
        .join(
          OneRowRelation()
            .select(extractorExpression(0, analyzedMergedSubquery.output, 2, "sum_a")))
        .join(
          OneRowRelation()
            .select(extractorExpression(0, analyzedMergedSubquery.output, 3, "avg_a"))),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Merge identical subqueries and aggregates") {
    val subquery1 = ScalarSubquery(testRelation.groupBy()(min($"a").as("min_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy()(min($"a").as("min_a_2")))
    val agg1 = testRelation.groupBy()(min($"a").as("min_a_3"))
    val agg2 = testRelation.groupBy()(min($"a").as("min_a_4"))
    val originalQuery =
      testRelation
        .select(
          subquery1,
          subquery2)
        .join(agg1)
        .join(agg2)

    val mergedSubquery = testRelation
      .groupBy()(min($"a").as("min_a"))
      .select(
        CreateNamedStruct(Seq(Literal("min_a"), $"min_a")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation
        .select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 0))
        .join(
          OneRowRelation()
            .select(extractorExpression(0, analyzedMergedSubquery.output, 0, "min_a_3")))
        .join(
          OneRowRelation()
            .select(extractorExpression(0, analyzedMergedSubquery.output, 0, "min_a_4"))),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("SPARK-40193: Merge non-grouping subqueries with different filter conditions") {
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val npFilterAlias = Alias($"a" < 1, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        max($"a", Some(cpFilter)).as("max_a"),
        min($"a", Some(npFilter)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge three non-grouping subqueries with different filter conditions") {
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a")))
    val subquery3 = ScalarSubquery(testRelation.where($"a" === 1).groupBy()(sum($"a").as("sum_a")))
    val originalQuery = testRelation.select(subquery1, subquery2, subquery3)

    // Step 1: subquery1 (cp) and subquery2 (np) merge:
    //   f0 = Alias(a < 1, "propagatedFilter_0")  -- np / min
    //   f1 = Alias(a > 1, "propagatedFilter_1")  -- cp / max
    //   -> Project([a,b,c, f0Alias, f1Alias], testRelation)
    //   -> Filter(OR(f0, f1), above)  [tagged]
    //   propagates (Some(f0), Some(f1)) upward
    //
    // Step 2: subquery3 (np) merges with merged(1,2) (cp). The cp Filter is tagged, so only a
    // new np alias is created and flattened into the existing Project (no nested Projects):
    //   f2 = Alias(a === 1, "propagatedFilter_2")  -- np / sum
    //   -> Project([a,b,c, f0Alias, f1Alias, f2Alias], testRelation)
    //   -> Filter(OR(OR(f0, f1), f2), above)  [tagged]
    //   propagates (Some(f2), None) upward
    //
    // Aggregate: cp agg expressions already carry their FILTERs from step 1 and are unchanged.
    //   max(a) FILTER f1  -- a > 1
    //   min(a) FILTER f0  -- a < 1
    //   sum(a) FILTER f2  -- a === 1
    val npFilter0Alias = Alias($"a" < 1, "propagatedFilter_0")()
    val cpFilter0Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter0 = npFilter0Alias.toAttribute
    val cpFilter0 = cpFilter0Alias.toAttribute
    val npFilter1Alias = Alias($"a" === 1, "propagatedFilter_2")()
    val npFilter1 = npFilter1Alias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(npFilter0Alias, cpFilter0Alias, npFilter1Alias): _*)
      .where(Or(Or(npFilter0, cpFilter0), npFilter1))
      .groupBy()(
        max($"a", Some(cpFilter0)).as("max_a"),
        min($"a", Some(npFilter0)).as("min_a"),
        sum($"a", Some(npFilter1)).as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a",
        Literal("sum_a"), $"sum_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1),
        extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge three non-grouping subqueries where the third has the same filter " +
    "condition as the first") {
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a")))
    val subquery3 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(sum($"a").as("sum_a")))
    val originalQuery = testRelation.select(subquery1, subquery2, subquery3)

    // Step 1: subquery1 (cp) and subquery2 (np) merge as usual:
    //   f0 = Alias(a < 1, "propagatedFilter_0")  -- np / min
    //   f1 = Alias(a > 1, "propagatedFilter_1")  -- cp / max
    //   -> Project([a,b,c, f0Alias, f1Alias], testRelation)
    //   -> Filter(OR(f0, f1), above)  [tagged]
    //
    // Step 2: subquery3 (np, condition a > 1) merges with merged(1,2) (cp). The cp Filter is
    // tagged and (a > 1) is already aliased as f1 in the child Project, so f1 is reused and no
    // new alias or extended OR condition is created. Only sum(a) FILTER f1 is added to the agg.
    val f0Alias = Alias($"a" < 1, "propagatedFilter_0")()
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f0 = f0Alias.toAttribute
    val f1 = f1Alias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias, f1Alias): _*)
      .where(Or(f0, f1))
      .groupBy()(
        max($"a", Some(f1)).as("max_a"),
        min($"a", Some(f0)).as("min_a"),
        sum($"a", Some(f1)).as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a",
        Literal("sum_a"), $"sum_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1),
        extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Do not merge non-grouping subqueries with different filter conditions when " +
    "disabled") {
    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "false") {
      val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
      val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a")))
      val originalQuery = testRelation.select(subquery1, subquery2)

      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-40193: Do not merge non-grouping subqueries with different filter conditions on " +
    "both sides when symmetric filter propagation is disabled") {
    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false") {
      val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
      val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a")))
      val originalQuery = testRelation.select(subquery1, subquery2)

      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping aggregates with different filter conditions") {
    val agg1 = testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a"))
    val agg2 = testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a"))
    val originalQuery = agg1.join(agg2)

    val npFilterAlias = Alias($"a" < 1, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        max($"a", Some(cpFilter)).as("max_a"),
        min($"a", Some(npFilter)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      OneRowRelation()
        .select(extractorExpression(0, analyzedMergedSubquery.output, 0, "max_a"))
        .join(
          OneRowRelation()
            .select(extractorExpression(0, analyzedMergedSubquery.output, 1, "min_a"))),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Do not merge grouping aggregates with different filter conditions") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"a" > 1).groupBy($"b")(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"a" < 1).groupBy($"b")(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries where only the new plan has a filter") {
    val subquery1 = ScalarSubquery(testRelation.groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val npFilterAlias = Alias($"a" < 1, "propagatedFilter_0")()
    val npFilter = npFilterAlias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(npFilterAlias): _*)
      .groupBy()(
        max($"a").as("max_a"),
        min($"a", Some(npFilter)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("SPARK-40193: Merge non-grouping subqueries where only the cached plan has a filter") {
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_0")()
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(cpFilterAlias): _*)
      .groupBy()(
        max($"a", Some(cpFilter)).as("max_a"),
        min($"a").as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("SPARK-40193: Merge non-grouping subqueries with multiple stacked filter conditions") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"a" > 1).where($"b" > 2).groupBy()(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"a" < 1).where($"b" < 2).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Inner level - (np: Filter(a < 1), cp: Filter(a > 1)):
    //   f0 = Alias(a < 1, "propagatedFilter_0")  -- np / min
    //   f1 = Alias(a > 1, "propagatedFilter_1")  -- cp / max
    //   -> Project([a,b,c, f0_alias, f1_alias], testRelation)
    //   -> Filter(OR(f0, f1), above)  [tagged]
    //   propagates (Some(f0), Some(f1)) upward
    //
    // Outer level - (np: Filter(b < 2), cp: Filter(b > 2)):
    //   f2 = Alias(AND(f0, b < 2), "propagatedFilter_2")  -- np
    //   f3 = Alias(AND(f1, b > 2), "propagatedFilter_3")  -- cp
    //   -> Project([a,b,c, f0, f1, f2_alias, f3_alias], innerFilter)
    //   -> Filter(OR(f2, f3), above)  [tagged]
    //   propagates (Some(f2), Some(f3)) upward
    //
    // Aggregate consumes f2/f3 as FILTER clauses:
    //   max(a) FILTER f3  -- AND(a > 1, b > 2)
    //   min(a) FILTER f2  -- AND(a < 1, b < 2)
    val f0Alias = Alias($"a" < 1, "propagatedFilter_0")()
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f0 = f0Alias.toAttribute
    val f1 = f1Alias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias, f1Alias): _*)
    val innerFilter = innerProject.where(Or(f0, f1))
    val f2Alias = Alias(And(f0, $"b" < 2), "propagatedFilter_2")()
    val f3Alias = Alias(And(f1, $"b" > 2), "propagatedFilter_3")()
    val f2 = f2Alias.toAttribute
    val f3 = f3Alias.toAttribute
    val mergedSubquery = innerFilter
      .select(innerFilter.output ++ Seq(f2Alias, f3Alias): _*)
      .where(Or(f2, f3))
      .groupBy()(
        max($"a", Some(f3)).as("max_a"),
        min($"a", Some(f2)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries where the new plan has more filter layers") {
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"a" < 1).where($"b" < 2).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Inner level - (np: Filter(a < 1), cp: testRelation):
    //   cp has no filter -> (np: Filter, cp) case. No Filter node needed.
    //   f0 = Alias(a < 1, "propagatedFilter_0")
    //   -> Project([a, b, c, f0Alias], testRelation)
    //   propagates (Some(f0), None) upward
    //
    // Outer level - (np: Filter(b < 2), cp: Filter(a > 1)):
    //   Both are Filters. Child result has (npFilter=Some(f0), cpFilter=None).
    //   f1 = Alias(AND(f0, b < 2), "propagatedFilter_1")  -- np combined condition
    //   f2 = Alias(a > 1,          "propagatedFilter_2")  -- cp condition
    //   -> Project([a, b, c, f0, f1Alias, f2Alias], innerProject)
    //   -> Filter(OR(f1, f2), above)  [tagged]
    //   propagates (Some(f1), Some(f2)) upward
    //
    // Aggregate:
    //   max(a) FILTER f2  -- cp: a > 1
    //   min(a) FILTER f1  -- np: a < 1 AND b < 2
    val f0Alias = Alias($"a" < 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias): _*)
    val f1Alias = Alias(And(f0, $"b" < 2), "propagatedFilter_1")()
    val f2Alias = Alias($"a" > 1, "propagatedFilter_2")()
    val f1 = f1Alias.toAttribute
    val f2 = f2Alias.toAttribute
    val mergedSubquery = innerProject
      .select(innerProject.output ++ Seq(f1Alias, f2Alias): _*)
      .where(Or(f1, f2))
      .groupBy()(
        max($"a", Some(f2)).as("max_a"),
        min($"a", Some(f1)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries where the cached plan has more filter layers") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"a" > 1).where($"b" > 2).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"a" < 1).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Inner level - (np: testRelation, cp: Filter(a > 1)):
    //   np has no filter -> (np, cp: Filter) case. No Filter node needed.
    //   f0 = Alias(a > 1, "propagatedFilter_0")
    //   -> Project([a, b, c, f0Alias], testRelation)
    //   propagates (None, Some(f0)) upward
    //
    // Outer level - (np: Filter(a < 1), cp: Filter(b > 2)):
    //   Both are Filters. Child result has (npFilter=None, cpFilter=Some(f0)).
    //   f1 = Alias(a < 1,          "propagatedFilter_1")  -- np condition
    //   f2 = Alias(AND(f0, b > 2), "propagatedFilter_2")  -- cp combined condition
    //   -> Project([a, b, c, f0, f1Alias, f2Alias], innerProject)
    //   -> Filter(OR(f1, f2), above)  [tagged]
    //   propagates (Some(f1), Some(f2)) upward
    //
    // Aggregate:
    //   max(a) FILTER f2  -- cp: a > 1 AND b > 2
    //   min(a) FILTER f1  -- np: a < 1
    val f0Alias = Alias($"a" > 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias): _*)
    val f1Alias = Alias($"a" < 1, "propagatedFilter_1")()
    val f2Alias = Alias(And(f0, $"b" > 2), "propagatedFilter_2")()
    val f1 = f1Alias.toAttribute
    val f2 = f2Alias.toAttribute
    val mergedSubquery = innerProject
      .select(innerProject.output ++ Seq(f1Alias, f2Alias): _*)
      .where(Or(f1, f2))
      .groupBy()(
        max($"a", Some(f2)).as("max_a"),
        min($"a", Some(f1)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries with equal outer stacked filter") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"a" > 1).where($"b" > 2).groupBy()(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"a" < 1).where($"b" > 2).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Inner level - (np: Filter(a < 1), cp: Filter(a > 1)):
    //   Different conditions -> first-time filter propagation.
    //   f0 = Alias(a < 1, "propagatedFilter_0")  -- np
    //   f1 = Alias(a > 1, "propagatedFilter_1")  -- cp
    //   -> Project([a, b, c, f0Alias, f1Alias], testRelation)
    //   -> Filter(OR(f0, f1))  [tagged]
    //   propagates (Some(f0), Some(f1)) upward
    //
    // Outer level - (np: Filter(b > 2), cp: Filter(b > 2)):
    //   Equal conditions -> Filter(b > 2, ...) passes filter attrs through.
    //   propagates (Some(f0), Some(f1)) unchanged
    //
    // Aggregate:
    //   max(a) FILTER f1  -- cp: a > 1 (plus the outer b > 2 applied to all rows)
    //   min(a) FILTER f0  -- np: a < 1 (plus the outer b > 2 applied to all rows)
    val f0Alias = Alias($"a" < 1, "propagatedFilter_0")()
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f0 = f0Alias.toAttribute
    val f1 = f1Alias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias, f1Alias): _*)
    val innerFilter = innerProject.where(Or(f0, f1))
    val mergedSubquery = innerFilter
      .where($"b" > 2)
      .groupBy()(
        max($"a", Some(f1)).as("max_a"),
        min($"a", Some(f0)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries with equal inner stacked filter") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"a" > 1).where($"b" > 2).groupBy()(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"a" > 1).where($"b" < 2).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Inner level - (np: Filter(a > 1), cp: Filter(a > 1)):
    //   checkIdenticalPlans matches -> no filter propagation needed.
    //   -> Filter(a > 1, testRelation)  (shared, unchanged)
    //   propagates (None, None) upward
    //
    // Outer level - (np: Filter(b < 2), cp: Filter(b > 2)):
    //   Different conditions -> first-time filter propagation.
    //   f0 = Alias(b < 2, "propagatedFilter_0")  -- np
    //   f1 = Alias(b > 2, "propagatedFilter_1")  -- cp
    //   -> Project([a, b, c, f0Alias, f1Alias], Filter(a > 1, testRelation))
    //   -> Filter(OR(f0, f1))  [tagged]
    //   propagates (Some(f0), Some(f1)) upward
    //
    // Aggregate:
    //   max(a) FILTER f1  -- cp: a > 1 AND b > 2
    //   min(a) FILTER f0  -- np: a > 1 AND b < 2
    val f0Alias = Alias($"b" < 2, "propagatedFilter_0")()
    val f1Alias = Alias($"b" > 2, "propagatedFilter_1")()
    val f0 = f0Alias.toAttribute
    val f1 = f1Alias.toAttribute
    val innerFilter = testRelation.where($"a" > 1)
    val mergedSubquery = innerFilter
      .select(innerFilter.output ++ Seq(f0Alias, f1Alias): _*)
      .where(Or(f0, f1))
      .groupBy()(
        max($"a", Some(f1)).as("max_a"),
        min($"a", Some(f0)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries where the new plan has an extra inner filter " +
      "below a shared outer filter") {
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"b" < 2).where($"a" > 1).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Inner level - (np: Filter(b < 2), cp: testRelation):
    //   cp has no filter -> (np: Filter, cp) case. No Filter node needed.
    //   f0 = Alias(b < 2, "propagatedFilter_0")
    //   -> Project([a, b, c, f0Alias], testRelation)
    //   propagates (Some(f0), None) upward
    //
    // Outer level - (np: Filter(a > 1), cp: Filter(a > 1)):
    //   Equal conditions -> just wraps with Filter(a > 1, ...) and passes filter attrs through.
    //   propagates (Some(f0), None) unchanged
    //
    // Aggregate:
    //   max(a) unfiltered  -- cp: all rows where a > 1 (from outer Filter)
    //   min(a) FILTER f0   -- np: rows where a > 1 AND b < 2
    val f0Alias = Alias($"b" < 2, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias): _*)
    val mergedSubquery = innerProject
      .where($"a" > 1)
      .groupBy()(
        max($"a").as("max_a"),
        min($"a", Some(f0)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("SPARK-40193: Merge non-grouping subqueries where the cached plan has an extra inner " +
      "filter below a shared outer filter") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"b" < 2).where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Inner level - (np: testRelation, cp: Filter(b < 2)):
    //   np has no filter -> (np, cp: Filter) case. No Filter node needed.
    //   f0 = Alias(b < 2, "propagatedFilter_0")
    //   -> Project([a, b, c, f0Alias], testRelation)
    //   propagates (None, Some(f0)) upward
    //
    // Outer level - (np: Filter(a > 1), cp: Filter(a > 1)):
    //   Equal conditions -> just wraps with Filter(a > 1, ...) and passes filter attrs through.
    //   propagates (None, Some(f0)) unchanged
    //
    // Aggregate:
    //   max(a) FILTER f0   -- cp: rows where a > 1 AND b < 2
    //   min(a) unfiltered  -- np: all rows where a > 1 (from outer Filter)
    val f0Alias = Alias($"b" < 2, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias): _*)
    val mergedSubquery = innerProject
      .where($"a" > 1)
      .groupBy()(
        max($"a", Some(f0)).as("max_a"),
        min($"a").as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("SPARK-40193: Merge non-grouping subqueries with equal conditions in reversed filter " +
      "order") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"a" > 1).where($"b" > 2).groupBy()(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"b" > 2).where($"a" > 1).groupBy()(min($"a").as("min_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge traversal (inner-to-outer):
    //
    // Because the conditions are in opposite order, each pair of Filter nodes has different
    // conditions and filter propagation is triggered at both levels, producing 4 filter
    // attributes in total even though both sides ultimately encode a > 1 AND b > 2.
    //
    // Inner level - (np: Filter(b > 2), cp: Filter(a > 1)):
    //   f0 = Alias(b > 2, "propagatedFilter_0")  -- np inner condition
    //   f1 = Alias(a > 1, "propagatedFilter_1")  -- cp inner condition
    //   -> Project([a, b, c, f0Alias, f1Alias], testRelation)
    //   -> Filter(OR(f0, f1))  [tagged]
    //   propagates (Some(f0), Some(f1)) upward
    //
    // Outer level - (np: Filter(a > 1), cp: Filter(b > 2)):
    //   f2 = Alias(AND(f0, a > 1), "propagatedFilter_2")  -- np: b > 2 AND a > 1
    //   f3 = Alias(AND(f1, b > 2), "propagatedFilter_3")  -- cp: a > 1 AND b > 2
    //   -> Project([a, b, c, f0, f1, f2Alias, f3Alias], innerFilter)
    //   -> Filter(OR(f2, f3))  [tagged]
    //   propagates (Some(f2), Some(f3)) upward
    //
    // Aggregate:
    //   max(a) FILTER f3  -- cp: a > 1 AND b > 2
    //   min(a) FILTER f2  -- np: b > 2 AND a > 1  (same predicate, different representation)
    val f0Alias = Alias($"b" > 2, "propagatedFilter_0")()
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f0 = f0Alias.toAttribute
    val f1 = f1Alias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias, f1Alias): _*)
    val innerFilter = innerProject.where(Or(f0, f1))
    val f2Alias = Alias(And(f0, $"a" > 1), "propagatedFilter_2")()
    val f3Alias = Alias(And(f1, $"b" > 2), "propagatedFilter_3")()
    val f2 = f2Alias.toAttribute
    val f3 = f3Alias.toAttribute
    val mergedSubquery = innerFilter
      .select(innerFilter.output ++ Seq(f2Alias, f3Alias): _*)
      .where(Or(f2, f3))
      .groupBy()(
        max($"a", Some(f3)).as("max_a"),
        min($"a", Some(f2)).as("min_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_a"), $"min_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries with distinct aggregate and different " +
      "filter conditions") {
    val subquery1 =
      ScalarSubquery(testRelation.where($"a" > 1).groupBy()(countDistinct($"a").as("cnt1")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"a" < 1).groupBy()(countDistinct($"a").as("cnt2")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val npFilterAlias = Alias($"a" < 1, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        countDistinctWithFilter(cpFilter, $"a").as("cnt1"),
        countDistinctWithFilter(npFilter, $"a").as("cnt2"))
      .select(CreateNamedStruct(Seq(
        Literal("cnt1"), $"cnt1",
        Literal("cnt2"), $"cnt2"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-40193: Merge non-grouping subqueries with If-wrapped computed Project expression") {
    val subquery1 = ScalarSubquery(testRelation.groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).select(($"a" + 1).as("d")).groupBy()(max($"d").as("max_d")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"a" > 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val dIfAlias =
      Alias(If(f0, $"a" + 1, Literal(null, testRelation.output.head.dataType)), "d")()
    val d = dIfAlias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias): _*)
      .select(testRelation.output ++ Seq(dIfAlias, f0): _*)
      .groupBy()(
        sum($"a").as("sum_a"),
        max(d, Some(f0)).as("max_d"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("max_d"), $"max_d"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("SPARK-40193: Merge non-grouping subqueries where one aggregate already carries a " +
      "FILTER clause") {
    val subquery1 = ScalarSubquery(testRelation.groupBy()(max($"a").as("max_a")))
    val subquery2 =
      ScalarSubquery(testRelation.where($"a" > 1).groupBy()(count($"a", Some($"b" > 0)).as("cnt")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"a" > 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias): _*)
      .groupBy()(
        max($"a").as("max_a"),
        count($"a", Some(And(f0, $"b" > 0))).as("cnt"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("cnt"), $"cnt"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}

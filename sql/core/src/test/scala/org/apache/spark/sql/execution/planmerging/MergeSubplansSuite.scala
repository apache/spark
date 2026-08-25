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

package org.apache.spark.sql.execution.planmerging

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Ascending, Attribute, AttributeReference, CreateNamedStruct, ExprId, GetStructField, If, Literal, Or, ScalarSubquery, SortOrder, TransformExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.{Expressions, FieldReference, SortDirection => V2SortDirection, SortOrder => V2SortOrder}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownLimit, SupportsPushDownOffset, SupportsPushDownRequiredColumns, SupportsPushDownTableSample, SupportsPushDownTopN, SupportsPushDownV2Filters, SupportsReportOrdering, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning => V2KeyGroupedPartitioning, Partitioning => V2Partitioning, UnknownPartitioning => V2UnknownPartitioning}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, V2ScanPartitioningAndOrdering, V2ScanRelationPushDown}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MergeSubplansSuite extends PlanTest {

  override def beforeEach(): Unit = {
    CTERelationDef.curId.set(0)
    PlanMerger.curId.set(0)
  }

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("MergeSubplans", Once, MergeSubplans) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".string)
  val testRelation2 = LocalRelation($"d".int, $"e".int)
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

  test("SPARK-56570: Merge non-grouping subqueries with different filter conditions and " +
      "non-attribute Project expressions on both sides") {
    val subquery1 = ScalarSubquery(
      testRelation.where($"a" > 1).select(($"a" * 2).as("x")).groupBy()(sum($"x").as("sum_x")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" < 1).select(($"a" + 1).as("y")).groupBy()(max($"y").as("max_y")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    // Merge steps:
    // Inner Filter level - (np: Filter(a < 1), cp: Filter(a > 1)):
    //   Different conditions -> symmetric first-time filter propagation.
    //   f0 = Alias(a < 1, "propagatedFilter_0")  -- np
    //   f1 = Alias(a > 1, "propagatedFilter_1")  -- cp
    //   -> Project([a, b, c, f0Alias, f1Alias], testRelation)
    //   -> Filter(OR(f0, f1), above)  [tagged]
    //   propagates (Some(f0), Some(f1)) upward
    //
    // Outer Project level - (np: Project(a + 1 AS y), cp: Project(a * 2 AS x)):
    //   Both sides have non-matching, non-attribute projections. Each side's projection is
    //   wrapped with its own filter: np's a + 1 with f0, cp's a * 2 with f1. The cached-side
    //   wrapping must touch only original cached entries; it must not double-wrap the np-
    //   appended entry with f1.
    //   -> Project([If(f1, a * 2, null) AS x, If(f0, a + 1, null) AS y, f0, f1], innerFilter)
    //
    // Aggregate consumes:
    //   sum(x) FILTER f1  -- cp: rows where a > 1
    //   max(y) FILTER f0  -- np: rows where a < 1
    val f0Alias = Alias($"a" < 1, "propagatedFilter_0")()
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f0 = f0Alias.toAttribute
    val f1 = f1Alias.toAttribute
    val intType = testRelation.output.head.dataType
    val xAlias = Alias(If(f1, $"a" * 2, Literal(null, intType)), "x")()
    val yAlias = Alias(If(f0, $"a" + 1, Literal(null, intType)), "y")()
    val x = xAlias.toAttribute
    val y = yAlias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias, f1Alias): _*)
      .where(Or(f0, f1))
      .select(xAlias, yAlias, f0, f1)
      .groupBy()(
        sum(x, Some(f1)).as("sum_x"),
        max(y, Some(f0)).as("max_y"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_x"), $"sum_x",
        Literal("max_y"), $"max_y"
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

  test("SPARK-56570: `(np: Filter, cp)` does not duplicate a cpFilter already present in " +
      "mergedChild") {
    // The `(np: Filter, cp)` create-new branch is only reached with a non-None recursion
    // `cpFilter` when cp has a shape that lets filter propagation bubble a cpFilter up through
    // the recursion without being consumed by an `Aggregate`. A Join with a Filter on one side
    // does this:
    //   - sq1 (cp): Aggregate -> Join(testRelation, Filter(e < 5, testRelation2), a = d).
    //   - sq2 (np): Aggregate -> Filter(a > 1) -> Join(testRelation, testRelation2, a = d).
    // At `(Agg, Agg)` children, the pair is `(Filter, Join)`. `(Filter, cp)` fires, peels np's
    // Filter and recurses `(Join, Join)`. The right-child recursion hits `(np, cp: Filter)`,
    // creates `propagatedFilter_0` for `e < 5`, and `(Join, Join)` propagates that as cpFilter
    // all the way back to the outer `(Filter, cp)` case. `mergedChild` at that point is a Join
    // whose output already contains the `propagatedFilter_0` attribute.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2.where($"e" < 5), Inner, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(
      testRelation.join(testRelation2, Inner, Some($"a" === $"d")).where($"a" > 1)
        .groupBy()(sum($"a").as("sum_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"e" < 5, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f1 = f1Alias.toAttribute
    val innerProject = testRelation2.select(testRelation2.output ++ Seq(f0Alias): _*)
    val joinNode = testRelation.join(innerProject, Inner, Some($"a" === $"d"))
    val mergedSubquery = joinNode
      .select(joinNode.output ++ Seq(f1Alias): _*)
      .groupBy()(
        max($"a", Some(f0)).as("max_a"),
        sum($"a", Some(f1)).as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("sum_a"), $"sum_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-56570: `(np, cp: Filter)` does not duplicate an npFilter already present in " +
      "mergedChild") {
    // Mirror of the previous test: the `(np, cp: Filter)` create-new branch is only reached with
    // a non-None recursion `npFilter` when np has a shape that lets filter propagation bubble up
    // through the recursion. A Join with a Filter on one side does this:
    //   - sq1 (cp): Aggregate -> Filter(a > 1) -> Join(testRelation, testRelation2, a = d).
    //   - sq2 (np): Aggregate -> Join(testRelation, Filter(e < 5, testRelation2), a = d).
    // At `(Agg, Agg)` children, the pair is `(Join, Filter)`. `(np, cp: Filter)` fires, peels
    // cp's Filter and recurses `(Join, Join)`. The right-child recursion hits `(np: Filter, cp)`,
    // creates `propagatedFilter_0` for `e < 5`, and `(Join, Join)` propagates that as npFilter
    // all the way back to the outer `(np, cp: Filter)` case. `mergedChild` at that point is a
    // Join whose output already contains the `propagatedFilter_0` attribute.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, Inner, Some($"a" === $"d")).where($"a" > 1)
        .groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(
      testRelation.join(testRelation2.where($"e" < 5), Inner, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"e" < 5, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f1 = f1Alias.toAttribute
    val innerProject = testRelation2.select(testRelation2.output ++ Seq(f0Alias): _*)
    val joinNode = testRelation.join(innerProject, Inner, Some($"a" === $"d"))
    val mergedSubquery = joinNode
      .select(joinNode.output ++ Seq(f1Alias): _*)
      .groupBy()(
        max($"a", Some(f1)).as("max_a"),
        sum($"a", Some(f0)).as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("sum_a"), $"sum_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-56570: tagged `(Filter, Filter)` reuse must keep mergedChild's appended columns") {
    // Round 1-2 build a tagged Filter (condition `OR(pf_0=(b<5), pf_1=(a>1))`) over a tagged
    // Project carrying both `propagatedFilter_*` aliases.
    // Round 3 merges a subplan whose Filter sits *above* a user Project introducing
    // `d = a + b`:
    //   sq3 = Aggregate(sum(d)) -> Filter(a>1) -> Project([d=(a+b), a, b, c]) -> testRelation.
    // At `(Agg, Agg)` children the pair is `(Filter(a>1) -> Project, Filter[tagged] -> Project)`
    // -- neither side is a Project at this level, so `(Filter, Filter)` tagged fires directly
    // and recurses on the children `(Project[d,a,b,c], Project[tagged][a,b,c,pf_0,pf_1])`. That
    // recursion's `(Project, Project)` case builds `Project([a,b,c,pf_0_alias,pf_1_alias,d], t)`
    // -- `mergedChild` now carries a column (`d`) that `cp.child` doesn't. The reuse check
    // finds `pf_1` already matches sq3's `(a > 1)`, so the tagged-reuse branch fires and must
    // rebuild the Filter over `mergedChild` so that `d` stays visible to the enclosing
    // Aggregate's `sum(d)`.
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"b" < 5).groupBy()(min($"b").as("min_b")))
    val subquery3 = ScalarSubquery(
      testRelation
        .select(($"a" + $"b").as("d"), $"a", $"b", $"c")
        .where($"a" > 1)
        .groupBy()(sum($"d").as("sum_d")))
    val originalQuery = testRelation.select(subquery1, subquery2, subquery3)

    val f0Alias = Alias($"b" < 5, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f1 = f1Alias.toAttribute
    val dAlias = Alias($"a" + $"b", "d")()
    val d = dAlias.toAttribute
    val innerProject = testRelation.select(testRelation.output ++ Seq(f0Alias, f1Alias, dAlias): _*)
    val mergedSubquery = innerProject
      .where(Or(f0, f1))
      .groupBy()(
        max($"a", Some(f1)).as("max_a"),
        min($"b", Some(f0)).as("min_b"),
        sum(d, Some(f1)).as("sum_d"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_b"), $"min_b",
        Literal("sum_d"), $"sum_d"
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

  test("SPARK-56570: `(np, cp: Filter)` drops a tagged cp Filter without synthesising a " +
      "redundant alias") {
    // Round 1+2: sq1 and sq2 merge via `(Filter, Filter)` first-time, creating a tagged Filter
    // (condition `OR(pf_0=(b<5), pf_1=(a>1))`) over a tagged Project carrying both aliases.
    // cp's aggregates are `[max(a) FILTER pf_1, min(b) FILTER pf_0]`.
    // Round 3: sq3 has no Filter, so `(np, cp: Filter)` with cp tagged fires. Synthesising a new
    // `propagatedFilter_2 = OR(pf_0, pf_1)` would leave the enclosing Aggregate wrapping cp's
    // already-filtered aggregates with `FILTER AND(OR(pf_0, pf_1), pf_i)` (which simplifies to
    // `FILTER pf_i`) -- wasted work and plan bloat. Dropping cp's Filter returns the recursion's
    // Project unchanged, leaves cp's per-side FILTER clauses untouched, and leaves the base
    // unrestricted for np's unfiltered aggregate.
    val subquery1 = ScalarSubquery(testRelation.where($"a" > 1).groupBy()(max($"a").as("max_a")))
    val subquery2 = ScalarSubquery(testRelation.where($"b" < 5).groupBy()(min($"b").as("min_b")))
    val subquery3 = ScalarSubquery(testRelation.groupBy()(sum($"a").as("sum_a")))
    val originalQuery = testRelation.select(subquery1, subquery2, subquery3)

    val f0Alias = Alias($"b" < 5, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f1 = f1Alias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias, f1Alias): _*)
      .groupBy()(
        max($"a", Some(f1)).as("max_a"),
        min($"b", Some(f0)).as("min_b"),
        sum($"a").as("sum_a"))
      .select(CreateNamedStruct(Seq(
        Literal("max_a"), $"max_a",
        Literal("min_b"), $"min_b",
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

  test("SPARK-56677: Merge non-grouping subqueries with filter on left join child") {
    // cp (subquery1): Aggregate([], [sum(a)], Join(testRelation, testRelation2, a=d))
    // np (subquery2): Aggregate([], [max(a)], Join(Filter(a>1, testRelation), testRelation2, a=d))
    // The filter on the left join child propagates as a boolean attribute through the Join node
    // and is consumed as a FILTER (WHERE ...) clause on the np-side aggregate expression.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, Inner, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).join(testRelation2, Inner, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"a" > 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias): _*)
      .join(testRelation2, Inner, Some($"a" === $"d"))
      .groupBy()(
        sum($"a").as("sum_a"),
        max($"a", Some(f0)).as("max_a"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("max_a"), $"max_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-56677: Merge non-grouping subqueries with filter on right join child") {
    // cp (subquery1): Aggregate([], [sum(a)], Join(testRelation, testRelation2, a=d))
    // np (subquery2): Aggregate([], [max(d)], Join(testRelation, Filter(d>1, testRelation2), a=d))
    // The filter on the right join child propagates analogously to the left-child case.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, Inner, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.join(testRelation2.where($"d" > 1), Inner, Some($"a" === $"d"))
        .groupBy()(max($"d").as("max_d")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"d" > 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val mergedSubquery = testRelation
      .join(
        testRelation2.select(testRelation2.output ++ Seq(f0Alias): _*),
        Inner, Some($"a" === $"d"))
      .groupBy()(
        sum($"a").as("sum_a"),
        max($"d", Some(f0)).as("max_d"))
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

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-56677: Merge non-grouping subqueries with filter on left child of a Cross join") {
    // Cross join never NULL-pads either side, so filter propagation is safe from both sides.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, Cross, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).join(testRelation2, Cross, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"a" > 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias): _*)
      .join(testRelation2, Cross, Some($"a" === $"d"))
      .groupBy()(
        sum($"a").as("sum_a"),
        max($"a", Some(f0)).as("max_a"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("max_a"), $"max_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-56677: Do not merge subqueries when both join children have independent filters") {
    // np has filters on BOTH left and right join children simultaneously. The guard in the
    // Join case prevents this merge because combining two independent filter attributes would
    // require ANDing them into a new alias, which is not yet supported.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, Inner, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).join(testRelation2.where($"d" > 1), Inner, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-56677: Merge non-grouping subqueries with filter on left side of LeftSemi join") {
    // Left-side filter attributes ARE in the LeftSemi join output, so propagation is safe.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, LeftSemi, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).join(testRelation2, LeftSemi, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    val f0Alias = Alias($"a" > 1, "propagatedFilter_0")()
    val f0 = f0Alias.toAttribute
    val mergedSubquery = testRelation
      .select(testRelation.output ++ Seq(f0Alias): _*)
      .join(testRelation2, LeftSemi, Some($"a" === $"d"))
      .groupBy()(
        sum($"a").as("sum_a"),
        max($"a", Some(f0)).as("max_a"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("max_a"), $"max_a"
      )).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }
  }

  test("SPARK-56677: Do not merge subqueries when filter is on the right side of a LeftSemi join") {
    // Right-side filter attributes are NOT in the LeftSemi join output (only left-side columns
    // are produced). Propagating such a filter would create an unresolvable attribute reference
    // in the parent Aggregate's FILTER clause.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, LeftSemi, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.join(testRelation2.where($"d" > 1), LeftSemi, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-56677: Do not merge subqueries when filter is on the nullable side of an outer " +
      "join") {
    // For a RightOuter join the left side is nullable: unmatched right rows produce NULL for all
    // left-side columns including the filter attribute f, so FILTER (WHERE f=NULL) would
    // incorrectly exclude those rows from the aggregate even though they appear in the join result.
    // The same problem applies to the right side of a LeftOuter join and both sides of FullOuter.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, RightOuter, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).join(testRelation2, RightOuter, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-56677: Do not merge subqueries when filter is on either side of a FullOuter join") {
    // For a FullOuter join both sides are nullable: unmatched rows from either side produce NULL
    // for the other side's columns. A filter attribute from either side would be NULL for those
    // unmatched rows, making propagation unsafe from both sides.
    val subquery1 = ScalarSubquery(
      testRelation.join(testRelation2, FullOuter, Some($"a" === $"d"))
        .groupBy()(sum($"a").as("sum_a")))
    val subquery2 = ScalarSubquery(
      testRelation.where($"a" > 1).join(testRelation2, FullOuter, Some($"a" === $"d"))
        .groupBy()(max($"a").as("max_a")))
    val originalQuery = testRelation.select(subquery1, subquery2)

    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-56677: Do not merge subqueries with filter propagation through join when disabled") {
    withSQLConf(SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_THROUGH_JOIN_ENABLED.key -> "false") {
      val subquery1 = ScalarSubquery(
        testRelation.join(testRelation2, Inner, Some($"a" === $"d"))
          .groupBy()(sum($"a").as("sum_a")))
      val subquery2 = ScalarSubquery(
        testRelation.where($"a" > 1).join(testRelation2, Inner, Some($"a" === $"d"))
          .groupBy()(max($"a").as("max_a")))
      val originalQuery = testRelation.select(subquery1, subquery2)

      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  // ---- SPARK-40259: generic DSv2 scan merge ----

  private val v2Table = new TestV2Table(StructType(Seq(
    StructField("a", IntegerType), StructField("b", IntegerType), StructField("c", StringType))))

  /** Same shape as [[v2Table]] but does NOT declare the `SCAN_MERGING` capability. */
  private val v2TableNoMerge = new TestV2Table(StructType(Seq(
    StructField("a", IntegerType), StructField("b", IntegerType), StructField("c", StringType))),
    supportsScanMerging = false)

  /**
   * Same shape as [[v2Table]], but its scans report `a ASC` as their output ordering, so a scan
   * rebuilt over this table re-derives a report instead of none.
   */
  private val v2TableReportingA = new TestV2Table(StructType(Seq(
    StructField("a", IntegerType), StructField("b", IntegerType), StructField("c", StringType))),
    reportedOrderingCols = Seq("a"))

  /**
   * Same shape as [[v2Table]], but its scans report `bucket(4, a)` as their key-grouped
   * partitioning -- a TRANSFORM report, which resolves to a `TransformExpression` rather than a
   * plain attribute (see [[v2ScanReportingOn]]).
   */
  private val v2TableBucketedOnA = new TestV2Table(StructType(Seq(
    StructField("a", IntegerType), StructField("b", IntegerType), StructField("c", StringType))),
    reportedBucketPartition = Some((4, "a")))

  /**
   * Same shape as [[v2Table]], but its scans report `a ASC` ONLY when no filter was pushed into
   * them, so the deferred build's two attempts (strict + best-effort, then strict-only) re-derive
   * different reports.
   */
  private val v2TablePushSensitiveReport = new TestV2Table(StructType(Seq(
    StructField("a", IntegerType), StructField("b", IntegerType), StructField("c", StringType))),
    reportedOrderingCols = Seq("a"), reportOnlyWhenNothingPushed = true)

  /** A `DataSourceV2ScanRelation` over `table` projecting only the given columns. */
  private def v2ScanReadingOn(table: TestV2Table, cols: Seq[String]): DataSourceV2ScanRelation = {
    val fullOutput = toAttributes(table.schema())
    val relation =
      DataSourceV2Relation(table, fullOutput, None, None, CaseInsensitiveStringMap.empty())
    val output = cols.map(c => fullOutput.find(_.name == c).get)
    val scan = TestV2Scan(StructType(output.map(a => StructField(a.name, a.dataType, a.nullable))))
    // These stand in for a plain scan produced by the column-pruning path, which is mergeable.
    DataSourceV2ScanRelation(relation, scan, output, mergeableScan = true)
  }

  /** A `DataSourceV2ScanRelation` over [[v2Table]] projecting only the given columns. */
  private def v2ScanReading(cols: String*): DataSourceV2ScanRelation =
    v2ScanReadingOn(v2Table, cols)

  /**
   * A `DataSourceV2ScanRelation` over `table` whose reported partitioning/ordering is derived the
   * way the optimizer derives it: by running `V2ScanPartitioningAndOrdering` over the table's own
   * scan, resolving transforms through a function catalog. Every call therefore binds the transform
   * function afresh, so the two sides of a merge hold DIFFERENT `BoundFunction` instances --
   * exactly as in production, where each scan relation is annotated by its own derivation.
   */
  private def v2ScanReportingOn(
      table: TestV2Table,
      cols: Seq[String],
      funCatalog: FunctionCatalog = TestFreshBindFunctionCatalog): DataSourceV2ScanRelation = {
    val fullOutput = toAttributes(table.schema())
    val relation = DataSourceV2Relation(
      table, fullOutput, Some(funCatalog), None, CaseInsensitiveStringMap.empty())
    val output = cols.map(c => fullOutput.find(_.name == c).get)
    val builder = table.newScanBuilder(CaseInsensitiveStringMap.empty())
    builder.asInstanceOf[SupportsPushDownRequiredColumns].pruneColumns(
      StructType(output.map(a => StructField(a.name, a.dataType, a.nullable))))
    V2ScanPartitioningAndOrdering(
      DataSourceV2ScanRelation(relation, builder.build(), output, mergeableScan = true))
      .asInstanceOf[DataSourceV2ScanRelation]
  }

  /** A `DataSourceV2ScanRelation` over a table without the `SCAN_MERGING` capability. */
  private def v2ScanReadingNoMerge(cols: String*): DataSourceV2ScanRelation =
    v2ScanReadingOn(v2TableNoMerge, cols)

  private def v2Scans(plan: LogicalPlan): Seq[DataSourceV2ScanRelation] =
    plan.collectWithSubqueries { case s: DataSourceV2ScanRelation => s }

  /**
   * Normalizes a merged plan so `comparePlans` can match it: (1) drops each DSv2 scan's
   * dynamically-built Scan to a schema-only placeholder (the pushed describe() strings are asserted
   * separately), and (2) resets the nested DataSourceV2Relation's output exprIds to a positional
   * scheme. The reset is needed because `DataSourceV2ScanRelation` is a `LeafNode`, so its
   * `relation` is a constructor arg rather than a child -- PlanTest's `normalizeExprIds` never
   * recurses into it, and those exprIds otherwise differ between the expected and actual plans.
   */
  private def normalizeScans(plan: LogicalPlan): LogicalPlan = plan.transformWithSubqueries {
    case s: DataSourceV2ScanRelation =>
      val fixedRelation = s.relation.copy(
        output = s.relation.output.zipWithIndex.map { case (a, i) => a.withExprId(ExprId(i)) })
      s.copy(relation = fixedRelation, scan = TestV2Scan(s.scan.readSchema()))
  }

  // Normalize merged DSv2 scans before the standard comparison (a no-op on plans without them),
  // so tests can call plain comparePlans. See normalizeScans for why this is needed.
  override protected def comparePlans(
      plan1: LogicalPlan,
      plan2: LogicalPlan,
      checkAnalysis: Boolean = true): Unit =
    super.comparePlans(normalizeScans(plan1), normalizeScans(plan2), checkAnalysis)

  test("SPARK-40259: merge DSv2 scans that differ only in projected columns") {
    val sub1 = ScalarSubquery(v2ScanReading("a").groupBy()(sum($"a").as("sum_a")))
    val sub2 = ScalarSubquery(v2ScanReading("b").groupBy()(sum($"b").as("sum_b")))
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected: the two scans fuse into one reading {a, b}, feeding a single aggregate. No filters,
    // so no OR-widen propagation.
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b"))
    val mergedSubquery = mergedScan
      .groupBy()(sum($"a").as("sum_a"), sum($"b").as("sum_b"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("sum_b"), $"sum_b")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))
    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("SPARK-40259: do not merge DSv2 scans when a pushdown is merge-blocking") {
    val sub1 = ScalarSubquery(v2ScanReading("a").groupBy()(sum($"a").as("sum_a")))
    val blockingScan = v2ScanReading("b").copy(mergeableScan = false)
    val sub2 = ScalarSubquery(blockingScan.groupBy()(sum($"b").as("sum_b")))
    val originalQuery = testRelation.select(sub1, sub2)

    // A merge-blocking pushdown declines the merge: the plan is left unchanged.
    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("SPARK-40259: do not merge DSv2 scans when a column is read at a nested-pruned type") {
    // A struct column read at a narrower (nested-pruned) type carries GetStructField ordinals
    // against that narrow layout. The merged scan reads the full struct, so remapping those
    // ordinals onto it would read the wrong field -- the merge must decline. np reads s pruned to
    // <b>, cp reads a plain column x; without the guard they fuse into a scan of {s:<a,b>, x}.
    val schema = StructType(Seq(
      StructField("s", StructType(Seq(
        StructField("a", IntegerType), StructField("b", IntegerType)))),
      StructField("x", IntegerType)))
    val relation = DataSourceV2Relation(
      new TestV2Table(schema), toAttributes(schema), None, None, CaseInsensitiveStringMap.empty())
    val sFull = relation.output.find(_.name == "s").get
    val xFull = relation.output.find(_.name == "x").get

    // np reads s at struct<b> (nested-pruned); cp reads x. Both opt in and are otherwise mergeable.
    val sPruned = AttributeReference(
      "s", StructType(Seq(StructField("b", IntegerType))))(sFull.exprId)
    val npScan = DataSourceV2ScanRelation(relation,
      TestV2Scan(StructType(Seq(StructField("s", sPruned.dataType)))),
      Seq(sPruned), mergeableScan = true)
    val cpScan = DataSourceV2ScanRelation(relation,
      TestV2Scan(StructType(Seq(StructField("x", IntegerType)))),
      Seq(xFull), mergeableScan = true)

    val sub1 = ScalarSubquery(npScan.groupBy()(sum(GetStructField(sPruned, 0)).as("sb")))
    val sub2 = ScalarSubquery(cpScan.groupBy()(sum(xFull).as("sx")))
    val originalQuery = testRelation.select(sub1, sub2)

    // The nested-pruned column declines the merge: the plan is left unchanged.
    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("SPARK-40259: merge DSv2 scans with different filters via OR-widen propagation") {
    val sub1 = ScalarSubquery(
      v2ScanReading("a").where($"a" > 1).groupBy()(sum($"a").as("sum_a")))    // cp
    val sub2 = ScalarSubquery(
      v2ScanReading("b").where($"b" > 2).groupBy()(sum($"b").as("sum_b")))    // np
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected: the two scans fuse into one reading {a, b}; the differing filters become
    // propagatedFilter aliases OR-widened in a Filter, and each side's aggregate carries its
    // filter as a FILTER clause (np = sub2 gets id 0, cp = sub1 gets id 1).
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b"))
    val npFilterAlias = Alias($"b" > 2, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = mergedScan
      .select(mergedScan.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        sum($"a", Some(cpFilter)).as("sum_a"),
        sum($"b", Some(npFilter)).as("sum_b"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("sum_b"), $"sum_b")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
      // Additional: the merged scan re-pushes OR(a > 1, b > 2) for row-group pruning, which the
      // structural comparison above normalizes out.
      val pushed = v2Scans(optimized).head.scan.asInstanceOf[TestV2Scan].pushed
      assert(pushed.nonEmpty && pushed.mkString.contains("a") && pushed.mkString.contains("b"),
        s"expected an OR pruning predicate over a and b pushed to the merged scan, got: $pushed")
    }
  }

  test("SPARK-40259: DSv2 symmetric merge honors its own config when the general one is off") {
    // Two DSv2 scans with equal (empty) strict filters but differing best-effort filters. The
    // general symmetric propagation is off, so a non-DSv2 pair would not merge; the DSv2-specific
    // config allows it, since the equal strict filters mean both sides read the same base set and
    // only the best-effort OR pruning is broadened.
    val sub1 = ScalarSubquery(
      v2ScanReading("a").where($"a" > 1).groupBy()(sum($"a").as("sum_a")))    // cp
    val sub2 = ScalarSubquery(
      v2ScanReading("b").where($"b" > 2).groupBy()(sum($"b").as("sum_b")))    // np
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected when the DSv2 config allows it: the two scans fuse into one reading {a, b}; the
    // differing filters become propagatedFilter aliases OR-widened in a Filter, and each side's
    // aggregate carries its filter as a FILTER clause (np = sub2 gets id 0, cp = sub1 gets id 1).
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b"))
    val npFilterAlias = Alias($"b" > 2, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = mergedScan
      .select(mergedScan.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        sum($"a", Some(cpFilter)).as("sum_a"),
        sum($"b", Some(npFilter)).as("sum_b"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("sum_b"), $"sum_b")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false",
        SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }

    // With the DSv2 config also off, the differing-filter pair must NOT merge (plan unchanged).
    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false",
        SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false") {
      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-40259: DSv2 merge fact propagates to an outer stacked Filter pair") {
    // Stacked Filters over each DSv2 scan (cp = sub1: inner a>1, outer a<100; np = sub2: inner b>2,
    // outer b<200). The leaf scans merge and the deferral is consumed at the INNER (Filter, Filter)
    // pair; the OUTER pair sees dsv2DeferredScan = None. The merge only fuses if the "a DSv2 merge
    // happened below" fact (TryMergeResult.dsv2Merged) still reaches the outer pair so it applies
    // the DSv2-symmetric exemption -- dsv2DeferredScan alone (consumed at the inner pair) would
    // not. Optimize runs only MergeSubplans, so the stacked Filters are not combined before the
    // rule sees them.
    val sub1 = ScalarSubquery(
      v2ScanReading("a").where($"a" > 1).where($"a" < 100).groupBy()(sum($"a").as("sum_a")))
    val sub2 = ScalarSubquery(
      v2ScanReading("b").where($"b" > 2).where($"b" < 200).groupBy()(sum($"b").as("sum_b")))
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected when the DSv2 config is on: the scans fuse into one reading {a, b}. Merge traversal
    // (inner-to-outer):
    //   Inner pair (np: b>2, cp: a>1) OR-widens:
    //     f0 = Alias(b > 2, "propagatedFilter_0")  -- np / sum_b
    //     f1 = Alias(a > 1, "propagatedFilter_1")  -- cp / sum_a
    //   Outer pair (np: b<200, cp: a<100) OR-widens, AND-combining each side's inner filter:
    //     f2 = Alias(AND(f0, b < 200), "propagatedFilter_2")  -- np
    //     f3 = Alias(AND(f1, a < 100), "propagatedFilter_3")  -- cp
    //   Aggregate consumes f2/f3 as FILTER clauses: sum_a FILTER f3, sum_b FILTER f2 -- i.e. each
    //   side's per-side AND(inner, outer).
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b"))
    val f0Alias = Alias($"b" > 2, "propagatedFilter_0")()
    val f1Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val f0 = f0Alias.toAttribute
    val f1 = f1Alias.toAttribute
    val innerFilter = mergedScan
      .select(mergedScan.output ++ Seq(f0Alias, f1Alias): _*)
      .where(Or(f0, f1))
    val f2Alias = Alias(And(f0, $"b" < 200), "propagatedFilter_2")()
    val f3Alias = Alias(And(f1, $"a" < 100), "propagatedFilter_3")()
    val f2 = f2Alias.toAttribute
    val f3 = f3Alias.toAttribute
    val mergedSubquery = innerFilter
      .select(innerFilter.output ++ Seq(f2Alias, f3Alias): _*)
      .where(Or(f2, f3))
      .groupBy()(
        sum($"a", Some(f3)).as("sum_a"),
        sum($"b", Some(f2)).as("sum_b"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("sum_b"), $"sum_b")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    // General symmetric off; the DSv2-specific config on. The flag lets BOTH the inner and outer
    // Filter pair OR-widen, so the two scans fuse into one and each aggregate FILTER is the
    // per-side AND(inner, outer).
    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false",
        SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }

    // With the DSv2 config off the inner pair already declines, so nothing fuses (plan unchanged).
    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false",
        SQLConf.MERGE_SUBPLANS_DSV2_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "false") {
      comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
    }
  }

  test("SPARK-40259: merge proceeds without pruning when the source rejects the pruning") {
    val rejecting = new TestV2Table(
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))),
      acceptsFilters = false)
    val s1 = v2ScanReadingOn(rejecting, Seq("a"))
    val s2 = v2ScanReadingOn(rejecting, Seq("b"))
    val sub1 = ScalarSubquery(s1.where(s1.output.head > 1).groupBy()(sum($"a").as("sum_a")))
    val sub2 = ScalarSubquery(s2.where(s2.output.head > 2).groupBy()(sum($"b").as("sum_b")))
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected: the same OR-widen merge as the accepting case -- only the pushed pruning differs
    // (a rejecting source records none). np = sub2 -> id 0, cp = sub1 -> id 1.
    val mergedScan = v2ScanReadingOn(rejecting, Seq("a", "b"))
    val npFilterAlias = Alias($"b" > 2, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = mergedScan
      .select(mergedScan.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        sum($"a", Some(cpFilter)).as("sum_a"),
        sum($"b", Some(npFilter)).as("sum_b"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("sum_b"), $"sum_b")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
      // Phase 2 degrades gracefully: the merge happens; a rejecting source records no pruning.
      assert(v2Scans(optimized).head.scan.asInstanceOf[TestV2Scan].pushed.isEmpty,
        "a rejecting source must not record any pushed pruning predicate")
    }
  }

  test("SPARK-40259: do not merge when a strict pushed filter is not re-enforced on rebuild") {
    // The scans claim a strict filter on "a" (in pushedFilters), but the source enforces nothing
    // strictly (empty strictColumns -> everything is best-effort). Re-pushing the "strict" filter
    // would leave it merely best-effort with nothing above to re-check it, so the merge must abort.
    val bestEffort = new TestV2Table(StructType(Seq(
      StructField("a", IntegerType), StructField("b", IntegerType), StructField("c", StringType))))
    val s1 = v2ScanReadingOn(bestEffort, Seq("a", "b"))
    val s2 = v2ScanReadingOn(bestEffort, Seq("a", "c"))
    val sub1 = ScalarSubquery(
      s1.copy(pushedFilters = Seq(s1.output.find(_.name == "a").get > 0))
        .groupBy()(sum($"b").as("sum_b")))
    val sub2 = ScalarSubquery(
      s2.copy(pushedFilters = Seq(s2.output.find(_.name == "a").get > 0))
        .groupBy()(sum($"c").as("max_c")))
    val originalQuery = testRelation.select(sub1, sub2)

    // A strict filter the rebuilt scan cannot re-enforce declines the merge: plan left unchanged.
    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("SPARK-40259: do not merge DSv2 scans of a table without the SCAN_MERGING capability") {
    val sub1 = ScalarSubquery(v2ScanReadingNoMerge("a").groupBy()(sum($"a").as("sum_a")))
    val sub2 = ScalarSubquery(v2ScanReadingNoMerge("b").groupBy()(sum($"b").as("sum_b")))
    val originalQuery = testRelation.select(sub1, sub2)

    // A table without the SCAN_MERGING capability declines the merge: plan left unchanged.
    comparePlans(Optimize.execute(originalQuery.analyze), originalQuery.analyze)
  }

  test("SPARK-40259: merge DSv2 scans with identical filters re-pushes the pruning") {
    // Identical post-scan filter (a > 1), differing columns: the two scans fuse and the identical
    // condition is re-pushed to the merged scan for row-group pruning (Phase 2, single condition).
    val sub1 = ScalarSubquery(
      v2ScanReading("a", "b").where($"a" > 1).groupBy()(sum($"b").as("sum_b")))
    val sub2 = ScalarSubquery(
      v2ScanReading("a", "c").where($"a" > 1).groupBy()(sum($"c").as("sum_c")))
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected: the identical filter a > 1 kept as a single Filter above the merged scan {a, b, c};
    // no OR-widen (the conditions match). The aggregates read b and c.
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b", "c"))
    val mergedSubquery = mergedScan.where($"a" > 1)
      .groupBy()(sum($"b").as("sum_b"), sum($"c").as("sum_c"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_b"), $"sum_b",
        Literal("sum_c"), $"sum_c")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))
    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, correctAnswer.analyze)
    // Additional: the identical a > 1 is re-pushed to the merged scan for pruning.
    val pushed = v2Scans(optimized).head.scan.asInstanceOf[TestV2Scan].pushed
    assert(pushed.nonEmpty && pushed.mkString.contains("a"),
      s"expected the identical filter a > 1 to be re-pushed to the merged scan, got: $pushed")
  }

  test("SPARK-40259: merge three DSv2 scans with differing filters re-pushes the full OR") {
    // Three-way merge exercises the tagged (MERGED_FILTER_TAG) branch: the leaf re-merge rebuilds
    // the scan strict-only each round, so the tagged branch must re-establish the full 3-way OR.
    val t = new TestV2Table(StructType(Seq(StructField("a", IntegerType),
      StructField("b", IntegerType), StructField("d", IntegerType))))
    val sub1 = ScalarSubquery(
      v2ScanReadingOn(t, Seq("a")).where($"a" > 1).groupBy()(sum($"a").as("s1")))
    val sub2 = ScalarSubquery(
      v2ScanReadingOn(t, Seq("b")).where($"b" > 2).groupBy()(sum($"b").as("s2")))
    val sub3 = ScalarSubquery(
      v2ScanReadingOn(t, Seq("d")).where($"d" > 3).groupBy()(sum($"d").as("s3")))
    val originalQuery = testRelation.select(sub1, sub2, sub3)

    // Expected: one merged scan reading {a, b, d}; step 1 merges sub1 (cp) + sub2 (np) ->
    // propagatedFilter_0 = b > 2 (np), _1 = a > 1 (cp); step 2 merges sub3 (np) -> _2 = d > 3,
    // extending the OR. Each aggregate carries its side's FILTER.
    val mergedScan = v2ScanReadingOn(t, Seq("a", "b", "d"))
    val npFilter0Alias = Alias($"b" > 2, "propagatedFilter_0")()
    val cpFilter0Alias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter1Alias = Alias($"d" > 3, "propagatedFilter_2")()
    val npFilter0 = npFilter0Alias.toAttribute
    val cpFilter0 = cpFilter0Alias.toAttribute
    val npFilter1 = npFilter1Alias.toAttribute
    val mergedSubquery = mergedScan
      .select(mergedScan.output ++ Seq(npFilter0Alias, cpFilter0Alias, npFilter1Alias): _*)
      .where(Or(Or(npFilter0, cpFilter0), npFilter1))
      .groupBy()(
        sum($"a", Some(cpFilter0)).as("s1"),
        sum($"b", Some(npFilter0)).as("s2"),
        sum($"d", Some(npFilter1)).as("s3"))
      .select(CreateNamedStruct(Seq(
        Literal("s1"), $"s1",
        Literal("s2"), $"s2",
        Literal("s3"), $"s3")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1),
        extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
      // Additional: the merged scan carries the full 3-way OR pruning over a, b and d.
      val pushed = v2Scans(optimized).head.scan.asInstanceOf[TestV2Scan].pushed.mkString
      assert(pushed.contains("a") && pushed.contains("b") && pushed.contains("d"),
        s"expected an OR pruning over a, b and d pushed to the merged scan, got: $pushed")
    }
  }

  test("SPARK-40259: three-way merge reuses an existing propagated filter (reuse branch)") {
    // sub3's filter (a > 1) matches sub1's, so the tagged-filter merge takes the reuse branch:
    // no new propagatedFilter alias is created and the OR stays two-way; sub3's aggregate reuses
    // the existing filter attribute. This exercises the reuse branch against a DSv2 scan, which
    // the all-distinct-filters three-way test above does not.
    val t = new TestV2Table(StructType(Seq(StructField("a", IntegerType),
      StructField("b", IntegerType), StructField("c", IntegerType))))
    val sub1 = ScalarSubquery(
      v2ScanReadingOn(t, Seq("a")).where($"a" > 1).groupBy()(sum($"a").as("s1")))
    val sub2 = ScalarSubquery(
      v2ScanReadingOn(t, Seq("b")).where($"b" > 2).groupBy()(sum($"b").as("s2")))
    val sub3 = ScalarSubquery(
      v2ScanReadingOn(t, Seq("a", "c")).where($"a" > 1).groupBy()(sum($"c").as("s3")))
    val originalQuery = testRelation.select(sub1, sub2, sub3)

    // Expected: one merged scan reading {a, b, c}; step 1 merges sub1 (cp) + sub2 (np) ->
    // propagatedFilter_0 = b > 2 (np), _1 = a > 1 (cp). Step 2 merges sub3 (np, a > 1): its
    // condition matches _1, so no new alias is created and the OR stays two-way; sub3 reuses _1.
    val mergedScan = v2ScanReadingOn(t, Seq("a", "b", "c"))
    val npFilterAlias = Alias($"b" > 2, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = mergedScan
      .select(mergedScan.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        sum($"a", Some(cpFilter)).as("s1"),
        sum($"b", Some(npFilter)).as("s2"),
        sum($"c", Some(cpFilter)).as("s3"))
      .select(CreateNamedStruct(Seq(
        Literal("s1"), $"s1",
        Literal("s2"), $"s2",
        Literal("s3"), $"s3")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1),
        extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
      // The OR references only the two distinct conditions (a > 1, b > 2); c is not a filter.
      val pushed = v2Scans(optimized).head.scan.asInstanceOf[TestV2Scan].pushed.mkString
      assert(pushed.contains("a") && pushed.contains("b"),
        s"expected an OR pruning over a and b pushed to the merged scan, got: $pushed")
    }
  }

  test("SPARK-40259: a non-deterministic filter conjunct is not pushed as a pruning predicate") {
    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      // Bare non-grouping aggregates (joined) go through the Aggregate extraction path, which --
      // unlike the ScalarSubquery path -- does not gate on determinism, so the OR-widen pruning
      // predicate carries a non-deterministic conjunct into the scan-merge Phase 2.
      val agg1 = v2ScanReading("a").where(($"a" > 1) && (rand(0) < Literal(0.5)))
        .groupBy()(sum($"a").as("s1"))
      val agg2 = v2ScanReading("b").where($"b" > 2).groupBy()(sum($"b").as("s2"))
      val originalQuery = agg1.join(agg2)

      val scans = v2Scans(Optimize.execute(originalQuery.analyze))
      assert(scans.length == 1, "the scans should still be fused")
      val pushed = scans.head.scan.asInstanceOf[TestV2Scan].pushed.mkString
      // Pruning predicate `(a > 1 AND rand() < 0.5) OR (b > 2)` is non-deterministic as a whole.
      // A source that prunes on it uses its own rand() draw, while the enclosing Filter re-checks
      // exactness with a different draw, so pruned rows would be lost. Best-effort pruning degrades
      // gracefully: the predicate is dropped rather than weakened, so nothing is pushed and no
      // rand() reaches the source. The merge itself is unaffected.
      assert(!pushed.contains("RAND"),
        s"the non-deterministic rand() conjunct must not be pushed as pruning, got: $pushed")
      assert(pushed.isEmpty,
        s"a non-deterministic pruning predicate must be dropped wholesale, got: $pushed")
    }
  }

  test("SPARK-40259: merge DSv2 scans that pushed the same strict filters (re-push path)") {
    // A source that fully enforces (strict) predicates on column "a".
    val strictOnA = new TestV2Table(
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType),
        StructField("c", StringType))),
      strictColumns = Set("a"))
    val s1 = v2ScanReadingOn(strictOnA, Seq("a", "b"))
    val s2 = v2ScanReadingOn(strictOnA, Seq("a", "c"))
    val sub1 = ScalarSubquery(
      s1.copy(pushedFilters = Seq(s1.output.find(_.name == "a").get > 0))
        .groupBy()(sum($"b").as("sum_b")))
    val sub2 = ScalarSubquery(
      s2.copy(pushedFilters = Seq(s2.output.find(_.name == "a").get > 0))
        .groupBy()(sum($"c").as("max_c")))
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected: one merged scan reading {a, b, c} that keeps the strict a > 0 in pushedFilters; the
    // two aggregates read b and c. No post-scan filter, so no OR-widen.
    val mergedScan0 = v2ScanReadingOn(strictOnA, Seq("a", "b", "c"))
    val mergedScan = mergedScan0.copy(
      pushedFilters = Seq(mergedScan0.output.find(_.name == "a").get > 0))
    val mergedSubquery = mergedScan
      .groupBy()(sum($"b").as("sum_b"), sum($"c").as("max_c"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_b"), $"sum_b",
        Literal("max_c"), $"max_c")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))
    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, correctAnswer.analyze)
    // Additional: the strict a > 0 is re-enforced on the merged scan (normalized out above).
    assert(v2Scans(optimized).head.pushedFilters.nonEmpty,
      "the merged relation must carry the strict pushed filters")
  }

  test("SPARK-40259: merge scans with equal strict filters and differing post-scan filters") {
    // Mixed source: "p" is fully enforced (strict), "a"/"b" are best-effort. Both sides push
    // the same strict filter p > 0 and carry a differing post-scan filter (a > 1 / b > 2). The
    // scans fuse on the equal strict p; the differing post-scan filters OR-widen above the
    // merged scan, so the rebuild re-pushes p > 0 (strict) and (a > 1 OR b > 2) (pruning) at
    // once -- the only path that drives buildMergedScan with both non-empty.
    val mixed = new TestV2Table(
      StructType(Seq(StructField("p", IntegerType), StructField("a", IntegerType),
        StructField("b", IntegerType))),
      strictColumns = Set("p"))
    val s1 = v2ScanReadingOn(mixed, Seq("p", "a"))
    val s2 = v2ScanReadingOn(mixed, Seq("p", "b"))
    val sub1 = ScalarSubquery(
      s1.copy(pushedFilters = Seq(s1.output.find(_.name == "p").get > 0))
        .where($"a" > 1).groupBy()(sum($"a").as("s1")))
    val sub2 = ScalarSubquery(
      s2.copy(pushedFilters = Seq(s2.output.find(_.name == "p").get > 0))
        .where($"b" > 2).groupBy()(sum($"b").as("s2")))
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected: one merged scan reading {p, a, b} that keeps p > 0 strict (pushedFilters), with
    // the differing post-scan filters OR-widened above it (np = sub2 -> id 0, cp = sub1 -> id 1).
    val mergedScan0 = v2ScanReadingOn(mixed, Seq("p", "a", "b"))
    val mergedScan = mergedScan0.copy(
      pushedFilters = Seq(mergedScan0.output.find(_.name == "p").get > 0))
    val npFilterAlias = Alias($"b" > 2, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = mergedScan
      .select(mergedScan.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        sum($"a", Some(cpFilter)).as("s1"),
        sum($"b", Some(npFilter)).as("s2"))
      .select(CreateNamedStruct(Seq(
        Literal("s1"), $"s1",
        Literal("s2"), $"s2")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
      // Additional: p stays strict (in pushedFilters) and the OR (a, b) is re-pushed as pruning,
      // both normalized out of the structural comparison above.
      val mergedResult = v2Scans(optimized).head
      assert(mergedResult.pushedFilters.exists(_.references.exists(_.name == "p")),
        s"the strict filter on p must stay enforced; got ${mergedResult.pushedFilters}")
      val pushed = mergedResult.scan.asInstanceOf[TestV2Scan].pushed.mkString
      assert(pushed.contains("a") && pushed.contains("b"),
        s"the differing post-scan filters must be re-pushed as OR pruning, got: $pushed")
    }
  }

  test("SPARK-40259: do not merge DSv2 scans when the merge would degrade a reported " +
    "key-grouped partitioning or ordering") {
    // An input reports key-grouped partitioning or ordering, but the merged scan -- rebuilt over a
    // TestV2Scan that reports neither -- re-derives nothing, so merging would degrade what the
    // input reported. With the degradation configs off (the default) the merge is declined on both
    // the np and cp side and the plan is left unchanged, rather than forcing a shuffle/sort the
    // original plan avoided.
    def assertDeclines(withField: DataSourceV2ScanRelation => DataSourceV2ScanRelation): Unit =
      Seq(
        (withField(v2ScanReading("a")), v2ScanReading("b")),
        (v2ScanReading("a"), withField(v2ScanReading("b")))).foreach { case (npScan, cpScan) =>
        val q = testRelation.select(
          ScalarSubquery(npScan.groupBy()(sum($"a").as("sa"))),
          ScalarSubquery(cpScan.groupBy()(sum($"b").as("sb"))))
        comparePlans(Optimize.execute(q.analyze), q.analyze)
      }

    assertDeclines(s => s.copy(keyGroupedPartitioning = Some(Seq(s.output.head))))
    assertDeclines(s => s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending)))))
  }

  test("SPARK-58549: do not merge DSv2 scans reporting incompatible kGP/ordering") {
    // Both inputs report a partitioning/ordering, but on the different column each reads, so no
    // single rebuilt scan could keep both not-worse. combineRequired* returns None (incompatible),
    // so the merge is declined at the leaf -- before any rebuild -- with the degradation configs
    // off (the default). This is distinct from the single-side case above (which rebuilds and then
    // finds the re-derived report degraded): here the two inputs disagree with each other up front.
    def assertDeclines(withField: DataSourceV2ScanRelation => DataSourceV2ScanRelation): Unit = {
      // withField applied to the "a" scan reports on a, applied to the "b" scan reports on b.
      val q = testRelation.select(
        ScalarSubquery(withField(v2ScanReading("a")).groupBy()(sum($"a").as("sa"))),
        ScalarSubquery(withField(v2ScanReading("b")).groupBy()(sum($"b").as("sb"))))
      comparePlans(Optimize.execute(q.analyze), q.analyze)
    }

    assertDeclines(s => s.copy(keyGroupedPartitioning = Some(Seq(s.output.head))))
    assertDeclines(s => s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending)))))
  }

  test("SPARK-58549: merge DSv2 scans reporting kGP/ordering when the degradation config allows") {
    // With the matching degradation config on, a merge that would drop a reported partitioning or
    // ordering proceeds anyway (trading it for a single scan). The merged scan re-derives no report
    // from the non-reporting TestV2Scan, so the fused plan is the plain column union.
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b"))
    val mergedSubquery = mergedScan
      .groupBy()(sum($"a").as("sum_a"), sum($"b").as("sum_b"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_a"), $"sum_a",
        Literal("sum_b"), $"sum_b")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    def assertMerges(
        withField: DataSourceV2ScanRelation => DataSourceV2ScanRelation,
        confKey: String,
        bothSides: Boolean = false): Unit = {
      val sub1 = ScalarSubquery(withField(v2ScanReading("a")).groupBy()(sum($"a").as("sum_a")))
      val cpScan = if (bothSides) withField(v2ScanReading("b")) else v2ScanReading("b")
      val sub2 = ScalarSubquery(cpScan.groupBy()(sum($"b").as("sum_b")))
      val originalQuery = testRelation.select(sub1, sub2)
      withSQLConf(confKey -> "true") {
        comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
      }
    }

    // One side reports: the report combines fine, and the config lets through the degradation the
    // post-rebuild check finds (the rebuilt scan re-derives nothing).
    assertMerges(s => s.copy(keyGroupedPartitioning = Some(Seq(s.output.head))),
      SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_KEY_GROUPED_PARTITIONING_DEGRADATION.key)
    assertMerges(s => s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending)))),
      SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_ORDERING_DEGRADATION.key)
    // Both sides report, each on the column it reads: INCOMPATIBLE, so here the config is what
    // skips the EARLY decline at the leaf, before any rebuild (the mirror of the incompatible
    // default-decline test above).
    assertMerges(s => s.copy(keyGroupedPartitioning = Some(Seq(s.output.head))),
      SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_KEY_GROUPED_PARTITIONING_DEGRADATION.key,
      bothSides = true)
    assertMerges(s => s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending)))),
      SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_ORDERING_DEGRADATION.key, bothSides = true)
  }

  test("SPARK-58549: merge DSv2 scans when the rebuilt scan re-derives the required ordering") {
    // The NOT-WORSE side of the check: both inputs report `a ASC` and the table reports the same,
    // so the rebuilt merged scan re-derives it and nothing is degraded -- the merge proceeds with
    // the configs off. Every other test here rebuilds over a plain TestV2Scan, which reports
    // nothing, so any non-empty requirement is degraded by construction and only the declining side
    // gets covered.
    val withOrdering = (s: DataSourceV2ScanRelation) =>
      s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending))))
    val q = testRelation.select(
      ScalarSubquery(withOrdering(v2ScanReadingOn(v2TableReportingA, Seq("a", "b")))
        .groupBy()(sum($"b").as("sum_b"))),
      ScalarSubquery(withOrdering(v2ScanReadingOn(v2TableReportingA, Seq("a", "c")))
        .groupBy()(sum($"c").as("sum_c"))))
    val optimized = Optimize.execute(q.analyze)

    val scans = v2Scans(optimized)
    assert(scans.length == 1, s"the two scans should be fused into one:\n$optimized")
    assert(scans.head.output.map(_.name).toSet == Set("a", "b", "c"),
      s"the merged scan should read the union of both columns; got ${scans.head.output}")
    assert(scans.head.ordering.exists(_.map(_.child).collect {
      case a: Attribute => a.name
    } == Seq("a")),
      s"the merged scan should re-derive the reported ordering; got ${scans.head.ordering}")
  }

  test("SPARK-58549: do not merge when the rebuilt scan re-derives less than the combined " +
    "ordering") {
    // One input reports `a ASC`, the other `a ASC, c ASC`, so the combined requirement is the
    // STRONGER of the two. The table reports only `a ASC`, so the rebuilt merged scan satisfies the
    // weaker input's ordering but not the combined one -- a degradation, declined with the configs
    // off. Guards that combineRequiredOrdering keeps the stronger side: keeping the weaker one
    // would let this merge through and lose the second sort key.
    val scanAB = v2ScanReadingOn(v2TableReportingA, Seq("a", "b"))
    val scanAC = v2ScanReadingOn(v2TableReportingA, Seq("a", "c"))
    val q = testRelation.select(
      ScalarSubquery(scanAB.copy(ordering = Some(Seq(SortOrder(scanAB.output.head, Ascending))))
        .groupBy()(sum($"b").as("sum_b"))),
      ScalarSubquery(scanAC.copy(ordering = Some(Seq(
          SortOrder(scanAC.output.head, Ascending), SortOrder(scanAC.output.last, Ascending))))
        .groupBy()(sum($"c").as("sum_c"))))
    comparePlans(Optimize.execute(q.analyze), q.analyze)
  }

  test("SPARK-58549: merge DSv2 scans reporting the same bucket transform partitioning") {
    // Both inputs report `bucket(4, a)`, each derived independently, so each holds its OWN
    // BoundFunction instance -- what production does, since V2ExpressionUtils binds the function
    // afresh per derivation. The two reports compare equal only because `TestBucketFunction`
    // implements `equals`/`hashCode` the way `BoundFunction` asks a connector to, so this is the
    // end-to-end check that Spark honours that contract: the merge proceeds and the rebuilt scan
    // re-derives the same report. It is also the only test here whose report is a transform rather
    // than a plain attribute, which is what an identity-partitioned source would report.
    val q = testRelation.select(
      ScalarSubquery(v2ScanReportingOn(v2TableBucketedOnA, Seq("a", "b"))
        .groupBy()(sum($"b").as("sum_b"))),
      ScalarSubquery(v2ScanReportingOn(v2TableBucketedOnA, Seq("a", "c"))
        .groupBy()(sum($"c").as("sum_c"))))
    val optimized = Optimize.execute(q.analyze)

    val scans = v2Scans(optimized)
    assert(scans.length == 1, s"the two scans should be fused into one:\n$optimized")
    val kgp = scans.head.keyGroupedPartitioning
    assert(kgp.exists(_.forall(_.isInstanceOf[TransformExpression])),
      s"the merged scan should preserve the reported bucket transform; got $kgp")
    assert(kgp.get.flatMap(_.references).exists(_.name == "a"),
      s"the preserved partitioning should be on a; got $kgp")
  }

  test("SPARK-58769: decline the merge when the reported transform is not comparable") {
    // The mirror of the test above, and the cost this documents: same query, same reported
    // `bucket(4, a)`, but the connector's function does not implement `equals`, so the two
    // independently bound instances do not compare equal and Spark cannot tell the two reports
    // apart from two different partitionings. It does not derive that identity itself (see
    // `BoundFunction#equals`), so the merge is declined rather than done on a report it cannot
    // verify, and the plan is left alone.
    val q = testRelation.select(
      ScalarSubquery(
        v2ScanReportingOn(v2TableBucketedOnA, Seq("a", "b"), TestNotComparableFunctionCatalog)
          .groupBy()(sum($"b").as("sum_b"))),
      ScalarSubquery(
        v2ScanReportingOn(v2TableBucketedOnA, Seq("a", "c"), TestNotComparableFunctionCatalog)
          .groupBy()(sum($"c").as("sum_c"))))
    comparePlans(Optimize.execute(q.analyze), q.analyze)
  }

  test("SPARK-58549: enforce the required report on the deferred under-Filter scan build") {
    // The above tests fuse scans directly under an Aggregate (no Filter), so they exercise the
    // scan build at the leaf. When the scans sit under an (identical) Filter the build is instead
    // DEFERRED to the enclosing Filter, and the required report is carried there through
    // DSv2DeferredScan. This test drives that deferred path: each scan reads {a, <col>} and reports
    // on a, both filter on `a > 1` (so the two fuse into {a, b, c} under one Filter). The merged
    // scan is rebuilt over a non-reporting TestV2Scan, so it re-derives no report -- a degradation.
    def reportsOnA(withField: DataSourceV2ScanRelation => DataSourceV2ScanRelation)
        : (ScalarSubquery, ScalarSubquery) = (
      ScalarSubquery(
        withField(v2ScanReading("a", "b")).where($"a" > 1).groupBy()(sum($"b").as("sum_b"))),
      ScalarSubquery(
        withField(v2ScanReading("a", "c")).where($"a" > 1).groupBy()(sum($"c").as("sum_c"))))

    // Default configs: the deferred build declines on the degradation, leaving the plan unchanged.
    def assertDeclines(withField: DataSourceV2ScanRelation => DataSourceV2ScanRelation): Unit = {
      val (sub1, sub2) = reportsOnA(withField)
      val q = testRelation.select(sub1, sub2)
      comparePlans(Optimize.execute(q.analyze), q.analyze)
    }
    assertDeclines(s => s.copy(keyGroupedPartitioning = Some(Seq(s.output.head))))
    assertDeclines(s => s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending)))))

    // With the matching config on, the deferred build proceeds: the two scans fuse into {a, b, c}
    // with the identical `a > 1` re-pushed for pruning (as in the identical-filter merge test).
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b", "c"))
    val mergedSubquery = mergedScan.where($"a" > 1)
      .groupBy()(sum($"b").as("sum_b"), sum($"c").as("sum_c"))
      .select(CreateNamedStruct(Seq(
        Literal("sum_b"), $"sum_b",
        Literal("sum_c"), $"sum_c")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    def assertMerges(
        withField: DataSourceV2ScanRelation => DataSourceV2ScanRelation, confKey: String): Unit = {
      val (sub1, sub2) = reportsOnA(withField)
      val q = testRelation.select(sub1, sub2)
      withSQLConf(confKey -> "true") {
        comparePlans(Optimize.execute(q.analyze), correctAnswer.analyze)
      }
    }
    assertMerges(s => s.copy(keyGroupedPartitioning = Some(Seq(s.output.head))),
      SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_KEY_GROUPED_PARTITIONING_DEGRADATION.key)
    assertMerges(s => s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending)))),
      SQLConf.MERGE_SUBPLANS_DSV2_ALLOW_ORDERING_DEGRADATION.key)
  }

  test("SPARK-58549: the deferred build checks the required report per attempt") {
    // The deferred build tries strict + best-effort first, then strict-only. The report is checked
    // on EACH attempt, not once on whichever came back, so the deferred path stays no weaker than
    // the leaf path: the strict-only attempt is exactly what the leaf builds when no Filter sits
    // above the scan, so a merge it can satisfy must not be lost just because the first attempt
    // could not. Here both inputs report `a ASC` and neither has a strict pushed filter, and the
    // source reports only when nothing was pushed: attempt 1 (offered the Filter's condition)
    // re-derives no ordering and is rejected, attempt 2 re-derives `a ASC` and the merge lands.
    val withOrdering = (s: DataSourceV2ScanRelation) =>
      s.copy(ordering = Some(Seq(SortOrder(s.output.head, Ascending))))
    val q = testRelation.select(
      ScalarSubquery(withOrdering(v2ScanReadingOn(v2TablePushSensitiveReport, Seq("a", "b")))
        .where($"a" > 1).groupBy()(sum($"b").as("sum_b"))),
      ScalarSubquery(withOrdering(v2ScanReadingOn(v2TablePushSensitiveReport, Seq("a", "c")))
        .where($"a" > 1).groupBy()(sum($"c").as("sum_c"))))
    val optimized = Optimize.execute(q.analyze)

    val scans = v2Scans(optimized)
    assert(scans.length == 1, s"the two scans should be fused into one:\n$optimized")
    assert(scans.head.ordering.exists(_.map(_.child).collect {
      case a: Attribute => a.name
    } == Seq("a")),
      s"the strict-only attempt should have satisfied the required ordering; " +
        s"got ${scans.head.ordering}")
    assert(scans.head.scan.asInstanceOf[TestV2ReportingScan].inner.pushed.isEmpty,
      "the surviving build should be the strict-only one, with nothing pushed")
  }

  test("SPARK-40259: merge DSv2 scans that report empty key-grouped partitioning or ordering") {
    // A source implementing SupportsReportPartitioning/SupportsReportOrdering but reporting nothing
    // yields Some(Nil), not None (V2ScanPartitioningAndOrdering sets the field unconditionally). An
    // empty report carries no partitioning/ordering to drop, so the merge should still proceed --
    // the gate tests the inner Seq, not the Option. The fused plan is the plain column union.
    def assertMerges(withEmptyField: DataSourceV2ScanRelation => DataSourceV2ScanRelation): Unit = {
      val sub1 = ScalarSubquery(withEmptyField(v2ScanReading("a")).groupBy()(sum($"a").as("sum_a")))
      val sub2 = ScalarSubquery(withEmptyField(v2ScanReading("b")).groupBy()(sum($"b").as("sum_b")))
      val originalQuery = testRelation.select(sub1, sub2)

      val mergedScan = v2ScanReadingOn(v2Table, Seq("a", "b"))
      val mergedSubquery = mergedScan
        .groupBy()(sum($"a").as("sum_a"), sum($"b").as("sum_b"))
        .select(CreateNamedStruct(Seq(
          Literal("sum_a"), $"sum_a",
          Literal("sum_b"), $"sum_b")).as("mergedValue"))
      val analyzedMergedSubquery = mergedSubquery.analyze
      val correctAnswer = WithCTE(
        testRelation.select(
          extractorExpression(0, analyzedMergedSubquery.output, 0),
          extractorExpression(0, analyzedMergedSubquery.output, 1)),
        Seq(definitionNode(analyzedMergedSubquery, 0)))
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    }

    assertMerges(_.copy(keyGroupedPartitioning = Some(Nil)))
    assertMerges(_.copy(ordering = Some(Nil)))
  }

  test("SPARK-40259: merge DSv2 scans reading identical columns but differing filters") {
    // Both scans read only "a", so the column union adds nothing (the np-only set is empty); the
    // merge is driven purely by the differing filters, and the OR is still re-pushed for pruning.
    // (sub1 is cp, sub2 is np.)
    val sub1 = ScalarSubquery(v2ScanReading("a").where($"a" > 1).groupBy()(sum($"a").as("s1")))
    val sub2 = ScalarSubquery(v2ScanReading("a").where($"a" > 2).groupBy()(sum($"a").as("s2")))
    val originalQuery = testRelation.select(sub1, sub2)

    // Expected: one merged scan reading just {a} (np-only set empty), the differing filters
    // OR-widened above it (np = sub2 -> id 0, cp = sub1 -> id 1).
    val mergedScan = v2ScanReadingOn(v2Table, Seq("a"))
    val npFilterAlias = Alias($"a" > 2, "propagatedFilter_0")()
    val cpFilterAlias = Alias($"a" > 1, "propagatedFilter_1")()
    val npFilter = npFilterAlias.toAttribute
    val cpFilter = cpFilterAlias.toAttribute
    val mergedSubquery = mergedScan
      .select(mergedScan.output ++ Seq(npFilterAlias, cpFilterAlias): _*)
      .where(Or(npFilter, cpFilter))
      .groupBy()(
        sum($"a", Some(cpFilter)).as("s1"),
        sum($"a", Some(npFilter)).as("s2"))
      .select(CreateNamedStruct(Seq(
        Literal("s1"), $"s1",
        Literal("s2"), $"s2")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))

    withSQLConf(
        SQLConf.MERGE_SUBPLANS_FILTER_PROPAGATION_ENABLED.key -> "true",
        SQLConf.MERGE_SUBPLANS_SYMMETRIC_FILTER_PROPAGATION_ENABLED.key -> "true") {
      val optimized = Optimize.execute(originalQuery.analyze)
      comparePlans(optimized, correctAnswer.analyze)
      val pushed = v2Scans(optimized).head.scan.asInstanceOf[TestV2Scan].pushed.mkString
      assert(pushed.contains("a"), s"the OR over a should be re-pushed as pruning, got: $pushed")
    }
  }

  test("SPARK-40259: merge three DSv2 scans that pushed the same strict filter") {
    // Round-2 merge feeds the already-merged relation back through samePushedFilters. All three
    // scans carry the same strict a > 0, so they fuse into a single scan reading a, b, c, d.
    val strictOnA = new TestV2Table(
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType),
        StructField("c", IntegerType), StructField("d", IntegerType))),
      strictColumns = Set("a"))
    def strictSub(col: String): ScalarSubquery = {
      val s = v2ScanReadingOn(strictOnA, Seq("a", col))
      ScalarSubquery(
        s.copy(pushedFilters = Seq(s.output.find(_.name == "a").get > 0))
          .groupBy()(sum(s.output.find(_.name == col).get).as(s"agg_$col")))
    }
    val originalQuery = testRelation.select(strictSub("b"), strictSub("c"), strictSub("d"))

    // Expected: one merged scan reading {a, b, c, d} keeping the shared strict a > 0; three
    // aggregates read b, c and d. No post-scan filter, so no OR-widen.
    val mergedScan0 = v2ScanReadingOn(strictOnA, Seq("a", "b", "c", "d"))
    val mergedScan = mergedScan0.copy(
      pushedFilters = Seq(mergedScan0.output.find(_.name == "a").get > 0))
    val mergedSubquery = mergedScan
      .groupBy()(sum($"b").as("agg_b"), sum($"c").as("agg_c"), sum($"d").as("agg_d"))
      .select(CreateNamedStruct(Seq(
        Literal("agg_b"), $"agg_b",
        Literal("agg_c"), $"agg_c",
        Literal("agg_d"), $"agg_d")).as("mergedValue"))
    val analyzedMergedSubquery = mergedSubquery.analyze
    val correctAnswer = WithCTE(
      testRelation.select(
        extractorExpression(0, analyzedMergedSubquery.output, 0),
        extractorExpression(0, analyzedMergedSubquery.output, 1),
        extractorExpression(0, analyzedMergedSubquery.output, 2)),
      Seq(definitionNode(analyzedMergedSubquery, 0)))
    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, correctAnswer.analyze)
    assert(v2Scans(optimized).head.pushedFilters.exists(_.references.exists(_.name == "a")),
      "the merged scan must keep the shared strict filter on a")
  }

  test("SPARK-40259: pushed limit/offset/top-N/sample block merging " +
      "(hasBlockingPushdown classification)") {
    // hasBlockingPushdown flags every pushdown a rebuilt scan cannot reproduce. Drive each term
    // through the real pushdown (the stub opts into all of them) so that a term silently dropped
    // from the denylist is caught. Limit, offset and sample each set only their own holder field,
    // so they are isolated; top-N sets sortOrders AND pushedLimit, so it exercises the top-N path
    // but does not isolate the sortOrders term (no plan pushes a sort order without a limit).
    def pushDown(op: DataSourceV2Relation => LogicalPlan): DataSourceV2ScanRelation = {
      val relation = DataSourceV2Relation(
        v2Table, toAttributes(v2Table.schema()), None, None, CaseInsensitiveStringMap.empty())
      V2ScanRelationPushDown(op(relation))
        .collectFirst { case s: DataSourceV2ScanRelation => s }.get
    }

    // Default-safe: only the plain column-pruning path grants mergeability, so a plain scan with
    // no non-reproducible pushdown must come out mergeable (guards the one granting site).
    assert(pushDown(r => Project(Seq(r.output.head), r)).mergeableScan,
      "a plain scan with only reproducible pushdowns must be mergeable")

    assert(!pushDown(r => Limit(Literal(1), r)).mergeableScan,
      "a pushed limit must block merging")
    assert(!pushDown(r => Offset(Literal(1), r)).mergeableScan,
      "a pushed offset must block merging")
    assert(
      !pushDown(r => Limit(Literal(1),
        Sort(Seq(SortOrder(r.output.head, Ascending)), global = true, r)))
        .mergeableScan,
      "a pushed top-N must block merging")
    assert(!pushDown(r => Sample(0.0, 0.5, withReplacement = false, Some(0L), r))
        .mergeableScan,
      "a pushed sample must block merging")

    // End-to-end: a scan that pushed a sample does not fuse with another (plain) scan of the same
    // table -- the classification declines the scan merge, so the plan is left unchanged. The two
    // read different columns, so they are not identical subplans and could only fuse via the
    // (blocked) scan merge, not the identical-plan reuse short-circuit.
    val sampled = pushDown(r => Sample(0.0, 0.5, withReplacement = false, Some(0L), r))
    val plain = v2ScanReading("a")
    val q = testRelation.select(
      ScalarSubquery(plain.groupBy()(sum($"a").as("s1"))),
      ScalarSubquery(sampled.groupBy()(sum(sampled.output(1)).as("s2"))))
    comparePlans(Optimize.execute(q.analyze), q.analyze)
  }
}

/**
 * Minimal readable DSv2 table whose scan opts into merging, for scan-merge tests.
 *
 * A pushed predicate that references only columns in `strictColumns` is fully enforced (strict):
 * accepted by the source and NOT returned as a post-scan filter. Any other predicate is
 * best-effort (stats): accepted for row-group pruning but returned so it is re-checked above the
 * scan. When `acceptsFilters` is false the source rejects everything: nothing is accepted and
 * every predicate is returned (used to exercise the "no pruning recovery" path).
 */
private class TestV2Table(
    tableSchema: StructType,
    acceptsFilters: Boolean = true,
    strictColumns: Set[String] = Set.empty,
    supportsScanMerging: Boolean = true,
    reportedOrderingCols: Seq[String] = Nil,
    reportedBucketPartition: Option[(Int, String)] = None,
    reportOnlyWhenNothingPushed: Boolean = false)
  extends Table with SupportsRead {
  override def name(): String = "test_v2_table"
  override def schema(): StructType = tableSchema
  override def capabilities(): java.util.Set[TableCapability] = {
    val caps = java.util.EnumSet.of(TableCapability.BATCH_READ)
    if (supportsScanMerging) caps.add(TableCapability.SCAN_MERGING)
    caps
  }
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new TestV2ScanBuilder(tableSchema, acceptsFilters, strictColumns, reportedOrderingCols,
      reportedBucketPartition, reportOnlyWhenNothingPushed)
}

private class TestV2ScanBuilder(
    tableSchema: StructType,
    acceptsFilters: Boolean,
    strictColumns: Set[String],
    reportedOrderingCols: Seq[String] = Nil,
    reportedBucketPartition: Option[(Int, String)] = None,
    reportOnlyWhenNothingPushed: Boolean = false)
  extends ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownV2Filters
    with SupportsPushDownLimit with SupportsPushDownOffset with SupportsPushDownTopN
    with SupportsPushDownTableSample {
  private var prunedSchema: StructType = tableSchema
  private var accepted: Array[Predicate] = Array.empty  // strict UNION stats (pushedPredicates)

  override def pruneColumns(requiredSchema: StructType): Unit = prunedSchema = requiredSchema

  // Opts into limit pushdown so V2ScanRelationPushDown records a merge-blocking pushedLimit.
  override def pushLimit(limit: Int): Boolean = true

  // Opts into offset/top-N/sample pushdown too, so V2ScanRelationPushDown records the matching
  // merge-blocking pushdown for each (used by the hasBlockingPushdown classification test).
  override def pushOffset(offset: Int): Boolean = true
  override def pushTopN(orders: Array[V2SortOrder], limit: Int): Boolean = true
  override def isPartiallyPushed(): Boolean = false
  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long): Boolean = true

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    if (!acceptsFilters) {
      predicates // reject everything: nothing accepted, all returned as post-scan
    } else {
      accepted = predicates
      predicates.filterNot(isStrict) // stats predicates are re-checked above the scan
    }
  }

  private def isStrict(p: Predicate): Boolean =
    p.references().nonEmpty &&
      p.references().forall(r => strictColumns.contains(r.fieldNames().mkString(".")))

  override def pushedPredicates(): Array[Predicate] = accepted
  override def build(): Scan = {
    val scan = TestV2Scan(prunedSchema, accepted.map(_.describe()).toSeq)
    if (reportedOrderingCols.isEmpty && reportedBucketPartition.isEmpty) {
      scan
    } else {
      TestV2ReportingScan(scan, reportedOrderingCols, reportedBucketPartition,
        reportOnlyWhenNothingPushed)
    }
  }
}

/** `pushed` records the `describe()` of the predicates pushed to the scan, for test assertions. */
private case class TestV2Scan(schema: StructType, pushed: Seq[String] = Seq.empty)
  extends Scan {
  override def readSchema(): StructType = schema
}

/**
 * A [[TestV2Scan]] that also reports an output ordering and/or a key-grouped partitioning, so a
 * scan rebuilt by the merge re-derives a report through `V2ScanPartitioningAndOrdering`. The plain
 * [[TestV2Scan]] reports nothing, which only ever exercises the degrading side of the not-worse
 * check.
 */
private case class TestV2ReportingScan(
    inner: TestV2Scan,
    orderingCols: Seq[String] = Nil,
    bucketPartition: Option[(Int, String)] = None,
    onlyWhenNothingPushed: Boolean = false)
  extends Scan with SupportsReportOrdering with SupportsReportPartitioning {
  override def readSchema(): StructType = inner.readSchema()

  // A source may report per scan: `outputOrdering`/`outputPartitioning` are answered by the Scan
  // the connector built AFTER seeing what it accepted, so an ordering that only holds over the
  // unpruned file set can legitimately disappear once a filter is pushed. `onlyWhenNothingPushed`
  // models that, so a build offered a best-effort filter and one without it report differently.
  private def reports: Boolean = !onlyWhenNothingPushed || inner.pushed.isEmpty

  override def outputOrdering(): Array[V2SortOrder] = if (reports) {
    orderingCols.map(c => Expressions.sort(FieldReference(c), V2SortDirection.ASCENDING)).toArray
  } else {
    Array.empty
  }

  override def outputPartitioning(): V2Partitioning = bucketPartition.filter(_ => reports) match {
    case Some((numBuckets, col)) =>
      new V2KeyGroupedPartitioning(Array(Expressions.bucket(numBuckets, col)), 0)
    case None => new V2UnknownPartitioning(0)
  }
}

/**
 * A `FunctionCatalog` that resolves `bucket`, binding it to a FRESH function instance every time --
 * as a real connector does, since `V2ExpressionUtils.loadV2FunctionOpt` binds the function afresh
 * for every derivation of a reported transform. Spark's own `UnboundBucketFunction` hands back a
 * singleton, which would hide the fact that relating two independently derived reports rests on the
 * function's own `equals` (see `BoundFunction#equals`).
 */
private object TestFreshBindFunctionCatalog extends FunctionCatalog {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = "test_fresh_bind"
  override def listFunctions(namespace: Array[String]): Array[Identifier] =
    Array(Identifier.of(Array.empty, "bucket"))
  override def loadFunction(ident: Identifier): UnboundFunction = ident.name() match {
    case "bucket" => TestUnboundBucketFunction
    // loadV2FunctionOpt treats this as "function not found" and reports no partitioning.
    case other => throw new UnsupportedOperationException(s"no such function: $other")
  }
}

private object TestUnboundBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = new TestBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

/**
 * A bound `bucket` that implements `equals`/`hashCode` over the state identifying it, as
 * `BoundFunction` asks a connector to. That is what lets Spark recognize two separately bound
 * instances as the same transform: `V2ExpressionUtils` binds afresh on every derivation, so two
 * reported partitionings hold two different instances and nothing but this comparison relates them.
 * `canonicalName` is stable too (its default returns a fresh random UUID), which is what a
 * storage-partitioned join compares.
 */
private class TestBucketFunction extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = "testcat.bucket"

  override def equals(other: Any): Boolean = other match {
    case that: TestBucketFunction =>
      canonicalName() == that.canonicalName() && resultType() == that.resultType() &&
        // `Array` equality is reference identity, so compare the elements.
        inputTypes().sameElements(that.inputTypes())
    case _ => false
  }

  override def hashCode(): Int = canonicalName().hashCode
}

/**
 * The same catalog with a bound `bucket` that does NOT implement `equals`/`hashCode` -- a connector
 * that meets only the `canonicalName` obligation. Two of its separately bound instances never
 * compare equal, so Spark cannot relate two reports of one transform.
 */
private object TestNotComparableFunctionCatalog extends FunctionCatalog {
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  override def name(): String = "test_not_comparable"
  override def listFunctions(namespace: Array[String]): Array[Identifier] =
    Array(Identifier.of(Array.empty, "bucket"))
  override def loadFunction(ident: Identifier): UnboundFunction = ident.name() match {
    case "bucket" => TestUnboundNotComparableBucketFunction
    case other => throw new UnsupportedOperationException(s"no such function: $other")
  }
}

private object TestUnboundNotComparableBucketFunction extends UnboundFunction {
  override def bind(inputType: StructType): BoundFunction = new TestNotComparableBucketFunction
  override def description(): String = name()
  override def name(): String = "bucket"
}

/** A plain class, so it inherits the identity comparison from `Object`. */
private class TestNotComparableBucketFunction extends ScalarFunction[Int] {
  override def inputTypes(): Array[DataType] = Array(IntegerType, IntegerType)
  override def resultType(): DataType = IntegerType
  override def name(): String = "bucket"
  override def canonicalName(): String = "testcat.bucket"
}

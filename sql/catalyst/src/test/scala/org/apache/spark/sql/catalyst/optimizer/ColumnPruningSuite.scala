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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}

class ColumnPruningSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Column pruning", FixedPoint(100),
      PushPredicateThroughNonJoin,
      ColumnPruning,
      RemoveNoopOperators,
      CollapseProject) :: Nil
  }

  test("Column pruning for Generate when Generate.unrequiredChildIndex = child.output") {
    val input = LocalRelation($"a".int, $"b".int, $"c".array(StringType))

    val query =
      input
        .generate(Explode($"c"), outputNames = "explode" :: Nil)
        .select($"c", $"explode")
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .select($"c")
        .generate(Explode($"c"), outputNames = "explode" :: Nil)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Fill Generate.unrequiredChildIndex if possible") {
    val input = LocalRelation($"b".array(StringType))

    val query =
      input
        .generate(Explode($"b"), outputNames = "explode" :: Nil)
        .select(($"explode" + 1).as("result"))
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .generate(Explode($"b"), unrequiredChildIndex = input.output.zipWithIndex.map(_._2),
          outputNames = "explode" :: Nil)
         .select(($"explode" + 1).as("result"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Another fill Generate.unrequiredChildIndex if possible") {
    val input = LocalRelation($"a".int, $"b".int, $"c1".string, $"c2".string)

    val query =
      input
        .generate(Explode(CreateArray(Seq($"c1", $"c2"))), outputNames = "explode" :: Nil)
        .select($"a", $"c1", $"explode")
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .select($"a", $"c1", $"c2")
        .generate(Explode(CreateArray(Seq($"c1", $"c2"))),
          unrequiredChildIndex = Seq(2),
          outputNames = "explode" :: Nil)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Nested column pruning for Generate") {
    def runTest(
        origGenerator: Generator,
        replacedGenerator: Seq[String] => Generator,
        aliasedExprs: Seq[String] => Seq[Expression],
        unrequiredChildIndex: Seq[Int],
        generatorOutputNames: Seq[String]): Unit = {
      withSQLConf(SQLConf.NESTED_PRUNING_ON_EXPRESSIONS.key -> "true") {
        val structType = StructType.fromDDL("d double, e array<string>, f double, g double, " +
          "h array<struct<h1: int, h2: double>>")
        val input = LocalRelation($"a".int, $"b".int, $"c".struct(structType))
        val generatorOutputs = generatorOutputNames.map(UnresolvedAttribute(_))

        val selectedExprs = Seq(UnresolvedAttribute("a"), $"c".getField("d")) ++
          generatorOutputs

        val query =
          input
            .generate(origGenerator, outputNames = generatorOutputNames)
            .select(selectedExprs: _*)
            .analyze

        val optimized = Optimize.execute(query)

        val aliases = NestedColumnAliasingSuite.collectGeneratedAliases(optimized).toSeq

        val selectedFields = UnresolvedAttribute("a") +: aliasedExprs(aliases)
        val finalSelectedExprs = Seq(UnresolvedAttribute("a"), $"${aliases(0)}".as("c.d")) ++
          generatorOutputs

        val correctAnswer =
          input
            .select(selectedFields: _*)
            .generate(replacedGenerator(aliases),
              unrequiredChildIndex = unrequiredChildIndex,
              outputNames = generatorOutputNames)
            .select(finalSelectedExprs: _*)
            .analyze

        comparePlans(optimized, correctAnswer)
      }
    }

    runTest(
      Explode($"c".getField("e")),
      aliases => Explode($"${aliases(1)}".as("c.e")),
      aliases => Seq($"c".getField("d").as(aliases(0)), $"c".getField("e").as(aliases(1))),
      Seq(2),
      Seq("explode")
    )
    runTest(Stack(2 :: $"c".getField("f") :: $"c".getField("g") :: Nil),
      aliases => Stack(2 :: $"${aliases(1)}".as("c.f") :: $"${aliases(2)}".as("c.g") :: Nil),
      aliases => Seq(
        $"c".getField("d").as(aliases(0)),
        $"c".getField("f").as(aliases(1)),
        $"c".getField("g").as(aliases(2))),
      Seq(2, 3),
      Seq("stack")
    )
    runTest(
      PosExplode($"c".getField("e")),
      aliases => PosExplode($"${aliases(1)}".as("c.e")),
      aliases => Seq($"c".getField("d").as(aliases(0)), $"c".getField("e").as(aliases(1))),
      Seq(2),
      Seq("pos", "explode")
    )
    runTest(
      Inline($"c".getField("h")),
      aliases => Inline($"${aliases(1)}".as("c.h")),
      aliases => Seq($"c".getField("d").as(aliases(0)), $"c".getField("h").as(aliases(1))),
      Seq(2),
      Seq("h1", "h2")
    )
  }

  test("Column pruning for Project on Sort") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double)

    val query = input.orderBy($"b".asc).select($"a").analyze
    val optimized = Optimize.execute(query)

    val correctAnswer = input.select($"a", $"b").orderBy($"b".asc).select($"a").analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning for Expand") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double)
    val query =
      Aggregate(
        Seq($"aa", $"gid"),
        Seq(sum($"c").as("sum")),
        Expand(
          Seq(
            Seq($"a", $"b", $"c", Literal.create(null, StringType), 1),
            Seq($"a", $"b", $"c", $"a", 2)),
          Seq($"a", $"b", $"c", $"aa".int, $"gid".int),
          input)).analyze
    val optimized = Optimize.execute(query)

    val expected =
      Aggregate(
        Seq($"aa", $"gid"),
        Seq(sum($"c").as("sum")),
        Expand(
          Seq(
            Seq($"c", Literal.create(null, StringType), 1),
            Seq($"c", $"a", 2)),
          Seq($"c", $"aa".int, $"gid".int),
          Project(Seq($"a", $"c"),
            input))).analyze

    comparePlans(optimized, expected)
  }

  test("Column pruning on Filter") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double)
    val plan1 = Filter($"a" > 1, input).analyze
    comparePlans(Optimize.execute(plan1), plan1)
    val query = Project($"a" :: Nil, Filter($"c" > Literal(0.0), input)).analyze
    comparePlans(Optimize.execute(query), query)
    val plan2 = Filter($"b" > 1, Project(Seq($"a", $"b"), input)).analyze
    val expected2 = Project(Seq($"a", $"b"), Filter($"b" > 1, input)).analyze
    comparePlans(Optimize.execute(plan2), expected2)
    val plan3 = Project(Seq($"a"), Filter($"b" > 1, Project(Seq($"a", $"b"), input))).analyze
    val expected3 = Project(Seq($"a"), Filter($"b" > 1, input)).analyze
    comparePlans(Optimize.execute(plan3), expected3)
  }

  test("Column pruning on except/intersect/distinct") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double)
    val query = Project($"a" :: Nil, Except(input, input, isAll = false)).analyze
    comparePlans(Optimize.execute(query), query)

    val query2 = Project($"a" :: Nil, Intersect(input, input, isAll = false)).analyze
    comparePlans(Optimize.execute(query2), query2)
    val query3 = Project($"a" :: Nil, Distinct(input)).analyze
    comparePlans(Optimize.execute(query3), query3)
  }

  test("Column pruning on Project") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double)
    val query = Project($"a" :: Nil, Project(Seq($"a", $"b"), input)).analyze
    val expected = Project(Seq($"a"), input).analyze
    comparePlans(Optimize.execute(query), expected)
  }

  test("Eliminate the Project with an empty projectList") {
    val input = OneRowRelation()
    val expected = Project(Literal(1).as("1") :: Nil, input).analyze

    val query1 =
      Project(Literal(1).as("1") :: Nil, Project(Literal(1).as("1") :: Nil, input)).analyze
    comparePlans(Optimize.execute(query1), expected)

    val query2 =
      Project(Literal(1).as("1") :: Nil, Project(Nil, input)).analyze
    comparePlans(Optimize.execute(query2), expected)

    // to make sure the top Project will not be removed.
    comparePlans(Optimize.execute(expected), expected)
  }

  test("column pruning for group") {
    val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
    val originalQuery =
      testRelation
        .groupBy($"a")($"a", count($"b"))
        .select($"a")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select($"a")
        .groupBy($"a")($"a").analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for group with alias") {
    val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

    val originalQuery =
      testRelation
        .groupBy($"a")($"a" as "c", count($"b"))
        .select($"c")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select($"a")
        .groupBy($"a")($"a" as "c").analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for Project(ne, Limit)") {
    val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

    val originalQuery =
      testRelation
        .select($"a", $"b")
        .limit(2)
        .select($"a")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select($"a")
        .limit(2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down project past sort") {
    val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
    val x = testRelation.subquery("x")

    // push down valid
    val originalQuery = {
      x.select($"a", $"b")
        .sortBy(SortOrder($"a", Ascending))
        .select($"a")
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.select($"a")
        .sortBy(SortOrder($"a", Ascending)).analyze

    comparePlans(optimized, correctAnswer)

    // push down invalid
    val originalQuery1 = {
      x.select($"a", $"b")
        .sortBy(SortOrder($"a", Ascending))
        .select($"b")
    }

    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 =
      x.select($"a", $"b")
        .sortBy(SortOrder($"a", Ascending))
        .select($"b").analyze

    comparePlans(optimized1, correctAnswer1)
  }

  test("Column pruning on Window with useless aggregate functions") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double, $"d".int)
    val winSpec = windowSpec($"a" :: Nil, $"d".asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count($"d"), winSpec)

    val originalQuery = input.groupBy($"a", $"c", $"d")($"a", $"c", $"d",
      winExpr.as("window")).select($"a", $"c")
    val correctAnswer = input.select($"a", $"c", $"d").groupBy($"a", $"c", $"d")($"a", $"c").analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Window with selected agg expressions") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double, $"d".int)
    val winSpec = windowSpec($"a" :: Nil, $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    val originalQuery =
      input.select($"a", $"b", $"c", $"d", winExpr.as("window"))
        .where($"window" > 1).select($"a", $"c")
    val correctAnswer =
      input.select($"a", $"b", $"c")
        .window(winExpr.as("window") :: Nil, $"a" :: Nil, $"b".asc :: Nil)
        .where($"window" > 1).select($"a", $"c").analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Window in select") {
    val input = LocalRelation($"a".int, $"b".string, $"c".double, $"d".int)
    val winSpec = windowSpec($"a" :: Nil, $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    val originalQuery = input.select($"a", $"b", $"c", $"d",
      winExpr.as("window")).select($"a", $"c")
    val correctAnswer = input.select($"a", $"c").analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Union") {
    val input1 = LocalRelation($"a".int, $"b".string, $"c".double)
    val input2 = LocalRelation($"c".int, $"d".string, $"e".double)
    val query = Project($"b" :: Nil, Union(input1 :: input2 :: Nil)).analyze
    val expected = Union(Project($"b" :: Nil, input1) :: Project($"d" :: Nil, input2) :: Nil)
      .analyze
    comparePlans(Optimize.execute(query), expected)
  }

  test("Remove redundant projects in column pruning rule") {
    val input = LocalRelation($"key".int, $"value".string)

    val query =
      Project(Seq($"x.key", $"y.key"),
        Join(
          SubqueryAlias("x", input),
          SubqueryAlias("y", input), Inner, None, JoinHint.NONE)).analyze

    val optimized = Optimize.execute(query)

    val expected =
      Join(
        Project(Seq($"x.key"), SubqueryAlias("x", input)),
        Project(Seq($"y.key"), SubqueryAlias("y", input)),
        Inner, None, JoinHint.NONE).analyze

    comparePlans(optimized, expected)
  }

  implicit private def productEncoder[T <: Product : TypeTag]: ExpressionEncoder[T] =
    ExpressionEncoder[T]()
  private val func = identity[Iterator[OtherTuple]] _

  test("Column pruning on MapPartitions") {
    val input = LocalRelation($"_1".int, $"_2".int, $"c".int)
    val plan1 = MapPartitions(func, input)
    val correctAnswer1 =
      MapPartitions(func, Project(Seq($"_1", $"_2"), input)).analyze
    comparePlans(Optimize.execute(plan1.analyze), correctAnswer1)
  }

  test("push project down into sample") {
    val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
    val x = testRelation.subquery("x")

    val query1 = Sample(0.0, 0.6, false, 11L, x).select($"a")
    val optimized1 = Optimize.execute(query1.analyze)
    val expected1 = Sample(0.0, 0.6, false, 11L, x.select($"a"))
    comparePlans(optimized1, expected1.analyze)

    val query2 = Sample(0.0, 0.6, false, 11L, x).select($"a" as "aa")
    val optimized2 = Optimize.execute(query2.analyze)
    val expected2 = Sample(0.0, 0.6, false, 11L, x.select($"a" as "aa"))
    comparePlans(optimized2, expected2.analyze)
  }

  test("SPARK-24696 ColumnPruning rule fails to remove extra Project") {
    val input = LocalRelation($"key".int, $"value".string)
    val query = input.select($"key").where(rand(0L) > 0.5).where($"key" < 10).analyze
    val optimized = Optimize.execute(query)
    val expected = input.where(rand(0L) > 0.5).where($"key" < 10).select($"key").analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-36559 Prune and drop distributed-sequence if the produced column is not referred") {
    val input = LocalRelation($"a".int, $"b".int, $"c".int)
    val plan1 = AttachDistributedSequence($"d".int, input).select($"a")
    val correctAnswer1 = Project(Seq($"a"), input).analyze
    comparePlans(Optimize.execute(plan1.analyze), correctAnswer1)
  }

  test("SPARK-39445: Remove the window if windowExpressions is empty in column pruning") {
    object CustomOptimize extends RuleExecutor[LogicalPlan] {
      val batches = Batch("Column pruning", FixedPoint(10),
        ColumnPruning,
        CollapseProject) :: Nil
    }

    val relation = LocalRelation($"a".int, $"b".string, $"c".double, $"d".int)
    val winSpec = windowSpec($"a" :: Nil, $"b".asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count($"b"), winSpec)

    val originalQuery = relation.select($"a", $"b", $"c", $"d",
      winExpr.as("window")).select($"a", $"c")
    val correctAnswer = relation.select($"a", $"c")

    comparePlans(CustomOptimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}

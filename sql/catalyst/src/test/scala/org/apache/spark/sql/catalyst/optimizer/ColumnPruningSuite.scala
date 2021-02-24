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
    val input = LocalRelation("a".attr.int, "b".attr.int, "c".attr.array(StringType))

    val query =
      input
        .generate(Explode("c".attr), outputNames = "explode" :: Nil)
        .select("c".attr, "explode".attr)
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .select("c".attr)
        .generate(Explode("c".attr), outputNames = "explode" :: Nil)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Fill Generate.unrequiredChildIndex if possible") {
    val input = LocalRelation("b".attr.array(StringType))

    val query =
      input
        .generate(Explode("b".attr), outputNames = "explode" :: Nil)
        .select(("explode".attr + 1).as("result"))
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .generate(Explode("b".attr), unrequiredChildIndex = input.output.zipWithIndex.map(_._2),
          outputNames = "explode" :: Nil)
         .select(("explode".attr + 1).as("result"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Another fill Generate.unrequiredChildIndex if possible") {
    val input = LocalRelation("a".attr.int, "b".attr.int, "c1".attr.string, "c2".attr.string)

    val query =
      input
        .generate(Explode(CreateArray(Seq("c1".attr, "c2".attr))), outputNames = "explode" :: Nil)
        .select("a".attr, "c1".attr, "explode".attr)
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .select("a".attr, "c1".attr, "c2".attr)
        .generate(Explode(CreateArray(Seq("c1".attr, "c2".attr))),
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
        val input = LocalRelation("a".attr.int, "b".attr.int, "c".attr.struct(structType))
        val generatorOutputs = generatorOutputNames.map(UnresolvedAttribute(_))

        val selectedExprs = Seq(UnresolvedAttribute("a"), "c".attr.getField("d")) ++
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
      Explode("c".attr.getField("e")),
      aliases => Explode($"${aliases(1)}".as("c.e")),
      aliases => Seq("c".attr.getField("d").as(aliases(0)), "c".attr.getField("e").as(aliases(1))),
      Seq(2),
      Seq("explode")
    )
    runTest(Stack(2 :: "c".attr.getField("f") :: "c".attr.getField("g") :: Nil),
      aliases => Stack(2 :: $"${aliases(1)}".as("c.f") :: $"${aliases(2)}".as("c.g") :: Nil),
      aliases => Seq(
        "c".attr.getField("d").as(aliases(0)),
        "c".attr.getField("f").as(aliases(1)),
        "c".attr.getField("g").as(aliases(2))),
      Seq(2, 3),
      Seq("stack")
    )
    runTest(
      PosExplode("c".attr.getField("e")),
      aliases => PosExplode($"${aliases(1)}".as("c.e")),
      aliases => Seq("c".attr.getField("d").as(aliases(0)), "c".attr.getField("e").as(aliases(1))),
      Seq(2),
      Seq("pos", "explode")
    )
    runTest(
      Inline("c".attr.getField("h")),
      aliases => Inline($"${aliases(1)}".as("c.h")),
      aliases => Seq("c".attr.getField("d").as(aliases(0)), "c".attr.getField("h").as(aliases(1))),
      Seq(2),
      Seq("h1", "h2")
    )
  }

  test("Column pruning for Project on Sort") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double)

    val query = input.orderBy("b".attr.asc).select("a".attr).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer =
      input.select("a".attr, "b".attr).orderBy("b".attr.asc).select("a".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning for Expand") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double)
    val query =
      Aggregate(
        Seq("aa".attr, "gid".attr),
        Seq(sum("c".attr).as("sum")),
        Expand(
          Seq(
            Seq("a".attr, "b".attr, "c".attr, Literal.create(null, StringType), 1),
            Seq("a".attr, "b".attr, "c".attr, "a".attr, 2)),
          Seq("a".attr, "b".attr, "c".attr, "aa".attr.int, "gid".attr.int),
          input)).analyze
    val optimized = Optimize.execute(query)

    val expected =
      Aggregate(
        Seq("aa".attr, "gid".attr),
        Seq(sum("c".attr).as("sum")),
        Expand(
          Seq(
            Seq("c".attr, Literal.create(null, StringType), 1),
            Seq("c".attr, "a".attr, 2)),
          Seq("c".attr, "aa".attr.int, "gid".attr.int),
          Project(Seq("a".attr, "c".attr),
            input))).analyze

    comparePlans(optimized, expected)
  }

  test("Column pruning for ScriptTransformation") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double)
    val query =
      ScriptTransformation(
        Seq("a".attr, "b".attr),
        "func",
        Seq.empty,
        input,
        null).analyze
    val optimized = Optimize.execute(query)

    val expected =
      ScriptTransformation(
        Seq("a".attr, "b".attr),
        "func",
        Seq.empty,
        Project(
          Seq("a".attr, "b".attr),
          input),
        null).analyze

    comparePlans(optimized, expected)
  }

  test("Column pruning on Filter") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double)
    val plan1 = Filter("a".attr > 1, input).analyze
    comparePlans(Optimize.execute(plan1), plan1)
    val query = Project("a".attr :: Nil, Filter("c".attr > Literal(0.0), input)).analyze
    comparePlans(Optimize.execute(query), query)
    val plan2 = Filter("b".attr > 1, Project(Seq("a".attr, "b".attr), input)).analyze
    val expected2 = Project(Seq("a".attr, "b".attr), Filter("b".attr > 1, input)).analyze
    comparePlans(Optimize.execute(plan2), expected2)
    val plan3 =
      Project(Seq("a".attr), Filter("b".attr > 1, Project(Seq("a".attr, "b".attr), input))).analyze
    val expected3 = Project(Seq("a".attr), Filter("b".attr > 1, input)).analyze
    comparePlans(Optimize.execute(plan3), expected3)
  }

  test("Column pruning on except/intersect/distinct") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double)
    val query = Project("a".attr :: Nil, Except(input, input, isAll = false)).analyze
    comparePlans(Optimize.execute(query), query)

    val query2 = Project("a".attr :: Nil, Intersect(input, input, isAll = false)).analyze
    comparePlans(Optimize.execute(query2), query2)
    val query3 = Project("a".attr :: Nil, Distinct(input)).analyze
    comparePlans(Optimize.execute(query3), query3)
  }

  test("Column pruning on Project") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double)
    val query = Project("a".attr :: Nil, Project(Seq("a".attr, "b".attr), input)).analyze
    val expected = Project(Seq("a".attr), input).analyze
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
    val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)
    val originalQuery =
      testRelation
        .groupBy("a".attr)("a".attr, count("b".attr))
        .select("a".attr)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select("a".attr)
        .groupBy("a".attr)("a".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for group with alias") {
    val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)

    val originalQuery =
      testRelation
        .groupBy("a".attr)("a".attr as "c", count("b".attr))
        .select("c".attr)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select("a".attr)
        .groupBy("a".attr)("a".attr as "c").analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for Project(ne, Limit)") {
    val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)

    val originalQuery =
      testRelation
        .select("a".attr, "b".attr)
        .limit(2)
        .select("a".attr)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select("a".attr)
        .limit(2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down project past sort") {
    val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)
    val x = testRelation.subquery("x")

    // push down valid
    val originalQuery = {
      x.select("a".attr, "b".attr)
        .sortBy(SortOrder("a".attr, Ascending))
        .select("a".attr)
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.select("a".attr)
        .sortBy(SortOrder("a".attr, Ascending)).analyze

    comparePlans(optimized, correctAnswer)

    // push down invalid
    val originalQuery1 = {
      x.select("a".attr, "b".attr)
        .sortBy(SortOrder("a".attr, Ascending))
        .select("b".attr)
    }

    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 =
      x.select("a".attr, "b".attr)
        .sortBy(SortOrder("a".attr, Ascending))
        .select("b".attr).analyze

    comparePlans(optimized1, correctAnswer1)
  }

  test("Column pruning on Window with useless aggregate functions") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double, "d".attr.int)
    val winSpec = windowSpec("a".attr :: Nil, "d".attr.asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count("d".attr), winSpec)

    val originalQuery = input
      .groupBy("a".attr, "c".attr, "d".attr)("a".attr, "c".attr, "d".attr, winExpr.as("window"))
      .select("a".attr, "c".attr)
    val correctAnswer = input.select("a".attr, "c".attr, "d".attr)
      .groupBy("a".attr, "c".attr, "d".attr)("a".attr, "c".attr).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Window with selected agg expressions") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double, "d".attr.int)
    val winSpec = windowSpec("a".attr :: Nil, "b".attr.asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count("b".attr), winSpec)

    val originalQuery = input
      .select("a".attr, "b".attr, "c".attr, "d".attr, winExpr.as("window"))
      .where("window".attr > 1).select("a".attr, "c".attr)
    val correctAnswer =
      input.select("a".attr, "b".attr, "c".attr)
        .window(winExpr.as("window") :: Nil, "a".attr :: Nil, "b".attr.asc :: Nil)
        .where("window".attr > 1).select("a".attr, "c".attr).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Window in select") {
    val input = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double, "d".attr.int)
    val winSpec = windowSpec("a".attr :: Nil, "b".attr.asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count("b".attr), winSpec)

    val originalQuery = input.select("a".attr, "b".attr, "c".attr, "d".attr,
      winExpr.as("window")).select("a".attr, "c".attr)
    val correctAnswer = input.select("a".attr, "c".attr).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Union") {
    val input1 = LocalRelation("a".attr.int, "b".attr.string, "c".attr.double)
    val input2 = LocalRelation("c".attr.int, "d".attr.string, "e".attr.double)
    val query = Project("b".attr :: Nil, Union(input1 :: input2 :: Nil)).analyze
    val expected =
      Union(Project("b".attr :: Nil, input1) :: Project("d".attr :: Nil, input2) :: Nil).analyze
    comparePlans(Optimize.execute(query), expected)
  }

  test("Remove redundant projects in column pruning rule") {
    val input = LocalRelation("key".attr.int, "value".attr.string)

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

  implicit private def productEncoder[T <: Product : TypeTag] = ExpressionEncoder[T]()
  private val func = identity[Iterator[OtherTuple]] _

  test("Column pruning on MapPartitions") {
    val input = LocalRelation("_1".attr.int, "_2".attr.int, "c".attr.int)
    val plan1 = MapPartitions(func, input)
    val correctAnswer1 =
      MapPartitions(func, Project(Seq("_1".attr, "_2".attr), input)).analyze
    comparePlans(Optimize.execute(plan1.analyze), correctAnswer1)
  }

  test("push project down into sample") {
    val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)
    val x = testRelation.subquery("x")

    val query1 = Sample(0.0, 0.6, false, 11L, x).select("a".attr)
    val optimized1 = Optimize.execute(query1.analyze)
    val expected1 = Sample(0.0, 0.6, false, 11L, x.select("a".attr))
    comparePlans(optimized1, expected1.analyze)

    val query2 = Sample(0.0, 0.6, false, 11L, x).select("a".attr as "aa")
    val optimized2 = Optimize.execute(query2.analyze)
    val expected2 = Sample(0.0, 0.6, false, 11L, x.select("a".attr as "aa"))
    comparePlans(optimized2, expected2.analyze)
  }

  test("SPARK-24696 ColumnPruning rule fails to remove extra Project") {
    val input = LocalRelation("key".attr.int, "value".attr.string)
    val query = input.select("key".attr).where(rand(0L) > 0.5).where("key".attr < 10).analyze
    val optimized = Optimize.execute(query)
    val expected = input.where(rand(0L) > 0.5).where("key".attr < 10).select("key".attr).analyze
    comparePlans(optimized, expected)
  }
  // todo: add more tests for column pruning
}

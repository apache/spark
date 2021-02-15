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
    val input = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").array(StringType))

    val query =
      input
        .generate(Explode(Symbol("c")), outputNames = "explode" :: Nil)
        .select(Symbol("c"), Symbol("explode"))
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .select(Symbol("c"))
        .generate(Explode(Symbol("c")), outputNames = "explode" :: Nil)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Fill Generate.unrequiredChildIndex if possible") {
    val input = LocalRelation(Symbol("b").array(StringType))

    val query =
      input
        .generate(Explode(Symbol("b")), outputNames = "explode" :: Nil)
        .select((Symbol("explode") + 1).as("result"))
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .generate(Explode(Symbol("b")), unrequiredChildIndex = input.output.zipWithIndex.map(_._2),
          outputNames = "explode" :: Nil)
         .select((Symbol("explode") + 1).as("result"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Another fill Generate.unrequiredChildIndex if possible") {
    val input =
      LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c1").string, Symbol("c2").string)

    val query =
      input
        .generate(
          Explode(CreateArray(Seq(Symbol("c1"), Symbol("c2")))), outputNames = "explode" :: Nil)
        .select(Symbol("a"), Symbol("c1"), Symbol("explode"))
        .analyze

    val optimized = Optimize.execute(query)

    val correctAnswer =
      input
        .select(Symbol("a"), Symbol("c1"), Symbol("c2"))
        .generate(Explode(CreateArray(Seq(Symbol("c1"), Symbol("c2")))),
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
        val input = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").struct(structType))
        val generatorOutputs = generatorOutputNames.map(UnresolvedAttribute(_))

        val selectedExprs = Seq(UnresolvedAttribute("a"), Symbol("c").getField("d")) ++
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
      Explode(Symbol("c").getField("e")),
      aliases => Explode($"${aliases(1)}".as("c.e")),
      aliases =>
        Seq(Symbol("c").getField("d").as(aliases(0)), Symbol("c").getField("e").as(aliases(1))),
      Seq(2),
      Seq("explode")
    )
    runTest(Stack(2 :: Symbol("c").getField("f") :: Symbol("c").getField("g") :: Nil),
      aliases => Stack(2 :: $"${aliases(1)}".as("c.f") :: $"${aliases(2)}".as("c.g") :: Nil),
      aliases => Seq(
        Symbol("c").getField("d").as(aliases(0)),
        Symbol("c").getField("f").as(aliases(1)),
        Symbol("c").getField("g").as(aliases(2))),
      Seq(2, 3),
      Seq("stack")
    )
    runTest(
      PosExplode(Symbol("c").getField("e")),
      aliases => PosExplode($"${aliases(1)}".as("c.e")),
      aliases =>
        Seq(Symbol("c").getField("d").as(aliases(0)), Symbol("c").getField("e").as(aliases(1))),
      Seq(2),
      Seq("pos", "explode")
    )
    runTest(
      Inline(Symbol("c").getField("h")),
      aliases => Inline($"${aliases(1)}".as("c.h")),
      aliases =>
        Seq(Symbol("c").getField("d").as(aliases(0)), Symbol("c").getField("h").as(aliases(1))),
      Seq(2),
      Seq("h1", "h2")
    )
  }

  test("Column pruning for Project on Sort") {
    val input = LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double)

    val query = input.orderBy(Symbol("b").asc).select(Symbol("a")).analyze
    val optimized = Optimize.execute(query)

    val correctAnswer = input.select(
      Symbol("a"), Symbol("b")).orderBy(Symbol("b").asc).select(Symbol("a")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning for Expand") {
    val input = LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double)
    val query =
      Aggregate(
        Seq(Symbol("aa"), Symbol("gid")),
        Seq(sum(Symbol("c")).as("sum")),
        Expand(
          Seq(
            Seq(Symbol("a"), Symbol("b"), Symbol("c"), Literal.create(null, StringType), 1),
            Seq(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("a"), 2)),
          Seq(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("aa").int, Symbol("gid").int),
          input)).analyze
    val optimized = Optimize.execute(query)

    val expected =
      Aggregate(
        Seq(Symbol("aa"), Symbol("gid")),
        Seq(sum(Symbol("c")).as("sum")),
        Expand(
          Seq(
            Seq(Symbol("c"), Literal.create(null, StringType), 1),
            Seq(Symbol("c"), Symbol("a"), 2)),
          Seq(Symbol("c"), Symbol("aa").int, Symbol("gid").int),
          Project(Seq(Symbol("a"), Symbol("c")),
            input))).analyze

    comparePlans(optimized, expected)
  }

  test("Column pruning for ScriptTransformation") {
    val input = LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double)
    val query =
      ScriptTransformation(
        Seq(Symbol("a"), Symbol("b")),
        "func",
        Seq.empty,
        input,
        null).analyze
    val optimized = Optimize.execute(query)

    val expected =
      ScriptTransformation(
        Seq(Symbol("a"), Symbol("b")),
        "func",
        Seq.empty,
        Project(
          Seq(Symbol("a"), Symbol("b")),
          input),
        null).analyze

    comparePlans(optimized, expected)
  }

  test("Column pruning on Filter") {
    val input = LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double)
    val plan1 = Filter(Symbol("a") > 1, input).analyze
    comparePlans(Optimize.execute(plan1), plan1)
    val query = Project(Symbol("a") :: Nil, Filter(Symbol("c") > Literal(0.0), input)).analyze
    comparePlans(Optimize.execute(query), query)
    val plan2 = Filter(Symbol("b") > 1, Project(Seq(Symbol("a"), Symbol("b")), input)).analyze
    val expected2 = Project(Seq(Symbol("a"), Symbol("b")), Filter(Symbol("b") > 1, input)).analyze
    comparePlans(Optimize.execute(plan2), expected2)
    val plan3 = Project(Seq(Symbol("a")), Filter(Symbol("b") > 1,
      Project(Seq(Symbol("a"), Symbol("b")), input))).analyze
    val expected3 = Project(Seq(Symbol("a")), Filter(Symbol("b") > 1, input)).analyze
    comparePlans(Optimize.execute(plan3), expected3)
  }

  test("Column pruning on except/intersect/distinct") {
    val input = LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double)
    val query = Project(Symbol("a") :: Nil, Except(input, input, isAll = false)).analyze
    comparePlans(Optimize.execute(query), query)

    val query2 = Project(Symbol("a") :: Nil, Intersect(input, input, isAll = false)).analyze
    comparePlans(Optimize.execute(query2), query2)
    val query3 = Project(Symbol("a") :: Nil, Distinct(input)).analyze
    comparePlans(Optimize.execute(query3), query3)
  }

  test("Column pruning on Project") {
    val input = LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double)
    val query = Project(Symbol("a") :: Nil, Project(Seq(Symbol("a"), Symbol("b")), input)).analyze
    val expected = Project(Seq(Symbol("a")), input).analyze
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
    val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)
    val originalQuery =
      testRelation
        .groupBy(Symbol("a"))(Symbol("a"), count(Symbol("b")))
        .select(Symbol("a"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Symbol("a"))
        .groupBy(Symbol("a"))(Symbol("a")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for group with alias") {
    val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)

    val originalQuery =
      testRelation
        .groupBy(Symbol("a"))(Symbol("a") as Symbol("c"), count(Symbol("b")))
        .select(Symbol("c"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Symbol("a"))
        .groupBy(Symbol("a"))(Symbol("a") as Symbol("c")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("column pruning for Project(ne, Limit)") {
    val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)

    val originalQuery =
      testRelation
        .select(Symbol("a"), Symbol("b"))
        .limit(2)
        .select(Symbol("a"))

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select(Symbol("a"))
        .limit(2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("push down project past sort") {
    val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)
    val x = testRelation.subquery(Symbol("x"))

    // push down valid
    val originalQuery = {
      x.select(Symbol("a"), Symbol("b"))
        .sortBy(SortOrder(Symbol("a"), Ascending))
        .select(Symbol("a"))
    }

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      x.select(Symbol("a"))
        .sortBy(SortOrder(Symbol("a"), Ascending)).analyze

    comparePlans(optimized, correctAnswer)

    // push down invalid
    val originalQuery1 = {
      x.select(Symbol("a"), Symbol("b"))
        .sortBy(SortOrder(Symbol("a"), Ascending))
        .select(Symbol("b"))
    }

    val optimized1 = Optimize.execute(originalQuery1.analyze)
    val correctAnswer1 =
      x.select(Symbol("a"), Symbol("b"))
        .sortBy(SortOrder(Symbol("a"), Ascending))
        .select(Symbol("b")).analyze

    comparePlans(optimized1, correctAnswer1)
  }

  test("Column pruning on Window with useless aggregate functions") {
    val input =
      LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double, Symbol("d").int)
    val winSpec = windowSpec(Symbol("a") :: Nil, Symbol("d").asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("d")), winSpec)

    val originalQuery = input.groupBy(Symbol("a"), Symbol("c"), Symbol("d"))(
      Symbol("a"), Symbol("c"), Symbol("d"),
      winExpr.as(Symbol("window"))).select(Symbol("a"), Symbol("c"))
    val correctAnswer = input.select(
      Symbol("a"), Symbol("c"), Symbol("d")).groupBy(Symbol("a"),
      Symbol("c"), Symbol("d"))(Symbol("a"), Symbol("c")).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Window with selected agg expressions") {
    val input =
      LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double, Symbol("d").int)
    val winSpec = windowSpec(Symbol("a") :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("b")), winSpec)

    val originalQuery =
      input.select(Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"),
        winExpr.as(Symbol("window"))).where(Symbol("window") > 1).select(Symbol("a"), Symbol("c"))
    val correctAnswer =
      input.select(Symbol("a"), Symbol("b"), Symbol("c"))
        .window(winExpr.as(Symbol("window")) :: Nil, Symbol("a") :: Nil, Symbol("b").asc :: Nil)
        .where(Symbol("window") > 1).select(Symbol("a"), Symbol("c")).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Window in select") {
    val input =
      LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double, Symbol("d").int)
    val winSpec = windowSpec(Symbol("a") :: Nil, Symbol("b").asc :: Nil, UnspecifiedFrame)
    val winExpr = windowExpr(count(Symbol("b")), winSpec)

    val originalQuery = input.select(
      Symbol("a"), Symbol("b"), Symbol("c"), Symbol("d"),
      winExpr.as(Symbol("window"))).select(Symbol("a"), Symbol("c"))
    val correctAnswer = input.select(Symbol("a"), Symbol("c")).analyze
    val optimized = Optimize.execute(originalQuery.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("Column pruning on Union") {
    val input1 = LocalRelation(Symbol("a").int, Symbol("b").string, Symbol("c").double)
    val input2 = LocalRelation(Symbol("c").int, Symbol("d").string, Symbol("e").double)
    val query = Project(Symbol("b") :: Nil, Union(input1 :: input2 :: Nil)).analyze
    val expected = Union(
      Project(Symbol("b") :: Nil, input1) :: Project(Symbol("d") :: Nil, input2) :: Nil).analyze
    comparePlans(Optimize.execute(query), expected)
  }

  test("Remove redundant projects in column pruning rule") {
    val input = LocalRelation(Symbol("key").int, Symbol("value").string)

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
    val input = LocalRelation(Symbol("_1").int, Symbol("_2").int, Symbol("c").int)
    val plan1 = MapPartitions(func, input)
    val correctAnswer1 =
      MapPartitions(func, Project(Seq(Symbol("_1"), Symbol("_2")), input)).analyze
    comparePlans(Optimize.execute(plan1.analyze), correctAnswer1)
  }

  test("push project down into sample") {
    val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)
    val x = testRelation.subquery(Symbol("x"))

    val query1 = Sample(0.0, 0.6, false, 11L, x).select(Symbol("a"))
    val optimized1 = Optimize.execute(query1.analyze)
    val expected1 = Sample(0.0, 0.6, false, 11L, x.select(Symbol("a")))
    comparePlans(optimized1, expected1.analyze)

    val query2 = Sample(0.0, 0.6, false, 11L, x).select(Symbol("a") as Symbol("aa"))
    val optimized2 = Optimize.execute(query2.analyze)
    val expected2 = Sample(0.0, 0.6, false, 11L, x.select(Symbol("a") as Symbol("aa")))
    comparePlans(optimized2, expected2.analyze)
  }

  test("SPARK-24696 ColumnPruning rule fails to remove extra Project") {
    val input = LocalRelation(Symbol("key").int, Symbol("value").string)
    val query = input.select(Symbol("key")).where(rand(0L) > 0.5).where(Symbol("key") < 10).analyze
    val optimized = Optimize.execute(query)
    val expected = input.where(
      rand(0L) > 0.5).where(Symbol("key") < 10).select(Symbol("key")).analyze
    comparePlans(optimized, expected)
  }
  // todo: add more tests for column pruning
}

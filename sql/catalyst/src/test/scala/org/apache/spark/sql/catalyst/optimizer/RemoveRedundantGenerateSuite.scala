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
import org.apache.spark.sql.catalyst.expressions.{ArrayDistinct, Explode, Literal, Rand, Size, Stack}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType, StringType}

class RemoveRedundantGenerateSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("RemoveRedundantGenerate", FixedPoint(10), RemoveRedundantGenerate) :: Nil
  }

  private val relation = LocalRelation(
    $"id".int,
    $"items".array(IntegerType),
    Symbol("m").map(StringType, IntegerType))

  test("remove Explode under DISTINCT when the generated column is not referenced") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .groupBy($"id")($"id")
      .analyze
    val expected = relation
      .where(Size($"items") > 0)
      .groupBy($"id")($"id")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("remove Generate when the DISTINCT output aliases a grouping key") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .groupBy($"id")($"id".as("gid"))
      .analyze
    val expected = relation
      .where(Size($"items") > 0)
      .groupBy($"id")($"id".as("gid"))
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("remove Generate when the DISTINCT output contains a foldable literal") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .groupBy($"id", Literal("tag"))($"id", Literal("tag").as("tag"))
      .analyze
    val expected = relation
      .where(Size($"items") > 0)
      .groupBy($"id", Literal("tag"))($"id", Literal("tag").as("tag"))
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("remove Generate when grouping by a deterministic expression over child columns") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .groupBy($"id" + 1)(($"id" + 1).as("k"))
      .analyze
    val expected = relation
      .where(Size($"items") > 0)
      .groupBy($"id" + 1)(($"id" + 1).as("k"))
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("remove Generate when the explode input is a deterministic expression") {
    val query = relation
      .generate(Explode(ArrayDistinct($"items")), outputNames = Seq("item"))
      .groupBy($"id")($"id")
      .analyze
    val expected = relation
      .where(Size(ArrayDistinct($"items")) > 0)
      .groupBy($"id")($"id")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("remove Explode over a map under DISTINCT") {
    val query = relation
      .generate(Explode($"m"), outputNames = Seq("k", "v"))
      .groupBy($"id")($"id")
      .analyze
    val expected = relation
      .where(Size($"m") > 0)
      .groupBy($"id")($"id")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("remove outer Generate under DISTINCT") {
    val query = relation
      .generate(Explode($"items"), outer = true, outputNames = Seq("item"))
      .groupBy($"id")($"id")
      .analyze
    val expected = relation
      .groupBy($"id")($"id")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("do not remove Generate when the generated column is referenced") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .groupBy($"id", $"item")($"id", $"item")
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("do not remove Generate when the aggregate has aggregate functions") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .groupBy($"id")($"id", count($"item"))
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("do not remove Generate when a grouping expression is non-deterministic") {
    val rnd = Rand(0)
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .groupBy(rnd)(rnd.as("r"))
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("do not remove Generate for a generator that is not an ExplodeBase") {
    val query = relation
      .generate(Stack(Seq(Literal(1), Literal(1))), outputNames = Seq("col"))
      .groupBy($"id")($"id")
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("remove Generate through a deterministic projection under DISTINCT") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select($"id")
      .groupBy($"id")($"id")
      .analyze
    val expected = relation
      .where(Size($"items") > 0)
      .select($"id")
      .groupBy($"id")($"id")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("remove outer Generate through a deterministic projection under DISTINCT") {
    val query = relation
      .generate(Explode($"items"), outer = true, outputNames = Seq("item"))
      .select($"id")
      .groupBy($"id")($"id")
      .analyze
    val expected = relation
      .select($"id")
      .groupBy($"id")($"id")
      .analyze

    comparePlans(Optimize.execute(query), expected)
  }

  test("do not remove Generate when the projection references the generated column") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select($"id", $"item")
      .groupBy($"id", $"item")($"id", $"item")
      .analyze

    comparePlans(Optimize.execute(query), query)
  }

  test("do not remove Generate when the projection is non-deterministic") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("item"))
      .select(Rand(0).as("r"))
      .groupBy($"r")($"r")
      .analyze

    comparePlans(Optimize.execute(query), query)
  }
}

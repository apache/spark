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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Explode, JsonTuple, Literal}
import org.apache.spark.sql.catalyst.optimizer.NestedColumnAliasingSuite.collectGeneratedAliases
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{StringType, StructType}

class GenerateOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("GenerateOptimization", FixedPoint(100),
      ColumnPruning,
      CollapseProject,
      GenerateOptimization) :: Nil
  }

  private val item = StructType.fromDDL("item_id int, item_data string, item_price int")
  private val relation = LocalRelation($"items".array(item))

  test("Prune unnecessary field on Explode from count-only aggregate") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("explode"))
      .select($"explode")
      .groupBy()(count(1))
      .analyze

    val optimized = Optimize.execute(query)

    val aliases = collectGeneratedAliases(optimized)

    val expected = relation
      .select(
        $"items".getField("item_id").as(aliases(0)))
      .generate(Explode($"${aliases(0)}"),
        unrequiredChildIndex = Seq(0),
        outputNames = Seq("explode"))
      .select()
      .groupBy()(count(1))
      .analyze
    comparePlans(optimized, expected)
  }

  test("Do not prune field from Explode if the struct is needed") {
    val query = relation
      .generate(Explode($"items"), outputNames = Seq("explode"))
      .select($"explode")
      .groupBy()(count(1), collectList($"explode"))
      .analyze

    val optimized = Optimize.execute(query)

    val expected = relation
      .generate(Explode($"items"), unrequiredChildIndex = Seq(0), outputNames = Seq("explode"))
      .select($"explode")
      .groupBy()(count(1), collectList($"explode"))
      .analyze

    comparePlans(optimized, expected)
  }

  test("SPARK-53124: prune unnecessary fields from JsonTuple") {
    val jsonStrAttr = AttributeReference("json_str", StringType)()
    val aAttr = AttributeReference("a", StringType)()
    val bAttr = AttributeReference("b", StringType)()
    val cAttr = AttributeReference("c", StringType)()
    val jsonRelation = LocalRelation(jsonStrAttr)

    // Create JsonTuple generator with 3 fields
    val jsonTuple = JsonTuple(Seq(jsonStrAttr, Literal("a"), Literal("b"), Literal("c")))
    val generate = Generate(
      generator = jsonTuple,
      unrequiredChildIndex = Nil,
      outer = false,
      qualifier = None,
      generatorOutput = Seq(aAttr, bAttr, cAttr),
      child = jsonRelation)

    // Case 1: No generator outputs used - should eliminate Generate completely
    val projectNone = Project(Seq(jsonStrAttr), generate).analyze
    val optimizedNone = Optimize.execute(projectNone)
    val expectedNone = jsonRelation.select(jsonStrAttr).analyze
    comparePlans(optimizedNone, expectedNone)

    // Case 2: Some generator outputs used (just 'b')
    val projectSome = Project(Seq(bAttr), generate).analyze
    val optimizedSome = Optimize.execute(projectSome)
    val expectedSome = Project(
      Seq(bAttr),
      Generate(
        generator = JsonTuple(Seq(jsonStrAttr, Literal("b"))),
        unrequiredChildIndex = Seq(0),
        outer = false,
        qualifier = None,
        generatorOutput = Seq(bAttr),
        child = jsonRelation)).analyze
    comparePlans(optimizedSome, expectedSome)

    // Case 3: All generator outputs used - plan should remain unchanged
    val projectAll = Project(Seq(aAttr, bAttr, cAttr), generate)
    val optimizedAll = Optimize.execute(projectAll)
    val expectedAll = Project(
      Seq(aAttr, bAttr, cAttr),
      Generate(
        generator = jsonTuple,
        unrequiredChildIndex = Seq(0),
        outer = false,
        qualifier = None,
        generatorOutput = Seq(aAttr, bAttr, cAttr),
        child = jsonRelation)).analyze
    comparePlans(optimizedAll, expectedAll)
  }
}

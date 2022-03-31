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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.MetadataBuilder

class RemoveRedundantAliasAndProjectSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch(
      "RemoveAliasOnlyProject",
      FixedPoint(50),
      PushProjectionThroughUnion,
      RemoveRedundantAliases,
      RemoveNoopOperators) :: Nil
  }

  test("all expressions in project list are aliased child output") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as Symbol("a"), $"b" as Symbol("b")).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("all expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"b" as Symbol("b"), $"a" as Symbol("a")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"b", $"a").analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are aliased child output") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as Symbol("a"), $"b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("some expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"b" as Symbol("b"), $"a").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"b", $"a").analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are not Alias or Attribute") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as Symbol("a"), $"b" + 1).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"a", $"b" + 1).analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are aliased child output but with metadata") {
    val relation = LocalRelation($"a".int, $"b".int)
    val metadata = new MetadataBuilder().putString("x", "y").build()
    val aliasWithMeta = Alias($"a", "a")(explicitMetadata = Some(metadata))
    val query = relation.select(aliasWithMeta, $"b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("remove redundant project with self-join") {
    val relation = LocalRelation($"a".int)
    val fragment = relation.select($"a" as Symbol("a"))
    val query = fragment.select($"a" as Symbol("a"))
      .join(fragment.select($"a" as Symbol("a"))).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.join(relation).analyze
    comparePlans(optimized, expected)
  }

  test("alias removal should not break after push project through union") {
    val r1 = LocalRelation($"a".int)
    val r2 = LocalRelation($"b".int)
    val query = r1.select($"a" as Symbol("a"))
      .union(r2.select($"b" as Symbol("b"))).select($"a").analyze
    val optimized = Optimize.execute(query)
    val expected = r1.union(r2)
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from aggregate") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.groupBy($"a" as Symbol("a"))($"a" as Symbol("a"), sum($"b")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.groupBy($"a")($"a", sum($"b")).analyze
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from window") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.window(Seq($"b" as Symbol("b")), Seq($"a" as Symbol("a")), Seq()).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.window(Seq($"b"), Seq($"a"), Seq()).analyze
    comparePlans(optimized, expected)
  }

  test("do not remove output attributes from a subquery") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = Subquery(
      relation.select($"a" as "a", $"b" as "b").where($"b" < 10).select($"a").analyze,
      correlated = false)
    val optimized = Optimize.execute(query)
    val expected = Subquery(
      relation.select($"a" as "a", $"b").where($"b" < 10).select($"a").analyze,
      correlated = false)
    comparePlans(optimized, expected)
  }
}

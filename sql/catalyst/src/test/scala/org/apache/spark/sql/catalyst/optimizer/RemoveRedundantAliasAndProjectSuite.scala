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
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.select("a".attr as "a", "b".attr as "b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("all expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.select("b".attr as "b", "a".attr as "a").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select("b".attr, "a".attr).analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are aliased child output") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.select("a".attr as "a", "b".attr).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("some expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.select("b".attr as "b", "a".attr).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select("b".attr, "a".attr).analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are not Alias or Attribute") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.select("a".attr as "a", "b".attr + 1).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select("a".attr, "b".attr + 1).analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are aliased child output but with metadata") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val metadata = new MetadataBuilder().putString("x", "y").build()
    val aliasWithMeta = Alias("a".attr, "a")(explicitMetadata = Some(metadata))
    val query = relation.select(aliasWithMeta, "b".attr).analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)
  }

  test("retain deduplicating alias in self-join") {
    val relation = LocalRelation("a".attr.int)
    val fragment = relation.select("a".attr as "a")
    val query = fragment.select("a".attr as "a").join(fragment.select("a".attr as "a")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.join(relation.select("a".attr as "a")).analyze
    comparePlans(optimized, expected)
  }

  test("alias removal should not break after push project through union") {
    val r1 = LocalRelation("a".attr.int)
    val r2 = LocalRelation("b".attr.int)
    val query =
      r1.select("a".attr as "a").union(r2.select("b".attr as "b")).select("a".attr).analyze
    val optimized = Optimize.execute(query)
    val expected = r1.union(r2)
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from aggregate") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.groupBy("a".attr as "a")("a".attr as "a", sum("b".attr)).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.groupBy("a".attr)("a".attr, sum("b".attr)).analyze
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from window") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.window(Seq("b".attr as "b"), Seq("a".attr as "a"), Seq()).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.window(Seq("b".attr), Seq("a".attr), Seq()).analyze
    comparePlans(optimized, expected)
  }

  test("do not remove output attributes from a subquery") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = Subquery(
      relation.
        select("a".attr as "a", "b".attr as "b")
        .where("b".attr < 10)
        .select("a".attr)
        .analyze,
      correlated = false)
    val optimized = Optimize.execute(query)
    val expected = Subquery(
      relation.select("a".attr as "a", "b".attr).where("b".attr < 10).select("a".attr).analyze,
      correlated = false)
    comparePlans(optimized, expected)
  }
}

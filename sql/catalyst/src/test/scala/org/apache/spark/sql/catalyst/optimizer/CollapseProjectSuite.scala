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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Rand}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.MetadataBuilder

class CollapseProjectSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", FixedPoint(10), EliminateSubqueryAliases) ::
      Batch("CollapseProject", Once, CollapseProject) :: Nil
  }

  val testRelation = LocalRelation("a".attr.int, "b".attr.int)

  test("collapse two deterministic, independent projects into one") {
    val query = testRelation
      .select(("a".attr + 1).as("a_plus_1"), "b".attr)
      .select("a_plus_1".attr, ("b".attr + 1).as("b_plus_1"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer =
      testRelation.select(("a".attr + 1).as("a_plus_1"), ("b".attr + 1).as("b_plus_1")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two deterministic, dependent projects into one") {
    val query = testRelation
      .select(("a".attr + 1).as("a_plus_1"), "b".attr)
      .select(("a_plus_1".attr + 1).as("a_plus_2"), "b".attr)

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(
      (("a".attr + 1).as("a_plus_1") + 1).as("a_plus_2"),
      "b".attr).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse nondeterministic projects") {
    val query = testRelation
      .select(Rand(10).as("rand"))
      .select(("rand".attr + 1).as("rand1"), ("rand".attr + 2).as("rand2"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two nondeterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as("rand"))
      .select(Rand(20).as("rand2"))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(Rand(20).as("rand2")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse one nondeterministic, one deterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as("rand"), "a".attr)
      .select(("a".attr + 1).as("a_plus_1"))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(("a".attr + 1).as("a_plus_1")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse project into aggregate") {
    val query = testRelation
      .groupBy("a".attr, "b".attr)(("a".attr + 1).as("a_plus_1"), "b".attr)
      .select("a_plus_1".attr, ("b".attr + 1).as("b_plus_1"))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .groupBy("a".attr, "b".attr)(("a".attr + 1).as("a_plus_1"), ("b".attr + 1).as("b_plus_1"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse common nondeterministic project and aggregate") {
    val query = testRelation
      .groupBy("a".attr)("a".attr, Rand(10).as("rand"))
      .select(("rand".attr + 1).as("rand1"), ("rand".attr + 2).as("rand2"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("preserve top-level alias metadata while collapsing projects") {
    def hasMetadata(logicalPlan: LogicalPlan): Boolean = {
      logicalPlan.asInstanceOf[Project].projectList.exists(_.metadata.contains("key"))
    }

    val metadata = new MetadataBuilder().putLong("key", 1).build()
    val analyzed =
      Project(Seq(Alias("a_with_metadata".attr, "b")()),
        Project(Seq(Alias("a".attr, "a_with_metadata")(explicitMetadata = Some(metadata))),
          testRelation.logicalPlan)).analyze
    require(hasMetadata(analyzed))

    val optimized = Optimize.execute(analyzed)
    val projects = optimized.collect { case p: Project => p }
    assert(projects.size === 1)
    assert(hasMetadata(optimized))
  }

  test("collapse redundant alias through limit") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.select("a".attr as "b").limit(1).select("b".attr as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select("a".attr as "c").limit(1).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through local limit") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = LocalLimit(1, relation.select("a".attr as "b")).select("b".attr as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = LocalLimit(1, relation.select("a".attr as "c")).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through repartition") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = relation.select("a".attr as "b").repartition(1).select("b".attr as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select("a".attr as "c").repartition(1).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through sample") {
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val query = Sample(0.0, 0.6, false, 11L, relation.select("a".attr as "b"))
      .select("b".attr as "c").analyze
    val optimized = Optimize.execute(query)
    val expected = Sample(0.0, 0.6, false, 11L, relation.select("a".attr as "c")).analyze
    comparePlans(optimized, expected)
  }
}

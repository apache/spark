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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder

class RemoveRedundantAliasAndProjectSuite extends PlanTest {

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
    val query = relation.select($"a" as "a", $"b" as "b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("all expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"b" as "b", $"a" as "a").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"b", $"a").analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are aliased child output") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as "a", $"b").analyze
    val optimized = Optimize.execute(query)
    comparePlans(optimized, relation)
  }

  test("some expressions in project list are aliased child output but with different order") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"b" as "b", $"a").analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"b", $"a").analyze
    comparePlans(optimized, expected)
  }

  test("some expressions in project list are not Alias or Attribute") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as "a", $"b" + 1).analyze
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
    val fragment = relation.select($"a" as "a")
    val query = fragment.select($"a" as "a")
      .join(fragment.select($"a" as "a")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.join(relation).analyze
    comparePlans(optimized, expected)
  }

  test("alias removal should not break after push project through union") {
    val r1 = LocalRelation($"a".int)
    val r2 = LocalRelation($"b".int)
    val query = r1.select($"a" as "a")
      .union(r2.select($"b" as "b")).select($"a").analyze
    val optimized = Optimize.execute(query)
    val expected = r1.select($"a" as "a").union(r2).analyze
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from aggregate") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.groupBy($"a" as "a")($"a" as "a", sum($"b")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.groupBy($"a")($"a", sum($"b")).analyze
    comparePlans(optimized, expected)
  }

  test("remove redundant alias from window") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.window(Seq($"b" as "b"), Seq($"a" as "a"), Seq()).analyze
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

  test("SPARK-46640: do not remove outer references from a subquery expression") {
    val a = $"a".int
    val a_alias = Alias(a, "a")()
    val a_alias_attr = a_alias.toAttribute
    val b = $"b".int

    // The original input query
    //  Filter exists [a#1 && (a#1 = b#2)]
    //  :  +- LocalRelation <empty>, [b#2]
    //    +- Project [a#0 AS a#1]
    //    +- LocalRelation <empty>, [a#0]
    val query = Filter(
      Exists(
        LocalRelation(b),
        outerAttrs = Seq(a_alias_attr),
        joinCond = Seq(EqualTo(a_alias_attr, b))
      ),
      Project(Seq(a_alias), LocalRelation(a))
    )

    // The alias would not be removed if excluding subquery references is enabled.
    val expectedWhenExcluded = query

    // The alias would have been removed if excluding subquery references is disabled.
    //  Filter exists [a#0 && (a#0 = b#2)]
    //  :  +- LocalRelation <empty>, [b#2]
    //    +- LocalRelation <empty>, [a#0]
    val expectedWhenNotExcluded = Filter(
      Exists(
        LocalRelation(b),
        outerAttrs = Seq(a),
        joinCond = Seq(EqualTo(a, b))
      ),
      LocalRelation(a)
    )

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "true") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenExcluded)
    }

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "false") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenNotExcluded)
    }
  }

  test("SPARK-46640: exclude outer references accounts for children of plan expression") {
    val a = $"a".int
    val a_alias = Alias(a, "a")()
    val a_alias_attr = a_alias.toAttribute

    // The original input query
    //  Project [CASE WHEN exists#2 [a#1 && (a#1 = a#0)] THEN 1 ELSE 2 END AS result#3]
    //  :  +- LocalRelation <empty>, [a#0]
    //  +- Project [a#0 AS a#1]
    //    +- LocalRelation <empty>, [a#0]
    // The subquery expression (`exists#2`) is wrapped in a CaseWhen and an Alias.
    // Without the fix on excluding outer references, the rewritten plan would have been:
    //  Project [CASE WHEN exists#2 [a#0 && (a#0 = a#0)] THEN 1 ELSE 2 END AS result#3]
    //  :  +- LocalRelation <empty>, [a#0]
    //  +- LocalRelation <empty>, [a#0]
    // This plan would then fail later with the error -- conflicting a#0 in join condition.

    val query = Project(Seq(
      Alias(
        CaseWhen(Seq((
          Exists(
            LocalRelation(a),
            outerAttrs = Seq(a_alias_attr),
            joinCond = Seq(EqualTo(a_alias_attr, a))
          ), Literal(1))),
          Some(Literal(2))),
        "result"
      )()),
      Project(Seq(a_alias), LocalRelation(a))
    )

    // The alias would not be removed if excluding subquery references is enabled.
    val expectedWhenExcluded = query

    // The alias would be removed and we would have conflicting expression ID(s) in the join cond
    val expectedWhenNotEnabled = Project(Seq(
      Alias(
        CaseWhen(Seq((
          Exists(
            LocalRelation(a),
            outerAttrs = Seq(a),
            joinCond = Seq(EqualTo(a, a))
          ), Literal(1))),
          Some(Literal(2))),
        "result"
      )()),
      LocalRelation(a)
    )

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "true") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenExcluded)
    }

    withSQLConf(SQLConf.EXCLUDE_SUBQUERY_EXP_REFS_FROM_REMOVE_REDUNDANT_ALIASES.key -> "false") {
      val optimized = Optimize.execute(query)
      comparePlans(optimized, expectedWhenNotEnabled)
    }
  }
}

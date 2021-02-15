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
import org.apache.spark.sql.catalyst.expressions.{IsNull, ListQuery, Not}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor


class RewriteSubquerySuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Column Pruning", FixedPoint(100), ColumnPruning) ::
      Batch("Rewrite Subquery", FixedPoint(1),
        RewritePredicateSubquery,
        ColumnPruning,
        CollapseProject,
        RemoveNoopOperators) :: Nil
  }

  test("Column pruning after rewriting predicate subquery") {
    val relation = LocalRelation(Symbol("a").int, Symbol("b").int)
    val relInSubquery = LocalRelation(Symbol("x").int, Symbol("y").int, Symbol("z").int)

    val query = relation.where(Symbol("a").in(
      ListQuery(relInSubquery.select(Symbol("x"))))).select(Symbol("a"))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = relation
      .select(Symbol("a"))
      .join(relInSubquery.select(Symbol("x")), LeftSemi, Some(Symbol("a") === Symbol("x")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("NOT-IN subquery nested inside OR") {
    val relation1 = LocalRelation(Symbol("a").int, Symbol("b").int)
    val relation2 = LocalRelation(Symbol("c").int, Symbol("d").int)
    val exists = Symbol("exists").boolean.notNull

    val query = relation1.where(Symbol("b") === 1 ||
      Not(Symbol("a").in(ListQuery(relation2.select(Symbol("c")))))).select(Symbol("a"))
    val correctAnswer = relation1
      .join(relation2.select(Symbol("c")), ExistenceJoin(exists),
        Some(Symbol("a") === Symbol("c") || IsNull(Symbol("a") === Symbol("c"))))
      .where(Symbol("b") === 1 || Not(exists))
      .select(Symbol("a"))
      .analyze
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, correctAnswer)
  }
}

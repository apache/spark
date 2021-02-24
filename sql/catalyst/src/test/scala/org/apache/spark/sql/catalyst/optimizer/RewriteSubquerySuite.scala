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
    val relation = LocalRelation("a".attr.int, "b".attr.int)
    val relInSubquery = LocalRelation("x".attr.int, "y".attr.int, "z".attr.int)

    val query =
      relation.where("a".attr.in(ListQuery(relInSubquery.select("x".attr)))).select("a".attr)

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = relation
      .select("a".attr)
      .join(relInSubquery.select("x".attr), LeftSemi, Some("a".attr === "x".attr))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("NOT-IN subquery nested inside OR") {
    val relation1 = LocalRelation("a".attr.int, "b".attr.int)
    val relation2 = LocalRelation("c".attr.int, "d".attr.int)
    val exists = "exists".attr.boolean.notNull

    val query = relation1.
      where("b".attr === 1 || Not("a".attr.in(ListQuery(relation2.select("c".attr)))))
      .select("a".attr)
    val correctAnswer = relation1
      .join(relation2.select("c".attr), ExistenceJoin(exists),
        Some("a".attr === "c".attr || IsNull("a".attr === "c".attr)))
      .where("b".attr === 1 || Not(exists))
      .select("a".attr)
      .analyze
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, correctAnswer)
  }
}

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
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, IsNull, ListQuery, Not}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf


class RewriteSubquerySuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Column Pruning", FixedPoint(100), ColumnPruning) ::
      Batch("Rewrite Subquery", FixedPoint(1),
        RewritePredicateSubquery,
        ColumnPruning,
        InferFiltersFromConstraints,
        PushDownPredicate,
        CollapseProject,
        CombineFilters,
        RemoveNoopOperators) :: Nil
  }

  val relation = LocalRelation('a.int, 'b.int)
  val relInSubquery = LocalRelation('x.int, 'y.int, 'z.int)

  test("Column pruning after rewriting predicate subquery") {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      val query = relation.where('a.in(ListQuery(relInSubquery.select('x)))).select('a)
      val optimized = Optimize.execute(query.analyze)

      val correctAnswer = relation
        .select('a)
        .join(relInSubquery.select('x), LeftSemi, Some('a === 'x))
        .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("Infer filters and push down predicate after rewriting predicate subquery") {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "true") {
      val query = relation.where('a.in(ListQuery(relInSubquery.select('x)))).select('a)
      val optimized = Optimize.execute(query.analyze)

      val correctAnswer = relation
        .where(IsNotNull('a)).select('a)
        .join(relInSubquery.where(IsNotNull('x)).select('x), LeftSemi, Some('a === 'x))
        .analyze

      comparePlans(optimized, correctAnswer)
    }
  }

  test("combine filters after rewriting predicate subquery") {
    val query = relation.where('a.in(ListQuery(relInSubquery.select('x).where('y > 1)))).select('a)
    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = relation
      .where(IsNotNull('a)).select('a)
      .join(relInSubquery.where(IsNotNull('x) && IsNotNull('y) && 'y > 1).select('x),
        LeftSemi, Some('a === 'x))

  test("NOT-IN subquery nested inside OR") {
    val relation1 = LocalRelation('a.int, 'b.int)
    val relation2 = LocalRelation('c.int, 'd.int)
    val exists = 'exists.boolean.notNull

    val query = relation1.where('b === 1 || Not('a.in(ListQuery(relation2.select('c))))).select('a)
    val correctAnswer = relation1
      .join(relation2.select('c), ExistenceJoin(exists), Some('a === 'c || IsNull('a === 'c)))
      .where('b === 1 || Not(exists))
      .select('a)
      .analyze
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, correctAnswer)
  }
}

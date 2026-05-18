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

import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo, IsNull, ListQuery, Not}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LocalRelation, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.LongType


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
    val relation = LocalRelation($"a".int, $"b".int)
    val relInSubquery = LocalRelation($"x".int, $"y".int, $"z".int)

    val query = relation.where($"a".in(ListQuery(relInSubquery.select($"x")))).select($"a")

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = relation
      .select($"a")
      .join(relInSubquery.select($"x"), LeftSemi, Some($"a" === $"x"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("NOT-IN subquery nested inside OR") {
    val relation1 = LocalRelation($"a".int, $"b".int)
    val relation2 = LocalRelation($"c".int, $"d".int)
    val exists = $"exists".boolean.notNull

    val query = relation1.where($"b" === 1
      || Not($"a".in(ListQuery(relation2.select($"c"))))).select($"a")
    val correctAnswer = relation1
      .join(relation2.select($"c"), ExistenceJoin(exists), Some($"a" === $"c"
        || IsNull($"a" === $"c")))
      .where($"b" === 1 || Not(exists))
      .select($"a")
      .analyze
    val optimized = Optimize.execute(query.analyze)

    comparePlans(optimized, correctAnswer)
  }

  test("single-column top-level NOT IN decomposes into a union and equi anti join") {
    val relation1 = LocalRelation($"a".int, $"b".int)
    val relation2 = LocalRelation($"c".int)

    val query = relation1.where(Not($"a".in(ListQuery(relation2)))).select($"a")
    var optimized: LogicalPlan = null
    withSQLConf(SQLConf.OPTIMIZE_TOP_LEVEL_SINGLE_COLUMN_NOT_IN_WITH_UNION.key -> "true") {
      optimized = Optimize.execute(query.analyze)
    }

    assert(optimized.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Union]))
    assert(optimized.exists {
      case Join(_, _, LeftAnti, Some(_: EqualTo), _) => true
      case _ => false
    })
    assert(!optimized.exists {
      case Join(_, _, LeftAnti, Some(condition), _) =>
        condition.exists {
          case IsNull(_: EqualTo) => true
          case _ => false
        }
      case _ => false
    })
  }

  test("single-column top-level NOT IN union rewrite can be disabled") {
    val relation1 = LocalRelation($"a".int, $"b".int)
    val relation2 = LocalRelation($"c".int)

    val query = relation1.where(Not($"a".in(ListQuery(relation2)))).select($"a")
    var optimized: LogicalPlan = null
    withSQLConf(SQLConf.OPTIMIZE_TOP_LEVEL_SINGLE_COLUMN_NOT_IN_WITH_UNION.key -> "false") {
      optimized = Optimize.execute(query.analyze)
    }

    assert(!optimized.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Union]))
  }

  test("multiple single-column top-level NOT IN predicates skip union rewrite") {
    val relation1 = LocalRelation($"a".int, $"b".int)
    val relation2 = LocalRelation($"c".int)
    val relation3 = LocalRelation($"d".int)

    val query = relation1
      .where(Not($"a".in(ListQuery(relation2))) && Not($"b".in(ListQuery(relation3))))
      .select($"a")
    var optimized: LogicalPlan = null
    withSQLConf(SQLConf.OPTIMIZE_TOP_LEVEL_SINGLE_COLUMN_NOT_IN_WITH_UNION.key -> "true") {
      optimized = Optimize.execute(query.analyze)
    }

    assert(!optimized.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Union]))
  }

  test("single-column top-level NOT IN with another top-level subquery skips union rewrite") {
    val relation1 = LocalRelation($"a".int, $"b".int)
    val relation2 = LocalRelation($"c".int)
    val relation3 = LocalRelation($"d".int)

    val query = relation1
      .where(Not($"a".in(ListQuery(relation2))) && $"b".in(ListQuery(relation3)))
      .select($"a")
    var optimized: LogicalPlan = null
    withSQLConf(SQLConf.OPTIMIZE_TOP_LEVEL_SINGLE_COLUMN_NOT_IN_WITH_UNION.key -> "true") {
      optimized = Optimize.execute(query.analyze)
    }

    assert(!optimized.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Union]))
  }

  test("single-column top-level NOT IN with RHS limit skips union rewrite") {
    val relation1 = LocalRelation($"a".int, $"b".int)
    val relation2 = LocalRelation($"c".int)

    val query = relation1.where(
      Not($"a".in(ListQuery(relation2.limit(1))))).select($"a")
    var optimized: LogicalPlan = null
    withSQLConf(SQLConf.OPTIMIZE_TOP_LEVEL_SINGLE_COLUMN_NOT_IN_WITH_UNION.key -> "true") {
      optimized = Optimize.execute(query.analyze)
    }

    assert(!optimized.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Union]))
  }

  test("single-column top-level NOT IN with RHS offset skips union rewrite") {
    val relation1 = LocalRelation($"a".int, $"b".int)
    val relation2 = LocalRelation($"c".int)

    val query = relation1.where(
      Not($"a".in(ListQuery(relation2.offset(1))))).select($"a")
    var optimized: LogicalPlan = null
    withSQLConf(SQLConf.OPTIMIZE_TOP_LEVEL_SINGLE_COLUMN_NOT_IN_WITH_UNION.key -> "true") {
      optimized = Optimize.execute(query.analyze)
    }

    assert(!optimized.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Union]))
  }

  test("SPARK-34598: Filters without subquery must not be modified by RewritePredicateSubquery") {
    val relation = LocalRelation($"a".int, $"b".int, $"c".int, $"d".int)
    val query = relation.where(($"a" === 1 || $"b" === 2)
      && ($"c" === 3 && $"d" === 4)).select($"a")
    val tracker = new QueryPlanningTracker
    Optimize.executeAndTrack(query.analyze, tracker)
    assert(tracker.rules(RewritePredicateSubquery.ruleName).numEffectiveInvocations == 0)
  }

  test("SPARK-50091: Don't put aggregate expression in join condition") {
    val relation1 = LocalRelation($"c1".int, $"c2".int, $"c3".int)
    val relation2 = LocalRelation($"col1".int, $"col2".int, $"col3".int)
    val plan = relation2.groupBy()(sum($"col2").in(ListQuery(relation1.select($"c3"))))
    val optimized = Optimize.execute(plan.analyze)
    val aggregate = relation2
      .select($"col2")
      .groupBy()(sum($"col2").as("_aggregateexpression"))
    val correctAnswer = aggregate
      .join(relation1.select(Cast($"c3", LongType).as("c3")),
        ExistenceJoin($"exists".boolean.withNullability(false)),
        Some($"_aggregateexpression" === $"c3"))
      .select($"exists".as("(sum(col2) IN (listquery()))")).analyze
    comparePlans(optimized, correctAnswer)
  }
}

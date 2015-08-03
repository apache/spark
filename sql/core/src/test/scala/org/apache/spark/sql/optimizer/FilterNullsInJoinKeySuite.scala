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

package org.apache.spark.sql.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Not, AtLeastNNulls}
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.test.TestSQLContext

/** This is the test suite for FilterNullsInJoinKey optimization rule. */
class FilterNullsInJoinKeySuite extends PlanTest {

  // We add predicate pushdown rules at here to make sure we do not
  // create redundant Filter operators. Also, because the attribute ordering of
  // the Project operator added by ColumnPruning may be not deterministic
  // (the ordering may depend on the testing environment),
  // we first construct the plan with expected Filter operators and then
  // run the optimizer to add the the Project for column pruning.
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubQueries) ::
      Batch("Operator Optimizations", FixedPoint(100),
        FilterNullsInJoinKey(TestSQLContext), // This is the rule we test in this suite.
        CombineFilters,
        PushPredicateThroughProject,
        BooleanSimplification,
        PushPredicateThroughJoin,
        PushPredicateThroughGenerate,
        ColumnPruning,
        ProjectCollapsing) :: Nil
  }

  val leftRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.int)

  val rightRelation = LocalRelation('e.int, 'f.int, 'g.int, 'h.int)

  test("inner join") {
    val joinCondition =
      ('a === 'e && 'b + 1 === 'f) && ('d > 'h || 'd === 'g)

    val joinedPlan =
      leftRelation
        .join(rightRelation, Inner, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    val optimized = Optimize.execute(joinedPlan.analyze)

    // For an inner join, FilterNullsInJoinKey add filter to both side.
    val correctLeft =
      leftRelation
        .where(!(AtLeastNNulls(1, 'a.expr :: Nil)))

    val correctRight =
      rightRelation.where(!(AtLeastNNulls(1, 'e.expr :: 'f.expr :: Nil)))

    val correctAnswer =
      correctLeft
        .join(correctRight, Inner, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    comparePlans(optimized, Optimize.execute(correctAnswer.analyze))
  }

  test("make sure we do not keep adding filters") {
    val thirdRelation = LocalRelation('i.int, 'j.int, 'k.int, 'l.int)
    val joinedPlan =
      leftRelation
        .join(rightRelation, Inner, Some('a === 'e))
        .join(thirdRelation, Inner, Some('b === 'i && 'a === 'j))

    val optimized = Optimize.execute(joinedPlan.analyze)
    val conditions = optimized.collect {
      case Filter(condition @ Not(AtLeastNNulls(1, exprs)), _) => exprs
    }

    // Make sure that we have three Not(AtLeastNNulls(1, exprs)) for those three tables.
    assert(conditions.length === 3)

    // Make sure attribtues are indeed a, b, e, i, and j.
    assert(
      conditions.flatMap(exprs => exprs).toSet ===
        joinedPlan.select('a, 'b, 'e, 'i, 'j).analyze.output.toSet)
  }

  test("inner join (partially optimized)") {
    val joinCondition =
      ('a + 2 === 'e && 'b + 1 === 'f) && ('d > 'h || 'd === 'g)

    val joinedPlan =
      leftRelation
        .join(rightRelation, Inner, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    val optimized = Optimize.execute(joinedPlan.analyze)

    // We cannot extract attribute from the left join key.
    val correctRight =
      rightRelation.where(!(AtLeastNNulls(1, 'e.expr :: 'f.expr :: Nil)))

    val correctAnswer =
      leftRelation
        .join(correctRight, Inner, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    comparePlans(optimized, Optimize.execute(correctAnswer.analyze))
  }

  test("inner join (not optimized)") {
    val nonOptimizedJoinConditions =
      Some('c - 100 + 'd === 'g + 1 - 'h) ::
        Some('d > 'h || 'c === 'g) ::
        Some('d + 'g + 'c > 'd - 'h) :: Nil

    nonOptimizedJoinConditions.foreach { joinCondition =>
      val joinedPlan =
        leftRelation
          .join(rightRelation.select('f, 'g, 'h), Inner, joinCondition)
          .select('a, 'c, 'f, 'd, 'h, 'g)

      val optimized = Optimize.execute(joinedPlan.analyze)

      comparePlans(optimized, Optimize.execute(joinedPlan.analyze))
    }
  }

  test("left outer join") {
    val joinCondition =
      ('a === 'e && 'b + 1 === 'f) && ('d > 'h || 'd === 'g)

    val joinedPlan =
      leftRelation
        .join(rightRelation, LeftOuter, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    val optimized = Optimize.execute(joinedPlan.analyze)

    // For a left outer join, FilterNullsInJoinKey add filter to the right side.
    val correctRight =
      rightRelation.where(!(AtLeastNNulls(1, 'e.expr :: 'f.expr :: Nil)))

    val correctAnswer =
      leftRelation
        .join(correctRight, LeftOuter, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    comparePlans(optimized, Optimize.execute(correctAnswer.analyze))
  }

  test("right outer join") {
    val joinCondition =
      ('a === 'e && 'b + 1 === 'f) && ('d > 'h || 'd === 'g)

    val joinedPlan =
      leftRelation
        .join(rightRelation, RightOuter, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    val optimized = Optimize.execute(joinedPlan.analyze)

    // For a right outer join, FilterNullsInJoinKey add filter to the left side.
    val correctLeft =
      leftRelation
        .where(!(AtLeastNNulls(1, 'a.expr :: Nil)))

    val correctAnswer =
      correctLeft
        .join(rightRelation, RightOuter, Some(joinCondition))
        .select('a, 'f, 'd, 'h)


    comparePlans(optimized, Optimize.execute(correctAnswer.analyze))
  }

  test("full outer join") {
    val joinCondition =
      ('a === 'e && 'b + 1 === 'f) && ('d > 'h || 'd === 'g)

    val joinedPlan =
      leftRelation
        .join(rightRelation, FullOuter, Some(joinCondition))
        .select('a, 'f, 'd, 'h)

    // FilterNullsInJoinKey does not fire for a full outer join.
    val optimized = Optimize.execute(joinedPlan.analyze)

    comparePlans(optimized, Optimize.execute(joinedPlan.analyze))
  }

  test("left semi join") {
    val joinCondition =
      ('a === 'e && 'b + 1 === 'f) && ('d > 'h || 'd === 'g)

    val joinedPlan =
      leftRelation
        .join(rightRelation, LeftSemi, Some(joinCondition))
        .select('a, 'd)

    val optimized = Optimize.execute(joinedPlan.analyze)

    // For a left semi join, FilterNullsInJoinKey add filter to both side.
    val correctLeft =
      leftRelation
        .where(!(AtLeastNNulls(1, 'a.expr :: Nil)))

    val correctRight =
      rightRelation.where(!(AtLeastNNulls(1, 'e.expr :: 'f.expr :: Nil)))

    val correctAnswer =
      correctLeft
        .join(correctRight, LeftSemi, Some(joinCondition))
        .select('a, 'd)

    comparePlans(optimized, Optimize.execute(correctAnswer.analyze))
  }
}

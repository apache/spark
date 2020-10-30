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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class RemoveRedundantSortsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Remove Redundant Sorts", Once,
        RemoveRedundantSorts) ::
      Batch("Collapse Project", Once,
        CollapseProject) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("SPARK-33183: remove redundant sort by") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val unnecessaryReordered = orderedPlan.limit(2).select('a).sortBy('a.asc, 'b.desc_nullsFirst)
    val optimized = Optimize.execute(unnecessaryReordered.analyze)
    val correctAnswer = orderedPlan.limit(2).select('a).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: remove all redundant local sorts") {
    val orderedPlan = testRelation.sortBy('a.asc).orderBy('a.asc).sortBy('a.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = testRelation.orderBy('a.asc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: should not remove global sort") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val reordered = orderedPlan.limit(2).select('a).orderBy('a.asc, 'b.desc_nullsFirst)
    val optimized = Optimize.execute(reordered.analyze)
    val correctAnswer = reordered.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("do not remove sort if the order is different") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc_nullsFirst)
    val reorderedDifferently = orderedPlan.limit(2).select('a).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(reorderedDifferently.analyze)
    val correctAnswer = reorderedDifferently.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: remove top level local sort with filter operators") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.where('a > Literal(10)).sortBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = orderedPlan.where('a > Literal(10)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: keep top level global sort with filter operators") {
    val projectPlan = testRelation.select('a, 'b)
    val orderedPlan = projectPlan.orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.where('a > Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = projectPlan.where('a > Literal(10)).orderBy('a.asc, 'b.desc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: limits should not affect order for local sort") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.limit(Literal(10)).sortBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = orderedPlan.limit(Literal(10)).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: should not remove global sort with limit operators") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc, 'b.desc)
    val filteredAndReordered = orderedPlan.limit(Literal(10)).orderBy('a.asc, 'b.desc)
    val optimized = Optimize.execute(filteredAndReordered.analyze)
    val correctAnswer = filteredAndReordered.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("different sorts are not simplified if limit is in between") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('b.desc).limit(Literal(10))
      .orderBy('a.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = orderedPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: should not remove global sort with range operator") {
    val inputPlan = Range(1L, 1000L, 1, 10)
    val orderedPlan = inputPlan.orderBy('id.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = orderedPlan.analyze
    comparePlans(optimized, correctAnswer)

    val reversedPlan = inputPlan.orderBy('id.desc)
    val reversedOptimized = Optimize.execute(reversedPlan.analyze)
    val reversedCorrectAnswer = reversedPlan.analyze
    comparePlans(reversedOptimized, reversedCorrectAnswer)

    val negativeStepInputPlan = Range(10L, 1L, -1, 10)
    val negativeStepOrderedPlan = negativeStepInputPlan.orderBy('id.desc)
    val negativeStepOptimized = Optimize.execute(negativeStepOrderedPlan.analyze)
    val negativeStepCorrectAnswer = negativeStepOrderedPlan.analyze
    comparePlans(negativeStepOptimized, negativeStepCorrectAnswer)
  }

  test("SPARK-33183: remove local sort with range operator") {
    val inputPlan = Range(1L, 1000L, 1, 10)
    val orderedPlan = inputPlan.sortBy('id.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = inputPlan.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("sort should not be removed when there is a node which doesn't guarantee any order") {
    val orderedPlan = testRelation.select('a, 'b).orderBy('a.asc)
    val groupedAndResorted = orderedPlan.groupBy('a)(sum('a)).orderBy('a.asc)
    val optimized = Optimize.execute(groupedAndResorted.analyze)
    val correctAnswer = groupedAndResorted.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("remove two consecutive sorts") {
    val orderedTwice = testRelation.orderBy('a.asc).orderBy('b.desc)
    val optimized = Optimize.execute(orderedTwice.analyze)
    val correctAnswer = testRelation.orderBy('b.desc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("remove sorts separated by Filter/Project operators") {
    val orderedTwiceWithProject = testRelation.orderBy('a.asc).select('b).orderBy('b.desc)
    val optimizedWithProject = Optimize.execute(orderedTwiceWithProject.analyze)
    val correctAnswerWithProject = testRelation.select('b).orderBy('b.desc).analyze
    comparePlans(optimizedWithProject, correctAnswerWithProject)

    val orderedTwiceWithFilter =
      testRelation.orderBy('a.asc).where('b > Literal(0)).orderBy('b.desc)
    val optimizedWithFilter = Optimize.execute(orderedTwiceWithFilter.analyze)
    val correctAnswerWithFilter = testRelation.where('b > Literal(0)).orderBy('b.desc).analyze
    comparePlans(optimizedWithFilter, correctAnswerWithFilter)

    val orderedTwiceWithBoth =
      testRelation.orderBy('a.asc).select('b).where('b > Literal(0)).orderBy('b.desc)
    val optimizedWithBoth = Optimize.execute(orderedTwiceWithBoth.analyze)
    val correctAnswerWithBoth =
      testRelation.select('b).where('b > Literal(0)).orderBy('b.desc).analyze
    comparePlans(optimizedWithBoth, correctAnswerWithBoth)

    val orderedThrice = orderedTwiceWithBoth.select(('b + 1).as('c)).orderBy('c.asc)
    val optimizedThrice = Optimize.execute(orderedThrice.analyze)
    val correctAnswerThrice = testRelation.select('b).where('b > Literal(0))
      .select(('b + 1).as('c)).orderBy('c.asc).analyze
    comparePlans(optimizedThrice, correctAnswerThrice)
  }

  test("SPARK-33183: remove consecutive global sorts with the same ordering") {
    Seq(
      (testRelation.orderBy('a.asc).orderBy('a.asc), testRelation.orderBy('a.asc)),
      (testRelation.orderBy('a.asc, 'b.desc).orderBy('a.asc), testRelation.orderBy('a.asc))
    ).foreach { case (ordered, answer) =>
      val optimized = Optimize.execute(ordered.analyze)
      comparePlans(optimized, answer.analyze)
    }
  }

  test("SPARK-33183: remove consecutive local sorts with the same ordering") {
    val orderedPlan = testRelation.sortBy('a.asc).sortBy('a.asc).sortBy('a.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = testRelation.sortBy('a.asc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: remove consecutive local sorts with different ordering") {
    val orderedPlan = testRelation.sortBy('b.asc).sortBy('a.desc).sortBy('a.asc)
    val optimized = Optimize.execute(orderedPlan.analyze)
    val correctAnswer = testRelation.sortBy('a.asc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33183: should keep global sort when child is a local sort with the same ordering") {
    val correctAnswer = testRelation.orderBy('a.asc).analyze
    Seq(
      testRelation.sortBy('a.asc).orderBy('a.asc),
      testRelation.orderBy('a.asc).sortBy('a.asc).orderBy('a.asc)
    ).foreach { ordered =>
      val optimized = Optimize.execute(ordered.analyze)
      comparePlans(optimized, correctAnswer)
    }
  }
}

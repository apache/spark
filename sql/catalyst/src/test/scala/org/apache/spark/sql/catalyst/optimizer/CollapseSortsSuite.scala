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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
/**
 * Test class to test CollapseSorts rule
 * For adjacent sorts, collapse the sort if possible
 */
class CollapseSortsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Collapse Sorts", FixedPoint(10),
        CollapseSorts,
        CollapseProject,
        CombineLimits) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("collapsesorts: select has all columns used in sort") {
    val originalQuery =
      testRelation
        .select('a, 'b)
        .orderBy('b.asc)
        .orderBy('a.asc)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select('a, 'b)
        .orderBy('a.asc).analyze
    comparePlans(optimized, correctAnswer)
  }


  test("collapsesorts: combines two sorts project subset") {
    val originalQuery =
      testRelation
        .select('a, 'b, 'c)
        .orderBy('b.asc)
        .orderBy('a.asc)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select('a, 'b, 'c)
        .orderBy('a.asc).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapsesorts: select has all columns used in sort, desc") {
    val originalQuery =
      testRelation
        .select('a, 'b)
        .orderBy('b.desc)
        .orderBy('a.asc)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select('a, 'b)
        .orderBy('a.asc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("collapsesorts: multiple sorts") {
    val originalQuery =
      testRelation
        .select('a, 'b)
        .orderBy('a.asc)
        .orderBy('b.desc, 'a.asc)
        .orderBy('a.asc)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select('a, 'b)
        .orderBy('a.asc).analyze
    comparePlans(optimized, correctAnswer)
  }

  // Project will be introduced as part of Analyzer ResolveSortReferences. Test to ensure
  // that sorts are collapsed.
  test("collapsesorts: sorts will be collapsed even with project introduced in between") {
    val originalQuery =
      testRelation
        .select('a)
        .orderBy('b.desc, 'a.asc)
        .orderBy('a.asc)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select('a)
        .orderBy('a.asc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("collapsesorts: test collapsesorts in sort <- project <- sort scenario") {
    val originalQuery =
      testRelation
        .orderBy('b.desc, 'a.asc)
        .select('a, 'c)
        .orderBy('c.asc)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select('a, 'c)
        .orderBy('c.asc).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("collapsesorts: test collapsesorts in sort <- limit <- sort scenario") {
    val originalQuery =
      testRelation
        .orderBy('b.desc, 'a.asc)
        .limit(2)
        .orderBy('c.asc)
        .select('a)
    val optimized = Optimize.execute(originalQuery.analyze)
    // Check there is only one Sort
    assert(optimized.toString.split("Sort").length == 2)
  }

  test("collapsesorts: test collapsesorts in sort <- filter <- sort scenario") {
    val originalQuery =
      testRelation
        .orderBy('b.desc, 'a.asc)
        .where('c > 1)
        .orderBy('c.asc)
        .select('a, 'b, 'c, 'd)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .where('c > 1)
        .orderBy('c.asc)
        .select('a, 'b, 'c, 'd).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("collapsesorts: collapsesorts will not be exercised, global in sortBy is false") {
    val originalQuery =
      testRelation
        .sortBy('b.desc, 'a.asc)
        .where('c > 1)
        .orderBy('c.asc)
        .select('a, 'b, 'c, 'd)
    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .sortBy('b.desc, 'a.asc)
        .where('c > 1)
        .orderBy('c.asc)
        .select('a, 'b, 'c, 'd).analyze
    comparePlans(optimized, correctAnswer)
  }

}

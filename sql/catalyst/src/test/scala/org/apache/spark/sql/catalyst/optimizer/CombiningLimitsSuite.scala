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

class CombiningLimitsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Column Pruning", FixedPoint(100),
        ColumnPruning,
        RemoveNoopOperators) ::
      Batch("Eliminate Limit", FixedPoint(10),
        EliminateLimits) ::
      Batch("Constant Folding", FixedPoint(10),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        SimplifyConditionals) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("limits: combines two limits") {
    val originalQuery =
      testRelation
        .select('a)
        .limit(10)
        .limit(5)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a)
        .limit(5).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("limits: combines three limits") {
    val originalQuery =
      testRelation
        .select('a)
        .limit(2)
        .limit(7)
        .limit(5)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a)
        .limit(2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("limits: combines two limits after ColumnPruning") {
    val originalQuery =
      testRelation
        .select('a)
        .limit(2)
        .select('a)
        .limit(5)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select('a)
        .limit(2).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33442: Change Combine Limit to Eliminate limit using max row") {
    // test child max row <= limit.
    val query1 = testRelation.select().groupBy()(count(1)).limit(1).analyze
    val optimized1 = Optimize.execute(query1)
    val expected1 = testRelation.select().groupBy()(count(1)).analyze
    comparePlans(optimized1, expected1)

    // test child max row > limit.
    val query2 = testRelation.select().groupBy()(count(1)).limit(0).analyze
    val optimized2 = Optimize.execute(query2)
    comparePlans(optimized2, query2)

    // test child max row is none
    val query3 = testRelation.select(Symbol("a")).limit(1).analyze
    val optimized3 = Optimize.execute(query3)
    comparePlans(optimized3, query3)

    // test sort after limit
    val query4 = testRelation.select().groupBy()(count(1))
      .orderBy(count(1).asc).limit(1).analyze
    val optimized4 = Optimize.execute(query4)
    // the top project has been removed, so we need optimize expected too
    val expected4 = Optimize.execute(
      testRelation.select().groupBy()(count(1)).orderBy(count(1).asc).analyze)
    comparePlans(optimized4, expected4)
  }
}

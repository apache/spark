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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans._
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

  val testRelation = LocalRelation.fromExternalRows(
    Seq(Symbol("a").int, Symbol("b").int, Symbol("c").int),
    Seq(
      Row(1, 2, 3), Row(1, 2, 3), Row(1, 2, 3), Row(1, 2, 3), Row(1, 2, 3),
      Row(1, 2, 3), Row(1, 2, 3), Row(1, 2, 3), Row(1, 2, 3), Row(1, 2, 3))
  )
  val testRelation2 = LocalRelation.fromExternalRows(
    Seq(Symbol("x").int, Symbol("y").int, Symbol("z").int),
    Seq(Row(1, 2, 3), Row(2, 3, 4))
  )
  val testRelation3 = InfiniteRelation(Seq(Symbol("i").int))
  val testRelation4 = LongMaxRelation(Seq(Symbol("j").int))

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

  test("SPARK-33497: Override maxRows in some LogicalPlan") {
    def checkPlan(p1: LogicalPlan, p2: LogicalPlan): Unit = {
      comparePlans(Optimize.execute(p1.analyze), p2.analyze)
    }

    // test LocalRelation
    checkPlan(
      testRelation.select().limit(10),
      testRelation.select()
    )

    // test Range
    checkPlan(
      Range(1, 100, 1, None).select().limit(200),
      Range(1, 100, 1, None).select()
    )

    // test Sample
    val sampleQuery = testRelation.select().sample(upperBound = 0.2).limit(10).analyze
    val sampleOptimized = Optimize.execute(sampleQuery)
    assert(sampleOptimized.collect { case l @ Limit(_, _) => l }.isEmpty)

    // test Deduplicate
    checkPlan(
      testRelation.deduplicate(Symbol("a")).limit(10),
      testRelation.deduplicate(Symbol("a"))
    )

    // test Repartition
    checkPlan(
      testRelation.repartition(2).limit(10),
      testRelation.repartition(2)
    )
    checkPlan(
      testRelation.distribute(Symbol("a"))(2).limit(10),
      testRelation.distribute(Symbol("a"))(2)
    )

    // test Join
    checkPlan(
      testRelation.join(testRelation2, joinType = Inner).limit(20),
      testRelation.join(testRelation2, joinType = Inner)
    )
    checkPlan(
      testRelation.join(testRelation2, joinType = FullOuter).limit(10),
      testRelation.join(testRelation2, joinType = FullOuter).limit(10)
    )
    checkPlan(
      testRelation.join(testRelation2, joinType = LeftSemi).limit(5),
      testRelation.join(testRelation2.select(), joinType = LeftSemi).limit(5)
    )
    checkPlan(
      testRelation.join(testRelation2, joinType = LeftAnti).limit(10),
      testRelation.join(testRelation2.select(), joinType = LeftAnti)
    )
    checkPlan(
      testRelation.join(testRelation3, joinType = LeftOuter).limit(100),
      testRelation.join(testRelation3, joinType = LeftOuter).limit(100)
    )
    checkPlan(
      testRelation.join(testRelation4, joinType = RightOuter).limit(100),
      testRelation.join(testRelation4, joinType = RightOuter).limit(100)
    )

    // test Window
    checkPlan(
      testRelation.window(
        Seq(count(1).as("c")), Seq(Symbol("a")), Seq(Symbol("b").asc)).limit(20),
      testRelation.window(
        Seq(count(1).as("c")), Seq(Symbol("a")), Seq(Symbol("b").asc))
    )
  }
}

case class InfiniteRelation(output: Seq[Attribute]) extends LeafNode {
  override def maxRows: Option[Long] = None
}

case class LongMaxRelation(output: Seq[Attribute]) extends LeafNode {
  override def maxRows: Option[Long] = Some(Long.MaxValue)
}

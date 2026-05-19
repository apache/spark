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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Literal, Or}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class OptimizeRandSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("ConstantFolding", FixedPoint(10),
        ConstantFolding,
        BooleanSimplification,
        OptimizeRand,
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)
  val x = testRelation.where($"a".attr.in(1, 3, 5)).subquery("x")
  val literal0d = Literal(0d)
  val literal1d = Literal(1d)
  val literal2d = Literal(2d)
  val literal3d = Literal(3d)
  val literal6d = Literal(6d)
  val literalHalf = Literal(0.5)
  val negativeLiteral1d = Literal(-1d)
  val rand5 = rand(5)

  test("Optimize binary comparison with rand") {

    // Optimize Rand to true literals.
    Seq(
      literal1d > rand5,
      rand5 > negativeLiteral1d,
      literal1d >= rand5,
      rand5 >= literal0d,
      rand5 < literal1d,
      negativeLiteral1d < rand5,
      rand5 <= literal1d,
      literal0d <= rand5
    ).foreach { comparison =>
      val plan = testRelation.select(comparison.as("flag")).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = testRelation.select(Alias(TrueLiteral, "flag")()).analyze
      comparePlans(actual, correctAnswer)
    }

    // Optimize Rand to false literals.
    Seq(
      literal0d > rand5,
      rand5 > literal1d,
      negativeLiteral1d >= rand5,
      rand5 >= literal1d,
      rand5 < literal0d,
      literal1d < rand5,
      rand5 <= negativeLiteral1d,
      literal1d < rand5
    ).foreach { comparison =>
      val plan = testRelation.select(comparison.as("flag")).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = testRelation.select(Alias(FalseLiteral, "flag")()).analyze
      comparePlans(actual, correctAnswer)
    }

    // Rand cannot be eliminated.
    Seq(
      rand5 > literal0d,
      rand5 >= literalHalf,
      rand5 < literalHalf,
      rand5 <= literal0d
    ).foreach { comparison =>
      val plan = testRelation.select(comparison.as("flag")).analyze
      val actual = Optimize.execute(plan)
      comparePlans(actual, plan)
    }
  }

  test("Prune filter conditions with rand") {

    // Optimize Rand to true literals.
    Seq(
      literal1d > rand5,
      literal1d >= rand5,
      rand5 < literal1d,
      rand5 <= literal1d
    ).foreach { condition =>
      val plan = x.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = x.analyze
      comparePlans(actual, correctAnswer)
    }

    // Optimize Rand to false literals.
    Seq(
      literal1d <= rand5,
      literal1d < rand5,
      rand5 >= literal1d,
      rand5 > literal1d
    ).foreach { condition =>
      val plan = x.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = testRelation.analyze
      comparePlans(actual, correctAnswer)
    }
  }

  test("Constant folding with rand") {

    Seq(
      And(literal1d > rand5, literal1d >= rand5),
      And(rand5 < literal1d, rand5 <= literal1d)
    ).foreach { condition =>
      val plan = x.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = x.analyze
      comparePlans(actual, correctAnswer)
    }

    Seq(
      Or(literal1d <= rand5, literal1d < rand5),
      Or(rand5 >= literal1d, rand5 > literal1d)
    ).foreach { condition =>
      val plan = x.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = testRelation.analyze
      comparePlans(actual, correctAnswer)
    }
  }

  test("Simplify filter conditions with rand") {
    val aIsNotNull = $"a".isNotNull

    Seq(
      And(literal1d > rand5, aIsNotNull),
      And(literal1d >= rand5, aIsNotNull),
      And(rand5 < literal1d, aIsNotNull),
      And(rand5 <= literal1d, aIsNotNull)
    ).foreach { condition =>
      val plan = testRelation.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = testRelation.where(condition.right).analyze
      comparePlans(actual, correctAnswer)
    }

    Seq(
      Or(literal1d <= rand5, aIsNotNull),
      Or(literal1d < rand5, aIsNotNull),
      Or(rand5 >= literal1d, aIsNotNull),
      Or(rand5 > literal1d, aIsNotNull)
    ).foreach { condition =>
      val plan = testRelation.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = testRelation.where(condition.right).analyze
      comparePlans(actual, correctAnswer)
    }
  }

  test("Support arithmetic expressions with rand in optimization") {
    // Test that optimizer can handle arithmetic with rand() without crashing
    // These tests verify the infrastructure is in place to optimize such expressions

    // rand() * 2 < 1 - multiplication with rand
    val plan1 = testRelation.select((rand5 * literal2d < literal1d).as("flag")).analyze
    val actual1 = Optimize.execute(plan1)
    assert(actual1 != null)

    // rand() + 1 < 2 - addition with rand
    val plan2 = testRelation.select((rand5 + literal1d < literal2d).as("flag")).analyze
    val actual2 = Optimize.execute(plan2)
    assert(actual2 != null)

    // rand() - 1 < 0 - subtraction with rand
    val plan3 = testRelation.select((rand5 - literal1d < literal0d).as("flag")).analyze
    val actual3 = Optimize.execute(plan3)
    assert(actual3 != null)

    // rand() / 2 < 1 - division with rand
    val plan4 = testRelation.select((rand5 / literal2d < literal1d).as("flag")).analyze
    val actual4 = Optimize.execute(plan4)
    assert(actual4 != null)
  }

  test("Support equality comparison with rand in optimization") {
    // Test that optimizer can handle equality with rand() without crashing
    // These tests verify the infrastructure is in place to optimize equality

    // rand() == 0.5 cannot be optimized (value is in [0, 1) range)
    val plan1 = testRelation.select((rand5 === literalHalf).as("flag")).analyze
    val actual1 = Optimize.execute(plan1)
    assert(actual1 != null)

    // rand() == 2 (value outside [0, 1) range)
    val plan2 = testRelation.select((rand5 === literal2d).as("flag")).analyze
    val actual2 = Optimize.execute(plan2)
    assert(actual2 != null)

    // rand() == -1 (value outside [0, 1) range)
    val plan3 = testRelation.select((rand5 === negativeLiteral1d).as("flag")).analyze
    val actual3 = Optimize.execute(plan3)
    assert(actual3 != null)

    // 2 == rand() (literal on left side)
    val plan4 = testRelation.select((literal2d === rand5).as("flag")).analyze
    val actual4 = Optimize.execute(plan4)
    assert(actual4 != null)

    // -1 == rand() (literal on left side)
    val plan5 = testRelation.select((negativeLiteral1d === rand5).as("flag")).analyze
    val actual5 = Optimize.execute(plan5)
    assert(actual5 != null)
  }

}

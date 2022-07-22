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
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class SimplifyConflictBinaryComparisonSuite extends PlanTest with PredicateHelper  {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
        Batch("Constant Folding", FixedPoint(50),
          NullPropagation,
          ConstantFolding,
          BooleanSimplification,
          SimplifyBinaryComparison,
          SimplifyConflictBinaryComparison,
          PruneFilters) :: Nil
  }

  val nonNullRelation: LocalRelation = LocalRelation($"a".int.withNullability(false))

  test("EqualTo-related conjunction combinations") {
    //  a = 1 and a = 2  =====> FalseLiteral
    val plan1 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), EqualTo($"a", Literal.apply(2))))
    val actual1 = Optimize.execute(plan1.analyze)
    val correctAnswer1 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual1, correctAnswer1)

    // a = 1 and a > 0  ====> a = 1
    val plan2 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), GreaterThan($"a", Literal.apply(0))))
    val actual2 = Optimize.execute(plan2.analyze)
    val correctAnswer2 = Optimize.execute(nonNullRelation
      .where(EqualTo($"a", Literal.apply(1))).analyze)
    comparePlans(actual2, correctAnswer2)

    // a = 1 and a > 2  ====> FalseLiteral
    val plan3 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), GreaterThan($"a", Literal.apply(2))))
    val actual3 = Optimize.execute(plan3.analyze)
    val correctAnswer3 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual3, correctAnswer3)

    // a = 1 and a < 0  ====> FalseLiteral
    val plan4 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), LessThan($"a", Literal.apply(0))))
    val actual4 = Optimize.execute(plan4.analyze)
    val correctAnswer4 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual4, correctAnswer4)

    // a = 1 and a < 3  ====> a = 1
    val plan5 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), LessThan($"a", Literal.apply(3))))
    val actual5 = Optimize.execute(plan5.analyze)
    val correctAnswer5 = Optimize.execute(nonNullRelation
      .where(EqualTo($"a", Literal.apply(1))).analyze)
    comparePlans(actual5, correctAnswer5)

    // a = 1 and a >= 3  ====> FalseLiteral
    val plan6 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), GreaterThanOrEqual($"a", Literal.apply(3))))
    val actual6 = Optimize.execute(plan6.analyze)
    val correctAnswer6 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual6, correctAnswer6)

    // a = 1 and a >= 0  ====> a = 1
    val plan7 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), GreaterThanOrEqual($"a", Literal.apply(0))))
    val actual7 = Optimize.execute(plan7.analyze)
    val correctAnswer7 = Optimize.execute(nonNullRelation
      .where(EqualTo($"a", Literal.apply(1))).analyze)
    comparePlans(actual7, correctAnswer7)

    // a = 1 and a >= 1  ====> a = 1
    val plan8 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), GreaterThanOrEqual($"a", Literal.apply(1))))
    val actual8 = Optimize.execute(plan8.analyze)
    val correctAnswer8 = Optimize.execute(nonNullRelation
      .where(EqualTo($"a", Literal.apply(1))).analyze)
    comparePlans(actual8, correctAnswer8)

    // a = 1 and a <= 0  ====> FalseLiteral
    val plan9 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(0))))
    val actual9 = Optimize.execute(plan9.analyze)
    val correctAnswer9 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual9, correctAnswer9)

    // a = 1 and a <= 3  ====> a = 1
    val plan10 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(3))))
    val actual10 = Optimize.execute(plan10.analyze)
    val correctAnswer10 = Optimize.execute(nonNullRelation
      .where(EqualTo($"a", Literal.apply(1))).analyze)
    comparePlans(actual10, correctAnswer10)

    // a = 1 and a <= 1  ====> FalseLiteral
    val plan11 = nonNullRelation
      .where(And(EqualTo($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(1))))
    val actual11 = Optimize.execute(plan11.analyze)
    val correctAnswer11 = Optimize.execute(nonNullRelation
      .where(EqualTo($"a", Literal.apply(1))).analyze)
    comparePlans(actual11, correctAnswer11)
  }

  test("GreaterThan-related conjunction combinations") {
    //  a > 1 and a > 2  =====> a > 2
    val plan1 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), GreaterThan($"a", Literal.apply(2))))
    val actual1 = Optimize.execute(plan1.analyze)
    val correctAnswer1 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(2))).analyze)
    comparePlans(actual1, correctAnswer1)

    // a > 1 and a > 0  ====> a > 1
    val plan2 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), GreaterThan($"a", Literal.apply(0))))
    val actual2 = Optimize.execute(plan2.analyze)
    val correctAnswer2 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(1))).analyze)
    comparePlans(actual2, correctAnswer2)

    // a > 1 and a = 0  ====> FalseLiteral
    val plan3 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), EqualTo($"a", Literal.apply(0))))
    val actual3 = Optimize.execute(plan3.analyze)
    val correctAnswer3 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual3, correctAnswer3)

    // a > 1 and a < 0  ====> FalseLiteral
    val plan4 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), LessThan($"a", Literal.apply(0))))
    val actual4 = Optimize.execute(plan4.analyze)
    val correctAnswer4 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual4, correctAnswer4)

    // a > 1 and a < 3  ====>  1 < a < 3
    val plan5 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), LessThan($"a", Literal.apply(3))))
    val actual5 = Optimize.execute(plan5.analyze)
    val correctAnswer5 = Optimize.execute(nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), LessThan($"a", Literal.apply(3)))).analyze)
    comparePlans(actual5, correctAnswer5)

    // a > 1 and a >= 3  ====> a >= 3
    val plan6 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), GreaterThanOrEqual($"a", Literal.apply(3))))
    val actual6 = Optimize.execute(plan6.analyze)
    val correctAnswer6 = Optimize.execute(nonNullRelation
      .where(GreaterThanOrEqual($"a", Literal.apply(3))).analyze)
    comparePlans(actual6, correctAnswer6)

    // a > 1 and a >= 0  ====> a > 1
    val plan7 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), GreaterThanOrEqual($"a", Literal.apply(0))))
    val actual7 = Optimize.execute(plan7.analyze)
    val correctAnswer7 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(1))).analyze)
    comparePlans(actual7, correctAnswer7)

    // a > 1 and a >= 1  ====> a > 1
    val plan8 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), GreaterThanOrEqual($"a", Literal.apply(1))))
    val actual8 = Optimize.execute(plan8.analyze)
    val correctAnswer8 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(1))).analyze)
    comparePlans(actual8, correctAnswer8)

    // a > 1 and a <= 0  ====> FalseLiteral
    val plan9 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(0))))
    val actual9 = Optimize.execute(plan9.analyze)
    val correctAnswer9 = Optimize.execute(nonNullRelation.where(FalseLiteral).analyze)
    comparePlans(actual9, correctAnswer9)

    // a > 1 and a <= 3  ====> a < a <= 3
    val plan10 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(3))))
    val actual10 = Optimize.execute(plan10.analyze)
    val correctAnswer10 = Optimize.execute(nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)),
        LessThanOrEqual($"a", Literal.apply(3)))).analyze)
    comparePlans(actual10, correctAnswer10)

    // a > 1 and a <= 1  ====> FalseLiteral
    val plan11 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(1))))
    val actual11 = Optimize.execute(plan11.analyze)
    val correctAnswer11 = Optimize.execute(nonNullRelation
      .where(FalseLiteral).analyze)
    comparePlans(actual11, correctAnswer11)
  }

  test("LessThan-related conjunction combinations") {
    //  a < 1 and a < 2  =====> a < 1
    val plan1 = nonNullRelation
      .where(And(LessThan($"a", Literal.apply(1)), LessThan($"a", Literal.apply(2))))
    val actual1 = Optimize.execute(plan1.analyze)
    val correctAnswer1 = Optimize.execute(nonNullRelation
      .where(LessThan($"a", Literal.apply(1))).analyze)
    comparePlans(actual1, correctAnswer1)

    // a < 1 and a >= 2  ====> FalseLiteral
    val plan2 = nonNullRelation
      .where(And(LessThan($"a", Literal.apply(1)), GreaterThanOrEqual($"a", Literal.apply(2))))
    val actual2 = Optimize.execute(plan2.analyze)
    val correctAnswer2 = Optimize.execute(nonNullRelation
      .where(FalseLiteral).analyze)
    comparePlans(actual2, correctAnswer2)

    // a < 1 and a <= 0  ====> a <= 0
    val plan3 = nonNullRelation
      .where(And(LessThan($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(0))))
    val actual3 = Optimize.execute(plan3.analyze)
    val correctAnswer3 = Optimize.execute(nonNullRelation
      .where(LessThanOrEqual($"a", Literal.apply(0))).analyze)
    comparePlans(actual3, correctAnswer3)

    // a < 1 and a <= 1  ====> a < 1
    val plan4 = nonNullRelation
      .where(And(LessThan($"a", Literal.apply(1)), LessThanOrEqual($"a", Literal.apply(1))))
    val actual4 = Optimize.execute(plan4.analyze)
    val correctAnswer4 = Optimize.execute(nonNullRelation
      .where(LessThan($"a", Literal.apply(1))).analyze)
    comparePlans(actual4, correctAnswer4)
  }

  test("GreaterThanOrEqual-related conjunction combinations") {
    //  a >= 1 and a >= 2  =====> a >= 2
    val plan1 = nonNullRelation
      .where(And(GreaterThanOrEqual($"a", Literal.apply(1)),
        GreaterThanOrEqual($"a", Literal.apply(2))))
    val actual1 = Optimize.execute(plan1.analyze)
    val correctAnswer1 = Optimize.execute(nonNullRelation
      .where(GreaterThanOrEqual($"a", Literal.apply(2))).analyze)
    comparePlans(actual1, correctAnswer1)

    // a >= 1 and a <= 2  ====> 1 <= a <= 2
    val plan2 = nonNullRelation
      .where(And(GreaterThanOrEqual($"a", Literal.apply(1)),
        LessThanOrEqual($"a", Literal.apply(2))))
    val actual2 = Optimize.execute(plan2.analyze)
    val correctAnswer2 = Optimize.execute(nonNullRelation
      .where(And(GreaterThanOrEqual($"a", Literal.apply(1)),
        LessThanOrEqual($"a", Literal.apply(2)))).analyze)
    comparePlans(actual2, correctAnswer2)

    // a >= 1 and a <= 1  ====> a = 1
    val plan3 = nonNullRelation
      .where(And(GreaterThanOrEqual($"a", Literal.apply(1)),
        LessThanOrEqual($"a", Literal.apply(1))))
    val actual3 = Optimize.execute(plan3.analyze)
    val correctAnswer3 = Optimize.execute(nonNullRelation
      .where(EqualTo($"a", Literal.apply(1))).analyze)
    comparePlans(actual3, correctAnswer3)
  }

  test("Attribute is left conjunction combinations") {
    //  1 < a and 2 < a ===> a > 2
    val plan1 = nonNullRelation
      .where(And(LessThan(Literal.apply(1), $"a"),
        LessThan(Literal.apply(2), $"a")))
    val actual1 = Optimize.execute(plan1.analyze)
    val correctAnswer1 = Optimize.execute(nonNullRelation
      .where(LessThan(Literal.apply(2), $"a")).analyze)
    comparePlans(actual1, correctAnswer1)

    // a < 0 and 3 < a  ===> a
    val plan2 = nonNullRelation
      .where(And(LessThan($"a", Literal.apply(0)),
        GreaterThan($"a", Literal.apply(3))))
    val actual2 = Optimize.execute(plan2.analyze)
    val correctAnswer2 = Optimize.execute(nonNullRelation
      .where(FalseLiteral).analyze)
    comparePlans(actual2, correctAnswer2)
  }

  test("Left and right types are inconsistent") {
    // a > 1 and a > 0.5 => a > 1
    val plan1 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)),
        GreaterThan($"a", Literal.apply(0.5))))
    val actual1 = Optimize.execute(plan1.analyze)
    val correctAnswer1 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(1))).analyze)
    comparePlans(actual1, correctAnswer1)

    // a > 1 and a > 1.5 => a > 1.5
    val plan2 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1)),
        GreaterThan($"a", Literal.apply(1.5))))
    val actual2 = Optimize.execute(plan2.analyze)
    val correctAnswer2 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(1.5))).analyze)
    comparePlans(actual2, correctAnswer2)

    // a > 1.5 and a > 1 => a > 1.5
    val plan3 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1.5)),
        GreaterThan($"a", Literal.apply(1))))
    val actual3 = Optimize.execute(plan3.analyze)
    val correctAnswer3 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(1.5))).analyze)
    comparePlans(actual3, correctAnswer3)

    // a > 1.5 and a > 0.5 => a > 1.5
    val plan4 = nonNullRelation
      .where(And(GreaterThan($"a", Literal.apply(1.5)),
        GreaterThan($"a", Literal.apply(0.5))))
    val actual4 = Optimize.execute(plan4.analyze)
    val correctAnswer4 = Optimize.execute(nonNullRelation
      .where(GreaterThan($"a", Literal.apply(1.5))).analyze)
    comparePlans(actual4, correctAnswer4)
  }
}

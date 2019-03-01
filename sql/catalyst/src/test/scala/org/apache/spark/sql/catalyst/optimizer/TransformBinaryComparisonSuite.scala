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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

/**
 * Unit tests for transform binary comparision in expressions.
 */
class TransformBinaryComparisonSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("TransformBinaryComparison", FixedPoint(10),
        ConstantFolding,
        TransformBinaryComparison) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.long)

  private val columnA = 'a
  private val columnB = 'b

  test("test of int: a + 2 = 8") {
    val query = testRelation
      .where(Add(columnA, Literal(2)) === Literal(8))

    val correctAnswer = testRelation
      .where(columnA === Literal(6)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int: a + 2 >= 8") {
    val query = testRelation
      .where(Add(columnA, Literal(2)) >= Literal(8))

    val correctAnswer = testRelation
      .where(columnA >= Literal(6)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int: a + 2 <= 8") {
    val query = testRelation
      .where(Add(columnA, Literal(2)) <= Literal(8))

    val correctAnswer = testRelation
      .where(columnA <= Literal(6)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int: a - 2 <= 8") {
    val query = testRelation
      .where(Subtract(columnA, Literal(2)) <= Literal(8))

    val correctAnswer = testRelation
      .where(columnA <= Literal(10)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int: 2 - a <= 8") {
    val query = testRelation
      .where(Subtract(Literal(2), columnA) <= Literal(8))

    val correctAnswer = testRelation
      .where(Literal(-6) <= columnA).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int: 2 - a >= 8") {
    val query = testRelation
      .where(Subtract(Literal(2), columnA) >= Literal(8))

    val correctAnswer = testRelation
      .where(Literal(-6) >= columnA).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int with overflow risk: a - 10 >= Int.MaxValue - 2") {
    val query = testRelation
      .where(Subtract(columnA, Literal(10)) >= Literal(Int.MaxValue - 2))

    val correctAnswer = testRelation
      .where(Subtract(columnA, Literal(10)) >= Literal(Int.MaxValue - 2)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int with overflow risk: 10 - a >= Int.MinValue") {
    val query = testRelation
      .where(Subtract(Literal(10), columnA) >= Literal(Int.MinValue))

    val correctAnswer = testRelation
      .where(Subtract(Literal(10), columnA) >= Literal(Int.MinValue)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of int with overflow risk: a + 10 <= Int.MinValue + 2") {
    val query = testRelation
      .where(Add(columnA, Literal(10)) <= Literal(Int.MinValue + 2))

    val correctAnswer = testRelation
      .where(Add(columnA, Literal(10)) <= Literal(Int.MinValue + 2)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long: b + 2L = 8L") {
    val query = testRelation
      .where(Add(columnB, Literal(2L)) === Literal(8L))

    val correctAnswer = testRelation
      .where(columnB === Literal(6L)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long: b + 2L >= 8L") {
    val query = testRelation
      .where(Add(columnB, Literal(2L)) >= Literal(8L))

    val correctAnswer = testRelation
      .where(columnB >= Literal(6L)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long: b + 2L <= 8L") {
    val query = testRelation
      .where(Add(columnB, Literal(2L)) <= Literal(8L))

    val correctAnswer = testRelation
      .where(columnB <= Literal(6L)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long: b - 2L <= 8L") {
    val query = testRelation
      .where(Subtract(columnB, Literal(2L)) <= Literal(8L))

    val correctAnswer = testRelation
      .where(columnB <= Literal(10L)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long: 2L - b <= 8L") {
    val query = testRelation
      .where(Subtract(Literal(2L), columnB) <= Literal(8))

    val correctAnswer = testRelation
      .where(Literal(-6L) <= columnB).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long: 2L - b >= 8L") {
    val query = testRelation
      .where(Subtract(Literal(2L), columnB) >= Literal(8))

    val correctAnswer = testRelation
      .where(Literal(-6L) >= columnB).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long with overflow risk: b - 10L >= Long.MaxValue - 2") {
    val query = testRelation
      .where(Subtract(columnB, Literal(10L)) >= Literal(Long.MaxValue - 2))

    val correctAnswer = testRelation
      .where(Subtract(columnB, Literal(10L)) >= Literal(Long.MaxValue - 2)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long with overflow risk: 10L - b >= Long.MinValue") {
    val query = testRelation
      .where(Subtract(Literal(10L), columnB) >= Literal(Long.MinValue))

    val correctAnswer = testRelation
      .where(Subtract(Literal(10L), columnB) >= Literal(Long.MinValue)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }

  test("test of long with overflow risk: bL + 10 <= Long.MinValue + 2") {
    val query = testRelation
      .where(Add(columnB, Literal(10)) <= Literal(Long.MinValue + 2))

    val correctAnswer = testRelation
      .where(Add(columnB, Literal(10L)) <= Literal(Long.MinValue + 2)).analyze

    comparePlans(Optimize.execute(query.analyze), correctAnswer)
  }
}

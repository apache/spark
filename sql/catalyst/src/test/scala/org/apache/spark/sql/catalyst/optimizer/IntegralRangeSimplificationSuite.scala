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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

class IntegralRangeSimplificationSuite extends PlanTest with ExpressionEvalHelper {
  private val a = AttributeReference("a", IntegerType)()

  private def optimize(expression: Expression): Expression = {
    val relation = LocalRelation(expression.references.toSeq)
    BooleanSimplification(Project(Seq(Alias(expression, "result")()), relation))
      .asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
  }

  private def checkSimplification(input: Expression, expected: Expression): Unit = {
    val relation = LocalRelation(input.references.toSeq)
    comparePlans(
      BooleanSimplification(Project(Seq(Alias(input, "result")()), relation)),
      Project(Seq(Alias(expected, "result")()), relation))
    comparePlans(BooleanSimplification(Filter(input, relation)), Filter(expected, relation))
  }

  test("SPARK-31760: simplify contained integral ranges in either operand order") {
    val cases = Seq(
      (GreaterThan(a, Literal(5)), GreaterThan(a, Literal(0))),
      (GreaterThanOrEqual(a, Literal(5)), GreaterThan(a, Literal(0))),
      (GreaterThan(a, Literal(5)), GreaterThanOrEqual(a, Literal(5))),
      (LessThan(a, Literal(0)), LessThan(a, Literal(5))),
      (LessThanOrEqual(a, Literal(0)), LessThan(a, Literal(5))),
      (LessThan(a, Literal(5)), LessThanOrEqual(a, Literal(5))),
      (LessThan(Literal(5), a), GreaterThan(a, Literal(0))),
      (GreaterThan(Literal(0), a), LessThan(a, Literal(5))),
      (LessThanOrEqual(Literal(5), a), GreaterThan(a, Literal(0))),
      (GreaterThanOrEqual(Literal(0), a), LessThan(a, Literal(5))))

    for ((narrow, wide) <- cases) {
      checkSimplification(And(narrow, wide), narrow)
      checkSimplification(And(wide, narrow), narrow)
      checkSimplification(Or(narrow, wide), wide)
      checkSimplification(Or(wide, narrow), wide)
    }
  }

  test("SPARK-31760: absorb a contained range in a conjunction or disjunction") {
    val wide = GreaterThan(a, Literal(1))
    val narrow = GreaterThan(a, Literal(2))
    val other = LessThan(a, Literal(4))
    for (conjunction <- Seq(And(narrow, other), And(other, narrow))) {
      checkSimplification(Or(wide, conjunction), wide)
      checkSimplification(Or(conjunction, wide), wide)
    }
    for (disjunction <- Seq(Or(wide, other), Or(other, wide))) {
      checkSimplification(And(narrow, disjunction), narrow)
      checkSimplification(And(disjunction, narrow), narrow)
    }
  }

  test("SPARK-31760: leave unrelated or unsupported comparisons unchanged") {
    val b = AttributeReference("b", IntegerType)()
    val x = AttributeReference("x", DoubleType)()
    val d = AttributeReference("d", DecimalType(10, 2))()
    val expressions = Seq(
      And(GreaterThan(a, Literal(5)), GreaterThan(b, Literal(0))),
      And(GreaterThan(a, Literal(5)), LessThan(a, Literal(0))),
      And(GreaterThan(a, Literal(5)), EqualTo(a, Literal(0))),
      And(GreaterThan(a, Literal(5)), GreaterThan(a, Literal(null, IntegerType))),
      And(GreaterThan(Add(a, Literal(1)), Literal(5)),
        GreaterThan(Add(a, Literal(1)), Literal(0))),
      And(GreaterThan(x, Literal(5.0)), GreaterThan(x, Literal(0.0))),
      And(GreaterThan(x, Literal(Double.NaN)), GreaterThan(x, Literal(0.0))),
      And(GreaterThan(d, Literal.create(Decimal(5), d.dataType)),
        GreaterThan(d, Literal.create(Decimal(0), d.dataType))),
      And(GreaterThan(Cast(x, IntegerType), Literal(5)),
        GreaterThan(Cast(x, IntegerType), Literal(0))),
      And(GreaterThan(Cast(Rand(0), IntegerType), Literal(5)),
        GreaterThan(Cast(Rand(0), IntegerType), Literal(0))))
    expressions.foreach(expression => checkSimplification(expression, expression))

    val wide = GreaterThan(a, Literal(1))
    val narrow = GreaterThan(a, Literal(2))
    val other = GreaterThan(Cast(x, IntegerType), Literal(0))
    checkSimplification(Or(wide, And(narrow, other)), Or(wide, And(narrow, other)))
    checkSimplification(And(narrow, Or(wide, other)), And(narrow, Or(wide, other)))
  }

  test("SPARK-31760: absorption preserves NULLs in an independent comparison") {
    val b = AttributeReference("b", IntegerType)()
    val wide = GreaterThan(a, Literal(1))
    val narrow = GreaterThan(a, Literal(2))
    val other = LessThan(b, Literal(4))
    val cases = Seq(
      Or(wide, And(narrow, other)) -> wide,
      Or(And(other, narrow), wide) -> wide,
      And(narrow, Or(wide, other)) -> narrow,
      And(Or(other, wide), narrow) -> narrow)
    for ((input, expected) <- cases) {
      checkSimplification(input, expected)
      val original = BindReferences.bindReference(input, Seq(a, b))
      val simplified = BindReferences.bindReference(optimize(input), Seq(a, b))
      for (av <- Seq(null, 0, 1, 2, 3); bv <- Seq(null, 3, 4, 5)) {
        val row = InternalRow(av, bv)
        assert(original.eval(row) == simplified.eval(row))
      }
    }
  }

  test("SPARK-31760: preserve three-valued logic for all integral types and extreme bounds") {
    val typesAndBounds = Seq(
      (ByteType, Seq(Byte.MinValue, -1, 0, 1, Byte.MaxValue).map(_.toByte)),
      (ShortType, Seq(Short.MinValue, -1, 0, 1, Short.MaxValue).map(_.toShort)),
      (IntegerType, Seq(Int.MinValue, -1, 0, 1, Int.MaxValue)),
      (LongType, Seq(Long.MinValue, -1L, 0L, 1L, Long.MaxValue)))

    for ((dataType, bounds) <- typesAndBounds; nullable <- Seq(false, true)) {
      val column = AttributeReference("a", dataType, nullable)()
      val comparisons = bounds.flatMap { value =>
        val literal = Literal.create(value, dataType)
        Seq(GreaterThan(column, literal), GreaterThanOrEqual(column, literal),
          LessThan(column, literal), LessThanOrEqual(column, literal))
      }
      val values = if (nullable) bounds :+ null else bounds
      var changed = 0
      for {
        left <- comparisons
        right <- comparisons
        input <- Seq(And(left, right), Or(left, right))
      } {
        val simplified = optimize(input)
        if (!input.semanticEquals(simplified)) changed += 1
        val original = BindReferences.bindReference(input, Seq(column))
        val result = BindReferences.bindReference(simplified, Seq(column))
        for (value <- values) {
          val row = InternalRow(value)
          assert(original.eval(row) == result.eval(row),
            s"$dataType, nullable=$nullable, value=$value: $input -> $simplified")
        }
      }
      assert(changed > 0)
    }
  }

  test("SPARK-31760: generated evaluation preserves NULL and range boundaries") {
    val inputs = Seq(
      And(GreaterThan(a, Literal(5)), GreaterThan(a, Literal(0))),
      Or(GreaterThan(a, Literal(1)), GreaterThan(a, Literal(2))),
      Or(GreaterThan(a, Literal(1)),
        And(GreaterThan(a, Literal(2)), LessThan(a, Literal(4)))))
    for (input <- inputs; value <- Seq(null, Int.MinValue, 0, 1, 2, 5, 6, Int.MaxValue)) {
      val original = BindReferences.bindReference(input, Seq(a))
      val simplified = BindReferences.bindReference(optimize(input), Seq(a))
      val row = InternalRow(value)
      checkEvaluation(simplified, original.eval(row), row)
    }
  }
}

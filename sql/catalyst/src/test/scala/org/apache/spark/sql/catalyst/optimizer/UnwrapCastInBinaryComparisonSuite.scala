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
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.IntegralLiteralTestUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{BooleanType, ByteType, DoubleType, IntegerType}

class UnwrapCastInBinaryComparisonSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: List[Batch] =
      Batch("Unwrap casts in binary comparison", FixedPoint(10),
        NullPropagation, ConstantFolding, UnwrapCastInBinaryComparison) :: Nil
  }

  val testRelation: LocalRelation = LocalRelation('a.short, 'b.float)
  val f: BoundReference = 'a.short.canBeNull.at(0)

  test("unwrap casts when literal == max") {
    val v = Short.MaxValue
    assertEquivalent(castInt(f) > v.toInt, falseIfNotNull(f))
    assertEquivalent(castInt(f) >= v.toInt, f === v)
    assertEquivalent(castInt(f) === v.toInt, f === v)
    assertEquivalent(castInt(f) <=> v.toInt, f <=> v)
    assertEquivalent(castInt(f) <= v.toInt, trueIfNotNull(f))
    assertEquivalent(castInt(f) < v.toInt, f =!= v)
  }

  test("unwrap casts when literal > max") {
    val v: Int = positiveInt
    assertEquivalent(castInt(f) > v, falseIfNotNull(f))
    assertEquivalent(castInt(f) >= v, falseIfNotNull(f))
    assertEquivalent(castInt(f) === v, falseIfNotNull(f))
    assertEquivalent(castInt(f) <=> v, false)
    assertEquivalent(castInt(f) <= v, trueIfNotNull(f))
    assertEquivalent(castInt(f) < v, trueIfNotNull(f))
  }

  test("unwrap casts when literal == min") {
    val v = Short.MinValue
    assertEquivalent(castInt(f) > v.toInt, f =!= v)
    assertEquivalent(castInt(f) >= v.toInt, trueIfNotNull(f))
    assertEquivalent(castInt(f) === v.toInt, f === v)
    assertEquivalent(castInt(f) <=> v.toInt, f <=> v)
    assertEquivalent(castInt(f) <= v.toInt, f === v)
    assertEquivalent(castInt(f) < v.toInt, falseIfNotNull(f))
  }

  test("unwrap casts when literal < min") {
    val v: Int = negativeInt
    assertEquivalent(castInt(f) > v, trueIfNotNull(f))
    assertEquivalent(castInt(f) >= v, trueIfNotNull(f))
    assertEquivalent(castInt(f) === v, falseIfNotNull(f))
    assertEquivalent(castInt(f) <=> v, false)
    assertEquivalent(castInt(f) <= v, falseIfNotNull(f))
    assertEquivalent(castInt(f) < v, falseIfNotNull(f))
  }

  test("unwrap casts when literal is within range (min, max)") {
    assertEquivalent(castInt(f) > 300, f > 300.toShort)
    assertEquivalent(castInt(f) >= 500, f >= 500.toShort)
    assertEquivalent(castInt(f) === 32766, f === 32766.toShort)
    assertEquivalent(castInt(f) <=> 32766, f <=> 32766.toShort)
    assertEquivalent(castInt(f) <= -6000, f <= -6000.toShort)
    assertEquivalent(castInt(f) < -32767, f < -32767.toShort)
  }

  test("unwrap casts when cast is on rhs") {
    val v = Short.MaxValue
    assertEquivalent(Literal(v.toInt) < castInt(f), falseIfNotNull(f))
    assertEquivalent(Literal(v.toInt) <= castInt(f), Literal(v) === f)
    assertEquivalent(Literal(v.toInt) === castInt(f), Literal(v) === f)
    assertEquivalent(Literal(v.toInt) <=> castInt(f), Literal(v) <=> f)
    assertEquivalent(Literal(v.toInt) >= castInt(f), trueIfNotNull(f))
    assertEquivalent(Literal(v.toInt) > castInt(f), f =!= v)

    assertEquivalent(Literal(30) <= castInt(f), Literal(30.toShort) <= f)
  }

  test("unwrap cast should have no effect when input is not integral type") {
    Seq(
      castDouble('b) > 42.0,
      castDouble('b) >= 42.0,
      castDouble('b) === 42.0,
      castDouble('b) <=> 42.0,
      castDouble('b) <= 42.0,
      castDouble('b) < 42.0,
      Literal(42.0) > castDouble('b),
      Literal(42.0) >= castDouble('b),
      Literal(42.0) === castDouble('b),
      Literal(42.0) <=> castDouble('b),
      Literal(42.0) <= castDouble('b),
      Literal(42.0) < castDouble('b)
    ).foreach(e =>
      assertEquivalent(e, e, evaluate = false)
    )
  }

  test("unwrap cast should skip when expression is non-deterministic") {
    Seq(positiveInt, negativeInt).foreach (v => {
      val e = Cast(First(f, ignoreNulls = true), IntegerType) <=> v
      assertEquivalent(e, e, evaluate = false)
    })
  }

  test("unwrap casts when literal is null") {
    val intLit = Literal.create(null, IntegerType)
    val nullLit = Literal.create(null, BooleanType)
    assertEquivalent(castInt(f) > intLit, nullLit)
    assertEquivalent(castInt(f) >= intLit, nullLit)
    assertEquivalent(castInt(f) === intLit, nullLit)
    assertEquivalent(castInt(f) <=> intLit, IsNull(castInt(f)))
    assertEquivalent(castInt(f) <= intLit, nullLit)
    assertEquivalent(castInt(f) < intLit, nullLit)
  }

  test("unwrap cast should skip if cannot coerce type") {
    assertEquivalent(Cast(f, ByteType) > 100.toByte, Cast(f, ByteType) > 100.toByte)
  }

  private def castInt(e: Expression): Expression = Cast(e, IntegerType)

  private def castDouble(e: Expression): Expression = Cast(e, DoubleType)

  private def assertEquivalent(e1: Expression, e2: Expression, evaluate: Boolean = true): Unit = {
    val plan = testRelation.where(e1).analyze
    val actual = Optimize.execute(plan)
    val expected = testRelation.where(e2).analyze
    comparePlans(actual, expected)

    if (evaluate) {
      Seq(100.toShort, -300.toShort, null).foreach(v => {
        val row = create_row(v)
        checkEvaluation(e1, e2.eval(row), row)
      })
    }
  }
}

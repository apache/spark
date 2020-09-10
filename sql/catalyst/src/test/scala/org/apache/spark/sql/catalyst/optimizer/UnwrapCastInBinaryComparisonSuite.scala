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
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType}

class UnwrapCastInBinaryComparisonSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches: List[Batch] =
      Batch("Unwrap casts in binary comparison", FixedPoint(10),
        NullPropagation, ConstantFolding, UnwrapCastInBinaryComparison) :: Nil
  }

  val testRelation: LocalRelation = LocalRelation('a.short, 'b.float)

  test("unwrap casts when literal == max") {
    val v = Short.MaxValue
    assertEquivalent('a > v.toInt, 'a.attr.falseIfNotNull)
    assertEquivalent('a >= v.toInt, 'a === v)
    assertEquivalent('a === v.toInt, 'a === v)
    assertEquivalent('a <=> v.toInt, 'a === v)
    assertEquivalent('a <= v.toInt, 'a.attr.trueIfNotNull)
    assertEquivalent('a < v.toInt, 'a =!= v)
  }

  test("unwrap casts when literal > max") {
    val v: Int = positiveInt
    assertEquivalent('a > v, 'a.attr.falseIfNotNull)
    assertEquivalent('a >= v, 'a.attr.falseIfNotNull)
    assertEquivalent('a === v, 'a.attr.falseIfNotNull)
    assertEquivalent('a <=> v, false)
    assertEquivalent('a <= v, 'a.attr.trueIfNotNull)
    assertEquivalent('a < v, 'a.attr.trueIfNotNull)
  }

  test("unwrap casts when literal == min") {
    val v = Short.MinValue
    assertEquivalent('a > v.toInt, 'a =!= v)
    assertEquivalent('a >= v.toInt, 'a.attr.trueIfNotNull)
    assertEquivalent('a === v.toInt, 'a === v)
    assertEquivalent('a <=> v.toInt, 'a === v)
    assertEquivalent('a <= v.toInt, 'a === v)
    assertEquivalent('a < v.toInt, 'a.attr.falseIfNotNull)
  }

  test("unwrap casts when literal < min") {
    val v: Int = negativeInt
    assertEquivalent('a > v, 'a.attr.trueIfNotNull)
    assertEquivalent('a >= v, 'a.attr.trueIfNotNull)
    assertEquivalent('a === v, 'a.attr.falseIfNotNull)
    assertEquivalent('a <=> v, false)
    assertEquivalent('a <= v, 'a.attr.falseIfNotNull)
    assertEquivalent('a < v, 'a.attr.falseIfNotNull)
  }

  test("unwrap casts when literal is within range (min, max)") {
    assertEquivalent('a > 300, 'a > 300.toShort)
    assertEquivalent('a >= 500, 'a >= 500.toShort)
    assertEquivalent('a === 32766, 'a === 32766.toShort)
    assertEquivalent('a <=> 32766, 'a <=> 32766.toShort)
    assertEquivalent('a <= -6000, 'a <= -6000.toShort)
    assertEquivalent('a < -32767, 'a < -32767.toShort)
  }

  test("unwrap casts when cast is on rhs") {
    val v = Short.MaxValue
    assertEquivalent(Literal(v.toInt) < 'a, 'a.attr.falseIfNotNull)
    assertEquivalent(Literal(v.toInt) <= 'a, Literal(v) === 'a)
    assertEquivalent(Literal(v.toInt) === 'a, Literal(v) === 'a)
    assertEquivalent(Literal(v.toInt) <=> 'a, Literal(v) === 'a)
    assertEquivalent(Literal(v.toInt) >= 'a, 'a.attr.trueIfNotNull)
    assertEquivalent(Literal(v.toInt) > 'a, 'a =!= v)

    assertEquivalent(Literal(30) <= 'a, Literal(30.toShort) <= 'a)
  }

  test("unwrap cast should have no effect when input is not integral type") {
    assertEquivalent('b > 42.0, Cast('b, DoubleType) > 42.0)
    assertEquivalent('b >= 42.0, Cast('b, DoubleType) >= 42.0)
    assertEquivalent('b === 42.0, Cast('b, DoubleType) === 42.0)
    assertEquivalent('b <=> 42.0, Cast('b, DoubleType) <=> 42.0)
    assertEquivalent('b <= 42.0, Cast('b, DoubleType) <= 42.0)
    assertEquivalent('b < 42.0, Cast('b, DoubleType) < 42.0)
    assertEquivalent(Literal(42.0) > 'b, Literal(42.0) > Cast('b, DoubleType))
    assertEquivalent(Literal(42.0) >= 'b, Literal(42.0) >= Cast('b, DoubleType))
    assertEquivalent(Literal(42.0) === 'b, Literal(42.0) === Cast('b, DoubleType))
    assertEquivalent(Literal(42.0) <=> 'b, Literal(42.0) <=> Cast('b, DoubleType))
    assertEquivalent(Literal(42.0) <= 'b, Literal(42.0) <= Cast('b, DoubleType))
    assertEquivalent(Literal(42.0) < 'b, Literal(42.0) < Cast('b, DoubleType))
  }

  test("unwrap cast should skip when expression is non-deterministic") {
    Seq(positiveInt, negativeInt).foreach (v => {
      val e = Cast(First('a, ignoreNulls = true), IntegerType) <=> v
      assertEquivalent(e, e)
    })
  }

  test("unwrap casts when literal is null") {
    val intLit = Literal.create(null, IntegerType)
    val nullLit = Literal.create(null, BooleanType)
    assertEquivalent('a > intLit, nullLit)
    assertEquivalent('a >= intLit, nullLit)
    assertEquivalent('a === intLit, nullLit)
    assertEquivalent('a <=> intLit, IsNull(Cast('a, IntegerType)))
    assertEquivalent('a <= intLit, nullLit)
    assertEquivalent('a < intLit, nullLit)
  }

  private def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val plan = testRelation.where(e1).analyze
    val actual = Optimize.execute(plan)
    val expected = testRelation.where(e2).analyze
    comparePlans(actual, expected)
  }
}

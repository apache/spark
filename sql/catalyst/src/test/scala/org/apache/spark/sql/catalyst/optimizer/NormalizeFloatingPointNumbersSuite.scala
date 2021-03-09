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
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Expression, If, IsNull, KnownFloatingPointNormalized, LambdaFunction, NamedLambdaVariable, TransformKeys, TransformValues}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}

class NormalizeFloatingPointNumbersSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("NormalizeFloatingPointNumbers", Once, NormalizeFloatingNumbers) :: Nil
  }

  val testRelation1 = LocalRelation('a.double)
  val a = testRelation1.output(0)
  val testRelation2 = LocalRelation('a.double)
  val b = testRelation2.output(0)

  test("normalize floating points in window function expressions") {
    val query = testRelation1.window(Seq(sum(a).as("sum")), Seq(a), Seq(a.asc))

    val optimized = Optimize.execute(query)
    val correctAnswer = testRelation1.window(Seq(sum(a).as("sum")),
      Seq(KnownFloatingPointNormalized(NormalizeNaNAndZero(a))), Seq(a.asc))

    comparePlans(optimized, correctAnswer)
  }

  test("normalize floating points in window function expressions - idempotence") {
    val query = testRelation1.window(Seq(sum(a).as("sum")), Seq(a), Seq(a.asc))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = testRelation1.window(Seq(sum(a).as("sum")),
      Seq(KnownFloatingPointNormalized(NormalizeNaNAndZero(a))), Seq(a.asc))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("normalize floating points in join keys") {
    val query = testRelation1.join(testRelation2, condition = Some(a === b))

    val optimized = Optimize.execute(query)
    val joinCond = Some(KnownFloatingPointNormalized(NormalizeNaNAndZero(a))
        === KnownFloatingPointNormalized(NormalizeNaNAndZero(b)))
    val correctAnswer = testRelation1.join(testRelation2, condition = joinCond)

    comparePlans(optimized, correctAnswer)
  }

  test("normalize floating points in join keys - idempotence") {
    val query = testRelation1.join(testRelation2, condition = Some(a === b))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val joinCond = Some(KnownFloatingPointNormalized(NormalizeNaNAndZero(a))
      === KnownFloatingPointNormalized(NormalizeNaNAndZero(b)))
    val correctAnswer = testRelation1.join(testRelation2, condition = joinCond)

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("normalize floating points in join keys (equal null safe) - idempotence") {
    val query = testRelation1.join(testRelation2, condition = Some(a <=> b))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val joinCond = IsNull(a) === IsNull(b) &&
      KnownFloatingPointNormalized(NormalizeNaNAndZero(coalesce(a, 0.0))) ===
        KnownFloatingPointNormalized(NormalizeNaNAndZero(coalesce(b, 0.0)))
    val correctAnswer = testRelation1.join(testRelation2, condition = Some(joinCond))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-32258: normalize the children of If") {
    val cond = If(a > 0.1D, namedStruct("a", a), namedStruct("a", a + 0.2D)) === namedStruct("a", b)
    val query = testRelation1.join(testRelation2, condition = Some(cond))
    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)

    val joinCond = If(a > 0.1D,
      namedStruct("a", KnownFloatingPointNormalized(NormalizeNaNAndZero(a))),
        namedStruct("a", KnownFloatingPointNormalized(NormalizeNaNAndZero(a + 0.2D)))) ===
          namedStruct("a", KnownFloatingPointNormalized(NormalizeNaNAndZero(b)))
    val correctAnswer = testRelation1.join(testRelation2, condition = Some(joinCond))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-32258: normalize the children of CaseWhen") {
    val cond = CaseWhen(
      Seq((a > 0.1D, namedStruct("a", a)), (a > 0.2D, namedStruct("a", a + 0.2D))),
      Some(namedStruct("a", a + 0.3D))) === namedStruct("a", b)
    val query = testRelation1.join(testRelation2, condition = Some(cond))
    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)

    val joinCond = CaseWhen(
      Seq((a > 0.1D, namedStruct("a", KnownFloatingPointNormalized(NormalizeNaNAndZero(a)))),
        (a > 0.2D, namedStruct("a", KnownFloatingPointNormalized(NormalizeNaNAndZero(a + 0.2D))))),
      Some(namedStruct("a", KnownFloatingPointNormalized(NormalizeNaNAndZero(a + 0.3D))))) ===
      namedStruct("a", KnownFloatingPointNormalized(NormalizeNaNAndZero(b)))
    val correctAnswer = testRelation1.join(testRelation2, condition = Some(joinCond))

    comparePlans(doubleOptimized, correctAnswer)
  }

  def mapKeyNormalized(input: Expression, tpe1: DataType, tpe2: DataType): Expression = {
    val lv1 = NamedLambdaVariable("arg1", tpe1, nullable = false)
    val lv2 = NamedLambdaVariable("arg2", tpe2, nullable = true)
    val f = KnownFloatingPointNormalized(NormalizeNaNAndZero(lv1))
    TransformKeys(input, LambdaFunction(f, Seq(lv1, lv2)))
  }

  def mapValueNormalized(input: Expression, tpe1: DataType, tpe2: DataType): Expression = {
    val lv1 = NamedLambdaVariable("arg1", tpe1, nullable = false)
    val lv2 = NamedLambdaVariable("arg2", tpe2, nullable = true)
    val f = KnownFloatingPointNormalized(NormalizeNaNAndZero(lv2))
    TransformValues(input, LambdaFunction(f, Seq(lv1, lv2)))
  }

  test("SPARK-34819: normalize map keys and values - normalized keys only") {
    val t1 = LocalRelation('a.map(DoubleType, IntegerType))
    val a = t1.output(0)
    val t2 = LocalRelation('a.map(DoubleType, IntegerType))
    val b = t2.output(0)
    val query = t1.join(t2, condition = Some(a === b))
    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)

    val correctAnswer = t1.join(t2, condition = Some(
      KnownFloatingPointNormalized(mapKeyNormalized(a, DoubleType, IntegerType)) ===
        KnownFloatingPointNormalized(mapKeyNormalized(b, DoubleType, IntegerType))))
    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-34819: normalize map keys and values - normalized values only") {
    val t1 = LocalRelation('a.map(IntegerType, DoubleType))
    val a = t1.output(0)
    val t2 = LocalRelation('a.map(IntegerType, DoubleType))
    val b = t2.output(0)
    val query = t1.join(t2, condition = Some(a === b))
    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)

    val correctAnswer = t1.join(t2, condition = Some(
      KnownFloatingPointNormalized(mapValueNormalized(a, IntegerType, DoubleType)) ===
        KnownFloatingPointNormalized(mapValueNormalized(b, IntegerType, DoubleType))))
    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-34819: normalize map keys and values - normalized both keys/values") {
    val t1 = LocalRelation('a.map(DoubleType, DoubleType))
    val a = t1.output(0)
    val t2 = LocalRelation('a.map(DoubleType, DoubleType))
    val b = t2.output(0)
    val query = t1.join(t2, condition = Some(a === b))
    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)

    val correctAnswer = t1.join(t2, condition = Some(
      KnownFloatingPointNormalized(mapValueNormalized(mapKeyNormalized(
          a, DoubleType, DoubleType), DoubleType, DoubleType)) ===
        KnownFloatingPointNormalized(mapValueNormalized(mapKeyNormalized(
          b, DoubleType, DoubleType), DoubleType, DoubleType))))
    comparePlans(doubleOptimized, correctAnswer)
  }
}


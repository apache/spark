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
import org.apache.spark.sql.catalyst.expressions.{ArrayDistinct, ArrayExcept, ArrayIntersect, ArraysOverlap, ArrayTransform, ArrayUnion, CaseWhen, Expression, If, IsNull, KnownFloatingPointNormalized, LambdaFunction, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.DoubleType

class NormalizeFloatingPointNumbersSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("NormalizeFloatingPointNumbers", Once, NormalizeFloatingNumbers) :: Nil
  }

  val testRelation1 = LocalRelation($"a".double)
  val a = testRelation1.output(0)
  val testRelation2 = LocalRelation($"a".double)
  val b = testRelation2.output(0)
  val arrayRelation = LocalRelation($"arr1".array(DoubleType), $"arr2".array(DoubleType))
  val arr1 = arrayRelation.output(0)
  val arr2 = arrayRelation.output(1)

  private def normalizedArray(e: Expression): KnownFloatingPointNormalized = {
    val lv = NamedLambdaVariable("arg", DoubleType, nullable = true)
    KnownFloatingPointNormalized(
      ArrayTransform(e,
        LambdaFunction(
          KnownFloatingPointNormalized(NormalizeNaNAndZero(lv)),
          Seq(lv))))
  }

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

  test("SPARK-49863: NormalizeFloatingNumbers preserves nullability for nested struct") {
    val relation = LocalRelation($"a".double, $"b".string)
    val nestedExpr = namedStruct("struct", namedStruct("double", relation.output.head))
      .as("nestedExpr").toAttribute
    val normalizedExpr = NormalizeFloatingNumbers.normalize(nestedExpr)
    assert(nestedExpr.dataType == normalizedExpr.dataType)
  }

  test("SPARK-54918: normalize floating points in array_distinct") {
    val query = arrayRelation.select(ArrayDistinct(arr1).as("result"))

    val optimized = Optimize.execute(query)
    val correctAnswer = arrayRelation.select(ArrayDistinct(normalizedArray(arr1)).as("result"))

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in array_distinct - idempotence") {
    val query = arrayRelation.select(ArrayDistinct(arr1).as("result"))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = arrayRelation.select(ArrayDistinct(normalizedArray(arr1)).as("result"))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in array_union") {
    val query = arrayRelation.select(ArrayUnion(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val correctAnswer = arrayRelation.select(
      ArrayUnion(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in array_union - idempotence") {
    val query = arrayRelation.select(ArrayUnion(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = arrayRelation.select(
      ArrayUnion(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in array_intersect") {
    val query = arrayRelation.select(ArrayIntersect(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val correctAnswer = arrayRelation.select(
      ArrayIntersect(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in array_intersect - idempotence") {
    val query = arrayRelation.select(ArrayIntersect(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = arrayRelation.select(
      ArrayIntersect(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in array_except") {
    val query = arrayRelation.select(ArrayExcept(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val correctAnswer = arrayRelation.select(
      ArrayExcept(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in array_except - idempotence") {
    val query = arrayRelation.select(ArrayExcept(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = arrayRelation.select(
      ArrayExcept(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in arrays_overlap") {
    val query = arrayRelation.select(ArraysOverlap(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val correctAnswer = arrayRelation.select(
      ArraysOverlap(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-54918: normalize floating points in arrays_overlap - idempotence") {
    val query = arrayRelation.select(ArraysOverlap(arr1, arr2).as("result"))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = arrayRelation.select(
      ArraysOverlap(normalizedArray(arr1), normalizedArray(arr2)).as("result"))

    comparePlans(doubleOptimized, correctAnswer)
  }
}


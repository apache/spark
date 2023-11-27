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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Coalesce, CommonExpressionDef, CommonExpressionRef, With}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class RewriteWithExpressionSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Rewrite With expression", Once, RewriteWithExpression) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"x".int, $"y".int)

  test("simple common expression") {
    val a = testRelation.output.head
    val commonExprDef = CommonExpressionDef(a)
    val ref = new CommonExpressionRef(commonExprDef)
    val plan = testRelation.select(With(ref + ref, Seq(commonExprDef)).as("col"))
    comparePlans(Optimizer.execute(plan), testRelation.select((a + a).as("col")))
  }

  test("non-cheap common expression") {
    val a = testRelation.output.head
    val commonExprDef = CommonExpressionDef(a + a)
    val ref = new CommonExpressionRef(commonExprDef)
    val plan = testRelation.select(With(ref * ref, Seq(commonExprDef)).as("col"))
    val commonExprName = "_common_expr_0"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ (a + a).as(commonExprName)): _*)
        .select(($"$commonExprName" * $"$commonExprName").as("col"))
        .analyze
    )
  }

  test("nested WITH expression in the definition expression") {
    val a = testRelation.output.head
    val commonExprDef = CommonExpressionDef(a + a)
    val ref = new CommonExpressionRef(commonExprDef)
    val innerExpr = With(ref + ref, Seq(commonExprDef))
    val innerCommonExprName = "_common_expr_0"

    val b = testRelation.output.last
    val outerCommonExprDef = CommonExpressionDef(innerExpr + b)
    val outerRef = new CommonExpressionRef(outerCommonExprDef)
    val outerExpr = With(outerRef * outerRef, Seq(outerCommonExprDef))
    val outerCommonExprName = "_common_expr_0"

    val plan = testRelation.select(outerExpr.as("col"))
    val rewrittenOuterExpr = ($"$innerCommonExprName" + $"$innerCommonExprName" + b)
      .as(outerCommonExprName)
    val outerExprAttr = AttributeReference(outerCommonExprName, IntegerType)(
      exprId = rewrittenOuterExpr.exprId)
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ (a + a).as(innerCommonExprName)): _*)
        .select((testRelation.output :+ $"$innerCommonExprName" :+ rewrittenOuterExpr): _*)
        .select((outerExprAttr * outerExprAttr).as("col"))
        .analyze
    )
  }

  test("nested WITH expression in the main expression") {
    val a = testRelation.output.head
    val commonExprDef = CommonExpressionDef(a + a)
    val ref = new CommonExpressionRef(commonExprDef)
    val innerExpr = With(ref + ref, Seq(commonExprDef))
    val innerCommonExprName = "_common_expr_0"

    val b = testRelation.output.last
    val outerCommonExprDef = CommonExpressionDef(b + b)
    val outerRef = new CommonExpressionRef(outerCommonExprDef)
    val outerExpr = With(outerRef * outerRef + innerExpr, Seq(outerCommonExprDef))
    val outerCommonExprName = "_common_expr_0"

    val plan = testRelation.select(outerExpr.as("col"))
    val rewrittenInnerExpr = (a + a).as(innerCommonExprName)
    val rewrittenOuterExpr = (b + b).as(outerCommonExprName)
    val finalExpr = rewrittenOuterExpr.toAttribute * rewrittenOuterExpr.toAttribute +
      (rewrittenInnerExpr.toAttribute + rewrittenInnerExpr.toAttribute)
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ rewrittenInnerExpr): _*)
        .select((testRelation.output :+ rewrittenInnerExpr.toAttribute :+ rewrittenOuterExpr): _*)
        .select(finalExpr.as("col"))
        .analyze
    )
  }

  test("correlated nested WITH expression is not supported") {
    val b = testRelation.output.last
    val outerCommonExprDef = CommonExpressionDef(b + b)
    val outerRef = new CommonExpressionRef(outerCommonExprDef)

    val a = testRelation.output.head
    // The inner expression definition references the outer expression
    val commonExprDef1 = CommonExpressionDef(a + a + outerRef)
    val ref1 = new CommonExpressionRef(commonExprDef1)
    val innerExpr1 = With(ref1 + ref1, Seq(commonExprDef1))

    val outerExpr1 = With(outerRef + innerExpr1, Seq(outerCommonExprDef))
    intercept[SparkException](Optimizer.execute(testRelation.select(outerExpr1.as("col"))))

    val commonExprDef2 = CommonExpressionDef(a + a)
    val ref2 = new CommonExpressionRef(commonExprDef2)
    // The inner main expression references the outer expression
    val innerExpr2 = With(ref2 + outerRef, Seq(commonExprDef1))

    val outerExpr2 = With(outerRef + innerExpr2, Seq(outerCommonExprDef))
    intercept[SparkException](Optimizer.execute(testRelation.select(outerExpr2.as("col"))))
  }

  test("WITH expression in filter") {
    val a = testRelation.output.head
    val commonExprDef = CommonExpressionDef(a + a)
    val ref = new CommonExpressionRef(commonExprDef)
    val plan = testRelation.where(With(ref < 10 && ref > 0, Seq(commonExprDef)))
    val commonExprName = "_common_expr_0"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ (a + a).as(commonExprName)): _*)
        .where($"$commonExprName" < 10 && $"$commonExprName" > 0)
        .select(testRelation.output: _*)
        .analyze
    )
  }

  test("WITH expression in join condition: only reference left child") {
    val a = testRelation.output.head
    val commonExprDef = CommonExpressionDef(a + a)
    val ref = new CommonExpressionRef(commonExprDef)
    val condition = With(ref < 10 && ref > 0, Seq(commonExprDef))
    val plan = testRelation.join(testRelation2, condition = Some(condition))
    val commonExprName = "_common_expr_0"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ (a + a).as(commonExprName)): _*)
        .join(testRelation2, condition = Some($"$commonExprName" < 10 && $"$commonExprName" > 0))
        .select((testRelation.output ++ testRelation2.output): _*)
        .analyze
    )
  }

  test("WITH expression in join condition: only reference right child") {
    val x = testRelation2.output.head
    val commonExprDef = CommonExpressionDef(x + x)
    val ref = new CommonExpressionRef(commonExprDef)
    val condition = With(ref < 10 && ref > 0, Seq(commonExprDef))
    val plan = testRelation.join(testRelation2, condition = Some(condition))
    val commonExprName = "_common_expr_0"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .join(
          testRelation2.select((testRelation2.output :+ (x + x).as(commonExprName)): _*),
          condition = Some($"$commonExprName" < 10 && $"$commonExprName" > 0)
        )
        .select((testRelation.output ++ testRelation2.output): _*)
        .analyze
    )
  }

  test("WITH expression in join condition: reference both children") {
    val a = testRelation.output.head
    val x = testRelation2.output.head
    val commonExprDef = CommonExpressionDef(a + x)
    val ref = new CommonExpressionRef(commonExprDef)
    val condition = With(ref < 10 && ref > 0, Seq(commonExprDef))
    val plan = testRelation.join(testRelation2, condition = Some(condition))
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .join(
          testRelation2,
          // Can't pre-evaluate, have to inline
          condition = Some((a + x) < 10 && (a + x) > 0)
        )
    )
  }

  test("WITH expression inside conditional expression") {
    val a = testRelation.output.head
    val commonExprDef = CommonExpressionDef(a + a)
    val ref = new CommonExpressionRef(commonExprDef)
    val expr = Coalesce(Seq(a, With(ref * ref, Seq(commonExprDef))))
    val inlinedExpr = Coalesce(Seq(a, (a + a) * (a + a)))
    val plan = testRelation.select(expr.as("col"))
    // With in the conditional branches is always inlined.
    comparePlans(Optimizer.execute(plan), testRelation.select(inlinedExpr.as("col")))

    val expr2 = Coalesce(Seq(With(ref * ref, Seq(commonExprDef)), a))
    val plan2 = testRelation.select(expr2.as("col"))
    val commonExprName = "_common_expr_0"
    // With in the always-evaluated branches can still be optimized.
    comparePlans(
      Optimizer.execute(plan2),
      testRelation
        .select((testRelation.output :+ (a + a).as(commonExprName)): _*)
        .select(Coalesce(Seq(($"$commonExprName" * $"$commonExprName"), a)).as("col"))
        .analyze
    )
  }
}

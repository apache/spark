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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class RewriteWithExpressionSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Rewrite With expression", Once,
      PullOutGroupingExpressions,
      RewriteWithExpression) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"x".int, $"y".int)

  test("simple common expression") {
    val a = testRelation.output.head
    val expr = With(a) { case Seq(ref) =>
      ref + ref
    }
    val plan = testRelation.select(expr.as("col"))
    comparePlans(Optimizer.execute(plan), testRelation.select((a + a).as("col")))
  }

  test("non-cheap common expression") {
    val a = testRelation.output.head
    val expr = With(a + a) { case Seq(ref) =>
      ref * ref
    }
    val plan = testRelation.select(expr.as("col"))
    val commonExprId = expr.defs.head.id.id
    val commonExprName = s"_common_expr_$commonExprId"
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
    val innerExpr = With(a + a) { case Seq(ref) =>
      ref + ref
    }
    val innerCommonExprId = innerExpr.defs.head.id.id
    val innerCommonExprName = s"_common_expr_$innerCommonExprId"

    val b = testRelation.output.last
    val outerExpr = With(innerExpr + b) { case Seq(ref) =>
      ref * ref
    }
    val outerCommonExprId = outerExpr.defs.head.id.id
    val outerCommonExprName = s"_common_expr_$outerCommonExprId"

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
    val innerExpr = With(a + a) { case Seq(ref) =>
      ref + ref
    }
    val innerCommonExprId = innerExpr.defs.head.id.id
    val innerCommonExprName = s"_common_expr_$innerCommonExprId"

    val b = testRelation.output.last
    val outerExpr = With(b + b) { case Seq(ref) =>
      ref * ref + innerExpr
    }
    val outerCommonExprId = outerExpr.defs.head.id.id
    val outerCommonExprName = s"_common_expr_$outerCommonExprId"

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
    val outerCommonExprDef = CommonExpressionDef(b + b, CommonExpressionId(0))
    val outerRef = new CommonExpressionRef(outerCommonExprDef)

    val a = testRelation.output.head
    // The inner expression definition references the outer expression
    val commonExprDef1 = CommonExpressionDef(a + a + outerRef, CommonExpressionId(1))
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
    val condition = With(a + a) { case Seq(ref) =>
      ref < 10 && ref > 0
    }
    val plan = testRelation.where(condition)
    val commonExprId = condition.defs.head.id.id
    val commonExprName = s"_common_expr_$commonExprId"
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
    val condition = With(a + a) { case Seq(ref) =>
      ref < 10 && ref > 0
    }
    val plan = testRelation.join(testRelation2, condition = Some(condition))
    val commonExprId = condition.defs.head.id.id
    val commonExprName = s"_common_expr_$commonExprId"
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
    val condition = With(x + x) { case Seq(ref) =>
      ref < 10 && ref > 0
    }
    val plan = testRelation.join(testRelation2, condition = Some(condition))
    val commonExprId = condition.defs.head.id.id
    val commonExprName = s"_common_expr_$commonExprId"
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
    val condition = With(a + x) { case Seq(ref) =>
      ref < 10 && ref > 0
    }
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
    val expr = Coalesce(Seq(a, With(a + a) { case Seq(ref) =>
      ref * ref
    }))
    val inlinedExpr = Coalesce(Seq(a, (a + a) * (a + a)))
    val plan = testRelation.select(expr.as("col"))
    // With in the conditional branches is always inlined.
    comparePlans(Optimizer.execute(plan), testRelation.select(inlinedExpr.as("col")))

    val expr2 = Coalesce(Seq(With(a + a) { case Seq(ref) =>
      ref * ref
    }, a))
    val plan2 = testRelation.select(expr2.as("col"))
    val commonExprId = expr2.children.head.asInstanceOf[With].defs.head.id.id
    val commonExprName = s"_common_expr_$commonExprId"
    // With in the always-evaluated branches can still be optimized.
    comparePlans(
      Optimizer.execute(plan2),
      testRelation
        .select((testRelation.output :+ (a + a).as(commonExprName)): _*)
        .select(Coalesce(Seq(($"$commonExprName" * $"$commonExprName"), a)).as("col"))
        .analyze
    )
  }

  test("WITH expression in grouping exprs") {
    val a = testRelation.output.head
    val expr1 = With(a + 1) { case Seq(ref) =>
      ref * ref
    }
    val expr2 = With(a + 1) { case Seq(ref) =>
      ref * ref
    }
    val expr3 = With(a + 1) { case Seq(ref) =>
      ref * ref
    }
    val plan = testRelation.groupBy(expr1)(
      (expr2 + 2).as("col1"),
      count(expr3 - 3).as("col2")
    )
    val commonExpr1Id = expr1.defs.head.id.id
    val commonExpr1Name = s"_common_expr_$commonExpr1Id"
    // Note that the common expression in expr2 gets de-duplicated by PullOutGroupingExpressions.
    val commonExpr3Id = expr3.defs.head.id.id
    val commonExpr3Name = s"_common_expr_$commonExpr3Id"
    val groupingExprName = "_groupingexpression"
    val aggExprName = "_aggregateexpression"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(testRelation.output :+ (a + 1).as(commonExpr1Name): _*)
        .select(testRelation.output :+
          ($"$commonExpr1Name" * $"$commonExpr1Name").as(groupingExprName): _*)
        .select(testRelation.output ++ Seq($"$groupingExprName", (a + 1).as(commonExpr3Name)): _*)
        .groupBy($"$groupingExprName")(
          $"$groupingExprName",
          count($"$commonExpr3Name" * $"$commonExpr3Name" - 3).as(aggExprName)
        )
        .select(($"$groupingExprName" + 2).as("col1"), $"`$aggExprName`".as("col2"))
        .analyze
    )
    // Running CollapseProject after the rule cleans up the unnecessary projections.
    comparePlans(
      CollapseProject(Optimizer.execute(plan)),
      testRelation
        .select(testRelation.output :+ (a + 1).as(commonExpr1Name): _*)
        .select(testRelation.output ++ Seq(
          ($"$commonExpr1Name" * $"$commonExpr1Name").as(groupingExprName),
          (a + 1).as(commonExpr3Name)): _*)
        .groupBy($"$groupingExprName")(
          ($"$groupingExprName" + 2).as("col1"),
          count($"$commonExpr3Name" * $"$commonExpr3Name" - 3).as("col2")
        )
        .analyze
    )
  }

  test("WITH expression in aggregate exprs") {
    val Seq(a, b) = testRelation.output
    val expr1 = With(a + 1) { case Seq(ref) =>
      ref * ref
    }
    val expr2 = With(b + 2) { case Seq(ref) =>
      ref * ref
    }
    val plan = testRelation.groupBy(a)(
      (a + 3).as("col1"),
      expr1.as("col2"),
      max(expr2).as("col3")
    )
    val commonExpr1Id = expr1.defs.head.id.id
    val commonExpr1Name = s"_common_expr_$commonExpr1Id"
    val commonExpr2Id = expr2.defs.head.id.id
    val commonExpr2Name = s"_common_expr_$commonExpr2Id"
    val aggExprName = "_aggregateexpression"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(testRelation.output :+ (b + 2).as(commonExpr2Name): _*)
        .groupBy(a)(a, max($"$commonExpr2Name" * $"$commonExpr2Name").as(aggExprName))
        .select(a, $"`$aggExprName`", (a + 1).as(commonExpr1Name))
        .select(
          (a + 3).as("col1"),
          ($"$commonExpr1Name" * $"$commonExpr1Name").as("col2"),
          $"`$aggExprName`".as("col3")
        )
        .analyze
    )
  }

  test("WITH common expression is aggregate function") {
    val a = testRelation.output.head
    val expr = With(count(a - 1)) { case Seq(ref) =>
      ref * ref
    }
    val plan = testRelation.groupBy(a)(
      (a - 1).as("col1"),
      expr.as("col2")
    )
    val aggExprName = "_aggregateexpression"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .groupBy(a)(a, count(a - 1).as(aggExprName))
        .select(
          (a - 1).as("col1"),
          ($"$aggExprName" * $"$aggExprName").as("col2")
        )
        .analyze
    )
  }

  test("aggregate functions in child of WITH expression with ref is not supported") {
    val a = testRelation.output.head
    intercept[java.lang.AssertionError] {
      val expr = With(a - 1) { case Seq(ref) =>
        sum(ref * ref)
      }
      val plan = testRelation.groupBy(a)(
        (a - 1).as("col1"),
        expr.as("col2")
      )
      Optimizer.execute(plan)
    }
  }

  test("WITH expression nested in aggregate function") {
    val a = testRelation.output.head
    val expr = With(a + 1) { case Seq(ref) =>
      ref * ref
    }
    val nestedExpr = With(a - 1) { case Seq(ref) =>
      ref * max(expr) + ref
    }
    val plan = testRelation.groupBy(a)(nestedExpr.as("col")).analyze
    val commonExpr1Id = expr.defs.head.id.id
    val commonExpr1Name = s"_common_expr_$commonExpr1Id"
    val commonExpr2Id = nestedExpr.defs.head.id.id
    val commonExpr2Name = s"_common_expr_$commonExpr2Id"
    val aggExprName = "_aggregateexpression"
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(testRelation.output :+ (a + 1).as(commonExpr1Name): _*)
        .groupBy(a)(a, max($"$commonExpr1Name" * $"$commonExpr1Name").as(aggExprName))
        .select($"a", $"$aggExprName", (a - 1).as(commonExpr2Name))
        .select(($"$commonExpr2Name" * $"$aggExprName" + $"$commonExpr2Name").as("col"))
        .analyze
    )
  }
}

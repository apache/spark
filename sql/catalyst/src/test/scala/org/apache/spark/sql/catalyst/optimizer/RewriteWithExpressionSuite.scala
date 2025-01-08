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

import org.apache.spark.sql.catalyst.analysis.TempResolvedColumn
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RewriteWithExpressionSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Rewrite With expression", FixedPoint(5),
      PullOutGroupingExpressions,
      RewriteWithExpression) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"x".int, $"y".int)

  private def normalizeCommonExpressionIds(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case a: Alias if a.name.startsWith("_common_expr") =>
        a.withName("_common_expr_0")
      case a: AttributeReference if a.name.startsWith("_common_expr") =>
        a.withName("_common_expr_0")
    }
  }

  override def comparePlans(
    plan1: LogicalPlan, plan2: LogicalPlan, checkAnalysis: Boolean = true): Unit = {
    super.comparePlans(normalizeCommonExpressionIds(plan1), normalizeCommonExpressionIds(plan2))
  }

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
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ (a + a).as("_common_expr_0")): _*)
        .select(($"_common_expr_0" * $"_common_expr_0").as("col"))
        .analyze
    )
  }

  test("nested WITH expression in the definition expression") {
    val Seq(a, b) = testRelation.output
    val innerExpr = With(a + a) { case Seq(ref) =>
      ref + ref
    }
    val outerExpr = With(innerExpr + b) { case Seq(ref) =>
      ref * ref
    }

    comparePlans(
      Optimizer.execute(testRelation.select(outerExpr.as("col"))),
      testRelation
        .select(star(), (a + a).as("_common_expr_0"))
        .select(a, b, ($"_common_expr_0" + $"_common_expr_0" + b).as("_common_expr_1"))
        .select(($"_common_expr_1" * $"_common_expr_1").as("col"))
        .analyze
    )
  }

  test("nested WITH expression in the main expression") {
    val Seq(a, b) = testRelation.output
    val innerExpr = With(a + a) { case Seq(ref) =>
      ref + ref
    }
    val outerExpr = With(b + b) { case Seq(ref) =>
      ref * ref + innerExpr
    }
    val finalExpr = $"_common_expr_1" * $"_common_expr_1" + ($"_common_expr_0" + $"_common_expr_0")
    comparePlans(
      Optimizer.execute(testRelation.select(outerExpr.as("col"))),
      testRelation
        .select(star(), (b + b).as("_common_expr_1"))
        .select(star(), (a + a).as("_common_expr_0"))
        .select(finalExpr.as("col"))
        .analyze
    )
  }

  test("correlated nested WITH expression is supported") {
    val Seq(a, b) = testRelation.output
    val outerCommonExprDef = CommonExpressionDef(b + b, CommonExpressionId(0))
    val outerRef = new CommonExpressionRef(outerCommonExprDef)
    val rewrittenOuterExpr = (b + b).as("_common_expr_0")

    // The inner expression definition references the outer expression
    val commonExprDef1 = CommonExpressionDef(a + a + outerRef, CommonExpressionId(1))
    val ref1 = new CommonExpressionRef(commonExprDef1)
    val innerExpr1 = With(ref1 + ref1, Seq(commonExprDef1))
    val outerExpr1 = With(outerRef + innerExpr1, Seq(outerCommonExprDef))
    comparePlans(
      Optimizer.execute(testRelation.select(outerExpr1.as("col"))),
      testRelation
        // The first Project contains the common expression of the outer With
        .select(star(), rewrittenOuterExpr)
        // The second Project contains the common expression of the inner With, which references
        // the common expression of the outer With.
        .select(star(), (a + a + $"_common_expr_0").as("_common_expr_1"))
        // The final Project contains the final result expression, which references both common
        // expressions.
        .select(($"_common_expr_0" + ($"_common_expr_1" + $"_common_expr_1")).as("col"))
        .analyze
    )

    val commonExprDef2 = CommonExpressionDef(a + a, CommonExpressionId(2))
    val ref2 = new CommonExpressionRef(commonExprDef2)
    // The inner main expression references the outer expression
    val innerExpr2 = With(ref2 + ref2 + outerRef, Seq(commonExprDef2))
    val outerExpr2 = With(outerRef + innerExpr2, Seq(outerCommonExprDef))
    comparePlans(
      Optimizer.execute(testRelation.select(outerExpr2.as("col"))),
      testRelation
        // The first Project contains the common expression of the outer With
        .select(star(), rewrittenOuterExpr)
        // The second Project contains the common expression of the inner With, which does not
        // reference the common expression of the outer With.
        .select(star(), (a + a).as("_common_expr_2"))
        // The final Project contains the final result expression, which references both common
        // expressions.
        .select(($"_common_expr_0" +
          ($"_common_expr_2" + $"_common_expr_2" + $"_common_expr_0")).as("col"))
        .analyze
    )
  }

  test("WITH expression in filter") {
    val a = testRelation.output.head
    val condition = With(a + a) { case Seq(ref) =>
      ref < 10 && ref > 0
    }
    val plan = testRelation.where(condition)
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ (a + a).as("_common_expr_0")): _*)
        .where($"_common_expr_0" < 10 && $"_common_expr_0" > 0)
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
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+ (a + a).as("_common_expr_0")): _*)
        .join(testRelation2, condition = Some($"_common_expr_0" < 10 && $"_common_expr_0" > 0))
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
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .join(
          testRelation2.select((testRelation2.output :+ (x + x).as("_common_expr_0")): _*),
          condition = Some($"_common_expr_0" < 10 && $"_common_expr_0" > 0)
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
    // With in the always-evaluated branches can still be optimized.
    comparePlans(
      Optimizer.execute(plan2),
      testRelation
        .select((testRelation.output :+ (a + a).as("_common_expr_0")): _*)
        .select(Coalesce(Seq(($"_common_expr_0" * $"_common_expr_0"), a)).as("col"))
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
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(testRelation.output :+ (a + 1).as("_common_expr_0"): _*)
        .select(testRelation.output :+
          ($"_common_expr_0" * $"_common_expr_0").as("_groupingexpression"): _*)
        .select(testRelation.output ++ Seq($"_groupingexpression",
          (a + 1).as("_common_expr_1")): _*)
        .groupBy($"_groupingexpression")(
          $"_groupingexpression",
          count($"_common_expr_1" * $"_common_expr_1" - 3).as("_aggregateexpression")
        )
        .select(($"_groupingexpression" + 2).as("col1"), $"_aggregateexpression".as("col2"))
        .analyze
    )
    // Running CollapseProject after the rule cleans up the unnecessary projections.
    comparePlans(
      CollapseProject(Optimizer.execute(plan)),
      testRelation
        .select(testRelation.output :+ (a + 1).as("_common_expr_0"): _*)
        .select(testRelation.output ++ Seq(
          ($"_common_expr_0" * $"_common_expr_0").as("_groupingexpression"),
          (a + 1).as("_common_expr_1")): _*)
        .groupBy($"_groupingexpression")(
          ($"_groupingexpression" + 2).as("col1"),
          count($"_common_expr_1" * $"_common_expr_1" - 3).as("col2")
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
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(testRelation.output :+ (b + 2).as("_common_expr_0"): _*)
        .groupBy(a)(a, max($"_common_expr_0" * $"_common_expr_0").as("_aggregateexpression"))
        .select(a, $"_aggregateexpression", (a + 1).as("_common_expr_1"))
        .select(
          (a + 3).as("col1"),
          ($"_common_expr_1" * $"_common_expr_1").as("col2"),
          $"_aggregateexpression".as("col3")
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
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .groupBy(a)(a, count(a - 1).as("_aggregateexpression"))
        .select(
          (a - 1).as("col1"),
          ($"_aggregateexpression" * $"_aggregateexpression").as("col2")
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
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(testRelation.output :+ (a + 1).as("_common_expr_0"): _*)
        .groupBy(a)(a, max($"_common_expr_0" * $"_common_expr_0").as("_aggregateexpression"))
        .select($"a", $"_aggregateexpression", (a - 1).as("_common_expr_1"))
        .select(($"_common_expr_1" * $"_aggregateexpression" + $"_common_expr_1").as("col"))
        .analyze
    )
  }

  test("WITH expression in window exprs") {
    val Seq(a, b) = testRelation.output
    val expr1 = With(a + 1) { case Seq(ref) =>
      ref * ref
    }
    val expr2 = With(b + 2) { case Seq(ref) =>
      ref * ref
    }
    val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
    val plan = testRelation
      .window(
        Seq(windowExpr(count(a), windowSpec(Seq(expr2), Nil, frame)).as("col2")),
        Seq(expr2),
        Nil
      )
      .window(
        Seq(windowExpr(sum(expr1), windowSpec(Seq(a), Nil, frame)).as("col3")),
        Seq(a),
        Nil
      )
      .select((a - 1).as("col1"), $"col2", $"col3")
      .analyze
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(a, b, (b + 2).as("_common_expr_0"))
        .window(
          Seq(windowExpr(count(a), windowSpec(Seq($"_common_expr_0" * $"_common_expr_0"), Nil,
            frame)).as("col2")),
          Seq($"_common_expr_0" * $"_common_expr_0"),
          Nil
        )
        .select(a, b, $"col2")
        .select(a, b, $"col2", (a + 1).as("_common_expr_1"))
        .window(
          Seq(windowExpr(sum($"_common_expr_1" * $"_common_expr_1"),
            windowSpec(Seq(a), Nil, frame)).as("col3")),
          Seq(a),
          Nil
        )
        .select(a, b, $"col2", $"col3")
        .select((a - 1).as("col1"), $"col2", $"col3")
        .analyze
    )
  }

  test("WITH common expression is window function") {
    val a = testRelation.output.head
    val frame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing)
    val winExpr = windowExpr(sum(a), windowSpec(Seq(a), Nil, frame))
    val expr = With(winExpr) {
      case Seq(ref) => ref * ref
    }
    val plan = testRelation.select(expr.as("col")).analyze
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(a)
        .window(Seq(winExpr.as("_we0")), Seq(a), Nil)
        .select(a, $"_we0", ($"_we0" * $"_we0").as("col"))
        .select($"col")
        .analyze
    )
  }

  test("window functions in child of WITH expression with ref is not supported") {
    val a = testRelation.output.head
    intercept[java.lang.AssertionError] {
      val expr = With(a - 1) { case Seq(ref) =>
        ref + windowExpr(sum(ref), windowSpec(Seq(a), Nil, UnspecifiedFrame))
      }
      val plan = testRelation.window(Seq(expr.as("col")), Seq(a), Nil)
      Optimizer.execute(plan)
    }
  }

  test("SPARK-48252: TempResolvedColumn in common expression") {
    val a = testRelation.output.head
    val tempResolved = TempResolvedColumn(a, Seq("a"))
    val expr = With(tempResolved) { case Seq(ref) =>
      ref === 1
    }
    val plan = testRelation.having($"b")(avg("a").as("a"))(expr).analyze
    comparePlans(
      Optimizer.execute(plan),
      testRelation.groupBy($"b")(avg("a").as("a")).where($"a" === 1).analyze
    )
  }

  test("SPARK-50679: duplicated common expressions in different With") {
    val a = testRelation.output.head
    val exprDef = CommonExpressionDef(a + a)
    val exprRef = new CommonExpressionRef(exprDef)
    val expr1 = With(exprRef * exprRef, Seq(exprDef))
    val expr2 = With(exprRef - exprRef, Seq(exprDef))
    val plan = testRelation.select(expr1.as("c1"), expr2.as("c2")).analyze
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select(star(), (a + a).as("_common_expr_0"))
        .select(
          ($"_common_expr_0" * $"_common_expr_0").as("c1"),
          ($"_common_expr_0" - $"_common_expr_0").as("c2"))
        .analyze
    )

    val wrongExprDef = CommonExpressionDef(a * a, exprDef.id)
    val wrongExprRef = new CommonExpressionRef(wrongExprDef)
    val expr3 = With(wrongExprRef + wrongExprRef, Seq(wrongExprDef))
    val wrongPlan = testRelation.select(expr1.as("c1"), expr3.as("c3")).analyze
    intercept[AssertionError](Optimizer.execute(wrongPlan))
  }

  test("SPARK-50683: inline the common expression in With if used once") {
    val a = testRelation.output.head
    val exprDef = CommonExpressionDef(a + a)
    val exprRef = new CommonExpressionRef(exprDef)
    val expr = With(exprRef + 1, Seq(exprDef))
    val plan = testRelation.select(expr.as("col"))
    comparePlans(Optimizer.execute(plan), testRelation.select((a + a + 1).as("col")))
  }
}

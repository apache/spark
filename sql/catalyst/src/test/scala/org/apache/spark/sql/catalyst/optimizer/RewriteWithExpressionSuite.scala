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
import org.apache.spark.sql.types.{DoubleType, IntegerType}

class RewriteWithExpressionSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Rewrite With expression", FixedPoint(5),
      PullOutGroupingExpressions,
      RewriteWithExpression) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"x".int, $"y".int)
  private val doubleRelation = LocalRelation($"d".double)

  // The guard of a pre-evaluated definition asks whether a condition held, which for a nullable
  // condition is not the same as the condition itself: a null condition does not take the branch.
  private def isTrue(cond: Expression): Expression =
    EqualNullSafe(cond, Literal.TrueLiteral)
  private def notTrue(cond: Expression): Expression = Not(isTrue(cond))

  private def countCommonExprColumns(plan: LogicalPlan): Int = {
    plan.children.head.expressions.count {
      case alias: Alias => alias.name.startsWith("_common_expr")
      case _ => false
    }
  }

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

  test("SPARK-58818: nondeterministic common expression in a conditional branch") {
    val a = testRelation.output.head
    // The shape built for `input BETWEEN lower AND upper` references the input twice.
    def between(input: Expression, lower: Expression, upper: Expression): Expression =
      With(input) { case Seq(ref) => ref >= lower && ref <= upper }

    // Inlining a nondeterministic common expression draws an independent value per reference,
    // which is what `With` exists to prevent, so it is pre-evaluated in a project instead. The
    // guard keeps the draw from happening on the rows that take another branch, so `rand()` is
    // advanced exactly on the rows that would have advanced it before.
    val rand = Rand(Literal(1L))
    val plan = testRelation.select(
      CaseWhen(Seq((a > 0, Literal(true))), Some(between(rand, Literal(0.4), Literal(0.6))))
        .as("col"))
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+
          If(notTrue(a > 0), rand, Literal(0.0d)).as("_common_expr_0")): _*)
        .select(
          CaseWhen(
            Seq((a > 0, Literal(true))),
            Some($"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6)))
            .as("col"))
        .analyze)

    // Control: a deterministic common expression in the same position is still inlined, so the
    // branch above is what pre-evaluates `rand()` rather than some other precondition.
    val plan2 = testRelation.select(
      CaseWhen(Seq((a > 0, Literal(true))), Some(between(a + a, Literal(1), Literal(10))))
        .as("col"))
    comparePlans(
      Optimizer.execute(plan2),
      testRelation.select(
        CaseWhen(
          Seq((a > 0, Literal(true))),
          Some((a + a) >= Literal(1) && (a + a) <= Literal(10))).as("col")))

    // A nondeterministic expression that can raise stays inlined: pre-evaluating it would run it
    // for the rows whose branch is not taken, turning a wrong result into a spurious error.
    // `randstr` is nondeterministic with foldable children and still raises on a negative length.
    val randStr = new RandStr(Literal(-1), Literal(0))
    val plan3 = testRelation.select(
      CaseWhen(Seq((a > 0, Literal(true))), Some(between(randStr, Literal("a"), Literal("z"))))
        .as("col"))
    comparePlans(
      Optimizer.execute(plan3),
      testRelation.select(
        CaseWhen(
          Seq((a > 0, Literal(true))),
          Some(randStr >= Literal("a") && randStr <= Literal("z"))).as("col")))

    // Same for a nondeterministic expression that is not the root: `rand() / a` reads row data.
    val divide = Divide(rand, a.cast(DoubleType))
    val plan4 = testRelation.select(
      CaseWhen(Seq((a > 0, Literal(true))), Some(between(divide, Literal(0.4), Literal(0.6))))
        .as("col"))
    comparePlans(
      Optimizer.execute(plan4),
      testRelation.select(
        CaseWhen(
          Seq((a > 0, Literal(true))),
          Some(divide >= Literal(0.4) && divide <= Literal(0.6))).as("col")))

    // A single reference evaluates once either way, so there is nothing to fix and the branch keeps
    // deciding whether `rand()` runs at all.
    val plan5 = testRelation.select(
      CaseWhen(
        Seq((a > 0, Literal(0.0d))),
        Some(With(rand) { case Seq(ref) => ref + Literal(1.0d) })).as("col"))
    comparePlans(
      Optimizer.execute(plan5),
      testRelation.select(
        CaseWhen(Seq((a > 0, Literal(0.0d))), Some(rand + Literal(1.0d))).as("col")))

    // Each definition of one `With` is decided on its own: `rand()` is pre-evaluated while its
    // `randstr` sibling, which can raise, keeps its inlining rather than being dragged along.
    val plan6 = testRelation.select(
      CaseWhen(
        Seq((a > 0, Literal(true))),
        Some(With(rand, randStr) { case Seq(r1, r2) =>
          (r1 >= Literal(0.4) && r1 <= Literal(0.6)) && (r2 >= Literal("a") && r2 <= Literal("z"))
        })).as("col"))
    comparePlans(
      Optimizer.execute(plan6),
      testRelation
        .select((testRelation.output :+
          If(notTrue(a > 0), rand, Literal(0.0d)).as("_common_expr_0")): _*)
        .select(
          CaseWhen(
            Seq((a > 0, Literal(true))),
            Some(($"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6)) &&
              (randStr >= Literal("a") && randStr <= Literal("z")))).as("col"))
        .analyze)
  }

  test("SPARK-58818: the guard on a pre-evaluated definition tracks the branch reached") {
    val a = testRelation.output.head
    val rand = Rand(Literal(1L))
    def between(input: Expression, lower: Expression, upper: Expression): Expression =
      With(input) { case Seq(ref) => ref >= lower && ref <= upper }
    def guarded(guard: Expression): NamedExpression =
      If(guard, rand, Literal(0.0d)).as("_common_expr_0")

    // A later branch is only reached when no preceding condition held, so its guard accumulates
    // them.
    val b = testRelation.output.last
    val plan = testRelation.select(
      CaseWhen(
        Seq(
          (a > 0, Literal(true)),
          (b > 0, between(rand, Literal(0.4), Literal(0.6)))),
        Some(Literal(false))).as("col"))
    comparePlans(
      Optimizer.execute(plan),
      testRelation
        .select((testRelation.output :+
          guarded(notTrue(a > 0) && isTrue(b > 0))): _*)
        .select(
          CaseWhen(
            Seq(
              (a > 0, Literal(true)),
              (b > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false))).as("col"))
        .analyze)

    // A `With` in a condition is reached when no preceding condition held, and does not depend on
    // its own condition.
    val plan2 = testRelation.select(
      CaseWhen(
        Seq(
          (a > 0, Literal(true)),
          (between(rand, Literal(0.4), Literal(0.6)), Literal(true))),
        Some(Literal(false))).as("col"))
    comparePlans(
      Optimizer.execute(plan2),
      testRelation
        .select((testRelation.output :+ guarded(notTrue(a > 0))): _*)
        .select(
          CaseWhen(
            Seq(
              (a > 0, Literal(true)),
              ($"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6),
                Literal(true))),
            Some(Literal(false))).as("col"))
        .analyze)

    // `If` guards each of its two branches with the predicate and its negation.
    val plan3 = testRelation.select(
      If(a > 0, between(rand, Literal(0.4), Literal(0.6)), Literal(false)).as("col"))
    comparePlans(
      Optimizer.execute(plan3),
      testRelation
        .select((testRelation.output :+ guarded(isTrue(a > 0))): _*)
        .select(
          If(
            a > 0,
            $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6),
            Literal(false)).as("col"))
        .analyze)

    // `Coalesce` reaches a child only when every preceding child returned null.
    val pid = SparkPartitionID()
    val plan4 = testRelation.select(
      Coalesce(Seq(b, With(pid) { case Seq(ref) => ref + ref })).as("col"))
    comparePlans(
      Optimizer.execute(plan4),
      testRelation
        .select((testRelation.output :+
          If(IsNull(b), pid, Literal(0)).as("_common_expr_0")): _*)
        .select(Coalesce(Seq(b, $"_common_expr_0" + $"_common_expr_0")).as("col"))
        .analyze)

    // A nondeterministic preceding condition cannot be repeated as a guard: evaluating it a second
    // time in the project would draw its own value and select a different set of rows than the
    // branch does. The definition is still pre-evaluated, just without a guard, since inlining it
    // would hand each reference its own value -- the bug this pre-evaluation exists to fix.
    val plan5 = testRelation.select(
      CaseWhen(
        Seq(
          (Rand(Literal(2L)) > Literal(0.5d), Literal(true)),
          (b > 0, between(rand, Literal(0.4), Literal(0.6)))),
        Some(Literal(false))).as("col"))
    comparePlans(
      Optimizer.execute(plan5),
      testRelation
        .select((testRelation.output :+ rand.as("_common_expr_0")): _*)
        .select(
          CaseWhen(
            Seq(
              (Rand(Literal(2L)) > Literal(0.5d), Literal(true)),
              (b > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false))).as("col"))
        .analyze)

    // Arithmetic can raise, so a condition carrying it is not repeated as a guard: the project
    // evaluates the guard on every row, and subexpression elimination can hoist a repeated part of
    // it out of the `If`, so it would raise on rows the branch never reached. The definition is
    // still pre-evaluated, just without a guard.
    val plan6 = testRelation.select(
      CaseWhen(
        Seq(
          (between(a + a, Literal(1), Literal(10)), Literal(true)),
          (b > 0, between(rand, Literal(0.4), Literal(0.6)))),
        Some(Literal(false))).as("col"))
    val hoistedCond = ($"_common_expr_1" >= Literal(1)) && ($"_common_expr_1" <= Literal(10))
    comparePlans(
      Optimizer.execute(plan6),
      testRelation
        .select((testRelation.output ++ Seq(
          (a + a).as("_common_expr_1"),
          rand.as("_common_expr_0"))): _*)
        .select(
          CaseWhen(
            Seq(
              (hoistedCond, Literal(true)),
              (b > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false))).as("col"))
        .analyze)

    // A condition that reads no row data is measured by its value, so the cast an implicit coercion
    // puts around a literal -- not a node that can be repeated as a guard -- does not cost the
    // guard. `a > 0L` in SQL arrives here as this. The guard itself keeps the condition as written,
    // for `ConstantFolding` to fold along with the rest of the plan.
    val foldableCast =
      GreaterThan(a, Cast(Literal(0L), IntegerType, Some(conf.sessionLocalTimeZone)))
    val plan7 = testRelation.select(
      CaseWhen(
        Seq(
          (foldableCast, Literal(true)),
          (b > 0, between(rand, Literal(0.4), Literal(0.6)))),
        Some(Literal(false))).as("col"))
    comparePlans(
      Optimizer.execute(plan7),
      testRelation
        .select((testRelation.output :+
          guarded(notTrue(foldableCast) && isTrue(b > 0))): _*)
        .select(
          CaseWhen(
            Seq(
              (foldableCast, Literal(true)),
              (b > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false))).as("col"))
        .analyze)

    // A subtree that raises rather than returning a value is left as it is, so it is still a cast
    // when the condition is measured, and the guard is given up. Under ANSI it would otherwise
    // raise CAST_INVALID_INPUT on every row of the project.
    val raisingCast = GreaterThan(
      a, Cast(Literal("x"), IntegerType, Some(conf.sessionLocalTimeZone), EvalMode.ANSI))
    val plan8 = testRelation.select(
      CaseWhen(
        Seq(
          (raisingCast, Literal(true)),
          (b > 0, between(rand, Literal(0.4), Literal(0.6)))),
        Some(Literal(false))).as("col"))
    comparePlans(
      Optimizer.execute(plan8),
      testRelation
        .select((testRelation.output :+ rand.as("_common_expr_0")): _*)
        .select(
          CaseWhen(
            Seq(
              (raisingCast, Literal(true)),
              (b > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false))).as("col"))
        .analyze)

    // A node can report itself foldable while ignoring its children: `typeof` answers from the
    // child's type, so `typeof(a + a) = 'int'` and every comparison against it are foldable even
    // though the addition is not. Measuring the condition by its value would delete the addition
    // from what is measured while leaving it in the guard that runs, so a subtree is only measured
    // by its value when it reads no row data.
    val foldableOverRowData = EqualTo(TypeOf(a + a), Literal("int"))
    val plan9 = testRelation.select(
      CaseWhen(
        Seq(
          (foldableOverRowData, Literal(true)),
          (b > 0, between(rand, Literal(0.4), Literal(0.6)))),
        Some(Literal(false))).as("col"))
    assert(foldableOverRowData.foldable, "the condition must be foldable to test anything")
    comparePlans(
      Optimizer.execute(plan9),
      testRelation
        .select((testRelation.output :+ rand.as("_common_expr_0")): _*)
        .select(
          CaseWhen(
            Seq(
              (foldableOverRowData, Literal(true)),
              (b > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false))).as("col"))
        .analyze)

    // The guard comes from the conditions as they were before this pass rewrote them: the rewritten
    // first condition references a column this pass is still adding, which a `Project` cannot read
    // from its own project list. Here the definition it hoists is a column comparison, which cannot
    // raise, so inlining it into the guard keeps the guard available.
    val nonCheapSafeCond = With(a > 0) { case Seq(ref) => ref || ref }
    val plan10 = testRelation.select(
      CaseWhen(
        Seq(
          (nonCheapSafeCond, Literal(true)),
          (b > 0, between(rand, Literal(0.4), Literal(0.6)))),
        Some(Literal(false))).as("col"))
    val hoistedOr = $"_common_expr_1" || $"_common_expr_1"
    val inlinedOr = (a > 0) || (a > 0)
    comparePlans(
      Optimizer.execute(plan10),
      testRelation
        .select((testRelation.output ++ Seq(
          (a > 0).as("_common_expr_1"),
          If(notTrue(inlinedOr) && isTrue(b > 0), rand, Literal(0.0d)).as("_common_expr_0"))): _*)
        .select(
          CaseWhen(
            Seq(
              (hoistedOr, Literal(true)),
              (b > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false))).as("col"))
        .analyze)

    // `NaNvl` reaches its right child only when the left one is NaN. `IsNaN` is false for a null
    // input, so it is the whole guard.
    val d = doubleRelation.output.head
    val plan11 = doubleRelation.select(
      NaNvl(d, With(rand) { case Seq(ref) => ref + ref }).as("col"))
    comparePlans(
      Optimizer.execute(plan11),
      doubleRelation
        .select((doubleRelation.output :+ guarded(IsNaN(d))): _*)
        .select(NaNvl(d, $"_common_expr_0" + $"_common_expr_0").as("col"))
        .analyze)

    // A guard has to be evaluated by the same input plan as the definition. In a join condition
    // where the guard reads one side, the column goes to that side rather than losing the guard.
    val x = testRelation2.output.head
    val condition = CaseWhen(
      Seq((x > 0, between(rand, Literal(0.4), Literal(0.6)))), Some(Literal(false)))
    comparePlans(
      Optimizer.execute(testRelation.join(testRelation2, condition = Some(condition))),
      testRelation
        .join(
          testRelation2.select((testRelation2.output :+ guarded(isTrue(x > 0))): _*),
          condition = Some(CaseWhen(
            Seq((x > 0, $"_common_expr_0" >= Literal(0.4) && $"_common_expr_0" <= Literal(0.6))),
            Some(Literal(false)))))
        .select((testRelation.output ++ testRelation2.output): _*)
        .analyze)
  }

  test("SPARK-58818: a guarded pre-evaluated column is not read unconditionally") {
    val a = testRelation.output.head
    // One definition shared by a `With` inside a branch and a `With` outside it, so the same id
    // reaches both the branch path and the main rewrite.
    val exprDef = CommonExpressionDef(Rand(Literal(1L)))
    val ref = new CommonExpressionRef(exprDef)
    val inBranch = With(ref >= Literal(0.4d) && ref <= Literal(0.6d), Seq(exprDef))
    val unconditional = With(ref + ref, Seq(exprDef))

    // The branch gets a guarded column and the unconditional reference an unguarded one. Reading
    // the guarded column here would give the default value on the rows taking the other branch.
    val plan = testRelation.select(
      CaseWhen(Seq((a > 0, Literal(true))), Some(inBranch)).as("c1"),
      unconditional.as("c2"))
    val rewritten = Optimizer.execute(plan)
    val columns = rewritten.children.head.expressions.collect {
      case alias: Alias if alias.name.startsWith("_common_expr") => alias
    }
    assert(columns.length == 2)
    val guardedColumn = columns.filter(_.child.isInstanceOf[If])
    val plainColumn = columns.filterNot(_.child.isInstanceOf[If])
    assert(guardedColumn.length == 1 && plainColumn.length == 1)
    val Seq(c1, c2) = rewritten.expressions
    assert(c1.references.contains(guardedColumn.head.toAttribute))
    assert(c2.references == AttributeSet(plainColumn.head.toAttribute))

    // Two branches under the same guard share one column; under different guards they get one each,
    // since a column computed for one branch holds its default value on the other's rows.
    val sameGuard = testRelation.select(
      CaseWhen(Seq((a > 0, inBranch)), Some(Literal(false))).as("c1"),
      CaseWhen(Seq((a > 0, inBranch)), Some(Literal(false))).as("c2"))
    assert(countCommonExprColumns(Optimizer.execute(sameGuard)) == 1)
    val differentGuards = testRelation.select(
      CaseWhen(Seq((a > 0, inBranch)), Some(inBranch)).as("c1"))
    assert(countCommonExprColumns(Optimizer.execute(differentGuards)) == 2)

    // The branch rewrite also adds unguarded columns, when the branch's conditions cannot be
    // repeated as a guard. The main rewrite reads those, so the two paths have to agree on what the
    // column holds even though `semanticEquals` is false for a nondeterministic definition.
    val nondetCondition = testRelation.select(
      If(Rand(Literal(2L)) > Literal(0.5d), inBranch, Literal(false)).as("c1"),
      unconditional.as("c2"))
    assert(countCommonExprColumns(Optimizer.execute(nondetCondition)) == 1)
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

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

import java.time.Instant

import org.apache.spark.SparkException
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, EmptyTableFunctionRegistry, FakeV2SessionCatalog, TempResolvedColumn}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.DefaultCatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, TimestampNTZType, TimestampType, TimeType}

class RewriteWithExpressionSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Rewrite With expression", FixedPoint(5),
      PullOutGroupingExpressions,
      RewriteWithExpression) :: Nil
  }

  private val testRelation = LocalRelation($"a".int, $"b".int)
  private val testRelation2 = LocalRelation($"x".int, $"y".int)

  private val catalogManager = new DefaultCatalogManager(
    FakeV2SessionCatalog,
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, EmptyTableFunctionRegistry))

  /**
   * Runs the expression-level rewrites in the same order as the optimizer's `FinishAnalysis`
   * batch, then inlines the `With`.
   */
  private def finishAnalysisAndInline(expr: Expression, instant: Instant): Expression = {
    val afterReplace = ReplaceExpressions.replace(expr)
    val afterCurrentTime = ComputeCurrentTime.applyForExpression(afterReplace, instant)
    val afterCurrentLike = ReplaceCurrentLike(catalogManager).applyForExpression(afterCurrentTime)
    RewriteWithExpression.applyForExpression(
      SpecialDatetimeValues.applyForExpression(afterCurrentLike))
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

  test("applyForExpression inlines a literal referenced more than once") {
    val expr = With(Literal(1)) { case Seq(ref) =>
      ref + ref
    }
    val rewritten = RewriteWithExpression.applyForExpression(expr)
    assert(!rewritten.isInstanceOf[With])
    assert(rewritten == Literal(1) + Literal(1))
  }

  test("applyForExpression inlines a non-literal referenced at most once") {
    // Inlining does not duplicate the definition here, so it is safe whatever the definition is.
    val a = testRelation.output.head
    val referencedOnce = With(a + a) { case Seq(ref) =>
      ref * Literal(2)
    }
    assert(RewriteWithExpression.applyForExpression(referencedOnce) == (a + a) * Literal(2))
    val neverReferenced = With(a + a) { case Seq(_) =>
      Literal(3)
    }
    assert(RewriteWithExpression.applyForExpression(neverReferenced) == Literal(3))
  }

  test("applyForExpression rejects a non-literal referenced more than once") {
    // Inlining would duplicate `a + a` at every reference, violating the evaluate-once contract.
    val a = testRelation.output.head
    val expr = With(a + a) { case Seq(ref) =>
      ref * ref
    }
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(expr)
    }
  }

  test("applyForExpression inlines a tree of literals referenced more than once") {
    val expr = With(Literal(1) + Literal(1)) { case Seq(ref) =>
      ref * ref
    }
    val rewritten = RewriteWithExpression.applyForExpression(expr)
    assert(rewritten == (Literal(1) + Literal(1)) * (Literal(1) + Literal(1)))
  }

  test("applyForExpression rejects an impure foldable definition referenced more than once") {
    // aes_encrypt becomes a foldable StaticInvoke that draws a fresh random IV on every eval, so
    // two inlined copies would encrypt to different values.
    val aes = ReplaceExpressions.replace(
      new AesEncrypt(Literal("abc".getBytes), Literal("1234567890123456".getBytes)))
    assert(aes.foldable, "the AES rewrite is only interesting while it stays foldable")
    val expr = With(aes) { case Seq(ref) =>
      EqualTo(ref, ref)
    }
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(expr)
    }
  }

  test("applyForExpression rejects canonicalized common expression ids") {
    // Canonicalization re-numbers ids per `With`, starting from 1, so these two siblings both get
    // id 1: the safe (literal) definition would otherwise mark id 1 safe in the flat `safeIds` set
    // and authorize duplicating the unsafe (per-row attribute) definition with the same id.
    val a = testRelation.output.head
    val safe = With(Literal(1)) { case Seq(ref) => ref + ref }
    val unsafe = With(a) { case Seq(ref) => ref + ref }
    val expr = (safe + unsafe).canonicalized
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(expr)
    }
  }

  test("applyForExpression rejects current_time referenced more than once") {
    // CurrentTime is not a leaf and its only leaf is a literal precision, so the structural check
    // alone would accept it.
    val expr = With(CurrentTime()) { case Seq(ref) =>
      EqualTo(ref, ref)
    }
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(expr)
    }
  }

  test("applyForExpression rejects a TIME -> TIMESTAMP cast referenced more than once") {
    // The cast fills its date fields from the current date, so it is not safe to duplicate even
    // though its only leaf is a literal.
    val expr = With(Cast(Literal(0L, TimeType(6)), TimestampNTZType, Some("UTC"))) {
      case Seq(ref) => EqualTo(ref, ref)
    }
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(expr)
    }
  }

  test("applyForExpression rejects a TIME -> TIMESTAMP_LTZ cast referenced more than once") {
    // Same TimestampType target as the inlined string cast below; only the TIME source makes it
    // unsafe, so the node-level check must key on the source type, not the target.
    val expr = With(Cast(Literal(0L, TimeType(6)), TimestampType, Some("UTC"))) {
      case Seq(ref) => EqualTo(ref, ref)
    }
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(expr)
    }
  }

  test("applyForExpression inlines a non-TIME timestamp-target cast referenced more than once") {
    // A string -> TIMESTAMP cast carries the CAST_TO_TIMESTAMP tree pattern (keyed on target type
    // alone) but is an ordinary deterministic cast, so it must inline rather than be rejected.
    val cast = Cast(Literal("1970-01-01"), TimestampType, Some("UTC"))
    val expr = With(cast) { case Seq(ref) =>
      EqualTo(ref, ref)
    }
    assert(RewriteWithExpression.applyForExpression(expr) == EqualTo(cast, cast))
  }

  test("applyForExpression inlines an inner def referencing a foldable outer def") {
    val outer = With(Literal(1)) { case Seq(outerRef) =>
      With(outerRef + Literal(1)) { case Seq(innerRef) =>
        innerRef + innerRef
      }
    }
    val rewritten = RewriteWithExpression.applyForExpression(outer)
    assert(!rewritten.exists(_.isInstanceOf[With]))
    assert(!rewritten.exists(_.isInstanceOf[CommonExpressionRef]))
    val inlinedInner = Literal(1) + Literal(1)
    assert(rewritten == inlinedInner + inlinedInner)
  }

  test("applyForExpression rejects an inner def referencing a per-row outer def") {
    // A ref is only safe to duplicate when its definition is: here the outer definition is an
    // attribute, so duplicating `a + 1` would evaluate it once per reference.
    val a = testRelation.output.head
    val outer = With(a) { case Seq(outerRef) =>
      With(outerRef + Literal(1)) { case Seq(innerRef) =>
        innerRef + innerRef
      }
    }
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(outer)
    }
  }

  test("applyForExpression handles nested With") {
    val inner = With(Literal(1)) { case Seq(ref) =>
      ref * ref
    }
    val outer = With(inner + Literal(2)) { case Seq(ref) =>
      ref + ref
    }
    val rewritten = RewriteWithExpression.applyForExpression(outer)
    assert(!rewritten.exists(_.isInstanceOf[With]))
    assert(!rewritten.exists(_.isInstanceOf[CommonExpressionRef]))
    val inlinedInner = Literal(1) * Literal(1) + Literal(2)
    assert(rewritten == inlinedInner + inlinedInner)
  }

  test("applyForExpression leaves expressions without With unchanged") {
    val a = testRelation.output.head
    assert(RewriteWithExpression.applyForExpression(a + a) == a + a)
  }

  test("applyForExpression defers a ref to an outer common expression to the enclosing With") {
    // The inner `With` is rewritten first, so the outer ref it references is not yet in scope
    // and must be left for the enclosing `With` to inline.
    val outer = With(Literal(1)) { case Seq(outerRef) =>
      With(Literal(2)) { case Seq(innerRef) =>
        outerRef + innerRef
      }
    }
    val rewritten = RewriteWithExpression.applyForExpression(outer)
    assert(!rewritten.exists(_.isInstanceOf[With]))
    assert(!rewritten.exists(_.isInstanceOf[CommonExpressionRef]))
    assert(rewritten == Literal(1) + Literal(2))
  }

  test("applyForExpression rejects current_timestamp that ComputeCurrentTime has not folded") {
    // current_timestamp() is foldable, but each eval re-reads the clock, so inlining it at two
    // references could produce two different values.
    val expr = With(CurrentTimestamp()) { case Seq(ref) =>
      EqualTo(ref, ref)
    }
    intercept[SparkException] {
      RewriteWithExpression.applyForExpression(expr)
    }
  }

  test("applyForExpression inlines current_timestamp folded to one shared literal") {
    val instant = Instant.now()
    val expr = With(CurrentTimestamp()) { case Seq(ref) =>
      EqualTo(ref, ref)
    }
    val expected = Literal.create(DateTimeUtils.instantToMicros(instant), TimestampType)
    assert(finishAnalysisAndInline(expr, instant) == EqualTo(expected, expected))
  }

  test("applyForExpression inlines current_database folded by ReplaceCurrentLike") {
    val expr = With(CurrentDatabase()) { case Seq(ref) =>
      EqualTo(ref, ref)
    }
    val expected = Literal.create(catalogManager.currentNamespace.quoted, StringType)
    assert(finishAnalysisAndInline(expr, Instant.now()) == EqualTo(expected, expected))
  }

  test("applyForExpression inlines a special datetime value folded by SpecialDatetimeValues") {
    val expr = With(Cast(Literal("epoch"), DateType, Some("UTC"))) { case Seq(ref) =>
      EqualTo(ref, ref)
    }
    val expected = Literal(0, DateType)
    assert(finishAnalysisAndInline(expr, Instant.now()) == EqualTo(expected, expected))
  }

  test("applyForExpression inlines a TIME -> TIMESTAMP cast stabilized by ComputeCurrentTime") {
    // ComputeCurrentTime rewrites the cast to a pure makeTimestamp* builder StaticInvoke anchored
    // on a query-stable current-date literal. Even though ConstantFolding (not part of this
    // pipeline) has not folded it to a single literal, inlining it at both references is safe.
    val expr = With(Cast(Literal(0L, TimeType(6)), TimestampNTZType, Some("UTC"))) {
      case Seq(ref) => EqualTo(ref, ref)
    }
    val rewritten = finishAnalysisAndInline(expr, Instant.now())
    assert(!rewritten.exists(_.isInstanceOf[With]))
    val EqualTo(left, right) = rewritten: @unchecked
    assert(ComputeCurrentTime.isMakeTimestampBuilder(left))
    assert(left == right)
  }

  test("applyForExpression inlines a stabilized TIME -> TIMESTAMP_LTZ cast") {
    // The LTZ target reaches isMakeTimestampBuilder through the same gate as the NTZ case above.
    val expr = With(Cast(Literal(0L, TimeType(6)), TimestampType, Some("UTC"))) {
      case Seq(ref) => EqualTo(ref, ref)
    }
    val rewritten = finishAnalysisAndInline(expr, Instant.now())
    assert(!rewritten.exists(_.isInstanceOf[With]))
    val EqualTo(left, right) = rewritten: @unchecked
    assert(ComputeCurrentTime.isMakeTimestampBuilder(left))
    assert(left == right)
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

  test("SPARK-58902: a With left in a conditional branch of an aggregate still converges") {
    val Seq(a, b) = testRelation.output
    // Not cheap and referenced twice, so it stays a memoizing `With` rather than being inlined.
    val inBranch = With(a + b) { case Seq(ref) => ref * ref }
    val plan = testRelation.groupBy(a)(max(Coalesce(Seq(a, inBranch))).as("col"))
    // The `PhysicalAggregation` arm restructures the aggregate into a `Project` above it, and its
    // guard is "the expressions contain a `With`", which a surviving one keeps true on every
    // iteration of this fixed-point batch. Without the eq check in the rule this raises
    // `Max iterations (5) reached for batch Rewrite With expression`, one `Project` per iteration.
    val rewritten = Optimizer.execute(plan)
    // Idempotent: running the batch again changes nothing.
    comparePlans(Optimizer.execute(rewritten), rewritten)
    assert(rewritten.collect { case p: Project => p }.size <= 1,
      s"the rule stacked a Project per iteration:\n$rewritten")
  }

  test("SPARK-58902: a cheap or single-reference definition in a branch is still inlined") {
    val Seq(a, b) = testRelation.output
    // A bare attribute is cheap, so inlining it costs nothing.
    val cheap = With(a) { case Seq(ref) => ref * ref }
    comparePlans(
      Optimizer.execute(testRelation.select(Coalesce(Seq(b, cheap)).as("col"))),
      testRelation.select(Coalesce(Seq(b, a * a)).as("col")))

    // Referenced once, so memoizing it would save nothing.
    val singleRef = With(a + b) { case Seq(ref) => ref * Literal(2) }
    comparePlans(
      Optimizer.execute(testRelation.select(Coalesce(Seq(b, singleRef)).as("col"))),
      testRelation.select(Coalesce(Seq(b, (a + b) * Literal(2))).as("col")))

    // Expensive and referenced twice: this is the one worth a `With`.
    val kept = With(a + b) { case Seq(ref) => ref * ref }
    val keptPlan = testRelation.select(Coalesce(Seq(b, kept)).as("col"))
    comparePlans(Optimizer.execute(keptPlan), keptPlan)
  }

  test("SPARK-58902: an inner substitution that duplicates an outer reference is counted") {
    val Seq(a, b) = testRelation.output
    def udf(e: Expression, deterministic: Boolean): PythonUDF =
      PythonUDF("udf", null, IntegerType, Seq(e), PythonEvalType.SQL_BATCHED_UDF, deterministic)

    // With this conf off, `CollapseProject.isCheap` calls any `PythonUDF` cheap whatever its
    // children are, which is what lets an inner definition carrying an outer reference be
    // substituted at both of its references. Counting the outer reference before that happens says
    // once, and the branch path runs bottom-up, so a plan-wide count taken up front would inline
    // the nondeterministic outer definition into both copies -- two draws where there must be one.
    withSQLConf(SQLConf.AVOID_COLLAPSE_UDF_WITH_EXPENSIVE_EXPR.key -> "false") {
      val outerDef = CommonExpressionDef(udf(a, deterministic = false))
      val outerRef = new CommonExpressionRef(outerDef)
      val innerDef = CommonExpressionDef(udf(outerRef, deterministic = true))
      val innerRef = new CommonExpressionRef(innerDef)
      val inner = With(Add(innerRef, innerRef), Seq(innerDef))
      val outer = With(inner, Seq(outerDef))
      val rewritten = Optimizer.execute(testRelation.select(Coalesce(Seq(b, outer)).as("col")))

      val nondet = rewritten.expressions.flatMap(_.collect {
        case u: PythonUDF if !u.udfDeterministic => u
      })
      assert(nondet.length == 1, s"the nondeterministic definition was inlined twice:\n$rewritten")
      // It stayed memoized: the surviving `With` still defines it, and both reads go through a
      // reference to it.
      val withs = rewritten.expressions.flatMap(_.collect { case w: With => w })
      assert(withs.length == 1, s"expected one surviving With:\n$rewritten")
      assert(withs.head.defs.map(_.child) == nondet, s"the wrong definition survived:\n$rewritten")
      assert(withs.head.child.collect { case r: CommonExpressionRef => r }.length == 2,
        s"expected two references to the surviving definition:\n$rewritten")
    }
  }

  test("SPARK-58902: a reference inside a sibling definition keeps that definition memoized") {
    val Seq(a, b) = testRelation.output
    def udf(e: Expression, deterministic: Boolean): PythonUDF =
      PythonUDF("udf", null, IntegerType, Seq(e), PythonEvalType.SQL_BATCHED_UDF, deterministic)

    // The reference to the first definition sits inside the second one, once. Substituting the
    // second definition at both of its references would duplicate it, so counting it as read once
    // would inline the nondeterministic first definition into both copies. Nothing builds this
    // shape -- the helper puts references only in `child` -- but the rewrite is what would have to
    // survive it, and it cannot fall back on evaluation failing.
    withSQLConf(SQLConf.AVOID_COLLAPSE_UDF_WITH_EXPENSIVE_EXPR.key -> "false") {
      val firstDef = CommonExpressionDef(udf(a, deterministic = false))
      val firstRef = new CommonExpressionRef(firstDef)
      val secondDef = CommonExpressionDef(udf(firstRef, deterministic = true))
      val secondRef = new CommonExpressionRef(secondDef)
      val w = With(Add(secondRef, secondRef), Seq(firstDef, secondDef))
      val rewritten = Optimizer.execute(testRelation.select(Coalesce(Seq(b, w)).as("col")))

      val nondet = rewritten.expressions.flatMap(_.collect {
        case u: PythonUDF if !u.udfDeterministic => u
      })
      assert(nondet.length == 1,
        s"the nondeterministic definition was inlined more than once:\n$rewritten")
    }
  }

  test("SPARK-58902: a branch-local With keeps only the definitions worth memoizing") {
    val Seq(a, b) = testRelation.output
    // Two definitions in one `With`, one of each kind, so the rule has to rebuild the `With` around
    // what is left rather than inline all of them or keep all of them. The other branch-local tests
    // each use a single definition, which only exercises those two ends.
    val expr = With(a, a + b) { case Seq(cheap, expensive) =>
      Add(cheap * cheap, expensive * expensive)
    }
    val plan = testRelation.select(Coalesce(Seq(b, expr)).as("col"))
    val rewritten = Optimizer.execute(plan)

    val withs = rewritten.expressions.flatMap(_.collect { case w: With => w })
    assert(withs.length == 1, s"expected one surviving With:\n$rewritten")
    val kept = withs.head
    assert(kept.defs.map(_.child) == Seq(a + b), s"the wrong definition was kept:\n$rewritten")
    // The inlined definition's id is gone from the tree, not left as a reference nobody binds.
    val keptIds = kept.defs.map(_.id).toSet
    val refIds = rewritten.expressions.flatMap(_.collect { case r: CommonExpressionRef => r.id })
    assert(refIds.nonEmpty && refIds.forall(keptIds.contains),
      s"a reference to an inlined definition survived:\n$rewritten")
    // The cheap definition was substituted at both of its references, not just the first.
    assert(kept.child.collectFirst { case Add(l, _, _) => l }.contains(a * a),
      s"the cheap definition was not inlined at every reference:\n$rewritten")
    // The rebuilt `With` binds the references it carried over from the one it replaced, which is
    // what the value proves: a = 2 inlined twice, a + b = 5 memoized and squared.
    val bound = BindReferences.bindReference(
      rewritten.expressions.head.children.head.asInstanceOf[Coalesce].children.last,
      testRelation.output)
    assert(bound.eval(InternalRow(2, 3)) == 2 * 2 + 5 * 5)
    // Running the batch again changes nothing: the rebuilt `With` is a fixed point.
    comparePlans(Optimizer.execute(rewritten), rewritten)
  }

  test("SPARK-58902: a cheap definition is only inlined if it is also deterministic") {
    val Seq(a, b) = testRelation.output
    def udf(e: Expression, deterministic: Boolean): PythonUDF =
      PythonUDF("udf", null, IntegerType, Seq(e), PythonEvalType.SQL_BATCHED_UDF, deterministic)

    // `CollapseProject.isCheap` admits a `PythonUDF` whose arguments are cheap, but it answers what
    // one evaluation costs, not whether a second one would agree with the first. A nondeterministic
    // one referenced twice has to stay memoized.
    val nondet = With(udf(a, deterministic = false)) { case Seq(ref) => ref * ref }
    val nondetPlan = testRelation.select(Coalesce(Seq(b, nondet)).as("col"))
    comparePlans(Optimizer.execute(nondetPlan), nondetPlan)

    // The deterministic one is inlined, as cheapness alone would have it.
    val det = With(udf(a, deterministic = true)) { case Seq(ref) => ref * ref }
    comparePlans(
      Optimizer.execute(testRelation.select(Coalesce(Seq(b, det)).as("col"))),
      testRelation.select(
        Coalesce(Seq(b, udf(a, deterministic = true) * udf(a, deterministic = true))).as("col")))

    // The same conjunct governs the main rewrite path, which has no `Coalesce` above it: there the
    // definition is hoisted into a child `Project` instead of being substituted. Nothing in this
    // batch sends it back -- `PlanHelper.specialExpressionsInUnsupportedOperator` collects only
    // window, aggregate and generator expressions, so the `fakeProj` check does not force the
    // substitution. What keeps `CollapseProject` from copying the alias back into its two consumers
    // is that it requires a deterministic producer, which this suite's batch does not exercise.
    val nondetMainPath = With(udf(a, deterministic = false)) { case Seq(ref) => ref * ref }
    comparePlans(
      Optimizer.execute(testRelation.select(nondetMainPath.as("col"))),
      testRelation
        .select((testRelation.output :+ udf(a, deterministic = false).as("_common_expr_0")): _*)
        .select(($"_common_expr_0" * $"_common_expr_0").as("col"))
        .analyze)
  }

  test("WITH expression inside conditional expression") {
    val a = testRelation.output.head
    val inBranch = With(a + a) { case Seq(ref) => ref * ref }
    val expr = Coalesce(Seq(a, inBranch))
    val plan = testRelation.select(expr.as("col"))
    // A `With` in a conditional branch is left where it is: it cannot go into a project, which is
    // always evaluated, and it memoizes its definition per row itself, so the definition is still
    // evaluated once rather than once per reference.
    comparePlans(Optimizer.execute(plan), testRelation.select(expr.as("col")))

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

  test("WITH in a conditional branch referencing an outer common expression") {
    val a = testRelation.output.head
    // The conditional branch holds a nested `With` referencing the outer common expression.
    // Both are inlined; the outer ref must survive the inner rewrite.
    val expr = With(a + a) { case Seq(outerRef) =>
      Coalesce(Seq(a, With(a * a) { case Seq(innerRef) =>
        outerRef + innerRef
      }))
    }
    val plan = testRelation.select(expr.as("col"))
    val inlinedExpr = Coalesce(Seq(a, (a + a) + (a * a)))
    comparePlans(Optimizer.execute(plan), testRelation.select(inlinedExpr.as("col")))
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

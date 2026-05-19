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

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{Decimal, DecimalType, DoubleType, LongType}

class DecimalAggregatesSuite extends PlanTest with ScalaCheckDrivenPropertyChecks {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) :: Nil
  }

  val testRelation = LocalRelation($"a".decimal(2, 1), $"b".decimal(12, 1))

  test("Decimal Sum Aggregation: Optimized") {
    val originalQuery = testRelation.select(sum($"a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select(MakeDecimal(sum(UnscaledValue($"a")), 12, 1).as("sum(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(sum($"b"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Optimized") {
    val originalQuery = testRelation.select(avg($"a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select((avg(UnscaledValue($"a")) / 10.0).cast(DecimalType(6, 5)).as("avg(a)")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation: Not Optimized") {
    val originalQuery = testRelation.select(avg($"b"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  val testRelationC = LocalRelation($"c".decimal(7, 2))

  test("Decimal Average Aggregation widened-cast peel: Optimized (p=7, p'=12)") {
    val widened = $"c".cast(DecimalType(12, 2))
    val originalQuery = testRelationC.select(avg(widened))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelationC
      .select((avg(UnscaledValue($"c")) / 100.0).cast(DecimalType(16, 6))
        .as("avg(CAST(c AS DECIMAL(12,2)))")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation widened-cast peel: Not Optimized (narrowing cast)") {
    val testRelationD = LocalRelation($"d".decimal(10, 2))
    val narrowed = $"d".cast(DecimalType(8, 2))
    val originalQuery = testRelationD.select(avg(narrowed))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelationD
      .select((avg(UnscaledValue(narrowed)) / 100.0).cast(DecimalType(12, 6))
        .as("avg(CAST(d AS DECIMAL(8,2)))")).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation widened-cast peel: Not Optimized (scale change)") {
    val testRelationD = LocalRelation($"d".decimal(7, 2))
    val rescaled = $"d".cast(DecimalType(12, 4))
    val originalQuery = testRelationD.select(avg(rescaled))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation widened-cast peel: Not Optimized (boundary p=8)") {
    val testRelationE = LocalRelation($"e".decimal(8, 2))
    val widened = $"e".cast(DecimalType(13, 2))
    val originalQuery = testRelationE.select(avg(widened))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  // SPARK-56627 F2 regression: with `pPrime in [8, 11]`, the outer Cast's
  // dataType `Decimal(pPrime, s)` would let the un-widened existing
  // `Average(DecimalExpression)` arm match first via `prec + 4 <= MAX_DOUBLE_DIGITS`
  // (= pPrime <= 11). New AVG peel arm must be ordered before to win this band
  // and rewrite via the inner `p`-based UnscaledValue path.
  test("Decimal Average Aggregation widened-cast peel: " +
      "Optimized for pPrime band [8, 11] (must beat existing AVG fast-path arm)") {
    val testRelationE = LocalRelation($"e".decimal(7, 2))
    val widened = $"e".cast(DecimalType(10, 2))
    val originalQuery = testRelationE.select(avg(widened).as("avg_widened"))
    val optimized = Optimize.execute(originalQuery.analyze)
    // Expected: peeled via WidenedDecimalChild(inner=e, p=7, pPrime=10, s=2),
    // outer Cast bounded(pPrime+4=14, s+4=6). NOT
    // `MakeDecimal(Sum(UnscaledValue(cast(e as dec(10,2)))), 14, 2)` (existing
    // arm form), which would lose F2's intent of avoiding the widened-cast
    // intermediate.
    val correctAnswer = testRelationE
      .select(
        Cast(
          Divide(
            avg(UnscaledValue($"e")),
            Literal.create(math.pow(10.0, 2), DoubleType)),
          DecimalType.bounded(14, 6),
          Option(conf.sessionLocalTimeZone))
          .as("avg_widened"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  // SPARK-56627 F1 regression: `WidenedDecimalChild` must NOT peel when the
  // inner expression is a `CheckOverflow` (introduced by `DecimalPrecision`
  // analyzer for nullOnOverflow semantics). Peeling through `CheckOverflow`
  // would change the overflow behavior of the inner aggregate.
  //
  // The existing un-widened `Average(DecimalExpression)` arm still fires on
  // the outer Cast (dataType `Decimal(pPrime=10, s=2)`, `pPrime + 4 = 14 <= 15`),
  // so the optimized plan wraps `UnscaledValue` around the OUTER cast (not
  // the inner CheckOverflow). The peel-arm-fired form would instead be
  // `UnscaledValue(CheckOverflow(e))` (no outer cast), which we want to AVOID.
  test("Decimal Average Aggregation widened-cast peel: " +
      "Not peeled for Cast(CheckOverflow(inner), wider) form (F1 guard)") {
    val testRelationE = LocalRelation($"e".decimal(7, 2))
    val co = CheckOverflow($"e", DecimalType(7, 2), nullOnOverflow = true)
    val widened = Cast(co, DecimalType(10, 2))
    val originalQuery = testRelationE.select(avg(widened).as("avg_co"))
    val optimized = Optimize.execute(originalQuery.analyze)

    // Existing un-widened AVG arm fires on the outer Cast (pPrime=10,
    // pPrime + 4 = 14 <= 15), wrapping UnscaledValue around the OUTER cast.
    val correctAnswer = testRelationE
      .select(
        Cast(
          Divide(
            avg(UnscaledValue(widened)),
            Literal.create(math.pow(10.0, 2), DoubleType)),
          DecimalType(14, 6),
          Option(conf.sessionLocalTimeZone))
          .as("avg_co"))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq($"a"), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum($"a"), spec).as("sum_a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select($"a")
      .window(
        Seq(MakeDecimal(windowExpr(sum(UnscaledValue($"a")), spec), 12, 1).as("sum_a")),
        Seq($"a"),
        Nil)
      .select($"a", $"sum_a", $"sum_a")
      .select($"sum_a")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Sum Aggregation over Window: Not Optimized") {
    val spec = windowSpec($"b" :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(sum($"b"), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Optimized") {
    val spec = windowSpec(Seq($"a"), Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg($"a"), spec).as("avg_a"))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = testRelation
      .select($"a")
      .window(
        Seq((windowExpr(avg(UnscaledValue($"a")), spec) / 10.0).cast(DecimalType(6, 5))
          .as("avg_a")),
        Seq($"a"),
        Nil)
      .select($"a", $"avg_a", $"avg_a")
      .select($"avg_a")
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Decimal Average Aggregation over Window: Not Optimized") {
    val spec = windowSpec($"b" :: Nil, Nil, UnspecifiedFrame)
    val originalQuery = testRelation.select(windowExpr(avg($"b"), spec))
    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  // ---------------------------------------------------------------------------
  // Widened-Cast Peel (SUM-only) -- SPARK-56627
  //
  // Extractor `WidenedDecimalChild` recognises a scale-preserving widening
  // Cast, enabling the existing fast path to fire on `SUM(CAST(x, wider))`
  // patterns that previously fell off the p+10 <= MAX_LONG_DIGITS guard.
  //
  // These tests assert behavioural plan-shape invariants via the local
  // `Optimize` RuleExecutor (runs only DecimalAggregates). Literal-no-peel
  // is covered separately via SimpleTestOptimizer because the local
  // RuleExecutor here does not run ConstantFolding.
  // ---------------------------------------------------------------------------

  private val widenRel = LocalRelation(
    $"d7_2".decimal(7, 2),
    $"d8_2".decimal(8, 2),
    $"d9_2".decimal(9, 2),
    $"d17_2".decimal(17, 2),
    $"i".int)

  test("SPARK-56627: SUM(CAST(dec(7,2) AS dec(17,2))) peels via widened-Cast fast path") {
    // Witness chosen so p+10=17 <= MAX_LONG_DIGITS(18) < pPrime+10=27 -- the
    // new case fires (a bare-Cast inner cannot fall through to the existing
    // DecimalExpression case). Expected shape:
    //   Cast(MakeDecimal(Sum(UnscaledValue(d7_2)), p+10=17, s=2),
    //        DecimalType.bounded(pPrime+10=27, s=2),
    //        Option(conf.sessionLocalTimeZone))
    val q = widenRel.select(sum($"d7_2".cast(DecimalType(17, 2))))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = widenRel
      .select(Cast(
        MakeDecimal(sum(UnscaledValue($"d7_2")), 17, 2),
        DecimalType.bounded(27, 2),
        Option(conf.sessionLocalTimeZone))
        .as("sum(CAST(d7_2 AS DECIMAL(17,2)))")).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-56627: SUM(CAST(dec(7,2) AS dec(17,2))) -- peel preserves schema") {
    // Schema invariance via DataType equality (not string).
    // Top-level output type of SUM(dec(p,s)) is DecimalType(min(p+10,38), s);
    // peeled tree wraps inner with outer Cast(_, dec(pPrime+10,s)) = dec(27,2)
    // -- identical to baseline schema.
    val q = widenRel.select(sum($"d7_2".cast(DecimalType(17, 2))))
    val baselineSchema = q.analyze.schema
    val optimized = Optimize.execute(q.analyze)
    assert(optimized.schema === baselineSchema,
      s"peel changed schema: $baselineSchema -> ${optimized.schema}")
  }

  test("SPARK-56627: SUM(CAST(int AS dec(10,0))) does NOT peel (non-decimal inner)") {
    val q = widenRel.select(sum($"i".cast(DecimalType(10, 0))))
    val optimized = Optimize.execute(q.analyze)
    // Peel must NOT fire; plan shape == input analyze.
    val correctAnswer = q.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-56627: AVG(CAST(dec(7,2) AS dec(17,2))) -- peel preserves schema") {
    val q = widenRel.select(avg($"d7_2".cast(DecimalType(17, 2))))
    val baselineSchema = q.analyze.schema
    val optimized = Optimize.execute(q.analyze)
    assert(optimized.schema === baselineSchema,
      s"peel changed schema: $baselineSchema -> ${optimized.schema}")
  }

  test("SPARK-56627: SUM(CAST(dec(7,2) AS dec(18,6))) does NOT peel (scale change)") {
    val q = widenRel.select(sum($"d7_2".cast(DecimalType(18, 6))))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = q.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-56627: SUM(CAST(dec(17,2) AS dec(10,2))) does NOT peel (narrowing)") {
    val q = widenRel.select(sum($"d17_2".cast(DecimalType(10, 2))))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = q.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-56627: SUM(CheckOverflow(Cast(...))) does NOT peel") {
    val co = CheckOverflow(
      $"d7_2".cast(DecimalType(17, 2)), DecimalType(17, 2), nullOnOverflow = true)
    val q = widenRel.select(sum(co).as("s"))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = q.analyze

    comparePlans(optimized, correctAnswer)
  }

  // Pre-existing fast-path regression guard.
  // Witness: SUM(d7_2), no Cast. p+10 = 17 <= MAX_LONG_DIGITS(18) hits the
  // existing `Sum(e @ DecimalExpression(p, s))` case. The new peel case
  // prepended must NOT shadow the existing fast path on no-cast inputs.
  test("SPARK-56627: SUM(dec(7,2)) hits existing DecimalExpression fast path") {
    val expected = widenRel
      .select(MakeDecimal(sum(UnscaledValue($"d7_2")), 17, 2).as("sum(d7_2)")).analyze
    val q = widenRel.select(sum($"d7_2"))
    val optimized = Optimize.execute(q.analyze)
    comparePlans(optimized, expected)
  }

  // Literal-in-Cast no-peel regression guard.
  //
  // Uses `SimpleTestOptimizer` (full optimizer batches) rather than the local
  // `Optimize` RuleExecutor defined above, because this test depends on
  // `ConstantFolding` running before `DecimalAggregates`: the outer Cast on a
  // foldable Literal child is folded away before the peel rule ever sees it,
  // so there is no Cast left to peel. Post-optimization the plan contains
  // neither `MakeDecimal` nor an `UnscaledValue` call -- SUM sees a bare
  // `Literal(dec(17,2))` whose precision (17) already fails the existing
  // `prec + 10 <= MAX_LONG_DIGITS` guard (27 > 18), so the whole SUM arm is
  // a no-op. The assertion is deliberately absence-of-peel-shape rather than
  // structural equality, to survive unrelated ConstantFolding changes.
  test("SPARK-56627: SUM(CAST(Literal(dec(7,2)) AS dec(17,2))) does NOT peel " +
      "after ConstantFolding") {
    val lit = Literal.create(Decimal("1.23"), DecimalType(7, 2))
    val q = widenRel.select(sum(lit.cast(DecimalType(17, 2))))
    val optimized = SimpleTestOptimizer.execute(q.analyze)
    val hasMakeDecimal = optimized.expressions.exists(_.exists {
      case _: MakeDecimal => true
      case _ => false
    })
    val hasUnscaledValue = optimized.expressions.exists(_.exists {
      case _: UnscaledValue => true
      case _ => false
    })
    assert(!hasMakeDecimal,
      s"peel unexpectedly fired on a folded Literal child; plan:\n$optimized")
    assert(!hasUnscaledValue,
      s"UnscaledValue unexpectedly present on folded Literal child; plan:\n$optimized")
  }

  // Plan-shape invariant guards (null / empty-relation witnesses).
  //
  // DecimalAggregatesSuite is a PlanTest without a SparkSession; the local
  // `Optimize` RuleExecutor runs DecimalAggregates only. At plan level, an
  // all-null Literal-typed column shares the extractor path of any other
  // DecimalExpression, and an empty LocalRelation shares the shape of the
  // non-empty widenRel. These two witnesses assert the peel rule fires
  // identically to the canonical witness under both inputs -- rule body is
  // data-independent. End-to-end null-propagation semantics are covered
  // separately in the sql-core equivalence suite.

  test("SPARK-56627: SUM(CAST(Literal(null, dec(7,2)) AS dec(17,2))) peels " +
      "(null Literal in Cast, plan-shape invariant)") {
    val nullLit = Literal.create(null, DecimalType(7, 2))
    val q = widenRel.select(sum(nullLit.cast(DecimalType(17, 2))))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = widenRel
      .select(Cast(
        MakeDecimal(sum(UnscaledValue(nullLit)), 17, 2),
        DecimalType.bounded(27, 2),
        Option(conf.sessionLocalTimeZone))
        .as("sum(CAST(NULL AS DECIMAL(17,2)))")).analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-56627: SUM(CAST(dec(7,2) AS dec(17,2))) on empty LocalRelation peels " +
      "(empty-relation plan-shape invariant)") {
    val emptyRel = LocalRelation($"d7_2".decimal(7, 2))
    val q = emptyRel.select(sum($"d7_2".cast(DecimalType(17, 2))))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = emptyRel
      .select(Cast(
        MakeDecimal(sum(UnscaledValue($"d7_2")), 17, 2),
        DecimalType.bounded(27, 2),
        Option(conf.sessionLocalTimeZone))
        .as("sum(CAST(d7_2 AS DECIMAL(17,2)))")).analyze
    comparePlans(optimized, correctAnswer)
  }

  // Idempotence invariant guard.
  //
  // Post-peel, the `Sum` child is `UnscaledValue(DecimalExpression)` which
  // types to `LongType`, so the `WidenedDecimalChild` extractor (which
  // guards on `DecimalType(p, s)` with `p + 10 <= MAX_LONG_DIGITS < p' + 10`)
  // cannot re-match on the second pass. Use `canonicalized` (not `==`) to
  // neutralise `exprId` drift across `Sum` aggregate-expression allocation
  // in successive rule applications.
  test("SPARK-56627: DecimalAggregates is idempotent on canonical widened witness " +
      "(peel(peel(t)) == peel(t) under canonicalization)") {
    val q = widenRel.select(sum($"d7_2".cast(DecimalType(17, 2)))).analyze
    val once = DecimalAggregates(q)
    val twice = DecimalAggregates(DecimalAggregates(q))
    assert(once.canonicalized == twice.canonicalized,
      s"DecimalAggregates re-fired on already-peeled plan.\n" +
        s"once:\n$once\ntwice:\n$twice")
  }

  // RuleExecutor convergence: drive DecimalAggregates inside a fixed-point
  // RuleExecutor batch and assert it converges in <= 1 application after the
  // first peel. Catches accidental rewrite oscillations in fixed-point batches.
  test("SPARK-56627: DecimalAggregates converges under RuleExecutor on widened SUM") {
    object Once extends RuleExecutor[LogicalPlan] {
      val batches: Seq[Batch] =
        Seq(Batch("DecimalAggregates", FixedPoint(10), DecimalAggregates))
    }
    val q = widenRel.select(sum($"d7_2".cast(DecimalType(17, 2)))).analyze
    val once = DecimalAggregates(q)
    val converged = Once.execute(q)
    assert(once.canonicalized == converged.canonicalized,
      s"FixedPoint did not converge to single peel.\n" +
        s"once:\n$once\nconverged:\n$converged")
  }

  // Negative guard-miss: at p=9, the inner decimal already exceeds the
  // existing DecimalExpression fast path (p+10=19 > MAX_LONG_DIGITS=18) so
  // the peel rewrite must NOT fire. Pin via plan-equality against analyzed.
  test("SPARK-56627: SUM(CAST(dec(9,2) AS dec(19,2))) does NOT peel (p=9 guard)") {
    val rel = LocalRelation($"d9_2".decimal(9, 2))
    val q = rel.select(sum($"d9_2".cast(DecimalType(19, 2)))).analyze
    val optimized = Optimize.execute(q)
    comparePlans(optimized, q)
  }

  // Plan-shape property: structural invariants on the peeled tree.
  //
  // Sweeps the (p, p', s) lattice where the widened-cast peel fires:
  //   regime (ii):  p + 10 <= 18 <= p' + 10  (new arm, old fast-path off)
  //   regime (iii): p + 10 <= 18 < p' + 10 <= 38
  // Assertion (peel-on, structural -- NOT a hand-typed RHS clone):
  //   - aggregate expression is wrapped by exactly one outer Cast
  //   - the outer Cast wraps exactly one MakeDecimal
  //   - inside MakeDecimal, the Sum's child has dataType=LongType (i.e.
  //     UnscaledValue was inserted)
  //   - outer Cast target precision = p' + 10 (or 38, clamped)
  //   - outer Cast target scale = s
  // Reframed away from RHS-equality to detect behavioural regressions
  // rather than just refactor drift.
  // Peel-off branch: plan is unchanged relative to its analyzed form
  // (the local RuleExecutor runs only DecimalAggregates; no other rule
  // can rewrite the SUM when the peel does not fire for a Cast child).

  private case class PeelInputs(p: Int, pPrime: Int, s: Int)

  private val peelGen: Gen[PeelInputs] = Gen.frequency(
    5 -> (for {
      p <- Gen.choose(1, 8)
      pPrime <- Gen.choose(math.max(p + 1, 9), 28)
      s <- Gen.choose(0, p)
    } yield PeelInputs(p, pPrime, s)),
    5 -> (for {
      p <- Gen.choose(1, 8)
      pPrime <- Gen.choose(9, 28)
      s <- Gen.choose(0, p)
    } yield PeelInputs(p, pPrime, s))
  )

  private val boundaryGen: Gen[PeelInputs] = Gen.oneOf(
    PeelInputs(7, 17, 2), PeelInputs(7, 18, 2), PeelInputs(7, 19, 2))

  private val peelSpaceGen: Gen[PeelInputs] = Gen.frequency(
    8 -> peelGen,
    2 -> boundaryGen
  ).retryUntil(in => in.p + 10 <= 18 && in.p < in.pPrime && in.pPrime + 10 <= 38)

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 50, minSize = 0, sizeRange = 0)

  test("SPARK-56627: DecimalAggregates widened-Cast SUM peel -- plan-shape " +
      "structural-invariants property") {
    forAll(peelSpaceGen) { in =>
      val rel = LocalRelation($"x".decimal(in.p, in.s))
      val q = rel.select(sum($"x".cast(DecimalType(in.pPrime, in.s))))
      val analyzed = q.analyze

      val optimized = Optimize.execute(analyzed)

      // Structural invariants the peel rewrite must establish, regardless
      // of incidental tree-shape changes from neighbouring rules:
      //
      //   I1. exactly one Sum node, whose child has LongType (the peeled
      //       UnscaledValue feed);
      //   I2. exactly one MakeDecimal node in the tree (rebuilds Decimal
      //       from the LONG accumulator);
      //   I3. an outer Cast whose target DecimalType has precision at
      //       least as wide as the user-written widened cast, so we never
      //       narrow result precision below the baseline plan.
      val sums = optimized.expressions.flatMap(_.collect { case s: Sum => s })
      assert(sums.size == 1, s"expected exactly 1 Sum, got ${sums.size} in $optimized")
      assert(sums.head.child.dataType == LongType,
        s"expected Sum.child: LongType, got ${sums.head.child.dataType} in $optimized")

      val mds = optimized.expressions.flatMap(_.collect { case m: MakeDecimal => m })
      assert(mds.size == 1,
        s"expected exactly 1 MakeDecimal, got ${mds.size} in $optimized")

      val outerCasts = optimized.expressions.flatMap(_.collect {
        case c @ Cast(_, _: DecimalType, _, _) => c
      })
      assert(outerCasts.nonEmpty,
        s"expected an outer Cast to DecimalType, got none in $optimized")
      val outerPrec = outerCasts.map(_.dataType.asInstanceOf[DecimalType].precision).max
      assert(outerPrec >= in.pPrime,
        s"outer Cast precision $outerPrec < baseline ${in.pPrime} in $optimized")
    }
  }

  // ---------------------------------------------------------------------------
  // F5 (skeptic round 1): Long-accumulator / Double-regime safety boundary
  // invariant guards.
  //
  // Background: a strict "overflow oracle" cannot be written at unit-test
  // scale -- the existing fast-path guards (`p + 10 <= MAX_LONG_DIGITS = 18`
  // for SUM, `AVG_PEEL_MAX_INNER_PRECISION = 7` for AVG) keep the peel-eligible
  // inner-precision band so narrow that the Long accumulator (~9.22e18) cannot
  // wrap on any reachable peel input: at `p=8` we'd need ~9.22e10 rows. So
  // there is no production input that exercises a "peeled Long-wrap vs
  // un-peeled CheckOverflow" asymmetry to oracle against.
  //
  // What we CAN lock is the boundary itself: if someone in the future relaxes
  // either guard (raising `MAX_LONG_DIGITS - 10` for SUM, or
  // `AVG_PEEL_MAX_INNER_PRECISION` for AVG), the input shapes below WOULD
  // start peeling -- and the assertion that the rule is a no-op for these
  // inputs would fail. That is the safety net we want: a mechanical guard
  // that catches accidental widening of the peel-trigger surface.
  test("SPARK-56627: SUM(CAST(dec(9,2) AS dec(19,2))) does NOT peel " +
      "(Long-accumulator safety boundary)") {
    // Boundary witness: inner p=9 makes widened-arm `p + 10 = 19 > 18` reject,
    // AND outer-cast existing-arm `prec + 10 = 29 > 18` reject. Both arms are
    // no-ops by design -- peel cannot fire on this shape today, and must not
    // start firing if the inner-precision band is later widened without
    // re-deriving the Long-accumulator bound.
    val q = widenRel.select(sum($"d9_2".cast(DecimalType(19, 2))))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = q.analyze
    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-56627: AVG(CAST(dec(8,2) AS dec(20,2))) does NOT peel " +
      "(Double-regime / SPARK-37024 safety boundary)") {
    // Boundary witness: inner p=8 makes widened-AVG arm
    // `p > AVG_PEEL_MAX_INNER_PRECISION (7)` reject, AND outer-cast existing
    // AVG arm `prec + 4 = 24 > MAX_DOUBLE_DIGITS (15)` reject. The strict-
    // subset guard `p <= 7` keeps this rule's trigger surface strictly
    // inside the existing AVG fast path's surface, so SPARK-37024
    // (Double-regime silent precision loss) is not amplified. If someone
    // raises `AVG_PEEL_MAX_INNER_PRECISION` past 7 without first fixing
    // SPARK-37024, this test will start firing and flag the regression.
    val q = widenRel.select(avg($"d8_2".cast(DecimalType(20, 2))))
    val optimized = Optimize.execute(q.analyze)
    val correctAnswer = q.analyze
    comparePlans(optimized, correctAnswer)
  }

  // ---------------------------------------------------------------------------
  // SPARK-56949: DecimalAggregates must preserve evalMode / evalContext when
  // rewriting Sum / Average through the fast-path. The pre-fix rule called the
  // single-arg helper ctor `Sum(child)` / `Average(child)`, which re-reads
  // EvalMode from SQLConf and silently drops EvalMode.TRY from try_sum /
  // try_avg, breaking their "return NULL on overflow" semantics.
  //
  // Vanilla 3.5.3 ground-truth (rule OFF vs ON) recorded in todos repo:
  //   features/spark-decimal-aggregate-evalmode-preserve/docs/0001-idea.md (section 3)
  // ---------------------------------------------------------------------------

  private def findSum(plan: LogicalPlan): Seq[Sum] =
    plan.collect { case n => n.expressions }.flatten
      .flatMap(_.collect { case s: Sum => s })
  private def findAverage(plan: LogicalPlan): Seq[Average] =
    plan.collect { case n => n.expressions }.flatten
      .flatMap(_.collect { case a: Average => a })

  test("SPARK-56949: DecimalAggregates preserves Sum.evalContext for try_sum") {
    val trySum = Sum($"a", NumericEvalContext(EvalMode.TRY))
    val q = testRelation.select(trySum.toAggregateExpression().as("ts"))
    val optimized = Optimize.execute(q.analyze)
    val sums = findSum(optimized)
    assert(sums.nonEmpty, "DecimalAggregates fast path should fire for dec(2,1)")
    assert(sums.forall(_.evalContext.evalMode == EvalMode.TRY),
      s"evalMode should be preserved as TRY after rewrite, got " +
        sums.map(_.evalContext.evalMode).mkString(","))
  }

  test("SPARK-56949: DecimalAggregates preserves Average.evalMode for try_avg") {
    val tryAvg = Average($"a", EvalMode.TRY)
    val q = testRelation.select(tryAvg.toAggregateExpression().as("ta"))
    val optimized = Optimize.execute(q.analyze)
    val avgs = findAverage(optimized)
    assert(avgs.nonEmpty, "DecimalAggregates fast path should fire for dec(2,1)")
    assert(avgs.forall(_.evalMode == EvalMode.TRY),
      s"evalMode should be preserved as TRY after rewrite, got " +
        avgs.map(_.evalMode).mkString(","))
  }

  test("SPARK-56949: DecimalAggregates preserves Sum.evalContext " +
      "for try_sum on widened-cast peel arm") {
    val trySum = Sum($"d7_2".cast(DecimalType(12, 2)),
      NumericEvalContext(EvalMode.TRY))
    val q = widenRel.select(trySum.toAggregateExpression().as("ts"))
    val optimized = Optimize.execute(q.analyze)
    val sums = findSum(optimized)
    assert(sums.nonEmpty, "widened-cast SUM peel should fire for dec(7,2)->dec(12,2)")
    assert(sums.forall(_.evalContext.evalMode == EvalMode.TRY),
      s"evalMode should be preserved as TRY after rewrite, got " +
        sums.map(_.evalContext.evalMode).mkString(","))
  }

  test("SPARK-56949: DecimalAggregates preserves Average.evalMode " +
      "for try_avg on widened-cast peel arm") {
    val tryAvg = Average($"d7_2".cast(DecimalType(12, 2)), EvalMode.TRY)
    val q = widenRel.select(tryAvg.toAggregateExpression().as("ta"))
    val optimized = Optimize.execute(q.analyze)
    val avgs = findAverage(optimized)
    assert(avgs.nonEmpty, "widened-cast AVG peel should fire for dec(7,2)->dec(12,2)")
    assert(avgs.forall(_.evalMode == EvalMode.TRY),
      s"evalMode should be preserved as TRY after rewrite, got " +
        avgs.map(_.evalMode).mkString(","))
  }

  test("SPARK-56949: DecimalAggregates preserves Sum.evalContext " +
      "for try_sum over Window (un-widened arm)") {
    val spec = windowSpec(Seq($"a"), Nil, UnspecifiedFrame)
    val trySum = Sum($"a", NumericEvalContext(EvalMode.TRY))
    val q = testRelation.select(
      windowExpr(trySum.toAggregateExpression(), spec).as("ts"))
    val optimized = Optimize.execute(q.analyze)
    val sums = findSum(optimized)
    assert(sums.nonEmpty, "Window-arm SUM peel should fire for dec(2,1)")
    assert(sums.forall(_.evalContext.evalMode == EvalMode.TRY),
      s"evalMode should be preserved as TRY after rewrite, got " +
        sums.map(_.evalContext.evalMode).mkString(","))
  }

  test("SPARK-56949: DecimalAggregates preserves Average.evalMode " +
      "for try_avg over Window (un-widened arm)") {
    val spec = windowSpec(Seq($"a"), Nil, UnspecifiedFrame)
    val tryAvg = Average($"a", EvalMode.TRY)
    val q = testRelation.select(
      windowExpr(tryAvg.toAggregateExpression(), spec).as("ta"))
    val optimized = Optimize.execute(q.analyze)
    val avgs = findAverage(optimized)
    assert(avgs.nonEmpty, "Window-arm AVG peel should fire for dec(2,1)")
    assert(avgs.forall(_.evalMode == EvalMode.TRY),
      s"evalMode should be preserved as TRY after rewrite, got " +
        avgs.map(_.evalMode).mkString(","))
  }
}

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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, IntegerType, LongType, ShortType, StructField, StructType}

class BinaryComparisonSimplificationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Infer Filters", Once,
          InferFiltersFromConstraints) ::
      Batch("Compute current time", Once,
        ComputeCurrentTime) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        DeriveIntegralComparisonPredicates,
        SimplifyBinaryComparison,
        PruneFilters) :: Nil
  }

  // Mirrors the two batches that share `operatorOptimizationRuleSet` in the real optimizer.
  object OptimizeAcrossBatches extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Operator Optimization before Inferring Filters", FixedPoint(50),
        BooleanSimplification,
        DeriveIntegralComparisonPredicates) ::
      Batch("Operator Optimization after Inferring Filters", FixedPoint(50),
        BooleanSimplification,
        DeriveIntegralComparisonPredicates) :: Nil
  }

  object DeriveOnly extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Derive Integral Comparison Predicates", FixedPoint(50),
        DeriveIntegralComparisonPredicates) :: Nil
  }

  private def checkCondition(rel: LocalRelation, input: Expression, expected: Expression): Unit =
    comparePlans(Optimize.execute(rel.where(input).analyze), rel.where(expected).analyze)

  val nullableRelation = LocalRelation($"a".int.withNullability(true))
  val nonNullableRelation = LocalRelation($"a".int.withNullability(false))
  val boolRelation = LocalRelation($"a".boolean, $"b".boolean)

  test("derive pruning predicates from ANSI integral arithmetic comparisons") {
    val a = nonNullableRelation.output.head
    val add = Add(a, Literal(10), EvalMode.ANSI)
    val litFirstAdd = Add(Literal(10), a, EvalMode.ANSI)
    val subtract = Subtract(a, Literal(10), EvalMode.ANSI)
    val negated = Subtract(Literal(10), a, EvalMode.ANSI)
    val negatedLow = Subtract(Literal(-10), a, EvalMode.ANSI)
    val negatedZero = Subtract(Literal(0), a, EvalMode.ANSI)
    val negatedPivot = Subtract(Literal(-1), a, EvalMode.ANSI)
    val addOverflow = a > Literal(Int.MaxValue - 10)
    val subtractOverflow = a < Literal(Int.MinValue + 10)
    val negatedOverflow = a < Literal(-(Int.MaxValue - 10))
    val negatedLowOverflow = a > Literal(-(Int.MinValue + 10))
    val negatedZeroOverflow = a < Literal(-Int.MaxValue)

    val cases = Seq[(Expression, Expression)](
      (add < Literal(100), (a < Literal(90) || addOverflow) && add < Literal(100)),
      (add <= Literal(100), (a <= Literal(90) || addOverflow) && add <= Literal(100)),
      (add > Literal(100), a > Literal(90) && add > Literal(100)),
      (add >= Literal(100), a >= Literal(90) && add >= Literal(100)),
      (add === Literal(100), (a === Literal(90) || addOverflow) && add === Literal(100)),
      (add < Literal(Int.MinValue), addOverflow && add < Literal(Int.MinValue)),
      (add > Literal(Int.MaxValue), addOverflow && add > Literal(Int.MaxValue)),
      (subtract < Literal(100), a < Literal(110) && subtract < Literal(100)),
      (subtract <= Literal(100), a <= Literal(110) && subtract <= Literal(100)),
      (subtract > Literal(100),
        (a > Literal(110) || subtractOverflow) && subtract > Literal(100)),
      (subtract < Literal(Int.MinValue),
        subtractOverflow && subtract < Literal(Int.MinValue)),
      (subtract > Literal(Int.MaxValue),
        subtractOverflow && subtract > Literal(Int.MaxValue)),
      (Literal(100) < add, a > Literal(90) && Literal(100) < add),
      (litFirstAdd < Literal(100), (a < Literal(90) || addOverflow) && litFirstAdd < Literal(100)),
      (add < Literal(100) && subtract > Literal(100),
        (a < Literal(90) || addOverflow) && add < Literal(100) &&
          (a > Literal(110) || subtractOverflow) && subtract > Literal(100)),
      // Out-of-range thresholds hit the constant-fold branch (true folds are absorbed).
      (subtract < Literal(Int.MaxValue), subtract < Literal(Int.MaxValue)),
      (add > Literal(Int.MinValue), add > Literal(Int.MinValue)),
      (add === Literal(Int.MinValue), addOverflow && add === Literal(Int.MinValue)),
      // A literal-first subtract negates the column, reversing the derived comparison.
      (negated < Literal(100), (a > Literal(-90) || negatedOverflow) && negated < Literal(100)),
      (negated <= Literal(100),
        (a >= Literal(-90) || negatedOverflow) && negated <= Literal(100)),
      (negated > Literal(100), a < Literal(-90) && negated > Literal(100)),
      (negated >= Literal(100), a <= Literal(-90) && negated >= Literal(100)),
      (negated === Literal(100),
        (a === Literal(-90) || negatedOverflow) && negated === Literal(100)),
      (negatedLow < Literal(100), a > Literal(-110) && negatedLow < Literal(100)),
      (negatedLow <= Literal(100), a >= Literal(-110) && negatedLow <= Literal(100)),
      (negatedLow > Literal(100),
        (a < Literal(-110) || negatedLowOverflow) && negatedLow > Literal(100)),
      (negatedLow >= Literal(100),
        (a <= Literal(-110) || negatedLowOverflow) && negatedLow >= Literal(100)),
      (negatedLow === Literal(100),
        (a === Literal(-110) || negatedLowOverflow) && negatedLow === Literal(100)),
      // One step above the pivot, where a pivot of zero would drop the leg.
      (negatedZero < Literal(100),
        (a > Literal(-100) || negatedZeroOverflow) && negatedZero < Literal(100)),
      (negatedZero <= Literal(100),
        (a >= Literal(-100) || negatedZeroOverflow) && negatedZero <= Literal(100)),
      (negatedZero === Literal(100),
        (a === Literal(-100) || negatedZeroOverflow) && negatedZero === Literal(100)),
      // At the pivot the arithmetic maps the range onto itself, so there is no leg to emit.
      (negatedPivot < Literal(100), a > Literal(-101) && negatedPivot < Literal(100)),
      (negatedPivot === Literal(100), a === Literal(-101) && negatedPivot === Literal(100)),
      (negated < Literal(Int.MinValue), negatedOverflow && negated < Literal(Int.MinValue)),
      (negated > Literal(Int.MinValue), negated > Literal(Int.MinValue)),
      // Both the comparison and the subtract are literal-first, so the reversals compose.
      (Literal(100) > negated, (a > Literal(-90) || negatedOverflow) && Literal(100) > negated),
      (Literal(100) < negated, a < Literal(-90) && Literal(100) < negated))

    cases.foreach { case (input, expected) =>
      checkCondition(nonNullableRelation, input, expected)
    }
  }

  test("do not derive pruning predicates when disabled") {
    val a = nonNullableRelation.output.head
    val condition = Add(a, Literal(10), EvalMode.ANSI) > Literal(100)
    val plan = nonNullableRelation.where(condition).analyze

    withSQLConf(SQLConf.DERIVE_INTEGRAL_COMPARISON_PREDICATES_ENABLED.key -> "false") {
      comparePlans(DeriveOnly.execute(plan), plan)
    }
  }

  test("do not derive pruning predicates when arithmetic is not checked integral arithmetic") {
    val a = nonNullableRelation.output.head
    val cases = Seq(
      Add(a, Literal(10), EvalMode.LEGACY) > Literal(100),
      Multiply(a, Literal(10), EvalMode.ANSI) > Literal(100),
      Add(Cast(a, DoubleType), Literal(10.0), EvalMode.ANSI) > Literal(100.0),
      Add(a, a, EvalMode.ANSI) > Literal(100))

    cases.foreach { condition =>
      checkCondition(nonNullableRelation, condition, condition)
    }
  }

  test("do not derive pruning predicates when the operand is not a column") {
    val a = nonNullableRelation.output.head
    val nonColumnOperand = Add(a, Literal(1), EvalMode.ANSI)
    val condition = Add(nonColumnOperand, Literal(10), EvalMode.ANSI) > Literal(100)
    checkCondition(nonNullableRelation, condition, condition)
  }

  gridTest("derive pruning predicates across integral types")(
      Seq[DataType](ByteType, ShortType, LongType)) { dataType =>
    val relation = LocalRelation(AttributeReference("a", dataType, nullable = false)())
    val col = relation.output.head
    val (lit, overflowBound): (Long => Literal, Literal) = dataType match {
      case ByteType => ((v: Long) => Literal(v.toByte), Literal((Byte.MaxValue - 10).toByte))
      case ShortType => ((v: Long) => Literal(v.toShort), Literal((Short.MaxValue - 10).toShort))
      case _ => ((v: Long) => Literal(v), Literal(Long.MaxValue - 10L))
    }
    val add = Add(col, lit(10), EvalMode.ANSI)
    checkCondition(
      relation,
      add < lit(100),
      (col < lit(90) || col > overflowBound) && add < lit(100))
    checkCondition(relation, add > lit(100), col > lit(90) && add > lit(100))
  }


  test("derive pruning predicates when one analyzed plan is optimized twice") {
    val a = nonNullableRelation.output.head
    val add = Add(a, Literal(10), EvalMode.ANSI)
    val plan = nonNullableRelation.where(add > Literal(100)).analyze
    val expected = nonNullableRelation
      .where(a > Literal(90) && add > Literal(100))
      .analyze
    // The same plan instance has to be optimized twice; a rebuilt plan derives either way.
    comparePlans(Optimize.execute(plan), expected)
    comparePlans(Optimize.execute(plan), expected)
  }

  test("retain a freshly built comparison and leave the analyzed plan untagged") {
    val a = nonNullableRelation.output.head
    val add = Add(a, Literal(10), EvalMode.ANSI)
    val analyzed = nonNullableRelation.where(add > Literal(100)).analyze
    val analyzedComparisons = analyzed.expressions.flatMap(_.collect {
      case comparison: BinaryComparison => comparison
    })
    assert(analyzedComparisons.length == 1)

    val optimized = Optimize.execute(analyzed)
    val retainedComparisons = optimized.expressions.flatMap(_.collect {
      case comparison: BinaryComparison if comparison.left.isInstanceOf[Add] => comparison
    })
    assert(retainedComparisons.length == 1)

    // Plan shape alone does not discriminate here, because tags are not part of tree equality.
    assert(!(retainedComparisons.head eq analyzedComparisons.head))
    assert(analyzedComparisons.head.isTagsEmpty)
    assert(!retainedComparisons.head.isTagsEmpty)
  }

  test("derive pruning predicates once across the batches sharing the rule") {
    val a = nonNullableRelation.output.head
    val add = Add(a, Literal(10), EvalMode.ANSI)
    comparePlans(
      OptimizeAcrossBatches.execute(nonNullableRelation.where(add > Literal(100)).analyze),
      nonNullableRelation
        .where(a > Literal(90) && add > Literal(100))
        .analyze)
  }

  test("emit derived predicates in a shape the boolean simplifications leave alone") {
    val a = nonNullableRelation.output.head
    val add = Add(a, Literal(10), EvalMode.ANSI)
    val subtract = Subtract(a, Literal(10), EvalMode.ANSI)
    val addOverflow = a > Literal(Int.MaxValue - 10)
    val subtractOverflow = a < Literal(Int.MinValue + 10)

    val cases = Seq[(Expression, Expression)](
      // The algebraic leg is always true, so nothing is derived at all.
      (subtract < Literal(Int.MaxValue), subtract < Literal(Int.MaxValue)),
      (add > Literal(Int.MinValue), add > Literal(Int.MinValue)),
      // The algebraic leg is always false, so only the overflow leg is derived.
      (add === Literal(Int.MinValue), addOverflow && add === Literal(Int.MinValue)),
      (subtract > Literal(Int.MaxValue), subtractOverflow && subtract > Literal(Int.MaxValue)),
      // The overflow leg is implied by the algebraic leg, so only the latter is derived.
      (add > Literal(Int.MaxValue), addOverflow && add > Literal(Int.MaxValue)),
      (subtract < Literal(Int.MinValue), subtractOverflow && subtract < Literal(Int.MinValue)))

    cases.foreach { case (input, expected) =>
      comparePlans(
        DeriveOnly.execute(nonNullableRelation.where(input).analyze),
        nonNullableRelation.where(expected).analyze)
    }
  }

  test("do not derive pruning predicates outside the top-level conjuncts of a filter") {
    val a = nonNullableRelation.output.head
    val eligible = Add(a, Literal(10), EvalMode.ANSI) > Literal(100)

    checkCondition(nonNullableRelation, eligible || a < Literal(0), eligible || a < Literal(0))

    val projection = nonNullableRelation.select(Alias(eligible, "p")()).analyze
    comparePlans(Optimize.execute(projection), projection)

    val aggregate = nonNullableRelation.groupBy(eligible)(count(Literal(1))).analyze
    comparePlans(Optimize.execute(aggregate), aggregate)
  }

  test("Preserve nullable exprs when constraintPropagation is false") {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      val a = $"a"
      for (e <- Seq(a === a, a <= a, a >= a, a < a, a > a)) {
        val plan = nullableRelation.where(e).analyze
        val actual = Optimize.execute(plan)
        val correctAnswer = plan
        comparePlans(actual, correctAnswer)
      }
    }
  }

  test("Preserve non-deterministic exprs") {
    val plan = nonNullableRelation
      .where(Rand(0) === Rand(0) && Rand(1) <=> Rand(1)).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = plan
    comparePlans(actual, correctAnswer)
  }

  test("Nullable Simplification Primitive: <=>") {
    val plan = nullableRelation.select($"a" <=> $"a").analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nullableRelation.select(Alias(TrueLiteral, "(a <=> a)")()).analyze
    comparePlans(actual, correctAnswer)
  }

  test("Non-Nullable Simplification Primitive") {
    val plan = nonNullableRelation
      .select($"a" === $"a", $"a" <=> $"a", $"a" <= $"a", $"a" >= $"a", $"a" < $"a", $"a" > $"a")
      .analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nonNullableRelation
      .select(
        Alias(TrueLiteral, "(a = a)")(),
        Alias(TrueLiteral, "(a <=> a)")(),
        Alias(TrueLiteral, "(a <= a)")(),
        Alias(TrueLiteral, "(a >= a)")(),
        Alias(FalseLiteral, "(a < a)")(),
        Alias(FalseLiteral, "(a > a)")())
      .analyze
    comparePlans(actual, correctAnswer)
  }

  test("Expression Normalization") {
    val plan = nonNullableRelation.where(
      $"a" * Literal(100) + Pi() === Pi() + Literal(100) * $"a" &&
      DateAdd(CurrentDate(), $"a" + Literal(2)) <= DateAdd(CurrentDate(), Literal(2) + $"a"))
      .analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = nonNullableRelation.analyze
    comparePlans(actual, correctAnswer)
  }

  test("SPARK-26402: accessing nested fields with different cases in case insensitive mode") {
    val expId = NamedExpression.newExprId
    val qualifier = Seq.empty[String]
    val structType = StructType(
      StructField("a", StructType(StructField("b", IntegerType, false) :: Nil), false) :: Nil)

    val fieldA1 = GetStructField(
      GetStructField(
        AttributeReference("data1", structType, false)(expId, qualifier),
        0, Some("a1")),
      0, Some("b1"))
    val fieldA2 = GetStructField(
      GetStructField(
        AttributeReference("data2", structType, false)(expId, qualifier),
        0, Some("a2")),
      0, Some("b2"))

    // GetStructField with different names are semantically equal; thus, `EqualTo(fieldA1, fieldA2)`
    // will be optimized to `TrueLiteral` by `SimplifyBinaryComparison`.
    val originalQuery = nonNullableRelation.where(EqualTo(fieldA1, fieldA2))

    val optimized = Optimize.execute(originalQuery)
    val correctAnswer = nonNullableRelation.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Simplify null and nonnull with filter constraints") {
    val a = $"a"
    Seq(a === a, a <= a, a >= a, a < a, a > a).foreach { condition =>
      val plan = nonNullableRelation.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = nonNullableRelation.analyze
      comparePlans(actual, correctAnswer)
    }

    // infer filter constraints will add IsNotNull
    Seq(a === a, a <= a, a >= a).foreach { condition =>
      val plan = nullableRelation.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = nullableRelation.where($"a".isNotNull).analyze
      comparePlans(actual, correctAnswer)
    }

    Seq(a < a, a > a).foreach { condition =>
      val plan = nullableRelation.where(condition).analyze
      val actual = Optimize.execute(plan)
      val correctAnswer = nullableRelation.analyze
      comparePlans(actual, correctAnswer)
    }
  }

  test("Simplify nullable without constraints propagation") {
    withSQLConf(SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> "false") {
      val a = $"a"
      Seq(And(a === a, a.isNotNull),
        And(a <= a, a.isNotNull),
        And(a >= a, a.isNotNull)).foreach { condition =>
        val plan = nullableRelation.where(condition).analyze
        val actual = Optimize.execute(plan)
        val correctAnswer = nullableRelation.where($"a".isNotNull).analyze
        comparePlans(actual, correctAnswer)
      }

      Seq(And(a < a, a.isNotNull), And(a > a, a.isNotNull))
        .foreach { condition =>
        val plan = nullableRelation.where(condition).analyze
        val actual = Optimize.execute(plan)
        val correctAnswer = nullableRelation.analyze
        comparePlans(actual, correctAnswer)
      }
    }
  }

  test("SPARK-36359: Coalesce drop all expressions after the first non nullable expression") {
    val testRelation = LocalRelation(
      $"a".int.withNullability(false),
      $"b".int.withNullability(true),
      $"c".int.withNullability(false),
      $"d".int.withNullability(true))

    comparePlans(
      Optimize.execute(testRelation.select(Coalesce(Seq($"a", $"b", $"c", $"d")).as("out"))
        .analyze),
      testRelation.select($"a".as("out")).analyze)
    comparePlans(
      Optimize.execute(testRelation.select(Coalesce(Seq($"a", $"c")).as("out")).analyze),
      testRelation.select($"a".as("out")).analyze)
    comparePlans(
      Optimize.execute(testRelation.select(Coalesce(Seq($"b", $"c", $"d")).as("out")).analyze),
      testRelation.select(Coalesce(Seq($"b", $"c")).as("out")).analyze)
    comparePlans(
      Optimize.execute(testRelation.select(Coalesce(Seq($"b", $"d")).as("out")).analyze),
      testRelation.select(Coalesce(Seq($"b", $"d")).as("out")).analyze)
  }

  test("SPARK-36721: Simplify boolean equalities if one side is literal") {
    checkCondition(boolRelation, And($"a", $"b") === TrueLiteral, And($"a", $"b"))
    checkCondition(boolRelation, TrueLiteral === And($"a", $"b"), And($"a", $"b"))
    checkCondition(boolRelation, And($"a", $"b") === FalseLiteral, Or(Not($"a"), Not($"b")))
    checkCondition(boolRelation, FalseLiteral === And($"a", $"b"), Or(Not($"a"), Not($"b")))
    checkCondition(boolRelation, IsNull($"a") <=> TrueLiteral, IsNull($"a"))
    checkCondition(boolRelation, TrueLiteral <=> IsNull($"a"), IsNull($"a"))
    checkCondition(boolRelation, IsNull($"a") <=> FalseLiteral, IsNotNull($"a"))
    checkCondition(boolRelation, FalseLiteral <=> IsNull($"a"), IsNotNull($"a"))

    // Should not optimize for nullable <=> Literal
    checkCondition(boolRelation, And($"a", $"b") <=> TrueLiteral, And($"a", $"b") <=> TrueLiteral)
    checkCondition(boolRelation, TrueLiteral <=> And($"a", $"b"), TrueLiteral <=> And($"a", $"b"))
    checkCondition(boolRelation, And($"a", $"b") <=> FalseLiteral, And($"a", $"b") <=> FalseLiteral)
    checkCondition(boolRelation, FalseLiteral <=> And($"a", $"b"), FalseLiteral <=> And($"a", $"b"))
  }

  test("Simplify binary comparison when literal is null") {
    val nullLit = Literal.create(null, IntegerType)
    Seq($"a" > nullLit, $"a" >= nullLit, $"a" === nullLit, $"a" < nullLit, $"a" <= nullLit)
      .foreach { be =>
        checkCondition(nullableRelation, be, Literal.create(null, BooleanType) && $"a".isNotNull)
      }

    checkCondition(nullableRelation, $"a" <=> nullLit, $"a".isNull)
  }

  test("SPARK-43413: IN subquery nullability") {
    // The following cases are pairs of (relation, expression)
    // Cases we should not optimize because the IN subquery is nullable
    Seq(
      // IN subquery right-hand-side (ListQuery) is nullable
      (nonNullableRelation,
        InSubquery(Seq($"a"), ListQuery(nullableRelation.select($"a"))) <=> TrueLiteral),
      (nonNullableRelation,
        InSubquery(Seq($"a"), ListQuery(nullableRelation.select($"a"))) <=> FalseLiteral),
      // Left-hand-side of the IN is nullable
      (nullableRelation,
        InSubquery(Seq($"a"), ListQuery(nonNullableRelation.select($"a"))) <=> TrueLiteral),
      (nullableRelation,
        InSubquery(Seq($"a"), ListQuery(nonNullableRelation.select($"a"))) <=> FalseLiteral),
      // Both sides of the IN are nullable
      (nullableRelation,
        InSubquery(Seq($"a"), ListQuery(nullableRelation.select($"a"))) <=> TrueLiteral),
      (nullableRelation,
        InSubquery(Seq($"a"), ListQuery(nullableRelation.select($"a"))) <=> FalseLiteral)
    ).foreach {
      case (relation, expr) =>
      checkCondition(relation, expr, expr)
    }

    // Should optimize, since the IN is non-nullable
    val inExpr = InSubquery(Seq($"a"), ListQuery(nonNullableRelation.select($"a")))
    checkCondition(nonNullableRelation, inExpr <=> FalseLiteral, Not(inExpr))

    val inExpr2 = InSubquery(Seq($"a"), ListQuery(nonNullableRelation.select($"a")))
    checkCondition(nonNullableRelation, inExpr2 <=> TrueLiteral, inExpr2)
  }
}

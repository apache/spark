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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType

class ReorderAssociativeOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("ReorderAssociativeOperator", FixedPoint(10),
        ConstantFolding,
        ReorderAssociativeOperator) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

  val typedRelation = LocalRelation($"l".long, $"s".short, $"t".byte)

  test("Reorder associative operators") {
    val originalQuery =
      testRelation
        .select(
          (Literal(3) + ((Literal(1) + $"a") + 2)) + 4,
          $"b" * 1 * 2 * 3 * 4,
          ($"b" + 1) * 2 * 3 * 4,
          $"a" + 1 + $"b" + 2 + $"c" + 3,
          $"a" + 1 + $"b" * 2 + $"c" + 3,
          Rand(0) * 1.0 * 2.0 * 3.0 * 4.0)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          ($"a" + 10).as("((3 + ((1 + a) + 2)) + 4)"),
          ($"b" * 24).as("((((b * 1) * 2) * 3) * 4)"),
          (($"b" + 1) * 24).as("((((b + 1) * 2) * 3) * 4)"),
          ($"a" + $"b" + $"c" + 6).as("(((((a + 1) + b) + 2) + c) + 3)"),
          ($"a" + $"b" * 2 + $"c" + 4).as("((((a + 1) + (b * 2)) + c) + 3)"),
          Rand(0) * 1.0 * 2.0 * 3.0 * 4.0)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nested expression with aggregate operator") {
    val originalQuery =
      testRelation.as("t1")
        .join(testRelation.as("t2"), Inner, Some("t1.a".attr === "t2.a".attr))
        .groupBy("t1.a".attr + 1, "t2.a".attr + 1)(
          (("t1.a".attr + 1) + ("t2.a".attr + 1)).as("col"))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-49915: Handle zero and one in associative operators") {
    val originalQuery =
      testRelation.select(
        $"a" + 0,
        Literal(-3) + $"a" + 3,
        $"b" * 0 * 1 * 2 * 3,
        Count($"b") * 0,
        $"b" * 1 * 1,
        ($"b" + 0) * 1 * 2 * 3 * 4,
        $"a" + 0 + $"b" + 0 + $"c" + 0,
        $"a" + 0 + $"b" * 1 + $"c" + 0
      )

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          $"a".as("(a + 0)"),
          $"a".as("((-3 + a) + 3)"),
          ($"b" * 0).as("((((b * 0) * 1) * 2) * 3)"),
          Literal(0L).as("(count(b) * 0)"),
          $"b".as("((b * 1) * 1)"),
          ($"b" * 24).as("(((((b + 0) * 1) * 2) * 3) * 4)"),
          ($"a" + $"b" + $"c").as("""(((((a + 0) + b) + 0) + c) + 0)"""),
          ($"a" + $"b" + $"c").as("((((a + 0) + (b * 1)) + c) + 0)")
        ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-50380: conditional branches with error expression") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> true.toString) {
      val originalQuery1 = testRelation.select(If($"a" === 1, 1L, Literal(1).div(0) + $"b")).analyze
      val optimized1 = Optimize.execute(originalQuery1)
      comparePlans(optimized1, originalQuery1)

      val originalQuery2 = testRelation.select(
        If($"a" === 1, 1, ($"b" + Literal(Int.MaxValue)) + 1).as("col")).analyze
      val optimized2 = Optimize.execute(originalQuery2)
      val correctAnswer2 = testRelation.select(
        If($"a" === 1, 1, $"b" + (Literal(Int.MaxValue) + 1)).as("col")).analyze
      comparePlans(optimized2, correctAnswer2)
    }
  }

  test("Do not reorder associative operators across different evaluation modes") {
    // Numeric try_add and try_multiply are replaced by TRY-mode Add and Multiply.
    val tryAdd = Add($"a", $"b", EvalMode.TRY)
    val tryMultiply = Multiply($"a", $"b", EvalMode.TRY)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> true.toString) {
      val originalQuery = testRelation.select(
        (tryAdd + 1).as("try_add_plus_1"),
        (tryMultiply * 2).as("try_multiply_times_2"),
        Add($"a" + $"b", 1, EvalMode.TRY).as("try_add_of_ansi_add"),
        Multiply($"a" * $"b", 2, EvalMode.TRY).as("try_multiply_of_ansi_multiply"),
        (tryAdd + 1 + 2).as("try_add_plus_1_plus_2")).analyze

      val optimized = Optimize.execute(originalQuery)

      val correctAnswer = testRelation.select(
        (tryAdd + 1).as("try_add_plus_1"),
        (tryMultiply * 2).as("try_multiply_times_2"),
        Add($"a" + $"b", 1, EvalMode.TRY).as("try_add_of_ansi_add"),
        Multiply($"a" * $"b", 2, EvalMode.TRY).as("try_multiply_of_ansi_multiply"),
        (tryAdd + 3).as("try_add_plus_1_plus_2")).analyze
      comparePlans(optimized, correctAnswer)
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> false.toString) {
      // ANSI-mode operators can come from a view created with ANSI enabled.
      val ansiAdd = Add($"a", $"b", EvalMode.ANSI)
      val ansiMultiply = Multiply($"a", $"b", EvalMode.ANSI)
      val originalQuery = testRelation.select(
        (ansiAdd + 1 + 2).as("ansi_add_plus_1_plus_2"),
        (ansiMultiply * 2 * 3).as("ansi_multiply_times_2_times_3"),
        Add($"a" + 1 + 2, $"b", EvalMode.ANSI).as("ansi_add_of_legacy_add"),
        Multiply($"a" * 2 * 3, $"b", EvalMode.ANSI).as("ansi_multiply_of_legacy_multiply"),
        (tryAdd + 1 + 2).as("try_add_plus_1_plus_2"),
        (tryMultiply * 2 * 3).as("try_multiply_times_2_times_3")).analyze

      val optimized = Optimize.execute(originalQuery)

      val correctAnswer = testRelation.select(
        (ansiAdd + 3).as("ansi_add_plus_1_plus_2"),
        (ansiMultiply * 6).as("ansi_multiply_times_2_times_3"),
        Add($"a" + 3, $"b", EvalMode.ANSI).as("ansi_add_of_legacy_add"),
        Multiply($"a" * 6, $"b", EvalMode.ANSI).as("ansi_multiply_of_legacy_multiply"),
        (tryAdd + 3).as("try_add_plus_1_plus_2"),
        (tryMultiply * 6).as("try_multiply_times_2_times_3")).analyze
      comparePlans(optimized, correctAnswer)
    }
  }

  test("Do not regroup TRY-mode Add and Multiply chains") {
    def tryAdd(left: Expression, right: Expression): Expression = Add(left, right, EvalMode.TRY)
    def tryMultiply(left: Expression, right: Expression): Expression =
      Multiply(left, right, EvalMode.TRY)

    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        val tryChains = Seq(
          tryAdd(tryAdd($"a", -5), $"b").as("try_add_chain"),
          tryAdd(tryAdd($"a", Int.MaxValue), 1).as("try_add_chain_of_literals"),
          tryMultiply(tryMultiply($"a", -1), $"b").as("try_multiply_chain"),
          tryAdd(tryAdd($"a", 5), -3).as("try_add_chain_of_mixed_sign_literals"),
          tryAdd(tryAdd($"a", 1), tryAdd($"b", 2)).as("try_add_chain_of_two_operands"),
          tryMultiply(tryMultiply($"a", -1), -1).as("try_multiply_chain_of_negative_literals"),
          tryMultiply(tryMultiply($"a", 2), 0).as("try_multiply_chain_with_zero"),
          tryMultiply(tryMultiply($"a", 65536), 32768).as("try_multiply_chain_overflowing_literal"),
          tryAdd(tryAdd($"a", Literal(null, IntegerType)), 1).as("try_add_chain_of_null_literal"),
          (tryAdd(tryAdd($"a", -5), $"b") + 1).as("try_add_chain_plus_1"),
          (tryMultiply(tryMultiply($"a", -1), $"b") * 3).as("try_multiply_chain_times_3"))
        val tryOperand = tryAdd(tryAdd($"a", Int.MaxValue), 1)
        val originalQuery =
          testRelation.select(tryChains :+ (Literal(-5) + tryOperand).as("minus_5_plus"): _*)
            .analyze

        val optimized = Optimize.execute(originalQuery)

        // The TRY chains keep their written grouping and literals. Only the enclosing regular
        // operator moves its own literal after the TRY operand.
        val correctAnswer =
          testRelation.select(tryChains :+ (tryOperand + -5).as("minus_5_plus"): _*).analyze
        comparePlans(optimized, correctAnswer)

        // Literals whose folded value does not fit the type of the chain stay as written.
        val typedQuery = typedRelation.select(
          tryAdd(tryAdd($"l", Long.MaxValue), 1L).as("bigint_try_add_chain"),
          tryMultiply(tryMultiply($"l", 1L << 32), 1L << 31).as("bigint_try_multiply_chain"),
          tryAdd(tryAdd($"s", Literal(Short.MaxValue)), Literal(1.toShort))
            .as("smallint_try_add_chain"),
          tryAdd(tryAdd($"t", Literal(Byte.MinValue)), Literal((-1).toByte))
            .as("tinyint_try_add_chain")).analyze
        comparePlans(Optimize.execute(typedQuery), typedQuery)
      }
    }
  }

  test("Rewrite TRY-mode chains where that cannot change the result") {
    def tryAdd(left: Expression, right: Expression): Expression = Add(left, right, EvalMode.TRY)
    def tryMultiply(left: Expression, right: Expression): Expression =
      Multiply(left, right, EvalMode.TRY)

    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        val originalQuery = testRelation.select(
          tryAdd(tryAdd($"a", 1), 2).as("positive_addends"),
          tryAdd(tryAdd($"a", -1), -2).as("negative_addends"),
          tryAdd(Literal(1), tryAdd($"a", 0)).as("literal_first"),
          tryMultiply(tryMultiply($"a", 2), 3).as("positive_multipliers"),
          tryMultiply(tryMultiply(tryMultiply($"a", 1), 2), 3).as("three_multipliers"),
          // Steps that add 0 or multiply by 1 are removed.
          tryAdd($"a", 0).as("add_0"),
          tryAdd(Literal(0), $"a").as("0_add"),
          tryMultiply($"b", 1).as("multiply_by_1"),
          tryMultiply(Literal(1), $"b").as("1_multiply"),
          tryAdd(tryAdd($"a", 0), $"b").as("add_0_then_add_operand"),
          tryAdd(tryAdd($"a", 0), 0).as("add_0_twice"),
          tryAdd(tryAdd(tryAdd($"a", 5), -3), 0).as("mixed_sign_addends_then_add_0"),
          // Literals above a step with two non-literal operands fold into one step above it.
          tryAdd(tryAdd(tryAdd($"a", $"b"), 1), 2).as("addends_above_two_operands"),
          tryMultiply(tryMultiply(tryMultiply($"a", $"b"), 2), 3)
            .as("multipliers_above_two_operands"),
          tryAdd(tryAdd(tryAdd(tryAdd($"a", 1), $"b"), 2), 3).as("addends_around_an_operand"),
          tryAdd(tryAdd(tryAdd($"a", 1), 2), -3).as("addends_below_a_mixed_sign_addend"))
          .analyze

        val optimized = Optimize.execute(originalQuery)

        val correctAnswer = testRelation.select(
          tryAdd($"a", 3).as("positive_addends"),
          tryAdd($"a", -3).as("negative_addends"),
          tryAdd($"a", 1).as("literal_first"),
          tryMultiply($"a", 6).as("positive_multipliers"),
          tryMultiply($"a", 6).as("three_multipliers"),
          $"a".as("add_0"),
          $"a".as("0_add"),
          $"b".as("multiply_by_1"),
          $"b".as("1_multiply"),
          tryAdd($"a", $"b").as("add_0_then_add_operand"),
          $"a".as("add_0_twice"),
          tryAdd(tryAdd($"a", 5), -3).as("mixed_sign_addends_then_add_0"),
          tryAdd(tryAdd($"a", $"b"), 3).as("addends_above_two_operands"),
          tryMultiply(tryMultiply($"a", $"b"), 6).as("multipliers_above_two_operands"),
          // The literal 1 between a and b is not folded with the literals above b.
          tryAdd(tryAdd(tryAdd($"a", 1), $"b"), 5).as("addends_around_an_operand"),
          tryAdd(tryAdd($"a", 3), -3).as("addends_below_a_mixed_sign_addend")).analyze
        comparePlans(optimized, correctAnswer)

        val typedQuery =
          typedRelation.select(tryAdd(tryAdd($"l", Int.MaxValue.toLong), 1L).as("bigint")).analyze
        comparePlans(
          Optimize.execute(typedQuery),
          typedRelation.select(tryAdd($"l", Int.MaxValue.toLong + 1).as("bigint")).analyze)

        // Without the identity step, the filter is on the column itself and can be pushed down.
        val filterQuery = testRelation.where(tryAdd($"a", 0) > 5).analyze
        comparePlans(Optimize.execute(filterQuery), testRelation.where($"a" > 5).analyze)
      }
    }
  }

  test("Fold operators whose evaluation contexts differ only in decimal precision loss") {
    val noPrecisionLoss = NumericEvalContext(EvalMode.ANSI, allowDecimalPrecisionLoss = false)
    val precisionLoss = NumericEvalContext(EvalMode.ANSI, allowDecimalPrecisionLoss = true)
    val originalQuery = testRelation.select(
      Add(Add($"a", 1, noPrecisionLoss), 2, precisionLoss).as("add"),
      Multiply(Multiply($"a", 2, noPrecisionLoss), 3, precisionLoss).as("multiply")).analyze

    val optimized = Optimize.execute(originalQuery)

    val correctAnswer = testRelation.select(
      Add($"a", 3, precisionLoss).as("add"),
      Multiply($"a", 6, precisionLoss).as("multiply")).analyze
    comparePlans(optimized, correctAnswer)
  }
}

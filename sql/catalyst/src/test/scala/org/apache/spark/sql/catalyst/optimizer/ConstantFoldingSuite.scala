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

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.ByteArray

class ConstantFoldingSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("ConstantFolding", Once,
        OptimizeIn,
        ConstantFolding,
        BooleanSimplification) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery("y")
        .select($"a")

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select($"a".attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  /**
   * Unit tests for constant folding in expressions.
   */
  test("Constant folding test: expressions only have literals") {
    val originalQuery =
      testRelation
        .select(
          Literal(2) + Literal(3) + Literal(4) as "2+3+4",
          Literal(2) * Literal(3) + Literal(4) as "2*3+4",
          Literal(2) * (Literal(3) + Literal(4)) as "2*(3+4)")
        .where(
          Literal(1) === Literal(1) &&
          Literal(2) > Literal(3) ||
          Literal(3) > Literal(2) )
        .groupBy(
          Literal(2) * Literal(3) - Literal(6) / (Literal(4) - Literal(2))
        )(Literal(9) / Literal(3) as "9/3")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(9) as "2+3+4",
          Literal(10) as "2*3+4",
          Literal(14) as "2*(3+4)")
        .where(Literal(true))
        .groupBy(Literal(3.0))(Literal(3.0) as "9/3")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have attribute references and literals in " +
    "arithmetic operations") {
    val originalQuery =
      testRelation
        .select(
          Literal(2) + Literal(3) + $"a" as "c1",
          $"a" + Literal(2) + Literal(3) as "c2",
          Literal(2) * $"a" + Literal(4) as "c3",
          $"a" * (Literal(3) + Literal(4)) as "c4")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(5) + $"a" as "c1",
          $"a" + Literal(2) + Literal(3) as "c2",
          Literal(2) * $"a" + Literal(4) as "c3",
          $"a" * Literal(7) as "c4")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have attribute references and literals in " +
    "predicates") {
    val originalQuery =
      testRelation
        .where(
          (($"a" > 1 && Literal(1) === Literal(1)) ||
           ($"a" < 10 && Literal(1) === Literal(2)) ||
           (Literal(1) === Literal(1) && $"b" > 1) ||
           (Literal(1) === Literal(2) && $"b" < 10)) &&
           (($"a" > 1 || Literal(1) === Literal(1)) &&
            ($"a" < 10 || Literal(1) === Literal(2)) &&
            (Literal(1) === Literal(1) || $"b" > 1) &&
            (Literal(1) === Literal(2) || $"b" < 10)))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .where(($"a" > 1 || $"b" > 1) && ($"a" < 10 && $"b" < 10))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have foldable functions") {
    val originalQuery =
      testRelation
        .select(
          Cast(Literal("2"), IntegerType) + Literal(3) + $"a" as "c1",
          Coalesce(Seq(TryCast(Literal("abc"), IntegerType).replacement, Literal(3))) as "c2")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(5) + $"a" as "c1",
          Literal(3) as "c2")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have nonfoldable functions") {
    val originalQuery =
      testRelation
        .select(
          Rand(5L) + Literal(1) as "c1",
          sum($"a") as "c2")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Rand(5L) + Literal(1.0) as "c1",
          sum($"a") as "c2")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have null literals") {
    val originalQuery = testRelation.select(
      IsNull(Literal(null)) as "c1",
      IsNotNull(Literal(null)) as "c2",

      UnresolvedExtractValue(Literal.create(null, ArrayType(IntegerType)), 1) as "c3",
      UnresolvedExtractValue(
        Literal.create(Seq(1), ArrayType(IntegerType)),
        Literal.create(null, IntegerType)) as "c4",
      UnresolvedExtractValue(
        Literal.create(null, StructType(Seq(StructField("a", IntegerType, true)))),
        "a") as "c5",

      UnaryMinus(Literal.create(null, IntegerType)) as "c6",
      Cast(Literal(null), IntegerType) as "c7",
      Not(Literal.create(null, BooleanType)) as "c8",

      Add(Literal.create(null, IntegerType), 1) as "c9",
      Add(1, Literal.create(null, IntegerType)) as "c10",

      EqualTo(Literal.create(null, IntegerType), 1) as "c11",
      EqualTo(1, Literal.create(null, IntegerType)) as "c12",

      new Like(Literal.create(null, StringType), "abc") as "c13",
      new Like("abc", Literal.create(null, StringType)) as "c14",

      Upper(Literal.create(null, StringType)) as "c15",

      Substring(Literal.create(null, StringType), 0, 1) as "c16",
      Substring("abc", Literal.create(null, IntegerType), 1) as "c17",
      Substring("abc", 0, Literal.create(null, IntegerType)) as "c18",

      Contains(Literal.create(null, StringType), "abc") as "c19",
      Contains("abc", Literal.create(null, StringType)) as "c20"
    )

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(true) as "c1",
          Literal(false) as "c2",

          Literal.create(null, IntegerType) as "c3",
          Literal.create(null, IntegerType) as "c4",
          Literal.create(null, IntegerType) as "c5",

          Literal.create(null, IntegerType) as "c6",
          Literal.create(null, IntegerType) as "c7",
          Literal.create(null, BooleanType) as "c8",

          Literal.create(null, IntegerType) as "c9",
          Literal.create(null, IntegerType) as "c10",

          Literal.create(null, BooleanType) as "c11",
          Literal.create(null, BooleanType) as "c12",

          Literal.create(null, BooleanType) as "c13",
          Literal.create(null, BooleanType) as "c14",

          Literal.create(null, StringType) as "c15",

          Literal.create(null, StringType) as "c16",
          Literal.create(null, StringType) as "c17",
          Literal.create(null, StringType) as "c18",

          Literal.create(null, BooleanType) as "c19",
          Literal.create(null, BooleanType) as "c20"
        ).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: Fold In(v, list) into true or false") {
    val originalQuery =
      testRelation
        .select($"a")
        .where(In(Literal(1), Seq(Literal(1), Literal(2))))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select($"a")
        .where(Literal(true))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33544: Constant folding test with side effects") {
    val originalQuery =
      testRelation
        .select($"a")
        .where(Size(CreateArray(Seq(AssertTrue(false)))) > 0)

    val optimized = Optimize.execute(originalQuery.analyze)
    comparePlans(optimized, originalQuery.analyze)
  }

  object OptimizeForCreate extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("ConstantFolding", FixedPoint(4),
        OptimizeIn,
        ConstantFolding,
        PruneFilters) :: Nil
  }

  test("SPARK-33544: Constant folding test CreateArray") {
    val originalQuery =
      testRelation
        .select($"a")
        .where(Size(CreateArray(Seq($"a"))) > 0)

    val optimized = OptimizeForCreate.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select($"a")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-37907: InvokeLike support ConstantFolding") {
    val originalQuery =
      testRelation
        .select(
          StaticInvoke(
            classOf[ByteArray],
            BinaryType,
            "lpad",
            Seq(Literal("Spark".getBytes), Literal(7), Literal("W".getBytes)),
            Seq(BinaryType, IntegerType, BinaryType),
            returnNullable = false).as("c1"),
          Invoke(
            Literal.create("a", StringType),
            "substring",
            StringType,
            Seq(Literal(0), Literal(1))).as("c2"))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal("WWSpark".getBytes()).as("c1"),
          Literal.create("a", StringType).as("c2"))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-39106: Correct conditional expression constant folding") {
    val t = LocalRelation.fromExternalRows(
      $"c".double :: Nil,
      Row(1d) :: Row(null) :: Row(Double.NaN) :: Nil)

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      // conditional expression is foldable, throw exception during query compilation
      Seq(
        t.select(CaseWhen((Divide(1, 0) === 1, Add(1, 0)) :: Nil, Subtract(1, 0))),
        t.select(If(Divide(1, 0) === 1, Add(1, 0), Add(1, 0))),
        t.select(Coalesce(Divide(1, 0) :: Add(1, 0) :: Nil)),
        t.select(NaNvl(Divide(1, 0), Add(1, 0)))
      ).foreach { query =>
        intercept[SparkArithmeticException] {
          Optimize.execute(query.analyze)
        }
      }

      // conditional expression is not foldable, suppress the exception during query compilation
      Seq(
        t.select(CaseWhen(($"c" === 1d, Divide(1, 0)) :: Nil, 1d)),
        t.select(If($"c" === 1d, Divide(1, 0), 1d)),
        t.select(Coalesce($"c" :: Divide(1, 0) :: Nil)),
        t.select(NaNvl($"c", Divide(1, 0)))
      ).foreach { query =>
        val optimized = Optimize.execute(query.analyze)
        val failedToEvaluated = optimized.expressions.flatMap(_.collect {
          case e: Expression if e.getTagValue(ConstantFolding.FAILED_TO_EVALUATE).isDefined => e
        })
        assert(failedToEvaluated.size == 1)
        comparePlans(query.analyze, optimized)
      }
    }
  }
}

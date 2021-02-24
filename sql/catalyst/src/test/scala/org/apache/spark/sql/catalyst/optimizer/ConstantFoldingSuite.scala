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

import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types._

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

  val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)

  test("eliminate subqueries") {
    val originalQuery =
      testRelation
        .subquery("y")
        .select("a".attr)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select("a".attr)
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
          Literal(2) + Literal(3) + "a".attr as "c1",
          "a".attr + Literal(2) + Literal(3) as "c2",
          Literal(2) * "a".attr + Literal(4) as "c3",
          "a".attr * (Literal(3) + Literal(4)) as "c4")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(5) + "a".attr as "c1",
          "a".attr + Literal(2) + Literal(3) as "c2",
          Literal(2) * "a".attr + Literal(4) as "c3",
          "a".attr * Literal(7) as "c4")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have attribute references and literals in " +
    "predicates") {
    val originalQuery =
      testRelation
        .where(
          (("a".attr > 1 && Literal(1) === Literal(1)) ||
           ("a".attr < 10 && Literal(1) === Literal(2)) ||
           (Literal(1) === Literal(1) && "b".attr > 1) ||
           (Literal(1) === Literal(2) && "b".attr < 10)) &&
           (("a".attr > 1 || Literal(1) === Literal(1)) &&
            ("a".attr < 10 || Literal(1) === Literal(2)) &&
            (Literal(1) === Literal(1) || "b".attr > 1) &&
            (Literal(1) === Literal(2) || "b".attr < 10)))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .where(("a".attr > 1 || "b".attr > 1) && ("a".attr < 10 && "b".attr < 10))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have foldable functions") {
    val originalQuery =
      testRelation
        .select(
          Cast(Literal("2"), IntegerType) + Literal(3) + "a".attr as "c1",
          Coalesce(Seq(Cast(Literal("abc"), IntegerType), Literal(3))) as "c2")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Literal(5) + "a".attr as "c1",
          Literal(3) as "c2")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have nonfoldable functions") {
    val originalQuery =
      testRelation
        .select(
          Rand(5L) + Literal(1) as "c1",
          sum("a".attr) as "c2")

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          Rand(5L) + Literal(1.0) as "c1",
          sum("a".attr) as "c2")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Constant folding test: expressions have null literals") {
    val originalQuery = testRelation.select(
      IsNull(Literal(null)) as "c1",
      IsNotNull(Literal(null)) as "c2",

      UnresolvedExtractValue(Literal.create(null, ArrayType(IntegerType)), 1) as "c3",
      UnresolvedExtractValue(
        Literal.create(Seq(1), ArrayType(IntegerType)), Literal.create(null, IntegerType)) as "c4",
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
        .select("a".attr)
        .where(In(Literal(1), Seq(Literal(1), Literal(2))))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select("a".attr)
        .where(Literal(true))
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-33544: Constant folding test with side effects") {
    val originalQuery =
      testRelation
        .select("a".attr)
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
        .select("a".attr)
        .where(Size(CreateArray(Seq("a".attr))) > 0)

    val optimized = OptimizeForCreate.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select("a".attr)
        .analyze

    comparePlans(optimized, correctAnswer)
  }
}

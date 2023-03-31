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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StringType}

class SimplifyConditionalsInFilterSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("SimplifyConditionalsInFilter", FixedPoint(10),
        NullPropagation,
        BooleanSimplification,
        ConstantFolding,
        SimplifyConditionals,
        PushFoldableIntoBranches,
        RemoveDispensableExpressions,
        SimplifyBinaryComparison,
        ReplaceNullWithFalseInPredicate,
        SimplifyConditionalsInPredicate,
        SimplifyConditionalsInFilter) :: Nil
  }

  private val testRelation =
    LocalRelation('i.int,
      's1.string.withNullability(true),
      's2.string.withNullability(true),
      's3.string.withNullability(true),
      's4.string.withNullability(true))


  test("SimplifyConditionalsInFilter - Eliminate duplicates") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v1")), Literal("then11")),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))), None)

    val originalCond = EqualTo(cw1, Literal("then1"))
    val expectedCond = EqualTo('s1, Literal("s1v1"))

    testFilter(originalCond, expectedCond)
  }

  test("SimplifyConditionalsInFilter - Else null") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))),
      Some(Literal(null, StringType)))

    val originalCond = EqualTo(cw1, Literal("then1"))
    val expectedCond = EqualTo('s1, Literal("s1v1"))

    testFilter(originalCond, expectedCond)
  }

  test("SimplifyConditionalsInFilter - Merge conditionals 1") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v11")), Literal("then1")),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))), None)

    val originalCond = EqualTo(cw1, Literal("then1"))
    val expectedCond = In('s1, Seq(Literal("s1v1"), Literal("s1v11")))

    testFilter(originalCond, expectedCond)
  }

  test("SimplifyConditionalsInFilter - Merge conditionals 2") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v2")), Literal("then2")),
      (EqualTo('s1, Literal("s1v11")), Literal("then1"))), None)

    val originalCond = EqualTo(cw1, Literal("then1"))
    val expectedCond = In('s1, Seq(Literal("s1v1"), Literal("s1v11")))

    testFilter(originalCond, expectedCond)
  }

  test("SimplifyConditionalsInFilter - Merge conditionals 3") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s2, Literal("s2v2")), Literal("then2")),
      (EqualTo('s1, Literal("s1v11")), Literal("then1"))), None)

    val originalCond = EqualTo(cw1, Literal("then1"))
    val expectedCond = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), TrueLiteral),
      (EqualTo('s2, Literal("s2v2")), FalseLiteral),
      (EqualTo('s1, Literal("s1v11")), TrueLiteral)), Some(FalseLiteral))

    testFilter(originalCond, expectedCond)
  }

  test("SimplifyConditionalsInFilter - Merge conditionals 4") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v0")), Literal.create(null, StringType)),
      (EqualTo('s1, Literal("s1v00")), Literal.create(null, StringType)),
      (EqualTo('s1, Literal("s1v1")), Literal("then1"))), None)

    val originalCond = EqualTo(cw1, Literal("then1"))
    val expectedCond = EqualTo('s1, Literal("s1v1"))

    testFilter(originalCond, expectedCond)
  }

  test("Simplify conditionals in Filter - In") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v3")), Literal("then3")),
      (EqualTo('s1, Literal("s1v0")), Literal.create(null, StringType)),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))), None)

    val originalCond = In(cw1, Seq("then1", "then2"))
    val expectedCond = In('s1, Seq(Literal("s1v1"), Literal("s1v2")))

    testFilter(originalCond, expectedCond)
  }

  test("Simplify conditionals in Filter - And") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v0")), Literal.create(null, StringType)),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))), None)

    val expr1 = UnresolvedAttribute("i") > Literal(10)
    val expr2 = EqualTo(cw1, "then1")

    val originalCond = And(expr1, expr2)
    val expectedCond = And(expr1, EqualTo('s1, Literal("s1v1")))

    testFilter(originalCond, expectedCond)
  }

  test("Simplify conditionals in Filter - Or") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v0")), Literal(null, StringType)),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))), None)

    val expr1 = UnresolvedAttribute("i") > Literal(10)
    val expr2 = EqualTo(cw1, "then1")

    val originalCond = Or(expr1, expr2)
    val expectedCond = Or(expr1, EqualTo('s1, Literal("s1v1")))

    testFilter(originalCond, expectedCond)
  }

  test("Simplify conditionals in Filter - Coalesce") {
    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v0")), Literal(null, StringType)),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))), None)

    val cw2 = CaseWhen(Seq((EqualTo('s2, Literal("s2v1")), Literal("then1")),
      (EqualTo('s2, Literal("s2v0")), Literal(null, StringType)),
      (EqualTo('s2, Literal("s2v2")), Literal("then2"))), None)

    val cls1 = Coalesce(Seq(cw1, cw2))

    val originalCond = EqualTo(cls1, Literal("then1"))
    val expectedCond = Coalesce(Seq(CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), TrueLiteral),
        (EqualTo('s1, Literal("s1v0")), Literal.create(null, BooleanType)),
        (EqualTo('s1, Literal("s1v2")), FalseLiteral)), None),
      CaseWhen(Seq((EqualTo('s2, Literal("s2v1")), TrueLiteral),
        (EqualTo('s2, Literal("s2v0")), Literal.create(null, BooleanType)),
        (EqualTo('s2, Literal("s2v2")), FalseLiteral)), None)))

    testFilter(originalCond, expectedCond)
  }


  test("Simplify conditionals in Filter - Collapse conditionals 1") {

    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("s1then1")),
      (EqualTo('s1, Literal("s1v0")), Literal(null, StringType)),
      (EqualTo('s1, Literal("s1v2")), Literal("s1then2"))), None)

    val cw2 = CaseWhen(Seq((EqualTo('s2, Literal("s2v11")), Literal("s2then1")),
      (EqualTo('s2, Literal("s2v00")), Literal(null, StringType)),
      (EqualTo('s2, Literal("s2v22")), Literal("s2then2"))), None)

    val cw3 = CaseWhen(Seq((EqualTo(cw1, Literal("s1then1")), Literal("then1")),
      (EqualTo(cw2, Literal("s2then2")), Literal("then2"))), None)

    val originalCond = EqualTo(cw3, Literal("then1"))
    val expectedCond = EqualTo('s1, Literal("s1v1"))

    testFilter(originalCond, expectedCond)
  }

  test("Simplify conditionals in Filter - Collapse conditionals 2") {

    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("s1then1")),
      (EqualTo('s2, Literal("s2v0")), Literal.create(null, StringType)),
      (EqualTo('s3, Literal("s3v1")), Literal("s3then1"))), None)

    val cw2 = CaseWhen(Seq((EqualTo('s2, Literal("s2v11")), Literal("s2then1")),
      (EqualTo('s3, Literal("s3v00")), Literal.create(null, StringType)),
      (EqualTo('s4, Literal("s4v11")), Literal("s4then1"))), None)

    val cw3 = CaseWhen(Seq((EqualTo(cw1, Literal("s1then1")), Literal("then1")),
      (EqualTo(cw2, Literal("s2then1")), Literal("then1"))), None)

    val originalCond = EqualTo(cw3, Literal("then1"))
    val expectedCond = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), TrueLiteral),
      (EqualTo('s2, Literal("s2v11")), TrueLiteral)), Some(FalseLiteral))

    testFilter(originalCond, expectedCond)
  }

  test("Simplify conditionals in Filter - Collapse conditionals 3") {

    val cw1 = CaseWhen(Seq((EqualTo('s1, Literal("s1v1")), Literal("then1")),
      (EqualTo('s1, Literal("s1v2")), Literal("then2"))), None)

    val originalCond = IsNull(cw1)
    val expectedCond = Not(In('s1, Seq(Literal("s1v1"), Literal("s1v2"))))

    testFilter(originalCond, expectedCond)
  }

  private def testFilter(originalCond: Expression, expectedCond: Expression): Unit = {
    test((rel, exp) => rel.where(exp), originalCond, expectedCond)
  }

  private def test(
      func: (LogicalPlan, Expression) => LogicalPlan,
      originalExpr: Expression,
      expectedExpr: Expression): Unit = //
    withSQLConf(SQLConf.OPTIMIZER_MAX_CONDITIONALS_INFILTER.key -> "0") {
      val originalPlan = func(testRelation, originalExpr).analyze
      val optimizedPlan = Optimize.execute(originalPlan)
      val expectedPlan = func(testRelation, expectedExpr).analyze
      comparePlans(optimizedPlan, expectedPlan)
    }
}

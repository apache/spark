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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{BooleanType, IntegerType}


class PushFoldableIntoBranchesSuite
  extends PlanTest with ExpressionEvalHelper with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PushFoldableIntoBranches", FixedPoint(50),
      BooleanSimplification, ConstantFolding, SimplifyConditionals, PushFoldableIntoBranches) :: Nil
  }

  private val relation = LocalRelation('a.int, 'b.int, 'c.boolean)
  private val a = EqualTo(UnresolvedAttribute("a"), Literal(100))
  private val b = UnresolvedAttribute("b")
  private val c = EqualTo(UnresolvedAttribute("c"), Literal(true))
  private val ifExp = If(a, Literal(2), Literal(3))
  private val caseWhen = CaseWhen(Seq((a, Literal(1)), (c, Literal(2))), Some(Literal(3)))

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, relation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, relation).analyze)
    comparePlans(actual, correctAnswer)
  }

  private val normalBranch = (NonFoldableLiteral(true), Literal(10))

  test("SPARK-33798: Push down EqualTo through If") {
    assertEquivalent(EqualTo(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(EqualTo(ifExp, Literal(3)), If(a, FalseLiteral, TrueLiteral))
    assertEquivalent(EqualTo(ifExp, Literal("4")), FalseLiteral)
    assertEquivalent(EqualTo(ifExp, Literal("3")), If(a, FalseLiteral, TrueLiteral))

    // Do not simplify if it contains non foldable expressions.
    assertEquivalent(
      EqualTo(If(a, b, Literal(2)), Literal(2)),
      EqualTo(If(a, b, Literal(2)), Literal(2)))

    // Do not simplify if it contains non-deterministic expressions.
    val nonDeterministic = If(LessThan(Rand(1), Literal(0.5)), Literal(1), Literal(1))
    assert(!nonDeterministic.deterministic)
    assertEquivalent(EqualTo(nonDeterministic, Literal(-1)), EqualTo(nonDeterministic, Literal(-1)))

    // Handle Null values.
    assertEquivalent(
      EqualTo(If(a, Literal(null, IntegerType), Literal(1)), Literal(1)),
      If(a, Literal(null, BooleanType), TrueLiteral))
    assertEquivalent(
      EqualTo(If(a, Literal(null, IntegerType), Literal(1)), Literal(2)),
      If(a, Literal(null, BooleanType), FalseLiteral))
    assertEquivalent(
      EqualTo(If(a, Literal(1), Literal(2)), Literal(null, IntegerType)),
      Literal(null, BooleanType))
    assertEquivalent(
      EqualTo(If(a, Literal(null, IntegerType), Literal(null, IntegerType)), Literal(1)),
      Literal(null, BooleanType))
  }

  test("SPARK-33798: Push down other BinaryComparison through If") {
    assertEquivalent(EqualNullSafe(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThan(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThanOrEqual(ifExp, Literal(4)), FalseLiteral)
    assertEquivalent(LessThan(ifExp, Literal(4)), TrueLiteral)
    assertEquivalent(LessThanOrEqual(ifExp, Literal(4)), TrueLiteral)
  }

  test("SPARK-33798: Push down EqualTo through CaseWhen") {
    assertEquivalent(EqualTo(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(EqualTo(caseWhen, Literal(3)),
      CaseWhen(Seq((a, FalseLiteral), (c, FalseLiteral)), Some(TrueLiteral)))
    assertEquivalent(EqualTo(caseWhen, Literal("4")), FalseLiteral)
    assertEquivalent(EqualTo(caseWhen, Literal("3")),
      CaseWhen(Seq((a, FalseLiteral), (c, FalseLiteral)), Some(TrueLiteral)))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal("1")), (c, Literal("2"))), None), Literal("4")),
      CaseWhen(Seq((a, FalseLiteral), (c, FalseLiteral)), None))

    assertEquivalent(
      And(EqualTo(caseWhen, Literal(5)), EqualTo(caseWhen, Literal(6))),
      FalseLiteral)

    // Do not simplify if it contains non foldable expressions.
    assertEquivalent(EqualTo(caseWhen, NonFoldableLiteral(true)),
      EqualTo(caseWhen, NonFoldableLiteral(true)))
    val nonFoldable = CaseWhen(Seq(normalBranch, (a, b)), None)
    assertEquivalent(EqualTo(nonFoldable, Literal(1)), EqualTo(nonFoldable, Literal(1)))

    // Do not simplify if it contains non-deterministic expressions.
    val nonDeterministic = CaseWhen(Seq((LessThan(Rand(1), Literal(0.5)), Literal(1))), Some(b))
    assert(!nonDeterministic.deterministic)
    assertEquivalent(EqualTo(nonDeterministic, Literal(-1)), EqualTo(nonDeterministic, Literal(-1)))

    // Handle Null values.
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(1))), Literal(2)),
      CaseWhen(Seq((a, Literal(null, BooleanType))), Some(FalseLiteral)))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(1))), Some(Literal(2))), Literal(null, IntegerType)),
      Literal(null, BooleanType))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(1))), Literal(1)),
      CaseWhen(Seq((a, Literal(null, BooleanType))), Some(TrueLiteral)))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(null, IntegerType))),
        Literal(1)),
      Literal(null, BooleanType))
    assertEquivalent(
      EqualTo(CaseWhen(Seq((a, Literal(null, IntegerType))), Some(Literal(null, IntegerType))),
        Literal(null, IntegerType)),
      Literal(null, BooleanType))
  }

  test("SPARK-33798: Push down other BinaryComparison through CaseWhen") {
    assertEquivalent(EqualNullSafe(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThan(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(GreaterThanOrEqual(caseWhen, Literal(4)), FalseLiteral)
    assertEquivalent(LessThan(caseWhen, Literal(4)), TrueLiteral)
    assertEquivalent(LessThanOrEqual(caseWhen, Literal(4)), TrueLiteral)
  }
}

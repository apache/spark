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
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.{IntegerType, NullType}


class SimplifyConditionalSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SimplifyConditionals", FixedPoint(50),
      BooleanSimplification, ConstantFolding, SimplifyConditionals) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation()).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation()).analyze)
    comparePlans(actual, correctAnswer)
  }

  private val trueBranch = (TrueLiteral, Literal(5))
  private val normalBranch = (NonFoldableLiteral(true), Literal(10))
  private val unreachableBranch = (FalseLiteral, Literal(20))
  private val nullBranch = (Literal.create(null, NullType), Literal(30))

  val isNotNullCond = IsNotNull(UnresolvedAttribute(Seq("a")))
  val isNullCond = IsNull(UnresolvedAttribute("b"))
  val notCond = Not(UnresolvedAttribute("c"))

  test("simplify if") {
    assertEquivalent(
      If(TrueLiteral, Literal(10), Literal(20)),
      Literal(10))

    assertEquivalent(
      If(FalseLiteral, Literal(10), Literal(20)),
      Literal(20))

    assertEquivalent(
      If(Literal.create(null, NullType), Literal(10), Literal(20)),
      Literal(20))
  }

  test("remove unnecessary if when the outputs are semantic equivalence") {
    assertEquivalent(
      If(IsNotNull(UnresolvedAttribute("a")),
        Subtract(Literal(10), Literal(1)),
        Add(Literal(6), Literal(3))),
      Literal(9))

    // For non-deterministic condition, we don't remove the `If` statement.
    assertEquivalent(
      If(GreaterThan(Rand(0), Literal(0.5)),
        Subtract(Literal(10), Literal(1)),
        Add(Literal(6), Literal(3))),
      If(GreaterThan(Rand(0), Literal(0.5)),
        Literal(9),
        Literal(9)))
  }

  test("remove unreachable branches") {
    // i.e. removing branches whose conditions are always false
    assertEquivalent(
      CaseWhen(unreachableBranch :: normalBranch :: unreachableBranch :: nullBranch :: Nil, None),
      CaseWhen(normalBranch :: Nil, None))
  }

  test("remove entire CaseWhen if only the else branch is reachable") {
    assertEquivalent(
      CaseWhen(unreachableBranch :: unreachableBranch :: nullBranch :: Nil, Some(Literal(30))),
      Literal(30))

    assertEquivalent(
      CaseWhen(unreachableBranch :: unreachableBranch :: Nil, None),
      Literal.create(null, IntegerType))
  }

  test("remove entire CaseWhen if the first branch is always true") {
    assertEquivalent(
      CaseWhen(trueBranch :: normalBranch :: nullBranch :: Nil, None),
      Literal(5))

    // Test branch elimination and simplification in combination
    assertEquivalent(
      CaseWhen(unreachableBranch :: unreachableBranch :: nullBranch :: trueBranch :: normalBranch
        :: Nil, None),
      Literal(5))

    // Make sure this doesn't trigger if there is a non-foldable branch before the true branch
    assertEquivalent(
      CaseWhen(normalBranch :: trueBranch :: normalBranch :: Nil, None),
      CaseWhen(normalBranch :: trueBranch :: Nil, None))
  }

  test("simplify CaseWhen, prune branches following a definite true") {
    assertEquivalent(
      CaseWhen(normalBranch :: unreachableBranch ::
        unreachableBranch :: nullBranch ::
        trueBranch :: normalBranch ::
        Nil,
        None),
      CaseWhen(normalBranch :: trueBranch :: Nil, None))
  }

  test("simplify CaseWhen if all the outputs are semantic equivalence") {
    // When the conditions in `CaseWhen` are all deterministic, `CaseWhen` can be removed.
    assertEquivalent(
      CaseWhen((isNotNullCond, Subtract(Literal(3), Literal(2))) ::
        (isNullCond, Literal(1)) ::
        (notCond, Add(Literal(6), Literal(-5))) ::
        Nil,
        Add(Literal(2), Literal(-1))),
      Literal(1)
    )

    // For non-deterministic conditions, we don't remove the `CaseWhen` statement.
    assertEquivalent(
      CaseWhen((GreaterThan(Rand(0), Literal(0.5)), Subtract(Literal(3), Literal(2))) ::
        (LessThan(Rand(1), Literal(0.5)), Literal(1)) ::
        (EqualTo(Rand(2), Literal(0.5)), Add(Literal(6), Literal(-5))) ::
        Nil,
        Add(Literal(2), Literal(-1))),
      CaseWhen((GreaterThan(Rand(0), Literal(0.5)), Literal(1)) ::
        (LessThan(Rand(1), Literal(0.5)), Literal(1)) ::
        (EqualTo(Rand(2), Literal(0.5)), Literal(1)) ::
        Nil,
        Literal(1))
    )

    // When we have mixture of deterministic and non-deterministic conditions, we remove
    // the deterministic conditions from the tail until a non-deterministic one is seen.
    assertEquivalent(
      CaseWhen((GreaterThan(Rand(0), Literal(0.5)), Subtract(Literal(3), Literal(2))) ::
        (NonFoldableLiteral(true), Add(Literal(2), Literal(-1))) ::
        (LessThan(Rand(1), Literal(0.5)), Literal(1)) ::
        (NonFoldableLiteral(true), Add(Literal(6), Literal(-5))) ::
        (NonFoldableLiteral(false), Literal(1)) ::
        Nil,
        Add(Literal(2), Literal(-1))),
      CaseWhen((GreaterThan(Rand(0), Literal(0.5)), Literal(1)) ::
        (NonFoldableLiteral(true), Literal(1)) ::
        (LessThan(Rand(1), Literal(0.5)), Literal(1)) ::
        Nil,
        Literal(1))
    )
  }
}

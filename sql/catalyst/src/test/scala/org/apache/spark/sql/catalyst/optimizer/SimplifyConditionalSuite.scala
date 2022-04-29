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


class SimplifyConditionalSuite extends PlanTest with ExpressionEvalHelper with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("SimplifyConditionals", FixedPoint(50),
      BooleanSimplification, ConstantFolding, SimplifyConditionals) :: Nil
  }

  private val relation = LocalRelation($"a".int, $"b".int, $"c".boolean)

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, relation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, relation).analyze)
    comparePlans(actual, correctAnswer)
  }

  private val trueBranch = (TrueLiteral, Literal(5))
  private val normalBranch = (NonFoldableLiteral(true), Literal(10))
  private val unreachableBranch = (FalseLiteral, Literal(20))
  private val nullBranch = (Literal.create(null, BooleanType), Literal(30))

  test("simplify if") {
    assertEquivalent(
      If(TrueLiteral, Literal(10), Literal(20)),
      Literal(10))

    assertEquivalent(
      If(FalseLiteral, Literal(10), Literal(20)),
      Literal(20))

    assertEquivalent(
      If(Literal.create(null, BooleanType), Literal(10), Literal(20)),
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
      CaseWhen(($"a".isNotNull, Subtract(Literal(3), Literal(2))) ::
        ($"b".isNull, Literal(1)) ::
        (!$"c", Add(Literal(6), Literal(-5))) ::
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

  test("simplify if when one clause is null and another is boolean") {
    val p = IsNull($"a")
    val nullLiteral = Literal(null, BooleanType)
    assertEquivalent(If(p, nullLiteral, FalseLiteral), And(p, nullLiteral))
    assertEquivalent(If(p, nullLiteral, TrueLiteral), Or(IsNotNull($"a"), nullLiteral))
    assertEquivalent(If(p, FalseLiteral, nullLiteral), And(IsNotNull($"a"), nullLiteral))
    assertEquivalent(If(p, TrueLiteral, nullLiteral), Or(p, nullLiteral))

    // the rule should not apply to nullable predicate
    Seq(TrueLiteral, FalseLiteral).foreach { b =>
      assertEquivalent(If(GreaterThan($"a", 42), nullLiteral, b),
        If(GreaterThan($"a", 42), nullLiteral, b))
      assertEquivalent(If(GreaterThan($"a", 42), b, nullLiteral),
        If(GreaterThan($"a", 42), b, nullLiteral))
    }

    // check evaluation also
    Seq(TrueLiteral, FalseLiteral).foreach { b =>
      checkEvaluation(If(b, nullLiteral, FalseLiteral), And(b, nullLiteral).eval(EmptyRow))
      checkEvaluation(If(b, nullLiteral, TrueLiteral), Or(Not(b), nullLiteral).eval(EmptyRow))
      checkEvaluation(If(b, FalseLiteral, nullLiteral), And(Not(b), nullLiteral).eval(EmptyRow))
      checkEvaluation(If(b, TrueLiteral, nullLiteral), Or(b, nullLiteral).eval(EmptyRow))
    }

    // should have no effect on expressions with nullable if condition
    assert((Factorial(5) > 100L).nullable)
    Seq(TrueLiteral, FalseLiteral).foreach { b =>
      checkEvaluation(If(Factorial(5) > 100L, nullLiteral, b),
        If(Factorial(5) > 100L, nullLiteral, b).eval(EmptyRow))
      checkEvaluation(If(Factorial(5) > 100L, b, nullLiteral),
        If(Factorial(5) > 100L, b, nullLiteral).eval(EmptyRow))
    }
  }

  test("SPARK-33845: remove unnecessary if when the outputs are boolean type") {
    // verify the boolean equivalence of all transformations involved
    val fields = Seq(
      $"cond".boolean.notNull,
      $"cond_nullable".boolean,
      $"a".boolean,
      $"b".boolean
    )
    val Seq(cond, cond_nullable, a, b) = fields.zipWithIndex.map { case (f, i) => f.at(i) }

    val exprs = Seq(
      // actual expressions of the transformations: original -> transformed
      If(cond, true, false) -> cond,
      If(cond, false, true) -> !cond,
      If(cond_nullable, true, false) -> (cond_nullable <=> true),
      If(cond_nullable, false, true) -> (!(cond_nullable <=> true)))

    // check plans
    for ((originalExpr, expectedExpr) <- exprs) {
      assertEquivalent(originalExpr, expectedExpr)
    }

    // check evaluation
    val binaryBooleanValues = Seq(true, false)
    val ternaryBooleanValues = Seq(true, false, null)
    for (condVal <- binaryBooleanValues;
         condNullableVal <- ternaryBooleanValues;
         aVal <- ternaryBooleanValues;
         bVal <- ternaryBooleanValues;
         (originalExpr, expectedExpr) <- exprs) {
      val inputRow = create_row(condVal, condNullableVal, aVal, bVal)
      val optimizedVal = evaluateWithoutCodegen(expectedExpr, inputRow)
      checkEvaluation(originalExpr, optimizedVal, inputRow)
    }
  }

  test("SPARK-33847: Remove the CaseWhen if elseValue is empty and other outputs are null") {
    assertEquivalent(
      CaseWhen((GreaterThan($"a", 1), Literal.create(null, IntegerType)) :: Nil, None),
      Literal.create(null, IntegerType))

    assertEquivalent(
      CaseWhen((GreaterThan(Rand(0), 0.5), Literal.create(null, IntegerType)) :: Nil, None),
      CaseWhen((GreaterThan(Rand(0), 0.5), Literal.create(null, IntegerType)) :: Nil, None))
  }

  test("SPARK-33884: simplify CaseWhen clauses with (true and false) and (false and true)") {
    // verify the boolean equivalence of all transformations involved
    val fields = Seq(
      $"cond".boolean.notNull,
      $"cond_nullable".boolean,
      $"a".boolean,
      $"b".boolean
    )
    val Seq(cond, cond_nullable, a, b) = fields.zipWithIndex.map { case (f, i) => f.at(i) }

    val exprs = Seq(
      // actual expressions of the transformations: original -> transformed
      CaseWhen(Seq((cond, TrueLiteral)), FalseLiteral) -> cond,
      CaseWhen(Seq((cond, FalseLiteral)), TrueLiteral) -> !cond,
      CaseWhen(Seq((cond_nullable, TrueLiteral)), FalseLiteral) -> (cond_nullable <=> true),
      CaseWhen(Seq((cond_nullable, FalseLiteral)), TrueLiteral) -> (!(cond_nullable <=> true)))

    // check plans
    for ((originalExpr, expectedExpr) <- exprs) {
      assertEquivalent(originalExpr, expectedExpr)
    }

    // check evaluation
    val binaryBooleanValues = Seq(true, false)
    val ternaryBooleanValues = Seq(true, false, null)
    for (condVal <- binaryBooleanValues;
         condNullableVal <- ternaryBooleanValues;
         aVal <- ternaryBooleanValues;
         bVal <- ternaryBooleanValues;
         (originalExpr, expectedExpr) <- exprs) {
      val inputRow = create_row(condVal, condNullableVal, aVal, bVal)
      val optimizedVal = evaluateWithoutCodegen(expectedExpr, inputRow)
      checkEvaluation(originalExpr, optimizedVal, inputRow)
    }
  }

  test("SPARK-37270: Remove elseValue if it is null Literal") {
    assertEquivalent(
      CaseWhen((GreaterThan($"a", Rand(1)), Literal.create(null, BooleanType)) :: Nil,
        Some(Literal.create(null, BooleanType))),
      CaseWhen((GreaterThan($"a", Rand(1)), Literal.create(null, BooleanType)) :: Nil))

    assertEquivalent(
      CaseWhen((GreaterThan($"a", 1), Literal.create(1, IntegerType)) :: Nil,
        Some(Literal.create(null, IntegerType))),
      CaseWhen((GreaterThan($"a", 1), Literal.create(1, IntegerType)) :: Nil))
  }
}

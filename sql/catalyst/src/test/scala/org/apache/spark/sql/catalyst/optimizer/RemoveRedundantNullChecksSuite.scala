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
import org.apache.spark.sql.catalyst.plans.{PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{IntegerType}

class RemoveRedundantNullChecksSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("RemoveRedundantNullChecks", FixedPoint(50),
      RemoveRedundantNullChecks) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, LocalRelation('a.int)).analyze
    val actual = Optimize.execute(
      Project(Alias(e1, "out")() :: Nil, LocalRelation('a.int)).analyze)
    comparePlans(actual, correctAnswer)
  }

  val col = UnresolvedAttribute(Seq("a"))
  val isNotNullCond = IsNotNull(col)
  val isNullCond = IsNull(col)
  val nullValue = Literal.create(null, IntegerType)

  val nullIntolerantExp = Abs(col)
  val nullTolerantExp = Coalesce(Seq(nullValue, col, Literal(5)))

  test("If null check with nullIntolerant expression in false value") {
    assertEquivalent(
      If(isNullCond, nullValue, nullIntolerantExp),
      nullIntolerantExp)

    assertEquivalent(
      If(isNullCond, col, nullIntolerantExp),
      nullIntolerantExp)
  }

  test("If notNull check with nullIntolerant expression in true value") {
    assertEquivalent(
      If(isNotNullCond, nullIntolerantExp, nullValue),
      nullIntolerantExp)

    assertEquivalent(
      If(isNotNullCond, nullIntolerantExp, col),
      nullIntolerantExp)
  }

  test("CaseWhen null check with nullIntolerant expression in false value") {
      assertEquivalent(
        CaseWhen(Seq((isNullCond, nullValue)), Some(nullIntolerantExp)),
        nullIntolerantExp)

      assertEquivalent(
        CaseWhen(Seq((isNullCond, col)), Some(nullIntolerantExp)),
        nullIntolerantExp)
  }

  test("CaseWhen notNull check with nullIntolerant expression in true value") {
      assertEquivalent(
        CaseWhen(Seq((isNotNullCond, nullIntolerantExp))),
        nullIntolerantExp)

      assertEquivalent(
        CaseWhen(Seq((isNotNullCond, nullIntolerantExp)), Some(col)),
        nullIntolerantExp)

      assertEquivalent(
        CaseWhen(Seq((isNotNullCond, nullIntolerantExp)), Some(nullValue)),
        nullIntolerantExp)
  }

  test("Check with nullTolerant expression.") {
    // We do not remove the null check if the expression is nullTolerant
    assertEquivalent(
      If(isNullCond, nullValue, nullTolerantExp),
      If(isNullCond, nullValue, nullTolerantExp))

    assertEquivalent(
      If(isNotNullCond, nullTolerantExp, nullValue),
      If(isNotNullCond, nullTolerantExp, nullValue))

    assertEquivalent(
      CaseWhen(Seq((isNotNullCond, nullTolerantExp))),
      CaseWhen(Seq((isNotNullCond, nullTolerantExp))))

    assertEquivalent(
      CaseWhen(Seq((isNullCond, nullValue)), Some(nullTolerantExp)),
      CaseWhen(Seq((isNullCond, nullValue)), Some(nullTolerantExp)))
  }
}

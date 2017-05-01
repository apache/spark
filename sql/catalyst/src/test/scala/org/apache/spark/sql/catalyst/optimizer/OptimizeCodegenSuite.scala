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

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._


class OptimizeCodegenSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("OptimizeCodegen", Once, OptimizeCodegen(conf)) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Project(Alias(e2, "out")() :: Nil, OneRowRelation).analyze
    val actual = Optimize.execute(Project(Alias(e1, "out")() :: Nil, OneRowRelation).analyze)
    comparePlans(actual, correctAnswer)
  }

  test("Codegen only when the number of branches is small.") {
    assertEquivalent(
      CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)),
      CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen())

    assertEquivalent(
      CaseWhen(List.fill(100)(TrueLiteral, Literal(1)), Literal(2)),
      CaseWhen(List.fill(100)(TrueLiteral, Literal(1)), Literal(2)))
  }

  test("Nested CaseWhen Codegen.") {
    assertEquivalent(
      CaseWhen(
        Seq((CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)), Literal(3))),
        CaseWhen(Seq((TrueLiteral, Literal(4))), Literal(5))),
      CaseWhen(
        Seq((CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen(), Literal(3))),
        CaseWhen(Seq((TrueLiteral, Literal(4))), Literal(5)).toCodegen()).toCodegen())
  }

  test("Multiple CaseWhen in one operator.") {
    val plan = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)),
        CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6))).analyze
    val correctAnswer = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen(),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)).toCodegen(),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)),
        CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6)).toCodegen()).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, correctAnswer)
  }

  test("Multiple CaseWhen in different operators") {
    val plan = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      .where(
        LessThan(
          CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6)),
          CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      ).analyze
    val correctAnswer = OneRowRelation
      .select(
        CaseWhen(Seq((TrueLiteral, Literal(1))), Literal(2)).toCodegen(),
        CaseWhen(Seq((FalseLiteral, Literal(3))), Literal(4)).toCodegen(),
        CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      .where(
        LessThan(
          CaseWhen(Seq((TrueLiteral, Literal(5))), Literal(6)).toCodegen(),
          CaseWhen(List.fill(20)((TrueLiteral, Literal(0))), Literal(0)))
      ).analyze
    val optimized = Optimize.execute(plan)
    comparePlans(optimized, correctAnswer)
  }
}

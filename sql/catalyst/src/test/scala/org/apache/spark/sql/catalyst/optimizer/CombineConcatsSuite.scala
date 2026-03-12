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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._


class CombineConcatsSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("CombineConcatsSuite", FixedPoint(50), CombineConcats) :: Nil
  }

  protected def assertEquivalent(e1: Expression, e2: Expression): Unit = {
    val correctAnswer = Limit(Literal(1), Project(Alias(e2, "out")() :: Nil, OneRowRelation()))
      .analyze
    val actual = Optimize.execute(Limit(Literal(1), Project(Alias(e1, "out")() :: Nil,
      OneRowRelation())).analyze)
    comparePlans(actual, correctAnswer)
  }

  def str(s: String): Literal = Literal(s)
  def binary(s: String): Literal = Literal(s.getBytes)

  test("combine nested Concat exprs") {
    assertEquivalent(
      Concat(
        Concat(str("a") :: str("b") :: Nil) ::
        str("c") ::
        str("d") ::
        Nil),
      Concat(str("a") :: str("b") :: str("c") :: str("d") :: Nil))
    assertEquivalent(
      Concat(
        str("a") ::
        Concat(str("b") :: str("c") :: Nil) ::
        str("d") ::
        Nil),
      Concat(str("a") :: str("b") :: str("c") :: str("d") :: Nil))
    assertEquivalent(
      Concat(
        str("a") ::
        str("b") ::
        Concat(str("c") :: str("d") :: Nil) ::
        Nil),
      Concat(str("a") :: str("b") :: str("c") :: str("d") :: Nil))
    assertEquivalent(
      Concat(
        Concat(
          str("a") ::
          Concat(
            str("b") ::
            Concat(str("c") :: str("d") :: Nil) ::
            Nil) ::
          Nil) ::
        Nil),
      Concat(str("a") :: str("b") :: str("c") :: str("d") :: Nil))
  }

  test("combine string and binary exprs") {
    assertEquivalent(
      Concat(
        Concat(str("a") :: str("b") :: Nil) ::
        Concat(binary("c") :: binary("d") :: Nil) ::
        Nil),
      Concat(str("a") :: str("b") :: binary("c") :: binary("d") :: Nil))
  }
}

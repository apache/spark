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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Cast, Alias, Expression}
import org.apache.spark.sql.types.{DoubleType, ShortType, LongType}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

class CombiningCastsSuite extends PlanTest {

  object Optimizer extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Combine Cast", FixedPoint(10),
        CombineCasts) :: Nil
  }

  val testRelation = LocalRelation('a.int)

  private def checkResult(expression: Expression, expectedExpr: Expression): Unit = {
    val plan = Project(Seq(Alias(expression, "c")()), testRelation)
    val expectedPlan = Project(Seq(Alias(expectedExpr, "c")()), testRelation)
    assert(Optimizer(plan) === expectedPlan)
  }

  test("casts: combines two casts") {
    val expr = Cast(Cast(UnresolvedAttribute("a"), ShortType), LongType)
    val expectedExpr = Cast(UnresolvedAttribute("a"), LongType)
    checkResult(expr, expectedExpr)
  }

  test("casts: combines three casts") {
    val expr = Cast(Cast(Cast(UnresolvedAttribute("a"), ShortType), LongType), DoubleType)
    val expectedExpr = Cast(UnresolvedAttribute("a"), DoubleType)
    checkResult(expr, expectedExpr)
  }

}

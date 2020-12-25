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

import org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionEvalHelper, HigherOrderFunctionsHelper}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.IntegerType

class OptimizeHigherOrderFunctionsSuite
  extends PlanTest
    with ExpressionEvalHelper
    with HigherOrderFunctionsHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Optimize higher order functions", FixedPoint(10),
      OptimizeHigherOrderFunctions) :: Nil
  }

  val testRelation = LocalRelation('a.array(IntegerType))

  private def col(tr: LocalRelation, columnName: String): Expression =
    col(tr.analyze, columnName)

  private def col(plan: LogicalPlan, columnName: String): Expression =
    plan.resolveQuoted(columnName, caseInsensitiveResolution).get

  test("Combine array filters") {
    val plan = testRelation
      .select(filter(filter(col(testRelation, "a"), x => x > 0), x => x < 10) as "a")
      .analyze

    val actual = Optimize.execute(plan)
    val correctAnswer = testRelation
      .select(filter(col(testRelation, "a"), x => x > 0 && x < 10) as "a")
      .analyze
    comparePlans(actual, correctAnswer)
  }

}

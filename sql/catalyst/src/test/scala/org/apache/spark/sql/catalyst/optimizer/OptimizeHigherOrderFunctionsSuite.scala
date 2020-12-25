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
import org.apache.spark.sql.catalyst.expressions._
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

  val rel = LocalRelation(
    'a.array(IntegerType),
    'b.int,
    'm.map(IntegerType, IntegerType)
  )

  private def col(tr: LocalRelation, columnName: String): Expression =
    col(tr.analyze, columnName)

  private def col(plan: LogicalPlan, columnName: String): Expression =
    plan.resolveQuoted(columnName, caseInsensitiveResolution).get

  private def validate(plan: LogicalPlan): Unit = {
    val higherOrderFunctions = plan.collect {
      case lp: LogicalPlan => lp.expressions collect {
          case e => e collect {
            case f: HigherOrderFunction => f.bind(validateBinding)
          }
        }
    }.flatten.flatten

    assert(higherOrderFunctions.nonEmpty)
  }

  test("Combine array filters - column ref in outer") {
    val plan = rel
      .select(filter(filter(col(rel, "a"), x => x > 0), x => x < 'b) as "a")
      .analyze

    val correctAnswer = rel
      .select(filter(col(rel, "a"), x => x > 0 && x < 'b) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, correctAnswer)
    validate(optimized)
  }

  test("Combine array filters - column ref in inner") {
    val plan = rel
      .select(filter(filter(col(rel, "a"), x => x < 'b), x => x > 0) as "a")
      .analyze

    val correctAnswer = rel
      .select(filter(col(rel, "a"), x => x < 'b && x > 0) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, correctAnswer)
    validate(optimized)
  }

  test("Combine map filters - column ref in outer") {
    val plan = rel
      .select(mapFilter(mapFilter(col(rel, "m"), (k, _) => k > 0), (_, v) => v < 'b) as "m")
      .analyze

    val correctAnswer = rel
      .select(mapFilter(col(rel, "m"), (k, v) => k > 0 && v < 'b) as "m")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, correctAnswer)
    validate(optimized)
  }

  test("Combine map filters - column ref in inner") {
    val plan = rel
      .select(mapFilter(mapFilter(col(rel, "m"), (k, _) => k < 'b), (_, v) => v > 0) as "m")
      .analyze

    val correctAnswer = rel
      .select(mapFilter(col(rel, "m"), (k, v) => k < 'b && v > 0) as "m")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, correctAnswer)
    validate(optimized)
  }

  test("Swap array filter and sort") {
    val plan = rel
      .select(filter(arraySort(col(rel, "a")), x => x < 'b) as "a")
      .analyze

    val correctAnswer = rel
      .select(arraySort(filter(col(rel, "a"), x => x < 'b)) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, correctAnswer)
    validate(optimized)
  }

  test("Do not swap non-deterministic array filter and sort") {
    val plan = rel
      .select(filter(arraySort(col(rel, "a")), x => x < rand(0)) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, plan)
    validate(optimized)
  }

  test("Remove sort before forAll") {
    val plan = rel
      .select(forall(arraySort(col(rel, "a")), x => x < 'b) as "a")
      .analyze

    val correctAnswer = rel
      .select(forall(col(rel, "a"), x => x < 'b) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, correctAnswer)
    validate(optimized)
  }

  test("Do not remove sort before non-deterministic forAll") {
    val plan = rel
      .select(forall(arraySort(col(rel, "a")), x => x < rand(0)) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, plan)
    validate(optimized)
  }

  test("Remove sort before exists") {
    val plan = rel
      .select(exists(arraySort(col(rel, "a")), x => x < 'b) as "a")
      .analyze

    val correctAnswer = rel
      .select(exists(col(rel, "a"), x => x < 'b) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, correctAnswer)
    validate(optimized)
  }

  test("Do not remove sort before non-deterministic exists") {
    val plan = rel
      .select(exists(arraySort(col(rel, "a")), x => x < rand(0)) as "a")
      .analyze

    val optimized = Optimize.execute(plan)

    comparePlans(optimized, plan)
    validate(optimized)
  }
}

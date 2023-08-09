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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class NullDownPropagationSuite extends PlanTest with ExpressionEvalHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once, EliminateSubqueryAliases) ::
      Batch("Null Down Propagation", FixedPoint(50),
        NullPropagation,
        NullDownPropagation,
        ConstantFolding,
        SimplifyConditionals,
        BooleanSimplification,
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int, $"d".string,
    $"e".boolean, $"f".boolean, $"g".boolean, $"h".boolean)

  private def checkCondition(input: Expression, expected: Expression): Unit = {
    val plan = testRelation.where(input).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = testRelation.where(expected).analyze
    comparePlans(actual, correctAnswer)
  }

  test("Using IsNull(e(inputs)) == IsNull(input1) or IsNull(input2) ... rules") {
    checkCondition(IsNull(Not($"e")), IsNull($"e"))
    checkCondition(IsNotNull(Not($"e")), IsNotNull($"e"))
    checkCondition(IsNull($"a" > $"b"), Or(IsNull($"a"), IsNull($"b")))
    checkCondition(IsNotNull($"a" > $"b"), And(IsNotNull($"a"), IsNotNull($"b")))
  }
}

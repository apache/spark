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

import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf.{CASE_SENSITIVE, GROUP_BY_ORDINAL}

class AggregateOptimizeSuite extends AnalysisTest {
  val analyzer = getAnalyzer

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Aggregate", FixedPoint(100),
      FoldablePropagation,
      RemoveLiteralFromGroupExpressions,
      RemoveRepetitionFromGroupExpressions) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)

  test("remove literals in grouping expression") {
    val query = testRelation.groupBy('a, Literal("1"), Literal(1) + Literal(2))(sum('b))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.groupBy('a)(sum('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not remove all grouping expressions if they are all literals") {
    withSQLConf(CASE_SENSITIVE.key -> "false", GROUP_BY_ORDINAL.key -> "false") {
      val analyzer = getAnalyzer
      val query = testRelation.groupBy(Literal("1"), Literal(1) + Literal(2))(sum('b))
      val optimized = Optimize.execute(analyzer.execute(query))
      val correctAnswer = analyzer.execute(testRelation.groupBy(Literal(0))(sum('b)))

      comparePlans(optimized, correctAnswer)
    }
  }

  test("Remove aliased literals") {
    val query = testRelation.select('a, 'b, Literal(1).as('y)).groupBy('a, 'y)(sum('b))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.select('a, 'b, Literal(1).as('y)).groupBy('a)(sum('b)).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("remove repetition in grouping expression") {
    val query = testRelation.groupBy('a + 1, 'b + 2, Literal(1) + 'A, Literal(2) + 'B)(sum('c))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.groupBy('a + 1, 'b + 2)(sum('c)).analyze

    comparePlans(optimized, correctAnswer)
  }
}

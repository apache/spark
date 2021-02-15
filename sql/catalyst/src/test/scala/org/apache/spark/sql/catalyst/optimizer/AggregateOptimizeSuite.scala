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

  val testRelation = LocalRelation(Symbol("a").int, Symbol("b").int, Symbol("c").int)

  test("remove literals in grouping expression") {
    val query =
      testRelation.groupBy(Symbol("a"), Literal("1"), Literal(1) + Literal(2))(sum(Symbol("b")))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.groupBy(Symbol("a"))(sum(Symbol("b"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not remove all grouping expressions if they are all literals") {
    withSQLConf(CASE_SENSITIVE.key -> "false", GROUP_BY_ORDINAL.key -> "false") {
      val analyzer = getAnalyzer
      val query = testRelation.groupBy(Literal("1"), Literal(1) + Literal(2))(sum(Symbol("b")))
      val optimized = Optimize.execute(analyzer.execute(query))
      val correctAnswer = analyzer.execute(testRelation.groupBy(Literal(0))(sum(Symbol("b"))))

      comparePlans(optimized, correctAnswer)
    }
  }

  test("Remove aliased literals") {
    val query = testRelation.select(
      Symbol("a"),
      Symbol("b"),
      Literal(1).as(Symbol("y"))).groupBy(Symbol("a"), Symbol("y"))(sum(Symbol("b")))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer = testRelation.select(
      Symbol("a"),
      Symbol("b"),
      Literal(1).as(Symbol("y"))).groupBy(Symbol("a"))(sum(Symbol("b"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("remove repetition in grouping expression") {
    val query = testRelation.groupBy(
      Symbol("a") + 1,
      Symbol("b") + 2,
      Literal(1) + Symbol("A"),
      Literal(2) + Symbol("B"))(sum(Symbol("c")))
    val optimized = Optimize.execute(analyzer.execute(query))
    val correctAnswer =
      testRelation.groupBy(Symbol("a") + 1, Symbol("b") + 2)(sum(Symbol("c"))).analyze

    comparePlans(optimized, correctAnswer)
  }
}

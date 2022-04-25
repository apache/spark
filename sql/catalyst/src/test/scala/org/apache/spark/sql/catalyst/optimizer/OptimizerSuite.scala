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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo, IntegerLiteral, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Offset, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * A dummy optimizer rule for testing that decrements integer literals until 0.
 */
object DecrementLiterals extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformExpressions {
    case IntegerLiteral(i) if i > 0 => Literal(i - 1)
  }
}

class OptimizerSuite extends PlanTest {
  test("Optimizer exceeds max iterations") {
    val iterations = 5
    val maxIterationsNotEnough = 3
    val maxIterationsEnough = 10
    val analyzed = Project(Alias(Literal(iterations), "attr")() :: Nil, OneRowRelation()).analyze

    withSQLConf(SQLConf.OPTIMIZER_MAX_ITERATIONS.key -> maxIterationsNotEnough.toString) {
      val optimizer = new SimpleTestOptimizer() {
        override def defaultBatches: Seq[Batch] =
          Batch("test", fixedPoint,
            DecrementLiterals) :: Nil
      }

      val message1 = intercept[RuntimeException] {
        optimizer.execute(analyzed)
      }.getMessage
      assert(message1.startsWith(s"Max iterations ($maxIterationsNotEnough) reached for batch " +
        s"test, please set '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}' to a larger value."))

      withSQLConf(SQLConf.OPTIMIZER_MAX_ITERATIONS.key -> maxIterationsEnough.toString) {
        try {
          optimizer.execute(analyzed)
        } catch {
          case ex: AnalysisException
            if ex.getMessage.contains(SQLConf.OPTIMIZER_MAX_ITERATIONS.key) =>
              fail("optimizer.execute should not reach max iterations.")
        }
      }

      val message2 = intercept[RuntimeException] {
        optimizer.execute(analyzed)
      }.getMessage
      assert(message2.startsWith(s"Max iterations ($maxIterationsNotEnough) reached for batch " +
        s"test, please set '${SQLConf.OPTIMIZER_MAX_ITERATIONS.key}' to a larger value."))
    }
  }

  test("Test CheckOffsetOperator") {
    val optimizer = new SimpleTestOptimizer()

    val analyzed1 =
      Offset(Literal(2), Project(Alias(Literal(5), "attr")() :: Nil, OneRowRelation())).analyze
    val message1 = intercept[AnalysisException] {
      optimizer.execute(analyzed1)
    }.getMessage
    assert(message1.equals(
      s"""
         |The OFFSET clause is only allowed in the LIMIT clause, but the OFFSET
         |clause is found to be the outermost node.""".stripMargin.replace("\n", " ")))

    val analyzed2 =
      Filter(EqualTo(UnresolvedAttribute(Seq("attr")), Literal("alex")), analyzed1).analyze
    val message2 = intercept[AnalysisException] {
      optimizer.execute(analyzed2)
    }.getMessage
    assert(message2.equals(
      s"""
         |The OFFSET clause is only allowed in the LIMIT clause, but the OFFSET
         |clause is found in: ${analyzed2.nodeName}.""".stripMargin.replace("\n", " ")))
  }
}

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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.{ArrayType, IntegerType}

/**
 * Test suite for [[ResolveLambdaVariables]].
 */
class ResolveLambdaVariablesSuite extends PlanTest {
  import org.apache.spark.sql.catalyst.dsl.expressions._
  import org.apache.spark.sql.catalyst.dsl.plans._

  object Analyzer extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Resolution", FixedPoint(4), ResolveLambdaVariables(conf)) :: Nil
  }

  private val key = 'key.int
  private val values1 = 'values1.array(IntegerType)
  private val values2 = 'values2.array(ArrayType(ArrayType(IntegerType)))
  private val data = LocalRelation(Seq(key, values1, values2))
  private val lvInt = NamedLambdaVariable("x", IntegerType, nullable = true)
  private val lvHiddenInt = NamedLambdaVariable("col0", IntegerType, nullable = true)
  private val lvArray = NamedLambdaVariable("x", ArrayType(IntegerType), nullable = true)

  private def plan(e: Expression): LogicalPlan = data.select(e.as("res"))

  private def checkExpression(e1: Expression, e2: Expression): Unit = {
    comparePlans(Analyzer.execute(plan(e1)), plan(e2))
  }

  private def lv(s: Symbol) = UnresolvedNamedLambdaVariable(Seq(s.name))

  test("resolution - no op") {
    checkExpression(key, key)
  }

  test("resolution - simple") {
    val in = ArrayTransform(values1, LambdaFunction(lv('x) + 1, lv('x) :: Nil))
    val out = ArrayTransform(values1, LambdaFunction(lvInt + 1, lvInt :: Nil))
    checkExpression(in, out)
  }

  test("resolution - nested") {
    val in = ArrayTransform(values2, LambdaFunction(
      ArrayTransform(lv('x), LambdaFunction(lv('x) + 1, lv('x) :: Nil)), lv('x) :: Nil))
    val out = ArrayTransform(values2, LambdaFunction(
      ArrayTransform(lvArray, LambdaFunction(lvInt + 1, lvInt :: Nil)), lvArray :: Nil))
    checkExpression(in, out)
  }

  test("resolution - hidden") {
    val in = ArrayTransform(values1, key)
    val out = ArrayTransform(values1, LambdaFunction(key, lvHiddenInt :: Nil, hidden = true))
    checkExpression(in, out)
  }

  test("fail - name collisions") {
    val p = plan(ArrayTransform(values1,
      LambdaFunction(lv('x) + lv('X), lv('x) :: lv('X) :: Nil)))
    val msg = intercept[AnalysisException](Analyzer.execute(p)).getMessage
    assert(msg.contains("arguments should not have names that are semantically the same"))
  }

  test("fail - lambda arguments") {
    val p = plan(ArrayTransform(values1,
      LambdaFunction(lv('x) + lv('y) + lv('z), lv('x) :: lv('y) :: lv('z) :: Nil)))
    val msg = intercept[AnalysisException](Analyzer.execute(p)).getMessage
    assert(msg.contains("does not match the number of arguments expected"))
  }
}

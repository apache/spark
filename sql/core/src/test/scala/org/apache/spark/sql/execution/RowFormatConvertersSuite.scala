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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.test.TestSQLContext

class RowFormatConvertersSuite extends SparkPlanTest {

  private def getConverters(plan: SparkPlan): Seq[SparkPlan] = plan.collect {
    case c: ConvertToUnsafe => c
    case c: ConvertToSafe => c
  }

  private val outputsSafe = ExternalSort(Nil, false, PhysicalRDD(Seq.empty, null))
  assert(!outputsSafe.outputsUnsafeRows)
  private val outputsUnsafe = UnsafeExternalSort(Nil, false, PhysicalRDD(Seq.empty, null))
  assert(outputsUnsafe.outputsUnsafeRows)

  test("planner should insert unsafe->safe conversions when required") {
    val plan = Limit(10, outputsUnsafe)
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(preparedPlan.children.head.isInstanceOf[ConvertToSafe])
  }

  test("filter can process unsafe rows") {
    val plan = Filter(IsNull(null), outputsUnsafe)
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).isEmpty)
    assert(preparedPlan.outputsUnsafeRows)
  }

  test("filter can process safe rows") {
    val plan = Filter(IsNull(null), outputsSafe)
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).isEmpty)
    assert(!preparedPlan.outputsUnsafeRows)
  }

  test("execute() fails an assertion if inputs rows are of different formats") {
    val e = intercept[AssertionError] {
      Union(Seq(outputsSafe, outputsUnsafe)).execute()
    }
    assert(e.getMessage.contains("format"))
  }

  test("union requires all of its input rows' formats to agree") {
    val plan = Union(Seq(outputsSafe, outputsUnsafe))
    assert(plan.canProcessSafeRows && plan.canProcessUnsafeRows)
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(preparedPlan.outputsUnsafeRows)
  }

  test("union can process safe rows") {
    val plan = Union(Seq(outputsSafe, outputsSafe))
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(!preparedPlan.outputsUnsafeRows)
  }

  test("union can process unsafe rows") {
    val plan = Union(Seq(outputsUnsafe, outputsUnsafe))
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(preparedPlan.outputsUnsafeRows)
  }

  test("round trip with ConvertToUnsafe and ConvertToSafe") {
    val input = Seq(("hello", 1), ("world", 2))
    checkAnswer(
      TestSQLContext.createDataFrame(input),
      plan => ConvertToSafe(ConvertToUnsafe(plan)),
      input.map(Row.fromTuple)
    )
  }
}

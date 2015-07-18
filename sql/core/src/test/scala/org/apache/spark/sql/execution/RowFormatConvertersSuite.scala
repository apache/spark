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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.test.TestSQLContext

class RowFormatConvertersSuite extends SparkFunSuite {

  private def getConverters(plan: SparkPlan): Seq[SparkPlan] = plan.collect {
    case c: ConvertToUnsafe => c
    case c: ConvertFromUnsafe => c
  }

  test("planner should insert unsafe->safe conversions when required") {
    val plan = Limit(10, UnsafeExternalSort(Nil, false, PhysicalRDD(Seq.empty, null)))
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(preparedPlan.children.head.isInstanceOf[ConvertFromUnsafe])
  }

  test("filter can process unsafe rows") {
    val plan = Filter(IsNull(null), UnsafeExternalSort(Nil, false, PhysicalRDD(Seq.empty, null)))
    assert(plan.child.outputsUnsafeRows)
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).isEmpty)
  }

  test("filter can process safe rows") {
    val plan = Filter(IsNull(null), ExternalSort(Nil, false, PhysicalRDD(Seq.empty, null)))
    assert(!plan.child.outputsUnsafeRows)
    val preparedPlan = TestSQLContext.prepareForExecution.execute(plan)
    assert(getConverters(preparedPlan).isEmpty)
  }
}

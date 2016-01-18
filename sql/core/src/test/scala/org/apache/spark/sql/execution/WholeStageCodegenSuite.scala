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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.SharedSQLContext

class WholeStageCodegenSuite extends SparkPlanTest with SharedSQLContext {

  test("range/filter should be combined") {
    val df = sqlContext.range(10).filter("id = 1").selectExpr("id + 1")
    val plan = df.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[WholeStageCodegen]).isDefined)

    checkThatPlansAgree(
      sqlContext.range(100),
      (p: SparkPlan) =>
        WholeStageCodegen(Filter('a == 1, InputAdapter(p)), Seq()),
      (p: SparkPlan) => Filter('a == 1, p),
      sortAnswers = false
    )
  }
}

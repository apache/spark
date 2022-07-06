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

package org.apache.spark.sql.execution.ui

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.test.SharedSparkSession

class SparkPlanInfoSuite extends SharedSparkSession {

  import testImplicits._

  def validateSparkPlanInfo(sparkPlanInfo: SparkPlanInfo): Unit = {
    sparkPlanInfo.nodeName match {
      case "InMemoryTableScan" => assert(sparkPlanInfo.children.length == 1)
      case _ => sparkPlanInfo.children.foreach(validateSparkPlanInfo)
    }
  }

  test("SparkPlanInfo creation from SparkPlan with InMemoryTableScan node") {
    val dfWithCache = Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1").cache().repartition(10)

    val planInfoResult = SparkPlanInfo.fromSparkPlan(dfWithCache.queryExecution.executedPlan)

    validateSparkPlanInfo(planInfoResult)
  }
}

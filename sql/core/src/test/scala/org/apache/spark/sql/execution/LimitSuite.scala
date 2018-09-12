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

import scala.util.Random

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext


class LimitSuite extends SparkPlanTest with SharedSQLContext {

  private var rand: Random = _
  private var seed: Long = 0

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    seed = System.currentTimeMillis()
    rand = new Random(seed)
  }

  test("Produce ordered global limit if more than topKSortFallbackThreshold") {
    withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "100") {
      val df = LimitTest.generateRandomInputData(spark, rand).sort("a")

      val globalLimit = df.limit(99).queryExecution.executedPlan.collect {
        case g: GlobalLimitExec => g
      }
      assert(globalLimit.size == 0)

      val topKSort = df.limit(99).queryExecution.executedPlan.collect {
        case t: TakeOrderedAndProjectExec => t
      }
      assert(topKSort.size == 1)

      val orderedGlobalLimit = df.limit(100).queryExecution.executedPlan.collect {
        case g: GlobalLimitExec => g
      }
      assert(orderedGlobalLimit.size == 1 && orderedGlobalLimit(0).orderedLimit == true)
    }
  }

  test("Ordered global limit") {
    val baseDf = LimitTest.generateRandomInputData(spark, rand)
      .select("a").repartition(3).sort("a")

    withSQLConf(SQLConf.LIMIT_FLAT_GLOBAL_LIMIT.key -> "true") {
      val orderedGlobalLimit = GlobalLimitExec(3, baseDf.queryExecution.sparkPlan,
        orderedLimit = true)
      val orderedGlobalLimitResult = SparkPlanTest.executePlan(orderedGlobalLimit, spark.sqlContext)
        .map(_.getInt(0))

      val globalLimit = GlobalLimitExec(3, baseDf.queryExecution.sparkPlan, orderedLimit = false)
      val globalLimitResult = SparkPlanTest.executePlan(globalLimit, spark.sqlContext)
          .map(_.getInt(0))

      // Global limit without order takes values at each partition sequentially.
      // After global sort, the values in second partition must be larger than the values
      // in first partition.
      assert(orderedGlobalLimitResult(0) == globalLimitResult(0))
      assert(orderedGlobalLimitResult(1) < globalLimitResult(1))
      assert(orderedGlobalLimitResult(2) < globalLimitResult(2))
    }
  }
}


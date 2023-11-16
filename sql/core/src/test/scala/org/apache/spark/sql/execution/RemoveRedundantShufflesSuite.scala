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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class RemoveRedundantShufflesSuiteBase
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("Remove redundant shuffle") {
    withTempView("t1", "t2") {
      spark.range(10).select($"id").createOrReplaceTempView("t1")
      spark.range(20).select($"id").createOrReplaceTempView("t2")
      Seq(-1, 1000000).foreach { threshold =>
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> threshold.toString) {
          val query = spark.table("t1").repartition(10).join(spark.table("t2"), "id")
          val shuffleNum = collect(query.queryExecution.executedPlan) {
            case j: ShuffleExchangeExec => j
          }.size
          if (threshold > 0) {
            assert(shuffleNum === 1)
          } else {
            assert(shuffleNum === 2)
          }
        }
      }
    }
  }
}

class RemoveRedundantShufflesSuite extends RemoveRedundantShufflesSuiteBase
  with DisableAdaptiveExecutionSuite

class RemoveRedundantShufflesSuiteAE extends RemoveRedundantShufflesSuiteBase
  with EnableAdaptiveExecutionSuite

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

package org.apache.spark.sql.execution.adaptive

import org.scalatest.PrivateMethodTester

import org.apache.spark.internal.config
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class AdaptiveBloomFilterJoinSuite
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper
  with PrivateMethodTester {

  protected override def sparkConf = {
    super.sparkConf
      .set(config.MEMORY_STORAGE_FRACTION, 0.99999999)
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Adaptive Bloom Filter Join", FixedPoint(10),
      AdaptiveBloomFilterJoin(spark)) :: Nil
  }

  setupTestData()

  private def hasBloomFilterJoin(plan: SparkPlan): Seq[FilterExec] = {
    collectWithSubqueries(plan) {
      case f @ FilterExec(e, _) if e.isInstanceOf[BloomFilterMightContain] => f
    }
  }

  test("Adaptive add Bloom filter") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = 1")

      assert(findTopLevelSortMergeJoin(plan).size === 1)
      assert(hasBloomFilterJoin(plan).size === 0)
      assert(findTopLevelSortMergeJoin(adaptivePlan).size === 1)
      assert(hasBloomFilterJoin(adaptivePlan).size === 1)
    }
  }

  test("Do not add Bloom filter if convert to BroadcastHashJoin") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
      val (plan, adaptivePlan) = runAdaptiveAndVerifyResult(
        "SELECT * FROM testData join testData2 ON key = a where value = 1")

      assert(findTopLevelSortMergeJoin(plan).size === 1)
      assert(hasBloomFilterJoin(plan).size === 0)
      assert(findTopLevelBroadcastHashJoin(adaptivePlan).size == 1)
      assert(findTopLevelSortMergeJoin(adaptivePlan).size === 0)
      assert(hasBloomFilterJoin(adaptivePlan).size === 0)
    }
  }
}

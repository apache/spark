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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.{ProjectExec, RangeExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class AdaptiveHashAggregateExecSuite extends QueryTest
  with SharedSparkSession
  with SQLMetricsTestUtils
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("Partial aggregation adaptive") {
    Seq(0, 1, 10).foreach { partialAggThreshold =>
      withSQLConf(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_THRESHOLD.key -> s"$partialAggThreshold") {
        val agg = testData2.groupBy("a").sum("b")
        val hashAggregate = agg.queryExecution.sparkPlan.asInstanceOf[HashAggregateExec]
        val partialHashAggregate = hashAggregate.child.asInstanceOf[HashAggregateExec]

        assert(!hashAggregate.isPartialAgg)
        assert(!hashAggregate.isAdaptivePartialAggregationEnabled)

        assert(partialHashAggregate.isPartialAgg)
        if (partialAggThreshold > 0) {
          assert(partialHashAggregate.isAdaptivePartialAggregationEnabled)
        } else {
          assert(!partialHashAggregate.isAdaptivePartialAggregationEnabled)
        }

        val nodeIds = Set(6L)
        val metrics = getSparkPlanMetrics(agg, 2, nodeIds, true).get
        nodeIds.foreach { nodeId =>
          val outputRows = metrics(nodeId)._2("number of output rows").toString

          if (partialAggThreshold == 1) {
            assert(outputRows.toLong === 6)
            val skippedRows =
              metrics(nodeId)._2("number of skipped partial aggregate rows").toString
            assert(skippedRows.toLong === 4)
          } else {
            assert(outputRows.toLong === 4)
          }
        }

        checkAnswer(agg, Seq(Row(1, 3), Row(2, 3), Row(3, 3)))
      }
    }
  }

  test("HashAggregateExec's children contains Join") {
    withSQLConf(SQLConf.ADAPTIVE_PARTIAL_AGGREGATION_THRESHOLD.key -> "1") {
      val agg = testData2.join(testData, $"a" === $"key").groupBy("a").sum("b")

      val hashAggregate = agg.queryExecution.sparkPlan.asInstanceOf[HashAggregateExec]
      val partialHashAggregate = hashAggregate.child.asInstanceOf[HashAggregateExec]

      assert(!hashAggregate.isPartialAgg)
      assert(!hashAggregate.isAdaptivePartialAggregationEnabled)

      assert(partialHashAggregate.isPartialAgg)
      assert(!partialHashAggregate.isAdaptivePartialAggregationEnabled)
    }
  }

  test("Test collectUntil") {
    withTempView("v1", "v2") {
      // HashAggregate(keys=[a#2L], functions=[sum(b#15L)], output=[a#2L, sum(b)#19L])
      // +- HashAggregate(keys=[a#2L], functions=[partial_sum(b#15L)], output=[a#2L, sum#23L])
      //    +- Project [a#2L, b#15L]
      //       +- BroadcastHashJoin [a#2L], [a#14L], Inner, BuildLeft, false
      //          :- HashAggregate(keys=[a#2L], functions=[], output=[a#2L])
      //          :  +- HashAggregate(keys=[a#2L], functions=[], output=[a#2L])
      //          :     +- Project [id#0L AS a#2L]
      //          :        +- Range (0, 2, step=1, splits=2)
      //          +- Project [id#12L AS a#14L, id#12L AS b#15L]
      //             +- Range (0, 2, step=1, splits=2)
      spark.range(2)
        .selectExpr("id AS a", "id AS b")
        .groupBy("a").sum("b")
        .createTempView("v1")

      spark.range(2)
        .selectExpr("id AS a", "id AS b")
        .createTempView("v2")

      val df = sql("SELECT v1.a, sum(v2.b) FROM v1 join v2 ON v1.a = v2.a GROUP BY v1.a")
      val plan = df.queryExecution.sparkPlan

      assert(plan.collectUntil(_.isInstanceOf[HashAggregateExec]).isEmpty)
      assert(plan.collectUntil(_.isInstanceOf[ProjectExec]).size === 2)
      assert(plan.collectUntil(_.isInstanceOf[BroadcastHashJoinExec]).size === 3)
      assert(
        plan.collectUntil(_.isInstanceOf[RangeExec]).size === plan.collect { case p => p }.size - 2)
    }
  }
}

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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

class BroadcastVarPushDownSuite extends QueryTest with BroadcastVarPushdownUtils {

  test("test broadcast variables push on simple join") {
    val planToTest = () => non_part_table1.where('c1_1.attr > 10).join(non_part_table2, Inner,
        Option("c1_2".attr === "c2_2".attr))

    runTest(planToTest)
  }

  test("test  broadcast variables push on nested joins") {
    val planToTest = () => non_part_table1.where('c1_2.attr > 10).join(non_part_table2, Inner,
      Option("c1_1".attr === "c2_2".attr && "c1_3".attr === "c2_3".attr)).join(non_part_table3.
      where('c3_1.attr > 1), Inner, Option("c2_1".attr === "c3_2".attr))

    runTest(planToTest)
  }

  test("test broadcast var push with multi target batch scan") {
    val planToTest = () => non_part_table1.where('c1_2.attr > 10).join(non_part_table2, Inner,
        Option("c1_1".attr === "c2_2".attr && "c1_3".attr === "c2_3".attr)).join(non_part_table3.
        where('c3_1.attr > 1), Inner, Option("c1_1".attr === "c3_2".attr))
    runTest(planToTest)
  }

  private def runTest(planToTest: () => LogicalPlan): Unit = {
    val (dfWithBroadcastVarPush, resWithBCVar) = runWithDefaultConfig({
      val df = planToTest().toDF()
      df -> df.collect()
    })

    val (dfWithoutBroadcastVarPush, resWithoutBCVar) = runWithBroadcastVarPushOff({
      val df = planToTest().toDF()
      df -> df.collect()
    })

    assert(resWithoutBCVar.length > 0)
    QueryTest.sameRows(resWithBCVar, resWithoutBCVar)
    // verify that broadcast filter pushdown really reduced the streaming side rows seen by the
    // hash join
    // collect all batch scans
    val (batchScansWithBCVar, batchScansWithoutBCVar) = {
      val temp = Seq(dfWithBroadcastVarPush.queryExecution.executedPlan,
        dfWithoutBroadcastVarPush.queryExecution.executedPlan).map {
        case aqe: AdaptiveSparkPlanExec => BroadcastHashJoinUtil.getAllBatchScansForSparkPlan(
          aqe.finalPhysicalPlan)
        case x => BroadcastHashJoinUtil.getAllBatchScansForSparkPlan(x)
      }
      temp.head -> temp(1)
    }

    val pairedBatchScansWithBCVarToWthoutBCVar = batchScansWithBCVar.filter(
      x => x.scan.isInstanceOf[SupportsRuntimeFiltering] && x.scan
        .asInstanceOf[SupportsRuntimeFiltering].hasPushedBroadCastFilter).map(bs =>
      batchScansWithoutBCVar.find(x => x.table.name == bs.table.name && x.schema == bs.schema).
        map(bs -> _).get)
    assert(pairedBatchScansWithBCVarToWthoutBCVar.nonEmpty)
    assert(pairedBatchScansWithBCVarToWthoutBCVar.forall {
      case (withBc, withoutBC) => withBc.metrics("numOutputRows").value <
        withoutBC.metrics("numOutputRows").value
    })
  }
}

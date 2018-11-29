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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{RangeExec, SortExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class QueryStageTest extends SharedSQLContext {

  test("Replaces ShuffleExchangeExec/BroadcastExchangeExec with reuse disabled") {
    val plan = createMergeJoinPlan(100, 100)

    val resultQueryStage = ResultQueryStage(plan)

    resultQueryStage.execute()
  }

  def createMergeJoinPlan(leftNum: Int, rightNum: Int): SortMergeJoinExec = {
    val left = SortExec(
      Seq(SortOrder(UnresolvedAttribute("blah"), Ascending)),
      true,
      ShuffleExchangeExec(
        HashPartitioning(Seq(UnresolvedAttribute("blah")), 100),
        RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(1, leftNum, 1, 1))))

    val right = SortExec(
      Seq(SortOrder(UnresolvedAttribute("blah"), Ascending)),
      true,
      ShuffleExchangeExec(
        HashPartitioning(Seq(UnresolvedAttribute("blah")), 100),
        RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(1, rightNum, 1, 1))))

    SortMergeJoinExec(
      Seq(UnresolvedAttribute("blah")),
      Seq(UnresolvedAttribute("blah")),
      Inner,
      None,
      left,
      right)
  }
}

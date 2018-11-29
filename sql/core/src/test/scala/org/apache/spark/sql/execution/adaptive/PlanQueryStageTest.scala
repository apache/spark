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
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.RangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BuildRight, ShuffledHashJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class PlanQueryStageTest extends SharedSQLContext {

  test("Replaces ShuffleExchangeExec/BroadcastExchangeExec with reuse disabled") {
    val range = org.apache.spark.sql.catalyst.plans.logical.Range(1, 100, 1, 1)
    val originalPlan = ShuffleExchangeExec(
      HashPartitioning(Seq(UnresolvedAttribute("blah")), 100),
      RangeExec(range))

    val conf = new SQLConf
    conf.setConfString("spark.sql.exchange.reuse", "false")
    val planQueryStage = PlanQueryStage(conf)
    val newPlan = planQueryStage(originalPlan)

    val expectedPlan = ResultQueryStage(
      ShuffleQueryStageInput(
        ShuffleQueryStage(originalPlan),
        range.output))

    assert(newPlan == expectedPlan)
  }

  test("Reuses ShuffleQueryStage when possible") {
    val conf = new SQLConf
    conf.setConfString("spark.sql.exchange.reuse", "true")

    val planQueryStage = PlanQueryStage(conf)
    val newPlan = planQueryStage(createJoinExec(100, 100))

    val collected = newPlan.collect {
      case e: ShuffleQueryStageInput => e.childStage
    }

    assert(collected.length == 2)
    assert(collected(0).eq(collected(1)))
  }

  test("Creates multiple ShuffleQueryStages when stages are different") {
    val conf = new SQLConf
    conf.setConfString("spark.sql.exchange.reuse", "true")

    val planQueryStage = PlanQueryStage(conf)
    val newPlan = planQueryStage(createJoinExec(100, 101))

    val collected = newPlan.collect {
      case e: ShuffleQueryStageInput => e.childStage
    }

    assert(collected.length == 2)
    assert(!collected(0).eq(collected(1)))
  }

  def createJoinExec(leftNum: Int, rightNum: Int): ShuffledHashJoinExec = {
    val left = ShuffleExchangeExec(
      HashPartitioning(Seq(UnresolvedAttribute("blah")), 100),
      RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(1, leftNum, 1, 1)))

    val right = ShuffleExchangeExec(
      HashPartitioning(Seq(UnresolvedAttribute("blah")), 100),
      RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(1, rightNum, 1, 1)))

    ShuffledHashJoinExec(
      Seq(UnresolvedAttribute("blah")),
      Seq(UnresolvedAttribute("blah")),
      Inner,
      BuildRight,
      None,
      left,
      right)
  }
}

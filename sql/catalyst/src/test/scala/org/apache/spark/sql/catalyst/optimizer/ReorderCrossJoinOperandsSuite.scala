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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.{StatsEstimationTestBase, StatsTestPlan}
import org.apache.spark.sql.internal.SQLConf.CBO_ENABLED

class ReorderCrossJoinOperandsSuite extends PlanTest with StatsEstimationTestBase {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Optimize Cartesian Products", Once, ReorderCrossJoinOperands) :: Nil
  }

  var originalConfCBOEnabled = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    originalConfCBOEnabled = conf.cboEnabled
    conf.setConf(CBO_ENABLED, true)
  }

  override def afterAll(): Unit = {
    try {
      conf.setConf(CBO_ENABLED, originalConfCBOEnabled)
    } finally {
      super.afterAll()
    }
  }

  private val t1 = StatsTestPlan(
    outputList = Seq('t1_c1.int, 't1_c2.int),
    rowCount = 50,
    size = Some(3000),
    attributeStats = AttributeMap[ColumnStat](Seq.empty))

  private val t2 = StatsTestPlan(
    outputList = Seq('t2_c1.int, 't2_c2.int),
    rowCount = 10,
    size = Some(3000),
    attributeStats = AttributeMap[ColumnStat](Seq.empty))

  private val t3 = StatsTestPlan(
    outputList = Seq('t1_c1.int, 't1_c2.int),
    rowCount = 50,
    size = Some(3000),
    attributeStats = AttributeMap[ColumnStat](Seq.empty))

  private val noStatsTable = LocalRelation('a.int)

  test("Reorder sides when left is bigger than right") {
    val analyzedPlan = t1.join(t2, Cross, None).analyze
    val optimizedPlan = Optimize.execute(analyzedPlan)
    val correctAnswer = t2.join(t1, Cross, None).analyze
    comparePlans(optimizedPlan, correctAnswer)
  }

  test("Do not reorder when left is not bigger") {
    // The number of rows is the same so the plan should not be changed
    val analyzedPlan = t1.join(t3, Cross, None).analyze
    val optimizedPlan = Optimize.execute(analyzedPlan)
    comparePlans(optimizedPlan, analyzedPlan)

    // The number of rows is lower for left, so no changes here too
    val analyzedPlan2 = t2.join(t3, Cross, None).analyze
    val optimizedPlan2 = Optimize.execute(analyzedPlan2)
    comparePlans(optimizedPlan2, analyzedPlan2)
  }

  test("Do nothing if stats are not collected") {
    val analyzedPlan = t1.join(noStatsTable, Cross, None).analyze
    val optimizedPlan = Optimize.execute(analyzedPlan)
    comparePlans(optimizedPlan, analyzedPlan)
  }

  test("Optimize also if it is an implicit cartesian") {
    Seq(Inner, LeftOuter, RightOuter, FullOuter).foreach { joinType =>
      val analyzedPlan = t1.join(t2, joinType, None).analyze
      val optimizedPlan = Optimize.execute(analyzedPlan)
      val correctAnswer = t2.join(t1, joinType, None).analyze
      comparePlans(optimizedPlan, correctAnswer)
    }
  }
}

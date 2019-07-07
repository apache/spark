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
import org.apache.spark.sql.catalyst.expressions.Exists
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReplaceLeftSemiAntiJoinWithFilterSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Replace Left Semi/Anti Join With Filter", Once,
        ReplaceLeftSemiAntiJoinWithFilter) :: Nil
  }

  val testRelation = LocalRelation('t1a.int, 't1b.int)
  val testRelation2 = LocalRelation('t2a.int, 't2b.int)

  test("Replace left semi join with filter") {
    val plan = testRelation.join(
      testRelation2,
      LeftSemi,
      Some(('t1a === 1) && ('t2b > 2))
    )
    val answer = testRelation.where(('t1a === 1) && Exists(
      testRelation2.where('t2b > 2)
    )).analyze
    comparePlans(Optimize.execute(plan.analyze), answer)
  }

  test("Replace left anti join with filter") {
    val plan = testRelation.join(
      testRelation2,
      LeftAnti,
      Some(('t1a === 1) && ('t2b > 2))
    )
    val answer = testRelation.where(!(('t1a === 1) && Exists(
      testRelation2.where('t2b > 2)
    ))).analyze
    comparePlans(Optimize.execute(plan.analyze), answer)
  }


}

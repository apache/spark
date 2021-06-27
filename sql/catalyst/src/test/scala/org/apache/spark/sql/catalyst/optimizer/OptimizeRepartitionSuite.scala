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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class OptimizeRepartitionSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("OptimizeRepartition", FixedPoint(10),
        OptimizeRepartition) :: Nil
  }

  val testRelation = LocalRelation.fromExternalRows(
    Seq('a.int, 'b.int, 'c.int),
    Seq(Row(1, 2, 3), Row(4, 5, 6)))

  test("Remove repartition if the child maximum number of rows less than or equal to 1") {
    comparePlans(
      Optimize.execute(testRelation.limit(1).repartition(10).analyze),
      testRelation.limit(1).analyze)
    comparePlans(
      Optimize.execute(testRelation.groupBy()(count(1).as("cnt")).coalesce(1)).analyze,
      testRelation.groupBy()(count(1).as("cnt")).analyze)
  }
}

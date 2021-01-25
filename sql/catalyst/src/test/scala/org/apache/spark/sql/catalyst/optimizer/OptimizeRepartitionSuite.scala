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

import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class OptimizeRepartitionSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("OptimizeRepartition", Once,
        OptimizeRepartition) :: Nil
  }

  val range0 = Range.apply(1, 1, 1, 1)
  val range1 = Range.apply(1, 2, 1, 1)

  test("test child max row is zero") {
    val plan = range0.repartition(10)
    comparePlans(Optimize.execute(plan.analyze), range0.analyze)
  }

  test("test child max row < num partitions") {
    val plan1 = range1.repartition(2)
    comparePlans(Optimize.execute(plan1.analyze), range1.repartition(1).analyze)

    val plan2 = range1.distribute()(2)
    comparePlans(Optimize.execute(plan2.analyze), range1.distribute()(1).analyze)
  }

  test("test child max row >= num partitions") {
    val plan1 = range1.repartition(1)
    comparePlans(Optimize.execute(plan1.analyze), range1.repartition(1).analyze)

    val plan2 = range1.distribute()(1)
    comparePlans(Optimize.execute(plan2.analyze), range1.distribute()(1).analyze)
  }
}

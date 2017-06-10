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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Range}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class RemoveInvalidRangeSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("RemoveInvalidRange", Once,
        RemoveInvalidRange) :: Nil
  }

  test("preserve valid ranges") {
    Seq(Range(0, 10, 1, 1), Range(10, 0, -1, 1)).foreach { query =>
      val optimized = Optimize.execute(query.analyze)
      val correctAnswer = query

      comparePlans(optimized, correctAnswer)
    }
  }

  test("remove ranges with invalid combination of start/end/step") {
    Seq(Range(0, 0, 1, 1), Range(0, 0, -1, 1), Range(1, 10, -1, 1), Range(10, 1, 1, 1)).foreach {
      query =>
        val optimized = Optimize.execute(query.analyze)
        val correctAnswer = LocalRelation(query.output, data = Seq.empty)
        comparePlans(optimized, correctAnswer)
    }
  }
}

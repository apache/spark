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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Range}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class CombineRepartitionAndRangeSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("CombineRepartitionAndRange", FixedPoint(10),
        CombineRepartitionAndRange) :: Nil
  }

  test("Combine range repartition and immediate child range operation") {
    val query = Range(1, 100, 1, 10).distribute('id.asc)(20)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Range(1, 100, 1, 20)

    comparePlans(optimized, correctAnswer)
  }

  test("Effect if all intermediate nodes are OrderPreservingUnaryNode") {
    val query = Range(1, 100, 1, 10)
      .where('id > 2)
      .select('id, 'id * 2)
      .limit(50)
      .tail(10)
      .distribute('id.asc)(20)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Range(1, 100, 1, 20)
      .where('id > 2)
      .select('id, 'id * 2)
      .limit(50)
      .tail(10)
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Do not effect if not all intermediate nodes are OrderPreservingUnaryNode") {
        val query = Range(1, 100, 1, 10)
      .where('id > 2)
      .select('id, 'id * 2)
      .limit(50)
      .orderBy('id.desc)
      .tail(10)
      .distribute('id.asc)(20)
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }
}

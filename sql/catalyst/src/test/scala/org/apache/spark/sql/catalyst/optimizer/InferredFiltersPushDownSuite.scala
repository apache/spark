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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class InferredFiltersPushDownSuite  extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("NullFiltering", FixedPoint(15),
        SetOperationPushDown,
        PushPredicateThroughJoin,
        PushPredicateThroughProject,
        PushPredicateThroughGenerate,
        PushPredicateThroughAggregate,
        NullFiltering,
        CollapseProject,
        PruneFilters,
        CombineFilters) :: Nil
  }

  val testRelation1 = LocalRelation('a.string, 'b.int, 'c.int)
  val testRelation2 = LocalRelation('a.string, 'b.int, 'c.int)

  test("filter: do not push predicates") {
    val x2 = testRelation1.select("tst1".as("key"), 'b)
    val x1 = testRelation1.groupBy('a)('a.as("x2a"), 'b + 1)
    val y = testRelation1
    val union1 = x1.unionAll(x2)

    val originalQuery = union1.join(y, condition = Some('x2a === 'a)).analyze

    val x1Optimized =
      testRelation1.select("tst1".as("key"), 'b).where(IsNotNull('key))
    val x2Optimized = testRelation1.where(IsNotNull('a)).groupBy('a)('a.as("x2a"), 'b + 1)
    val unionOptimized = x2Optimized.unionAll(x1Optimized)
    val correctAnswer =
      unionOptimized.join(y.where(IsNotNull('a)), condition = Some('x2a === 'a)).analyze
    val optimized = Optimize.execute(originalQuery)
    comparePlans(optimized, correctAnswer)
  }
}

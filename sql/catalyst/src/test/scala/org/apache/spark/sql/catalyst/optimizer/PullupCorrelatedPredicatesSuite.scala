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
import org.apache.spark.sql.catalyst.expressions.{InSubquery, ListQuery}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class PullupCorrelatedPredicatesSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("PullupCorrelatedPredicates", Once,
        PullupCorrelatedPredicates) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.double)
  val testRelation2 = LocalRelation('c.int, 'd.double)

  test("PullupCorrelatedPredicates should not produce unresolved plan") {
    val correlatedSubquery =
      testRelation2
        .where('b < 'd)
        .select('c)
    val outerQuery =
      testRelation
        .where(InSubquery(Seq('a), ListQuery(correlatedSubquery)))
        .select('a).analyze
    assert(outerQuery.resolved)

    val optimized = Optimize.execute(outerQuery)
    assert(optimized.resolved)
  }
}

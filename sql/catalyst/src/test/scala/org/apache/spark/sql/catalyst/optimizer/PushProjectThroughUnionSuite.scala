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
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class PushProjectThroughUnionSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Optimizer Batch", FixedPoint(100),
      PushProjectionThroughUnion,
      FoldablePropagation) :: Nil
  }

  test("SPARK-25450 PushProjectThroughUnion rule uses the same exprId for project expressions " +
    "in each Union child, causing mistakes in constant propagation") {
    val testRelation1 = LocalRelation('a.string, 'b.int, 'c.string)
    val testRelation2 = LocalRelation('d.string, 'e.int, 'f.string)
    val query = testRelation1
      .union(testRelation2.select("bar".as("d"), 'e, 'f))
      .select('a.as("n"))
      .select('n, "dummy").analyze
    val optimized = Optimize.execute(query)

    val expected = testRelation1
      .select('a.as("n"))
      .select('n, "dummy")
      .union(testRelation2
        .select("bar".as("d"), 'e, 'f)
        .select("bar".as("n"))
        .select("bar".as("n"), "dummy")).analyze

    comparePlans(optimized, expected)
  }
}

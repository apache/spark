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

class PushProjectionThroughLimitSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Optimizer Batch",
      FixedPoint(100),
      PushProjectionThroughLimit,
      EliminateLimits) :: Nil
  }

  test("SPARK-40501: push projection through limit") {
    val testRelation = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(20).map(_ => Row(1, 2, 3)))

    val query1 = testRelation
      .limit(10)
      .select('a, 'b, 'c')
      .limit(15).analyze
    val optimized1 = Optimize.execute(query1)
    val expected1 = testRelation
      .select('a, 'b, 'c')
      .limit(10).analyze
    comparePlans(optimized1, expected1)

    val query2 = testRelation
      .sortBy($"a".asc)
      .limit(10)
      .select('a, 'b, 'c')
      .limit(15).analyze
    val optimized2 = Optimize.execute(query2)
    val expected2 = testRelation
      .sortBy($"a".asc)
      .select('a, 'b, 'c')
      .limit(10).analyze
    comparePlans(optimized2, expected2)

    val query3 = testRelation
      .limit(10)
      .select('a, 'b, 'c')
      .limit(20)
      .select('a)
      .limit(15).analyze
    val optimized3 = Optimize.execute(query3)
    val expected3 = testRelation
      .select('a, 'b, 'c')
      .select('a)
      .limit(10).analyze
    comparePlans(optimized3, expected3)

    val query4 = testRelation
      .sortBy($"a".asc)
      .limit(10)
      .select('a, 'b, 'c')
      .limit(20)
      .select('a)
      .limit(15).analyze
    val optimized4 = Optimize.execute(query4)
    val expected4 = testRelation
      .sortBy($"a".asc)
      .select('a, 'b, 'c')
      .select('a)
      .limit(10).analyze
    comparePlans(optimized4, expected4)
  }
}

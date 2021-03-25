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
import org.apache.spark.sql.types.{MapType, StringType}

class NormalizeMapTypeSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("NormalizeMapType", Once, NormalizeMapType) :: Nil
  }

  val testRelation1 = LocalRelation('a.int, 'm.map(MapType(StringType, StringType, false)))
  val a1 = testRelation1.output(0)
  val m1 = testRelation1.output(1)

  val testRelation2 = LocalRelation('a.int, 'm.map(MapType(StringType, StringType, false)))
  val a2 = testRelation2.output(0)
  val m2 = testRelation2.output(1)

  test("normalize map types in window function expressions") {
    val query = testRelation1.window(Seq(sum(a1).as("sum")), Seq(m1), Seq(m1.asc))
    val optimized = Optimize.execute(query)
    val correctAnswer = testRelation1.window(Seq(sum(a1).as("sum")),
      Seq(SortMapKey(m1)), Seq(m1.asc))

    comparePlans(optimized, correctAnswer)
  }

  test("normalize map types in window function expressions - idempotence") {
    val query = testRelation1.window(Seq(sum(a1).as("sum")), Seq(m1), Seq(m1.asc))
    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val correctAnswer = testRelation1.window(Seq(sum(a1).as("sum")),
      Seq(SortMapKey(m1)), Seq(m1.asc))

    comparePlans(doubleOptimized, correctAnswer)
  }

  test("normalize map types in join keys") {
    val query = testRelation1.join(testRelation2, condition = Some(m1 === m2))

    val optimized = Optimize.execute(query)
    val joinCond = Some(SortMapKey(m1) === SortMapKey(m2))
    val correctAnswer = testRelation1.join(testRelation2, condition = joinCond)

    comparePlans(optimized, correctAnswer)
  }

  test("normalize map types in join keys - idempotence") {
    val query = testRelation1.join(testRelation2, condition = Some(m1 === m2))

    val optimized = Optimize.execute(query)
    val doubleOptimized = Optimize.execute(optimized)
    val joinCond = Some(SortMapKey(m1) === SortMapKey(m2))
    val correctAnswer = testRelation1.join(testRelation2, condition = joinCond)

    comparePlans(doubleOptimized, correctAnswer)
  }
}



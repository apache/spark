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
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.DoubleType

class SimplifyAssociativeOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("SimplifyAssociativeOperator", FixedPoint(100),
        SimplifyAssociativeOperator) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.double, 'c.int)

  test("Reorder associative operators") {
    val originalQuery =
      testRelation
        .select(
          ((Literal(3) + ((Literal(1) + 'a) - 3)) - 1).as("a1"),
          ((Literal(2.0) + 'b) * 2.0 / 2.0 / 3.0 * 3.0).as("b1"),
          (('c + 1) / ('b + 2) * ('b + 2 - 'a + 'a)).as("c1"),
          Rand(0) * 1 * 2 * 3 * 4)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          ('a).as("a1"),
          (Literal(2.0) + 'b).as("b1"),
          (Cast('c + 1, DoubleType)).as("c1"),
          Rand(0) * 1 * 2 * 3 * 4)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nested expression with aggregate operator") {
    val originalQuery =
      testRelation.as("t1")
        .join(testRelation.as("t2"), Inner, Some("t1.a".attr === "t2.a".attr))
        .groupBy("t1.a".attr + "t1.a".attr - "t1.a".attr, "t2.a".attr + "t2.a".attr - "t2.a".attr)(
          ("t1.a".attr + "t1.a".attr - "t1.a".attr).as("col"))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }
}

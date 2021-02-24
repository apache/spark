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

class ReorderAssociativeOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("ReorderAssociativeOperator", Once,
        ReorderAssociativeOperator) :: Nil
  }

  val testRelation = LocalRelation("a".attr.int, "b".attr.int, "c".attr.int)

  test("Reorder associative operators") {
    val originalQuery =
      testRelation
        .select(
          (Literal(3) + ((Literal(1) + "a".attr) + 2)) + 4,
          "b".attr * 1 * 2 * 3 * 4,
          ("b".attr + 1) * 2 * 3 * 4,
          "a".attr + 1 + "b".attr + 2 + "c".attr + 3,
          "a".attr + 1 + "b".attr * 2 + "c".attr + 3,
          Rand(0) * 1 * 2 * 3 * 4)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          ("a".attr + 10).as("((3 + ((1 + a) + 2)) + 4)"),
          ("b".attr * 24).as("((((b * 1) * 2) * 3) * 4)"),
          (("b".attr + 1) * 24).as("((((b + 1) * 2) * 3) * 4)"),
          ("a".attr + "b".attr + "c".attr + 6).as("(((((a + 1) + b) + 2) + c) + 3)"),
          ("a".attr + "b".attr * 2 + "c".attr + 4).as("((((a + 1) + (b * 2)) + c) + 3)"),
          Rand(0) * 1 * 2 * 3 * 4)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("nested expression with aggregate operator") {
    val originalQuery =
      testRelation.as("t1")
        .join(testRelation.as("t2"), Inner, Some("t1.a".attr === "t2.a".attr))
        .groupBy("t1.a".attr + 1, "t2.a".attr + 1)(
          (("t1.a".attr + 1) + ("t2.a".attr + 1)).as("col"))

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }
}

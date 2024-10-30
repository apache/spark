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
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{Inner, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ReorderAssociativeOperatorSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("ReorderAssociativeOperator", Once,
        ReorderAssociativeOperator) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int, $"c".int)

  test("Reorder associative operators") {
    val originalQuery =
      testRelation
        .select(
          (Literal(3) + ((Literal(1) + $"a") + 2)) + 4,
          $"b" * 1 * 2 * 3 * 4,
          ($"b" + 1) * 2 * 3 * 4,
          $"a" + 1 + $"b" + 2 + $"c" + 3,
          $"a" + 1 + $"b" * 2 + $"c" + 3,
          Rand(0) * 1 * 2 * 3 * 4)

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          ($"a" + 10).as("((3 + ((1 + a) + 2)) + 4)"),
          ($"b" * 24).as("((((b * 1) * 2) * 3) * 4)"),
          (($"b" + 1) * 24).as("((((b + 1) * 2) * 3) * 4)"),
          ($"a" + $"b" + $"c" + 6).as("(((((a + 1) + b) + 2) + c) + 3)"),
          ($"a" + $"b" * 2 + $"c" + 4).as("((((a + 1) + (b * 2)) + c) + 3)"),
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

  test("SPARK-49915: Handle zero and one in associative operators") {
    val originalQuery =
      testRelation.select(
        $"a" + 0,
        Literal(-3) + $"a" + 3,
        $"b" * 0 * 1 * 2 * 3,
        Count($"b") * 0,
        $"b" * 1 * 1,
        ($"b" + 0) * 1 * 2 * 3 * 4,
        $"a" + 0 + $"b" + 0 + $"c" + 0,
        $"a" + 0 + $"b" * 1 + $"c" + 0
      )

    val optimized = Optimize.execute(originalQuery.analyze)

    val correctAnswer =
      testRelation
        .select(
          $"a".as("(a + 0)"),
          $"a".as("((-3 + a) + 3)"),
          ($"b" * 0).as("((((b * 0) * 1) * 2) * 3)"),
          Literal(0L).as("(count(b) * 0)"),
          $"b".as("((b * 1) * 1)"),
          ($"b" * 24).as("(((((b + 0) * 1) * 2) * 3) * 4)"),
          ($"a" + $"b" + $"c").as("""(((((a + 0) + b) + 0) + c) + 0)"""),
          ($"a" + $"b" + $"c").as("((((a + 0) + (b * 1)) + c) + 0)")
        ).analyze

    comparePlans(optimized, correctAnswer)
  }
}

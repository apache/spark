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
import org.apache.spark.sql.catalyst.expressions.{Add, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class EliminateOffsetsSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate Offset", FixedPoint(10), EliminateOffsets) :: Nil
  }

  val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(10).map(_ => Row(1, 2, 3))
  )

  test("Offsets: eliminate Offset operators if offset == 0") {
    val originalQuery =
      testRelation
        .select($"a")
        .offset(0)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select($"a")
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Offsets: cannot eliminate Offset operators if offset > 0") {
    val originalQuery =
      testRelation
        .select($"a")
        .offset(2)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      testRelation
        .select($"a")
        .offset(2)
        .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Replace Offset operators to empty LocalRelation if child max row <= offset") {
    val child = testRelation.select($"a").analyze
    val originalQuery = child.offset(10)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer =
      LocalRelation(child.output, data = Seq.empty, isStreaming = child.isStreaming).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Cannot replace Offset operators to empty LocalRelation if child max row > offset") {
    val child = testRelation.select($"a").analyze
    val originalQuery = child.offset(3)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = originalQuery.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("Combines Offset operators") {
    val child = testRelation.select($"a").analyze
    val originalQuery = child.offset(2).offset(3)

    val optimized = Optimize.execute(originalQuery.analyze)
    val correctAnswer = child.offset(Add(Literal(3), Literal(2))).analyze

    comparePlans(optimized, correctAnswer)
  }
}

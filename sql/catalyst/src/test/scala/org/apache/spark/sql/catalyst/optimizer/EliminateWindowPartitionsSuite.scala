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

class EliminateWindowPartitionsSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Eliminate window partitions", FixedPoint(20),
        EliminateWindowPartitions) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int)
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

  test("Remove foldable window partitions") {
    val originalQuery =
      testRelation
        .select(a, b,
          windowExpr(RowNumber(),
            windowSpec(Literal(1) :: Nil, b.desc :: Nil, windowFrame)).as("rn"))

    val correctAnswer =
      testRelation
        .select(a, b,
          windowExpr(RowNumber(),
            windowSpec(Nil, b.desc :: Nil, windowFrame)).as("rn"))
    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Remove part of window partitions which is foldable") {
    val originalQuery =
      testRelation
        .select(a, b,
          windowExpr(RowNumber(),
            windowSpec(a :: Literal(1) :: Nil, b.desc :: Nil, windowFrame)).as("rn"))

    val correctAnswer =
      testRelation
        .select(a, b,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, b.desc :: Nil, windowFrame)).as("rn"))
    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }

  test("Can't remove non-foldable window partitions") {
    val originalQuery =
      testRelation
        .select(a, b,
          windowExpr(RowNumber(),
            windowSpec(a :: Nil, b.desc :: Nil, windowFrame)).as("rn"))

    val correctAnswer = originalQuery
    comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
  }
}

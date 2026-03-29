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
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

class OptimizeJoinConditionSuite extends PlanTest {

  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Optimize join condition", FixedPoint(1),
        OptimizeJoinCondition) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int)
  val testRelation1 = LocalRelation($"c".int, $"d".int)

  test("Replace equivalent expression to <=> in join condition") {
    val x = testRelation.subquery("x")
    val y = testRelation1.subquery("y")
    val joinTypes = Seq(Inner, FullOuter, LeftOuter, RightOuter, LeftSemi, LeftSemi, Cross)
    joinTypes.foreach(joinType => {
      val originalQuery =
        x.join(y, joinType, Option($"a" === $"c" || ($"a".isNull && $"c".isNull)))
      val correctAnswer =
        x.join(y, joinType, Option($"a" <=> $"c"))
      comparePlans(Optimize.execute(originalQuery.analyze), correctAnswer.analyze)
    })
  }
}

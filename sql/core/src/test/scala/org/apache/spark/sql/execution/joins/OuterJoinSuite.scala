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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Expression, LessThan}
import org.apache.spark.sql.catalyst.plans.{FullOuter, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}

class OuterJoinSuite extends SparkPlanTest {

  val left = Seq(
    (1, 2.0),
    (2, 1.0),
    (3, 3.0)
  ).toDF("a", "b")

  val right = Seq(
    (2, 3.0),
    (3, 2.0),
    (4, 1.0)
  ).toDF("c", "d")

  val leftKeys: List[Expression] = 'a :: Nil
  val rightKeys: List[Expression] = 'c :: Nil
  val condition = Some(LessThan('b, 'd))

  test("shuffled hash outer join") {
    checkAnswer2(left, right, (left: SparkPlan, right: SparkPlan) =>
      ShuffledHashOuterJoin(leftKeys, rightKeys, LeftOuter, condition, left, right),
      Seq(
        (1, 2.0, null, null),
        (2, 1.0, 2, 3.0),
        (3, 3.0, null, null)
      ).map(Row.fromTuple))

    checkAnswer2(left, right, (left: SparkPlan, right: SparkPlan) =>
      ShuffledHashOuterJoin(leftKeys, rightKeys, RightOuter, condition, left, right),
      Seq(
        (2, 1.0, 2, 3.0),
        (null, null, 3, 2.0),
        (null, null, 4, 1.0)
      ).map(Row.fromTuple))

    checkAnswer2(left, right, (left: SparkPlan, right: SparkPlan) =>
      ShuffledHashOuterJoin(leftKeys, rightKeys, FullOuter, condition, left, right),
      Seq(
        (1, 2.0, null, null),
        (2, 1.0, 2, 3.0),
        (3, 3.0, null, null),
        (null, null, 3, 2.0),
        (null, null, 4, 1.0)
      ).map(Row.fromTuple))
  }

  test("broadcast hash outer join") {
    checkAnswer2(left, right, (left: SparkPlan, right: SparkPlan) =>
      BroadcastHashOuterJoin(leftKeys, rightKeys, LeftOuter, condition, left, right),
      Seq(
        (1, 2.0, null, null),
        (2, 1.0, 2, 3.0),
        (3, 3.0, null, null)
      ).map(Row.fromTuple))

    checkAnswer2(left, right, (left: SparkPlan, right: SparkPlan) =>
      BroadcastHashOuterJoin(leftKeys, rightKeys, RightOuter, condition, left, right),
      Seq(
        (2, 1.0, 2, 3.0),
        (null, null, 3, 2.0),
        (null, null, 4, 1.0)
      ).map(Row.fromTuple))
  }
}

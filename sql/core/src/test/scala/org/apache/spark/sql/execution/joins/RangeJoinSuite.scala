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
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}

class RangeJoinSuite extends SparkPlanTest {
  val intervals1 = Seq(
    (-1, 0),
    (0, 1),
    (0, 2),
    (1, 5)
  ).toDF("low1", "high1")
  val intervalKeys1 = Seq("low1", "high1").map(UnresolvedAttribute.apply)

  val intervals2 = Seq(
    (-2, -1),
    (-4, -2),
    (1 ,3),
    (5, 7)
  ).toDF("low2", "high2")
  val intervalKeys2 = Seq("low2", "high2").map(UnresolvedAttribute.apply)

  val points = Seq(-3, 1, 3, 6).map(Tuple1.apply).toDF("point")
  val pointKeys = Seq("point", "point").map(UnresolvedAttribute.apply)

  test("interval-point range join") {
    // low1 <= point && point < high1
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(intervalKeys1, pointKeys, true :: false :: Nil, BuildRight, left, right),
      Seq(
        (0, 2, 1),
        (1, 5, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    // low1 <= point && point < high1
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(intervalKeys1, pointKeys, false :: false :: Nil, BuildRight, left, right),
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))

    // low <= point && point <= high1
    checkAnswer2(points, intervals1, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(pointKeys, intervalKeys1, true :: true :: Nil, BuildRight, left, right),
      Seq(
        (1, 0, 1),
        (1, 0, 2),
        (1, 1, 5),
        (3, 1, 5)
      ).map(Row.fromTuple))

    // low1 < point && point < high1
    checkAnswer2(intervals1, points, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(intervalKeys1, pointKeys, false :: false :: Nil, BuildLeft, left, right),
      Seq(
        (0, 2, 1),
        (1, 5, 3)
      ).map(Row.fromTuple))
  }

  test("interval-interval range join") {
    // low1 <= high2 && low2 < high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(intervalKeys1, intervalKeys2, true :: false :: Nil, BuildRight, left, right),
      Seq(
        (-1, 0, -2, -1),
        (0, 2, 1, 3),
        (1, 5, 1, 3)
      ).map(Row.fromTuple))

    // low1 < high2 && low2 <= high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(intervalKeys1, intervalKeys2, false :: true :: Nil, BuildLeft, left, right),
      Seq(
        (0, 1, 1, 3),
        (0, 2, 1, 3),
        (1, 5, 1, 3),
        (1, 5, 5, 7)
      ).map(Row.fromTuple))

    // low1 < high2 && low2 < high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(intervalKeys1, intervalKeys2, false :: false :: Nil, BuildRight, left, right),
      Seq(
        (0, 2, 1, 3),
        (1, 5, 1, 3)
      ).map(Row.fromTuple))

    // low1 <= high2 && low2 <= high1
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) =>
      BroadcastRangeJoin(intervalKeys1, intervalKeys2, true :: true :: Nil, BuildLeft, left, right),
      Seq(
        (-1, 0, -2, -1),
        (0, 1, 1, 3),
        (0, 2, 1, 3),
        (1, 5, 1, 3),
        (1, 5, 5, 7)
      ).map(Row.fromTuple))
  }
}

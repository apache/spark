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

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{
  And, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.{GreaterPartialRangeJoin, IntervalOverlapJoin, LessPartialRangeJoin, PointInRangeJoin, RangeJoin}
import org.apache.spark.sql.catalyst.plans.{
  Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Operator results for [[BroadcastRangeJoinExec]]. The planner is covered by
 * `RangeJoinSQLSuite`; recognition and the index are covered below that.
 */
class RangeJoinSuite extends QueryTest with SharedSparkSession {
  private lazy val intervals1: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(-1, 0),
      Row(0, 1),
      Row(0, 2),
      Row(1, 5)
    )), new StructType().add("low1", IntegerType).add("high1", IntegerType))

  private lazy val intervals2: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(-2, -1),
      Row(1, 3),
      Row(5, 7)
    )), new StructType().add("low2", IntegerType).add("high2", IntegerType))

  private lazy val points: DataFrame = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(-3),
      Row(1),
      Row(3),
      Row(6)
    )), new StructType().add("point", IntegerType))

  /**
   * Operator-level helper. Keys are the original expressions from each child.
   * `condition` is the original join predicate and the only accept/reject check.
   */
  private def rangeJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      buildKeys: Seq[Expression],
      streamedKeys: Seq[Expression],
      condition: Expression,
      rangeJoin: RangeJoin = PointInRangeJoin): BroadcastRangeJoinExec = {
    val (leftKeys, rightKeys) = buildSide match {
      case BuildLeft => (buildKeys, streamedKeys)
      case BuildRight => (streamedKeys, buildKeys)
    }
    BroadcastRangeJoinExec(
      leftKeys, rightKeys, joinType, buildSide, Some(condition), rangeJoin, left, right)
  }

  private def pointCondition(
      point: Expression,
      low: Expression,
      high: Expression,
      lowInclusive: Boolean,
      highInclusive: Boolean,
      highFirst: Boolean = false): Expression = {
    val lowPred = if (lowInclusive) GreaterThanOrEqual(point, low) else GreaterThan(point, low)
    val highPred = if (highInclusive) LessThanOrEqual(point, high) else LessThan(point, high)
    if (highFirst) And(highPred, lowPred) else And(lowPred, highPred)
  }

  private def checkPointJoin(
      left: DataFrame,
      right: DataFrame,
      pointOnLeft: Boolean,
      buildSide: BuildSide,
      joinType: JoinType,
      lowInclusive: Boolean,
      highInclusive: Boolean,
      expected: Seq[Row],
      highFirst: Boolean = false): Unit = {
    checkAnswer2(left, right, (leftPlan: SparkPlan, rightPlan: SparkPlan) => {
      val pointPlan = if (pointOnLeft) leftPlan else rightPlan
      val boundPlan = if (pointOnLeft) rightPlan else leftPlan
      val point = pointPlan.output.head
      val bounds = boundPlan.output
      val pointKeys = point :: point :: Nil
      val buildIsPoint = buildSide match {
        case BuildRight => !pointOnLeft
        case BuildLeft => pointOnLeft
      }
      val (buildKeys, streamedKeys): (Seq[Expression], Seq[Expression]) =
        if (buildIsPoint) (pointKeys, bounds) else (bounds, pointKeys)
      rangeJoinExec(
        leftPlan, rightPlan, buildSide, joinType, buildKeys, streamedKeys,
        pointCondition(point, bounds(0), bounds(1), lowInclusive, highInclusive, highFirst))
    }, expected)
  }

  test("point-in-range keeps boundary inclusivity on the condition") {
    val lowInclHighExcl = Seq((0, 2, 1), (1, 5, 1), (1, 5, 3)).map(Row.fromTuple)
    val bothExcl = Seq((0, 2, 1), (1, 5, 3)).map(Row.fromTuple)
    val bothIncl = Seq((1, 0, 1), (1, 0, 2), (1, 1, 5), (3, 1, 5)).map(Row.fromTuple)

    Seq(false, true).foreach { highFirst =>
      checkPointJoin(
        intervals1, points, pointOnLeft = false, BuildRight, Inner,
        lowInclusive = true, highInclusive = false, lowInclHighExcl, highFirst)
    }
    checkPointJoin(
      intervals1, points, pointOnLeft = false, BuildRight, Inner,
      lowInclusive = false, highInclusive = false, bothExcl)
    checkPointJoin(
      points, intervals1, pointOnLeft = true, BuildRight, Inner,
      lowInclusive = true, highInclusive = true, bothIncl)
    checkPointJoin(
      intervals1, points, pointOnLeft = false, BuildLeft, Inner,
      lowInclusive = false, highInclusive = false, bothExcl)
    checkPointJoin(
      intervals1, points, pointOnLeft = false, BuildLeft, Inner,
      lowInclusive = true, highInclusive = false, lowInclHighExcl)
  }

  test("outer, semi, and anti preserve unmatched streamed rows") {
    // low <= point && point < high. (-1, 0) and (0, 1) contain no point.
    checkPointJoin(
      intervals1, points, pointOnLeft = false, BuildRight, LeftOuter,
      lowInclusive = true, highInclusive = false,
      Seq((-1, 0, null), (0, 1, null), (0, 2, 1), (1, 5, 1), (1, 5, 3)).map(Row.fromTuple))
    checkPointJoin(
      intervals1, points, pointOnLeft = false, BuildLeft, RightOuter,
      lowInclusive = true, highInclusive = false,
      Seq((null, null, -3), (0, 2, 1), (1, 5, 1), (1, 5, 3), (null, null, 6)).map(Row.fromTuple))
    checkPointJoin(
      intervals1, points, pointOnLeft = false, BuildRight, LeftSemi,
      lowInclusive = true, highInclusive = false,
      Seq((0, 2), (1, 5)).map(Row.fromTuple))
    checkPointJoin(
      intervals1, points, pointOnLeft = false, BuildRight, LeftAnti,
      lowInclusive = true, highInclusive = false,
      Seq((-1, 0), (0, 1)).map(Row.fromTuple))
  }

  test("a null streamed bound is not a match") {
    val nullInterval = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(null, 5))),
      new StructType().add("low1", IntegerType).add("high1", IntegerType))
    checkPointJoin(
      nullInterval, points, pointOnLeft = false, BuildRight, LeftOuter,
      lowInclusive = true, highInclusive = false, Seq(Row(null, 5, null)))
    checkPointJoin(
      nullInterval, points, pointOnLeft = false, BuildRight, LeftSemi,
      lowInclusive = true, highInclusive = false, Seq.empty)
    checkPointJoin(
      nullInterval, points, pointOnLeft = false, BuildRight, LeftAnti,
      lowInclusive = true, highInclusive = false, Seq(Row(null, 5)))
  }

  test("partial range scans upTo or from according to the lower bound") {
    // a.lo < b.hi builds the high side and scans from the stream key.
    // a.lo > b.hi builds the low side and scans up to the stream key.
    def check(rangeJoin: RangeJoin, condition: (SparkPlan, SparkPlan) => Expression,
        expected: Seq[Row]): Unit = {
      checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) => {
        rangeJoinExec(
          left, right, BuildRight, Inner,
          buildKeys = right.output(1) :: Nil,
          streamedKeys = left.output.head :: Nil,
          condition = condition(left, right),
          rangeJoin = rangeJoin)
      }, expected)
    }
    check(
      LessPartialRangeJoin,
      (left, right) => LessThan(left.output.head, right.output(1)),
      Seq(
        (-1, 0, 1, 3), (-1, 0, 5, 7),
        (0, 1, 1, 3), (0, 1, 5, 7),
        (0, 2, 1, 3), (0, 2, 5, 7),
        (1, 5, 1, 3), (1, 5, 5, 7)).map(Row.fromTuple))
    check(
      GreaterPartialRangeJoin,
      (left, right) => GreaterThan(left.output.head, right.output(1)),
      Seq((0, 1, -2, -1), (0, 2, -2, -1), (1, 5, -2, -1)).map(Row.fromTuple))
  }

  test("interval overlap probes both bounds") {
    val condition = (left: SparkPlan, right: SparkPlan) => And(
      LessThan(left.output(0), right.output(1)),
      LessThan(right.output(0), left.output(1)))
    checkAnswer2(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) => {
      rangeJoinExec(
        left, right, BuildRight, Inner,
        buildKeys = right.output,
        streamedKeys = left.output,
        condition = condition(left, right),
        rangeJoin = IntervalOverlapJoin)
    },
      Seq(
        (0, 2, 1, 3),
        (1, 5, 1, 3)
      ).map(Row.fromTuple))
  }

  test("condition and keys stay on the operator") {
    val leftPlan = intervals1.queryExecution.executedPlan
    val rightPlan = points.queryExecution.executedPlan
    val cond = pointCondition(
      rightPlan.output.head, leftPlan.output(0), leftPlan.output(1),
      lowInclusive = true, highInclusive = false)
    val exec = rangeJoinExec(
      leftPlan, rightPlan, BuildRight, Inner,
      buildKeys = rightPlan.output.head :: rightPlan.output.head :: Nil,
      streamedKeys = leftPlan.output,
      condition = cond)
    assert(exec.expressions.exists(_.semanticEquals(cond)))
    assert(exec.leftKeys == leftPlan.output)
    assert(!exec.verboseStringWithOperatorId().contains("none#"))
    assert(exec.verboseStringWithOperatorId().contains("low1"))
  }
}

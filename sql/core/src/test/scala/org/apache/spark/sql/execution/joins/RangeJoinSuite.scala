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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, GreaterPartialRangeJoin, IntervalOverlapJoin, LessPartialRangeJoin, PointInRangeJoin, RangeJoin}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}
import org.apache.spark.util.LongAccumulator

/**
 * Wraps `child` and adds to `counter` each time it is evaluated, interpreted or
 * generated. Lets a test count how many build-side candidates a join actually probes,
 * without needing a metric on the operator under test.
 */
private[joins] case class CountingPredicate(counter: LongAccumulator, child: Expression)
  extends UnaryExpression {

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  override def eval(input: InternalRow): Any = {
    counter.add(1)
    child.eval(input)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val counterTerm = ctx.addReferenceObj("counter", counter)
    val code = code"""
      |$counterTerm.add(1);
      |${childGen.code}
      |"""
    ev.copy(code = code, isNull = childGen.isNull, value = childGen.value)
  }

  override protected def withNewChildInternal(newChild: Expression): CountingPredicate =
    copy(child = newChild)
}

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
      left, right, buildSide, joinType, leftKeys, rightKeys, Some(condition), rangeJoin)
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

  private def checkAnswerBothModes(
      left: DataFrame,
      right: DataFrame,
      plan: (SparkPlan, SparkPlan) => SparkPlan,
      expected: Seq[Row]): Unit = {
    Seq(true, false).foreach { codegen =>
      withSQLConf(
        org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString,
        org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK.key -> "false") {
        checkAnswer2(left, right, plan, expected)
      }
    }
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
    checkAnswerBothModes(left, right, (leftPlan: SparkPlan, rightPlan: SparkPlan) => {
      val pointPlan = if (pointOnLeft) leftPlan else rightPlan
      val boundPlan = if (pointOnLeft) rightPlan else leftPlan
      val point = pointPlan.output.head
      val bounds = boundPlan.output
      val pointKeys = point :: Nil
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
    def check(
        rangeJoin: RangeJoin,
        condition: (SparkPlan, SparkPlan) => Expression,
        expected: Seq[Row],
        buildSide: BuildSide = BuildRight): Unit = {
      checkAnswerBothModes(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) => {
        val (buildKeys, streamedKeys) = buildSide match {
          case BuildRight => (right.output(1) :: Nil, left.output.head :: Nil)
          case BuildLeft => (left.output.head :: Nil, right.output(1) :: Nil)
        }
        rangeJoinExec(
          left, right, buildSide, Inner,
          buildKeys = buildKeys,
          streamedKeys = streamedKeys,
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
    // Same join, built from the other side: GreaterPartialRangeJoin + BuildLeft takes the
    // opposite PointIndex scan arm (from, not upTo) from the BuildRight case above.
    check(
      GreaterPartialRangeJoin,
      (left, right) => GreaterThan(left.output.head, right.output(1)),
      Seq((0, 1, -2, -1), (0, 2, -2, -1), (1, 5, -2, -1)).map(Row.fromTuple),
      buildSide = BuildLeft)
  }

  test("interval overlap probes both bounds") {
    val condition = (left: SparkPlan, right: SparkPlan) => And(
      LessThan(left.output(0), right.output(1)),
      LessThan(right.output(0), left.output(1)))
    checkAnswerBothModes(intervals1, intervals2, (left: SparkPlan, right: SparkPlan) => {
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

  test("left semi and left anti stop probing once the first candidate matches") {
    // Every build row is the same fully-overlapping interval, so every one of them
    // satisfies the point-in-range condition. Iterator.exists is then guaranteed,
    // deterministically, to stop at the first candidate it pulls from the index -
    // this is the mechanism leftExistenceJoin relies on for its early exit.
    val n = 2000
    val overlapping = spark.createDataFrame(
      sparkContext.parallelize((0 until n).map(_ => Row(0, 100))),
      new StructType().add("low2", IntegerType).add("high2", IntegerType))
    val onePoint = spark.createDataFrame(
      sparkContext.parallelize(Seq(Row(50))), new StructType().add("point", IntegerType))

    val leftPlan = onePoint.queryExecution.sparkPlan
    val rightPlan = overlapping.queryExecution.sparkPlan
    val point = leftPlan.output.head
    val bounds = rightPlan.output

    Seq(true, false).foreach { codegenEnabled =>
      val factoryMode = if (codegenEnabled) "CODEGEN_ONLY" else "NO_CODEGEN"
      withSQLConf(
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString,
        SQLConf.CODEGEN_FALLBACK.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> factoryMode) {
        Seq(LeftSemi, LeftAnti).foreach { joinType =>
          val counter = spark.sparkContext.longAccumulator("numCandidates")
          val baseCond =
            pointCondition(point, bounds(0), bounds(1), lowInclusive = true, highInclusive = true)
          val exec = rangeJoinExec(
            leftPlan, rightPlan, BuildRight, joinType,
            buildKeys = bounds, streamedKeys = point :: Nil,
            condition = CountingPredicate(counter, baseCond))
          QueryTest.executePlan(exec, spark.sqlContext)
          assert(counter.value < n,
            s"$joinType (codegen=$codegenEnabled) examined ${counter.value} of $n " +
              "fully overlapping candidates; the first candidate should have ended the probe")
        }
      }
    }
  }

  test("condition and keys stay on the operator") {
    val leftPlan = intervals1.queryExecution.executedPlan
    val rightPlan = points.queryExecution.executedPlan
    val cond = pointCondition(
      rightPlan.output.head, leftPlan.output(0), leftPlan.output(1),
      lowInclusive = true, highInclusive = false)
    val exec = rangeJoinExec(
      leftPlan, rightPlan, BuildRight, Inner,
      buildKeys = rightPlan.output.head :: Nil,
      streamedKeys = leftPlan.output,
      condition = cond)
    assert(exec.expressions.exists(_.semanticEquals(cond)))
    assert(exec.leftKeys == leftPlan.output)
    assert(!exec.verboseStringWithOperatorId().contains("none#"))
    assert(exec.verboseStringWithOperatorId().contains("low1"))
  }
}

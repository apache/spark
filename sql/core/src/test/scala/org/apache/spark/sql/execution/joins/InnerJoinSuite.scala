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

import org.apache.spark.sql.{DataFrame, execution, Row, SQLConf}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class InnerJoinSuite extends SparkPlanTest with SharedSQLContext {

  private lazy val myUpperCaseData = ctx.createDataFrame(
    ctx.sparkContext.parallelize(Seq(
      Row(1, "A"),
      Row(2, "B"),
      Row(3, "C"),
      Row(4, "D"),
      Row(5, "E"),
      Row(6, "F"),
      Row(null, "G")
    )), new StructType().add("N", IntegerType).add("L", StringType))

  private lazy val myLowerCaseData = ctx.createDataFrame(
    ctx.sparkContext.parallelize(Seq(
      Row(1, "a"),
      Row(2, "b"),
      Row(3, "c"),
      Row(4, "d"),
      Row(null, "e")
    )), new StructType().add("n", IntegerType).add("l", StringType))

  private lazy val myTestData = Seq(
    (1, 1),
    (1, 2),
    (2, 1),
    (2, 2),
    (3, 1),
    (3, 2)
  ).toDF("a", "b")

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  private def testInnerJoin(
      testName: String,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      condition: () => Expression,
      expectedAnswer: Seq[Product]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan, Inner, Some(condition()))
      ExtractEquiJoinKeys.unapply(join)
    }

    def makeBroadcastHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        boundCondition: Option[Expression],
        leftPlan: SparkPlan,
        rightPlan: SparkPlan,
        side: BuildSide) = {
      val broadcastHashJoin =
        execution.joins.BroadcastHashJoin(leftKeys, rightKeys, side, leftPlan, rightPlan)
      boundCondition.map(Filter(_, broadcastHashJoin)).getOrElse(broadcastHashJoin)
    }

    def makeShuffledHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        boundCondition: Option[Expression],
        leftPlan: SparkPlan,
        rightPlan: SparkPlan,
        side: BuildSide) = {
      val shuffledHashJoin =
        execution.joins.ShuffledHashJoin(leftKeys, rightKeys, side, leftPlan, rightPlan)
      val filteredJoin =
        boundCondition.map(Filter(_, shuffledHashJoin)).getOrElse(shuffledHashJoin)
      EnsureRequirements(sqlContext).apply(filteredJoin)
    }

    def makeSortMergeJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        boundCondition: Option[Expression],
        leftPlan: SparkPlan,
        rightPlan: SparkPlan) = {
      val sortMergeJoin =
        execution.joins.SortMergeJoin(leftKeys, rightKeys, leftPlan, rightPlan)
      val filteredJoin = boundCondition.map(Filter(_, sortMergeJoin)).getOrElse(sortMergeJoin)
      EnsureRequirements(sqlContext).apply(filteredJoin)
    }

    test(s"$testName using BroadcastHashJoin (build=left)") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeBroadcastHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, joins.BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using BroadcastHashJoin (build=right)") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeBroadcastHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, joins.BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using ShuffledHashJoin (build=left)") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeShuffledHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, joins.BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using ShuffledHashJoin (build=right)") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeShuffledHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, joins.BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using SortMergeJoin") {
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeSortMergeJoin(leftKeys, rightKeys, boundCondition, leftPlan, rightPlan),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }
  }

  testInnerJoin(
    "inner join, one match per row",
    myUpperCaseData,
    myLowerCaseData,
    () => (myUpperCaseData.col("N") === myLowerCaseData.col("n")).expr,
    Seq(
      (1, "A", 1, "a"),
      (2, "B", 2, "b"),
      (3, "C", 3, "c"),
      (4, "D", 4, "d")
    )
  )

  {
    lazy val left = myTestData.where("a = 1")
    lazy val right = myTestData.where("a = 1")
    testInnerJoin(
      "inner join, multiple matches",
      left,
      right,
      () => (left.col("a") === right.col("a")).expr,
      Seq(
        (1, 1, 1, 1),
        (1, 1, 1, 2),
        (1, 2, 1, 1),
        (1, 2, 1, 2)
      )
    )
  }

  {
    lazy val left = myTestData.where("a = 1")
    lazy val right = myTestData.where("a = 2")
    testInnerJoin(
      "inner join, no matches",
      left,
      right,
      () => (left.col("a") === right.col("a")).expr,
      Seq.empty
    )
  }

}

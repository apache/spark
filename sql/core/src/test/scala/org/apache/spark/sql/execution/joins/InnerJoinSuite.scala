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

import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.{execution, Row, DataFrame}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{Filter, joins, SparkPlan, SparkPlanTest}

class InnerJoinSuite extends SparkPlanTest {

  private def testInnerJoin(
      testName: String,
      leftRows: DataFrame,
      rightRows: DataFrame,
      condition: Expression,
      expectedAnswer: Seq[Product]): Unit = {

    val join = Join(leftRows.logicalPlan, rightRows.logicalPlan, Inner, Some(condition))
    ExtractEquiJoinKeys.unapply(join).foreach {
      case (joinType, leftKeys, rightKeys, boundCondition, leftChild, rightChild) =>

        def makeBroadcastHashJoin(left: SparkPlan, right: SparkPlan, side: BuildSide) = {
          val broadcastHashJoin =
            execution.joins.BroadcastHashJoin(leftKeys, rightKeys, side, left, right)
          boundCondition.map(Filter(_, broadcastHashJoin)).getOrElse(broadcastHashJoin)
        }

        def makeShuffledHashJoin(left: SparkPlan, right: SparkPlan, side: BuildSide) = {
          val shuffledHashJoin =
            execution.joins.ShuffledHashJoin(leftKeys, rightKeys, side, left, right)
          boundCondition.map(Filter(_, shuffledHashJoin)).getOrElse(shuffledHashJoin)
        }

        def makeSortMergeJoin(left: SparkPlan, right: SparkPlan) = {
          val sortMergeJoin =
            execution.joins.SortMergeJoin(leftKeys, rightKeys, left, right)
          boundCondition.map(Filter(_, sortMergeJoin)).getOrElse(sortMergeJoin)
        }

        test(s"$testName using BroadcastHashJoin (build=left)") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            makeBroadcastHashJoin(left, right, joins.BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }

        test(s"$testName using BroadcastHashJoin (build=right)") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            makeBroadcastHashJoin(left, right, joins.BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }

        test(s"$testName using ShuffledHashJoin (build=left)") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            makeShuffledHashJoin(left, right, joins.BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }

        test(s"$testName using ShuffledHashJoin (build=right)") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            makeShuffledHashJoin(left, right, joins.BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }

        test(s"$testName using SortMergeJoin") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            makeSortMergeJoin(left, right),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
    }
  }

  {
    val upperCaseData = Seq(
      (1, "A"),
      (2, "B"),
      (3, "C"),
      (4, "D"),
      (5, "E"),
      (6, "F")
    ).toDF("N", "L")

    val lowerCaseData = Seq(
      (1, "a"),
      (2, "b"),
      (3, "c"),
      (4, "d")
    ).toDF("n", "l")

    testInnerJoin(
      "inner join, one match per row",
      upperCaseData,
      lowerCaseData,
      (upperCaseData.col("N") === lowerCaseData.col("n")).expr,
      Seq(
        (1, "A", 1, "a"),
        (2, "B", 2, "b"),
        (3, "C", 3, "c"),
        (4, "D", 4, "d")
      )
    )
  }

  private val testData2 = Seq(
    (1, 1),
    (1, 2),
    (2, 1),
    (2, 2),
    (3, 1),
    (3, 2)
  ).toDF("a", "b")

  {
    val left = testData2.where("a = 1")
    val right = testData2.where("a = 1")
    testInnerJoin(
      "inner join, multiple matches",
      left,
      right,
      (left.col("a") === right.col("a")).expr,
      Seq(
        (1, 1, 1, 1),
        (1, 1, 1, 2),
        (1, 2, 1, 1),
        (1, 2, 1, 2)
      )
    )
  }

  {
    val left = testData2.where("a = 1")
    val right = testData2.where("a = 2")
    testInnerJoin(
      "inner join, no matches",
      left,
      right,
      (left.col("a") === right.col("a")).expr,
      Seq.empty
    )
  }

}

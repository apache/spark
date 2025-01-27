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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, EqualTo, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class InnerJoinSuite extends SparkPlanTest with SharedSparkSession {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder
  import testImplicits.toRichColumn

  private val EnsureRequirements = new EnsureRequirements()

  private lazy val myUpperCaseData = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, "A"),
      Row(2, "B"),
      Row(3, "C"),
      Row(4, "D"),
      Row(5, "E"),
      Row(6, "F"),
      Row(null, "G")
    )), new StructType().add("N", IntegerType).add("L", StringType))

  private lazy val myLowerCaseData = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, "a"),
      Row(2, "b"),
      Row(3, "c"),
      Row(4, "d"),
      Row(null, "e")
    )), new StructType().add("n", IntegerType).add("l", StringType))

  private lazy val myTestData1 = Seq(
    (1, 1),
    (1, 2),
    (2, 1),
    (2, 2),
    (3, 1),
    (3, 2)
  ).toDF("a", "b")

  private lazy val myTestData2 = Seq(
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
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan,
        Inner, Some(condition()), JoinHint.NONE)
      ExtractEquiJoinKeys.unapply(join)
    }

    def makeBroadcastHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        boundCondition: Option[Expression],
        leftPlan: SparkPlan,
        rightPlan: SparkPlan,
        side: BuildSide) = {
      val broadcastJoin = joins.BroadcastHashJoinExec(
        leftKeys,
        rightKeys,
        Inner,
        side,
        boundCondition,
        leftPlan,
        rightPlan)
      EnsureRequirements.apply(broadcastJoin)
    }

    def makeShuffledHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        boundCondition: Option[Expression],
        leftPlan: SparkPlan,
        rightPlan: SparkPlan,
        side: BuildSide) = {
      val shuffledHashJoin = joins.ShuffledHashJoinExec(leftKeys, rightKeys, Inner,
        side, None, leftPlan, rightPlan)
      val filteredJoin =
        boundCondition.map(FilterExec(_, shuffledHashJoin)).getOrElse(shuffledHashJoin)
      EnsureRequirements.apply(filteredJoin)
    }

    def makeSortMergeJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        boundCondition: Option[Expression],
        leftPlan: SparkPlan,
        rightPlan: SparkPlan) = {
      val sortMergeJoin = joins.SortMergeJoinExec(leftKeys, rightKeys, Inner, boundCondition,
        leftPlan, rightPlan)
      EnsureRequirements.apply(sortMergeJoin)
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastHashJoin (build=left)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeBroadcastHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastHashJoin (build=right)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeBroadcastHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using ShuffledHashJoin (build=left)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeShuffledHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildLeft),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using ShuffledHashJoin (build=right)") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
            makeShuffledHashJoin(
              leftKeys, rightKeys, boundCondition, leftPlan, rightPlan, BuildRight),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using SortMergeJoin") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          Seq(true, false).foreach { enable =>
            withSQLConf(SQLConf.USE_PARTITION_EVALUATOR.key -> enable.toString) {
              checkAnswer2(leftRows, rightRows, (leftPlan: SparkPlan, rightPlan: SparkPlan) =>
                makeSortMergeJoin(leftKeys, rightKeys, boundCondition, leftPlan, rightPlan),
                expectedAnswer.map(Row.fromTuple),
                sortAnswers = true)
            }
          }
        }
      }
    }

    test(s"$testName using CartesianProduct") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          CartesianProductExec(left, right, Some(condition())),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastNestedLoopJoin build left") { _ =>
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          BroadcastNestedLoopJoinExec(left, right, BuildLeft, Inner, Some(condition())),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastNestedLoopJoin build right") { _ =>
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          BroadcastNestedLoopJoinExec(left, right, BuildRight, Inner, Some(condition())),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }
  }

  testInnerJoin(
    "inner join, one match per row",
    myUpperCaseData,
    myLowerCaseData,
    () => EqualTo(myUpperCaseData.col("N").expr, myLowerCaseData.col("n").expr),
    Seq(
      (1, "A", 1, "a"),
      (2, "B", 2, "b"),
      (3, "C", 3, "c"),
      (4, "D", 4, "d")
    )
  )

  {
    lazy val left = myTestData1.where("a = 1")
    lazy val right = myTestData2.where("a = 1")
    testInnerJoin(
      "inner join, multiple matches",
      left,
      right,
      () => EqualTo(left.col("a").expr, right.col("a").expr),
      Seq(
        (1, 1, 1, 1),
        (1, 1, 1, 2),
        (1, 2, 1, 1),
        (1, 2, 1, 2)
      )
    )
  }

  {
    lazy val left = myTestData1.where("a = 1")
    lazy val right = myTestData2.where("a = 2")
    testInnerJoin(
      "inner join, no matches",
      left,
      right,
      () => EqualTo(left.col("a").expr, right.col("a").expr),
      Seq.empty
    )
  }

  {
    lazy val left = Seq((1, Some(0)), (2, None)).toDF("a", "b")
    lazy val right = Seq((1, Some(0)), (2, None)).toDF("a", "b")
    testInnerJoin(
      "inner join, null safe",
      left,
      right,
      () => EqualNullSafe(left.col("b").expr, right.col("b").expr),
      Seq(
        (1, 0, 1, 0),
        (2, null, 2, null)
      )
    )
  }

  {
    def df: DataFrame = spark.range(3).selectExpr("struct(id, id) as key", "id as value")
    lazy val left = df.selectExpr("key", "concat('L', value) as value").alias("left")
    lazy val right = df.selectExpr("key", "concat('R', value) as value").alias("right")
    testInnerJoin(
      "SPARK-15822 - test structs as keys",
      left,
      right,
      () => EqualTo(left.col("key").expr, right.col("key").expr),
      Seq(
        (Row(0, 0), "L0", Row(0, 0), "R0"),
        (Row(1, 1), "L1", Row(1, 1), "R1"),
        (Row(2, 2), "L2", Row(2, 2), "R2")))
  }
}

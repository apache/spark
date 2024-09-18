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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, Project}
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

class SingleJoinSuite extends SparkPlanTest with SharedSparkSession {
  import testImplicits.toRichColumn

  private val EnsureRequirements = new EnsureRequirements()

  private lazy val left = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(1, 2.0),
      Row(1, 2.0),
      Row(2, 1.0),
      Row(2, 1.0),
      Row(3, 3.0),
      Row(null, null),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("a", IntegerType).add("b", DoubleType))

  // (a > c && a != 6)

  private lazy val right = spark.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(4, 2.0),
      Row(null, null),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  private lazy val singleConditionEQ = EqualTo(left.col("a").expr, right.col("c").expr)

  private lazy val nonEqualityCond = And(GreaterThan(left.col("a").expr, right.col("c").expr),
    Not(EqualTo(left.col("a").expr, Literal(6))))



  private def testSingleJoin(
                              testName: String,
                              leftRows: => DataFrame,
                              rightRows: => DataFrame,
                              condition: => Option[Expression],
                              expectedAnswer: Seq[Row],
                              expectError: Boolean = false): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan,
        Inner, condition, JoinHint.NONE)
      ExtractEquiJoinKeys.unapply(join)
    }

    def checkSingleJoinError(planFunction: (SparkPlan, SparkPlan) => SparkPlan): Unit = {
      val outputPlan = planFunction(leftRows.queryExecution.sparkPlan,
        rightRows.queryExecution.sparkPlan)
      checkError(
        exception = intercept[SparkRuntimeException] {
          SparkPlanTest.executePlan(outputPlan, spark.sqlContext)
        },
        condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
        parameters = Map.empty
      )
    }

    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastHashJoin") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          val planFunction = (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements.apply(BroadcastHashJoinExec(
              leftKeys, rightKeys, LeftSingle, BuildRight, boundCondition, left, right))
          if (expectError) {
            checkSingleJoinError(planFunction)
          } else {
            checkAnswer2(leftRows, rightRows, planFunction,
              expectedAnswer,
              sortAnswers = true)
          }
        }
      }
    }
    testWithWholeStageCodegenOnAndOff(s"$testName using ShuffledHashJoin") { _ =>
      extractJoinParts().foreach { case (_, leftKeys, rightKeys, boundCondition, _, _, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          val planFunction = (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements.apply(
              ShuffledHashJoinExec(
                leftKeys, rightKeys, LeftSingle, BuildRight, boundCondition, left, right))
          if (expectError) {
            checkSingleJoinError(planFunction)
          } else {
            checkAnswer2(leftRows, rightRows, planFunction,
              expectedAnswer,
              sortAnswers = true)
          }
        }
      }
    }


    testWithWholeStageCodegenOnAndOff(s"$testName using BroadcastNestedLoopJoin") { _ =>
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        val planFunction = (left: SparkPlan, right: SparkPlan) =>
          EnsureRequirements.apply(
            BroadcastNestedLoopJoinExec(left, right, BuildRight, LeftSingle, condition))
        if (expectError) {
          checkSingleJoinError(planFunction)
        } else {
          checkAnswer2(leftRows, rightRows, planFunction,
            expectedAnswer,
            sortAnswers = true)
        }
      }
    }
  }

  testSingleJoin(
    "test single condition (equal) for a left single join",
    left,
    Project(Seq(right.col("c").expr.asInstanceOf[NamedExpression]), right.logicalPlan),
    Some(singleConditionEQ),
    Seq(Row(1, 2.0, null),
      Row(1, 2.0, null),
      Row(2, 1.0, 2),
      Row(2, 1.0, 2),
      Row(3, 3.0, 3),
      Row(6, null, 6),
      Row(null, 5.0, null),
      Row(null, null, null)))

  testSingleJoin(
    "test single condition (equal) for a left single join -- multiple matches",
    left,
    Project(Seq(right.col("d").expr.asInstanceOf[NamedExpression]), right.logicalPlan),
    Some(EqualTo(left.col("b").expr, right.col("d").expr)),
    Seq.empty, true)

  testSingleJoin(
    "test non-equality for a left single join",
    left,
    Project(Seq(right.col("c").expr.asInstanceOf[NamedExpression]), right.logicalPlan),
    Some(nonEqualityCond),
    Seq(Row(1, 2.0, null),
      Row(1, 2.0, null),
      Row(2, 1.0, null),
      Row(2, 1.0, null),
      Row(3, 3.0, 2),
      Row(6, null, null),
      Row(null, 5.0, null),
      Row(null, null, null)))

  testSingleJoin(
    "test non-equality for a left single join -- multiple matches",
    left,
    Project(Seq(right.col("c").expr.asInstanceOf[NamedExpression]), right.logicalPlan),
    Some(GreaterThan(left.col("a").expr, right.col("c").expr)),
    Seq.empty, expectError = true)

  private lazy val emptyFrame = spark.createDataFrame(
    spark.sparkContext.emptyRDD[Row], new StructType().add("c", IntegerType).add("d", DoubleType))

  testSingleJoin(
    "empty inner (right) side",
    left,
    Project(Seq(emptyFrame.col("c").expr.asInstanceOf[NamedExpression]), emptyFrame.logicalPlan),
    Some(GreaterThan(left.col("a").expr, emptyFrame.col("c").expr)),
    Seq(Row(1, 2.0, null),
      Row(1, 2.0, null),
      Row(2, 1.0, null),
      Row(2, 1.0, null),
      Row(3, 3.0, null),
      Row(6, null, null),
      Row(null, 5.0, null),
      Row(null, null, null)))

  testSingleJoin(
    "empty outer (left) side",
    Project(Seq(emptyFrame.col("c").expr.asInstanceOf[NamedExpression]), emptyFrame.logicalPlan),
    right,
    Some(EqualTo(emptyFrame.col("c").expr, right.col("c").expr)),
    Seq.empty)

}

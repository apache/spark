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

import org.apache.spark.sql.{DataFrame, Row, SQLConf}
import org.apache.spark.sql.catalyst.expressions.{And, Expression, LessThan}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftAnti, LeftExistenceJoin, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.{EnsureRequirements, SparkPlan, SparkPlanTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

trait ExistenceJoinSuite extends SparkPlanTest with SharedSQLContext {

  protected lazy val left = sqlContext.createDataFrame(
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

  protected lazy val right = sqlContext.createDataFrame(
    sparkContext.parallelize(Seq(
      Row(2, 3.0),
      Row(2, 3.0),
      Row(3, 2.0),
      Row(4, 1.0),
      Row(null, null),
      Row(null, 5.0),
      Row(6, null)
    )), new StructType().add("c", IntegerType).add("d", DoubleType))

  protected lazy val conditionEQ = {
    And((left.col("a") === right.col("c")).expr,
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  protected lazy val conditionNEQ = {
    And((left.col("a") < right.col("c")).expr,
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  // Note: the input dataframes and expression must be evaluated lazily because
  // the SQLContext should be used only within a test to keep SQL tests stable
  protected def testLeftExistenceEqualJoin(
      testName: String,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      condition: => Expression,
      jt: LeftExistenceJoin,
      expectedAnswer: Seq[Product]): Unit = {

    def extractJoinParts(): Option[ExtractEquiJoinKeys.ReturnType] = {
      val join = Join(leftRows.logicalPlan, rightRows.logicalPlan, Inner, Some(condition))
      ExtractEquiJoinKeys.unapply(join)
    }

    test(s"$testName using LeftExistenceJoinHash") {
      extractJoinParts().foreach { case (joinType, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            EnsureRequirements(left.sqlContext).apply(
              LeftExistenceJoinHash(leftKeys, rightKeys, left, right, boundCondition, jt)),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }

    test(s"$testName using BroadcastLeftExistenceJoinHash") {
      extractJoinParts().foreach { case (joinType, leftKeys, rightKeys, boundCondition, _, _) =>
        withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
          checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
            BroadcastLeftExistenceJoinHash(leftKeys, rightKeys, left, right, boundCondition, jt),
            expectedAnswer.map(Row.fromTuple),
            sortAnswers = true)
        }
      }
    }
  }

  protected def testLeftExistenceNonEqualJoin(
      testName: String,
      leftRows: => DataFrame,
      rightRows: => DataFrame,
      condition: => Expression,
      jt: LeftExistenceJoin,
      expectedAnswer: Seq[Product]): Unit = {
    test(s"$testName using LeftExistenceJoinBNL") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          LeftExistenceJoinBNL(left, right, Some(condition), jt),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }
  }
}

class LeftSemiJoinSuite extends ExistenceJoinSuite {
  testLeftExistenceEqualJoin(
    "basic test for left semi join",
    left,
    right,
    conditionEQ,
    LeftSemi,
    Seq(
      (2, 1.0),
      (2, 1.0)
    )
  )

  testLeftExistenceNonEqualJoin(
    "basic test for left semi equal join",
    left,
    right,
    conditionEQ,
    LeftSemi,
    Seq(
      (2, 1.0),
      (2, 1.0)
    )
  )

  testLeftExistenceNonEqualJoin(
    "basic test for left semi non equal join",
    left,
    right,
    conditionNEQ,
    LeftSemi,
    Seq(
      (1, 2.0),
      (1, 2.0),
      (2, 1.0),
      (2, 1.0)
    )
  )
}

class LeftAntiJoinSuite extends ExistenceJoinSuite {
  testLeftExistenceEqualJoin(
    "basic test for left anti join",
    left,
    right,
    conditionEQ,
    LeftAnti,
    Seq(
      (1, 2.0),
      (1, 2.0),
      (3, 3.0)
    )
  )

  testLeftExistenceNonEqualJoin(
    "basic test for left anti join",
    left,
    right,
    conditionNEQ,
    LeftAnti,
    Seq()
  )
}

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
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{SQLConf, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{And, LessThan, Expression}
import org.apache.spark.sql.execution.{EnsureRequirements, SparkPlan, SparkPlanTest}

class SemiJoinSuite extends SparkPlanTest with SQLTestUtils {

  private def testLeftSemiJoin(
      testName: String,
      leftRows: DataFrame,
      rightRows: DataFrame,
      condition: Expression,
      expectedAnswer: Seq[Product]): Unit = {
    val join = Join(leftRows.logicalPlan, rightRows.logicalPlan, Inner, Some(condition))
    ExtractEquiJoinKeys.unapply(join).foreach {
      case (joinType, leftKeys, rightKeys, boundCondition, leftChild, rightChild) =>
        test(s"$testName using LeftSemiJoinHash") {
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              EnsureRequirements(left.sqlContext).apply(
                LeftSemiJoinHash(leftKeys, rightKeys, left, right, boundCondition)),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }

        test(s"$testName using BroadcastLeftSemiJoinHash") {
          withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
            checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
              BroadcastLeftSemiJoinHash(leftKeys, rightKeys, left, right, boundCondition),
              expectedAnswer.map(Row.fromTuple),
              sortAnswers = true)
          }
        }
    }

    test(s"$testName using LeftSemiJoinBNL") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          LeftSemiJoinBNL(left, right, Some(condition)),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = true)
      }
    }
  }

  val left = sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(Seq(
    Row(1, 2.0),
    Row(1, 2.0),
    Row(2, 1.0),
    Row(2, 1.0),
    Row(3, 3.0),
    Row(null, null),
    Row(null, 5.0),
    Row(6, null)
  )), new StructType().add("a", IntegerType).add("b", DoubleType))

  val right = sqlContext.createDataFrame(sqlContext.sparkContext.parallelize(Seq(
    Row(2, 3.0),
    Row(2, 3.0),
    Row(3, 2.0),
    Row(4, 1.0),
    Row(null, null),
    Row(null, 5.0),
    Row(6, null)
  )), new StructType().add("c", IntegerType).add("d", DoubleType))

  val condition = {
    And(
      (left.col("a") === right.col("c")).expr,
      LessThan(left.col("b").expr, right.col("d").expr))
  }

  testLeftSemiJoin(
    "basic test",
    left,
    right,
    condition,
    Seq(
      (2, 1.0),
      (2, 1.0)
    )
  )
}

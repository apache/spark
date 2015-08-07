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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanTest}

class OuterJoinSuite extends SparkPlanTest {

  private def testOuterJoin(
      testName: String,
      leftRows: DataFrame,
      rightRows: DataFrame,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      expectedAnswer: Seq[Product]): Unit = {
    // Precondition: leftRows and rightRows should be sorted according to the join keys.

    test(s"$testName using ShuffledHashOuterJoin") {
      checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
        ShuffledHashOuterJoin(leftKeys, rightKeys, joinType, condition, left, right),
        expectedAnswer.map(Row.fromTuple),
        sortAnswers = false)
    }

    if (joinType != FullOuter) {
      test(s"$testName using BroadcastHashOuterJoin") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          BroadcastHashOuterJoin(leftKeys, rightKeys, joinType, condition, left, right),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = false)
      }

      test(s"$testName using SortMergeOuterJoin") {
        checkAnswer2(leftRows, rightRows, (left: SparkPlan, right: SparkPlan) =>
          SortMergeOuterJoin(leftKeys, rightKeys, joinType, condition, left, right),
          expectedAnswer.map(Row.fromTuple),
          sortAnswers = false)
      }
    }
  }

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

  // --- Basic outer joins ------------------------------------------------------------------------

  testOuterJoin(
    "basic left outer join",
    left,
    right,
    leftKeys,
    rightKeys,
    LeftOuter,
    condition,
    Seq(
      (1, 2.0, null, null),
      (2, 1.0, 2, 3.0),
      (3, 3.0, null, null)
    )
  )

  testOuterJoin(
    "basic right outer join",
    left,
    right,
    leftKeys,
    rightKeys,
    RightOuter,
    condition,
    Seq(
      (2, 1.0, 2, 3.0),
      (null, null, 3, 2.0),
      (null, null, 4, 1.0)
    )
  )

  testOuterJoin(
    "basic full outer join",
    left,
    right,
    leftKeys,
    rightKeys,
    FullOuter,
    condition,
    Seq(
      (1, 2.0, null, null),
      (2, 1.0, 2, 3.0),
      (3, 3.0, null, null),
      (null, null, 3, 2.0),
      (null, null, 4, 1.0)
    )
  )

  // --- Both inputs empty ------------------------------------------------------------------------

  testOuterJoin(
    "left outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    leftKeys,
    rightKeys,
    LeftOuter,
    condition,
    Seq.empty
  )

  testOuterJoin(
    "right outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    leftKeys,
    rightKeys,
    RightOuter,
    condition,
    Seq.empty
  )

  testOuterJoin(
    "full outer join with both inputs empty",
    left.filter("false"),
    right.filter("false"),
    leftKeys,
    rightKeys,
    FullOuter,
    condition,
    Seq.empty
  )
}

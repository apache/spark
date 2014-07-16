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

package org.apache.spark.sql

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.catalyst.plans.{LeftOuter, RightOuter, FullOuter, Inner}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

class JoinSuite extends QueryTest {

  // Ensures tables are loaded.
  TestData

  test("equi-join is hash-join") {
    val x = testData2.as('x)
    val y = testData2.as('y)
    val join = x.join(y, Inner, Some("x.a".attr === "y.a".attr)).queryExecution.analyzed
    val planned = planner.HashJoin(join)
    assert(planned.size === 1)
  }

  test("multiple-key equi-join is hash-join") {
    val x = testData2.as('x)
    val y = testData2.as('y)
    val join = x.join(y, Inner,
      Some("x.a".attr === "y.a".attr && "x.b".attr === "y.b".attr)).queryExecution.analyzed
    val planned = planner.HashJoin(join)
    assert(planned.size === 1)
  }

  test("inner join where, one match per row") {
    checkAnswer(
      upperCaseData.join(lowerCaseData, Inner).where('n === 'N),
      Seq(
        (1, "A", 1, "a"),
        (2, "B", 2, "b"),
        (3, "C", 3, "c"),
        (4, "D", 4, "d")
      ))
  }

  test("inner join ON, one match per row") {
    checkAnswer(
      upperCaseData.join(lowerCaseData, Inner, Some('n === 'N)),
      Seq(
        (1, "A", 1, "a"),
        (2, "B", 2, "b"),
        (3, "C", 3, "c"),
        (4, "D", 4, "d")
      ))
  }

  test("inner join, where, multiple matches") {
    val x = testData2.where('a === 1).as('x)
    val y = testData2.where('a === 1).as('y)
    checkAnswer(
      x.join(y).where("x.a".attr === "y.a".attr),
      (1,1,1,1) ::
      (1,1,1,2) ::
      (1,2,1,1) ::
      (1,2,1,2) :: Nil
    )
  }

  test("inner join, no matches") {
    val x = testData2.where('a === 1).as('x)
    val y = testData2.where('a === 2).as('y)
    checkAnswer(
      x.join(y).where("x.a".attr === "y.a".attr),
      Nil)
  }

  test("big inner join, 4 matches per row") {
    val bigData = testData.unionAll(testData).unionAll(testData).unionAll(testData)
    val bigDataX = bigData.as('x)
    val bigDataY = bigData.as('y)

    checkAnswer(
      bigDataX.join(bigDataY).where("x.key".attr === "y.key".attr),
      testData.flatMap(
        row => Seq.fill(16)((row ++ row).toSeq)).collect().toSeq)
  }

  test("cartisian product join") {
    checkAnswer(
      testData3.join(testData3),
      (1, null, 1, null) ::
      (1, null, 2, 2) ::
      (2, 2, 1, null) ::
      (2, 2, 2, 2) :: Nil)
  }

  test("left outer join") {
    checkAnswer(
      upperCaseData.join(lowerCaseData, LeftOuter, Some('n === 'N)),
      (1, "A", 1, "a") ::
      (2, "B", 2, "b") ::
      (3, "C", 3, "c") ::
      (4, "D", 4, "d") ::
      (5, "E", null, null) ::
      (6, "F", null, null) :: Nil)
  }

  test("right outer join") {
    checkAnswer(
      lowerCaseData.join(upperCaseData, RightOuter, Some('n === 'N)),
      (1, "a", 1, "A") ::
      (2, "b", 2, "B") ::
      (3, "c", 3, "C") ::
      (4, "d", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)
  }

  test("full outer join") {
    val left = upperCaseData.where('N <= 4).as('left)
    val right = upperCaseData.where('N >= 3).as('right)

    checkAnswer(
      left.join(right, FullOuter, Some("left.N".attr === "right.N".attr)),
      (1, "A", null, null) ::
      (2, "B", null, null) ::
      (3, "C", 3, "C") ::
      (4, "D", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)
  }
}

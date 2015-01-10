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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.test.TestSQLContext._

class JoinSuite extends QueryTest with BeforeAndAfterEach {
  // Ensures tables are loaded.
  TestData

  test("equi-join is hash-join") {
    val x = testData2.as('x)
    val y = testData2.as('y)
    val join = x.join(y, Inner, Some("x.a".attr === "y.a".attr)).queryExecution.analyzed
    val planned = planner.HashJoin(join)
    assert(planned.size === 1)
  }

  def assertJoin(sqlString: String, c: Class[_]): Any = {
    val rdd = sql(sqlString)
    val physical = rdd.queryExecution.sparkPlan
    val operators = physical.collect {
      case j: ShuffledHashJoin => j
      case j: HashOuterJoin => j
      case j: LeftSemiJoinHash => j
      case j: BroadcastHashJoin => j
      case j: LeftSemiJoinBNL => j
      case j: CartesianProduct => j
      case j: BroadcastNestedLoopJoin => j
    }

    assert(operators.size === 1)
    if (operators(0).getClass() != c) {
      fail(s"$sqlString expected operator: $c, but got ${operators(0)}\n physical: \n$physical")
    }
  }

  test("join operator selection") {
    clearCache()

    Seq(
      ("SELECT * FROM testData LEFT SEMI JOIN testData2 ON key = a", classOf[LeftSemiJoinHash]),
      ("SELECT * FROM testData LEFT SEMI JOIN testData2", classOf[LeftSemiJoinBNL]),
      ("SELECT * FROM testData JOIN testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData JOIN testData2 WHERE key = 2", classOf[CartesianProduct]),
      ("SELECT * FROM testData LEFT JOIN testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData RIGHT JOIN testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData FULL OUTER JOIN testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData LEFT JOIN testData2 WHERE key = 2", classOf[CartesianProduct]),
      ("SELECT * FROM testData RIGHT JOIN testData2 WHERE key = 2", classOf[CartesianProduct]),
      ("SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key = 2", classOf[CartesianProduct]),
      ("SELECT * FROM testData JOIN testData2 WHERE key > a", classOf[CartesianProduct]),
      ("SELECT * FROM testData FULL OUTER JOIN testData2 WHERE key > a", classOf[CartesianProduct]),
      ("SELECT * FROM testData JOIN testData2 ON key = a", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData JOIN testData2 ON key = a and key = 2", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData JOIN testData2 ON key = a where key = 2", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData LEFT JOIN testData2 ON key = a", classOf[HashOuterJoin]),
      ("SELECT * FROM testData RIGHT JOIN testData2 ON key = a where key = 2",
        classOf[HashOuterJoin]),
      ("SELECT * FROM testData right join testData2 ON key = a and key = 2",
        classOf[HashOuterJoin]),
      ("SELECT * FROM testData full outer join testData2 ON key = a", classOf[HashOuterJoin])
      // TODO add BroadcastNestedLoopJoin
    ).foreach { case (query, joinClass) => assertJoin(query, joinClass) }
  }

  test("broadcasted hash join operator selection") {
    clearCache()
    sql("CACHE TABLE testData")

    Seq(
      ("SELECT * FROM testData join testData2 ON key = a", classOf[BroadcastHashJoin]),
      ("SELECT * FROM testData join testData2 ON key = a and key = 2", classOf[BroadcastHashJoin]),
      ("SELECT * FROM testData join testData2 ON key = a where key = 2", classOf[BroadcastHashJoin])
    ).foreach { case (query, joinClass) => assertJoin(query, joinClass) }

    sql("UNCACHE TABLE testData")
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

    checkAnswer(
      upperCaseData.join(lowerCaseData, LeftOuter, Some('n === 'N && 'n > 1)),
      (1, "A", null, null) ::
      (2, "B", 2, "b") ::
      (3, "C", 3, "c") ::
      (4, "D", 4, "d") ::
      (5, "E", null, null) ::
      (6, "F", null, null) :: Nil)

    checkAnswer(
      upperCaseData.join(lowerCaseData, LeftOuter, Some('n === 'N && 'N > 1)),
      (1, "A", null, null) ::
      (2, "B", 2, "b") ::
      (3, "C", 3, "c") ::
      (4, "D", 4, "d") ::
      (5, "E", null, null) ::
      (6, "F", null, null) :: Nil)

    checkAnswer(
      upperCaseData.join(lowerCaseData, LeftOuter, Some('n === 'N && 'l > 'L)),
      (1, "A", 1, "a") ::
      (2, "B", 2, "b") ::
      (3, "C", 3, "c") ::
      (4, "D", 4, "d") ::
      (5, "E", null, null) ::
      (6, "F", null, null) :: Nil)

    // Make sure we are choosing left.outputPartitioning as the
    // outputPartitioning for the outer join operator.
    checkAnswer(
      sql(
        """
          |SELECT l.N, count(*)
          |FROM upperCaseData l LEFT OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY l.N
        """.stripMargin),
      (1, 1) ::
      (2, 1) ::
      (3, 1) ::
      (4, 1) ::
      (5, 1) ::
      (6, 1) :: Nil)

    checkAnswer(
      sql(
        """
          |SELECT r.a, count(*)
          |FROM upperCaseData l LEFT OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY r.a
        """.stripMargin),
      (null, 6) :: Nil)
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
    checkAnswer(
      lowerCaseData.join(upperCaseData, RightOuter, Some('n === 'N && 'n > 1)),
      (null, null, 1, "A") ::
      (2, "b", 2, "B") ::
      (3, "c", 3, "C") ::
      (4, "d", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)
    checkAnswer(
      lowerCaseData.join(upperCaseData, RightOuter, Some('n === 'N && 'N > 1)),
      (null, null, 1, "A") ::
      (2, "b", 2, "B") ::
      (3, "c", 3, "C") ::
      (4, "d", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)
    checkAnswer(
      lowerCaseData.join(upperCaseData, RightOuter, Some('n === 'N && 'l > 'L)),
      (1, "a", 1, "A") ::
      (2, "b", 2, "B") ::
      (3, "c", 3, "C") ::
      (4, "d", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)

    // Make sure we are choosing right.outputPartitioning as the
    // outputPartitioning for the outer join operator.
    checkAnswer(
      sql(
        """
          |SELECT l.a, count(*)
          |FROM allNulls l RIGHT OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY l.a
        """.stripMargin),
      (null, 6) :: Nil)

    checkAnswer(
      sql(
        """
          |SELECT r.N, count(*)
          |FROM allNulls l RIGHT OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY r.N
        """.stripMargin),
      (1, 1) ::
      (2, 1) ::
      (3, 1) ::
      (4, 1) ::
      (5, 1) ::
      (6, 1) :: Nil)
  }

  test("full outer join") {
    upperCaseData.where('N <= 4).registerTempTable("left")
    upperCaseData.where('N >= 3).registerTempTable("right")

    val left = UnresolvedRelation(Seq("left"), None)
    val right = UnresolvedRelation(Seq("right"), None)

    checkAnswer(
      left.join(right, FullOuter, Some("left.N".attr === "right.N".attr)),
      (1, "A", null, null) ::
      (2, "B", null, null) ::
      (3, "C", 3, "C") ::
      (4, "D", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)

    checkAnswer(
      left.join(right, FullOuter, Some(("left.N".attr === "right.N".attr) && ("left.N".attr !== 3))),
      (1, "A", null, null) ::
      (2, "B", null, null) ::
      (3, "C", null, null) ::
      (null, null, 3, "C") ::
      (4, "D", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)

    checkAnswer(
      left.join(right, FullOuter, Some(("left.N".attr === "right.N".attr) && ("right.N".attr !== 3))),
      (1, "A", null, null) ::
      (2, "B", null, null) ::
      (3, "C", null, null) ::
      (null, null, 3, "C") ::
      (4, "D", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)

    // Make sure we are UnknownPartitioning as the outputPartitioning for the outer join operator.
    checkAnswer(
      sql(
        """
          |SELECT l.a, count(*)
          |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY l.a
        """.stripMargin),
      (null, 10) :: Nil)

    checkAnswer(
      sql(
        """
          |SELECT r.N, count(*)
          |FROM allNulls l FULL OUTER JOIN upperCaseData r ON (l.a = r.N)
          |GROUP BY r.N
        """.stripMargin),
      (1, 1) ::
      (2, 1) ::
      (3, 1) ::
      (4, 1) ::
      (5, 1) ::
      (6, 1) ::
      (null, 4) :: Nil)

    checkAnswer(
      sql(
        """
          |SELECT l.N, count(*)
          |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY l.N
        """.stripMargin),
      (1, 1) ::
      (2, 1) ::
      (3, 1) ::
      (4, 1) ::
      (5, 1) ::
      (6, 1) ::
      (null, 4) :: Nil)

    checkAnswer(
      sql(
        """
          |SELECT r.a, count(*)
          |FROM upperCaseData l FULL OUTER JOIN allNulls r ON (l.N = r.a)
          |GROUP BY r.a
        """.stripMargin),
      (null, 10) :: Nil)
  }
}

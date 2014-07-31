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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.TestData._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.{LeftOuter, RightOuter, FullOuter, Inner, LeftSemi}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

class JoinSuite extends QueryTest with BeforeAndAfterEach {

  // Ensures tables are loaded.
  TestData

  var left: UnresolvedRelation = _
  var right: UnresolvedRelation = _

  override def beforeEach() {
    super.beforeEach()
    left = UnresolvedRelation(None, "left", None)
    right = UnresolvedRelation(None, "right", None)
  }

  override def afterEach() {
    super.afterEach()

    TestSQLContext.catalog.unregisterTable(None, "left")
    TestSQLContext.catalog.unregisterTable(None, "right")
  }
  
  def check(run: () => Unit) {
    // TODO hack the logical statistics for cost based optimization.
    run()
  }

  test("equi-join is hash-join") {
    val x = testData2.as('x)
    val y = testData2.as('y)
    val join = x.join(y, Inner, Some("x.a".attr === "y.a".attr)).queryExecution.analyzed
    val planned = planner.HashJoin(join)
    assert(planned.size === 1)
  }

  test("join operator selection") {
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

    val cases1 = Seq(
      ("SELECT * FROM testData left semi join testData2 ON key = a", classOf[LeftSemiJoinHash]),
      ("SELECT * FROM testData left semi join testData2", classOf[LeftSemiJoinBNL]),
      ("SELECT * FROM testData join testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData join testData2 where key=2", classOf[CartesianProduct]),
      ("SELECT * FROM testData left join testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData right join testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData full outer join testData2", classOf[CartesianProduct]),
      ("SELECT * FROM testData left join testData2 where key=2", classOf[CartesianProduct]),
      ("SELECT * FROM testData right join testData2 where key=2", classOf[CartesianProduct]),
      ("SELECT * FROM testData full outer join testData2 where key=2", classOf[CartesianProduct]),
      ("SELECT * FROM testData join testData2 where key>a", classOf[CartesianProduct]),
      ("SELECT * FROM testData full outer join testData2 where key>a", classOf[CartesianProduct]),
      ("SELECT * FROM testData join testData2 ON key = a", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData join testData2 ON key = a and key=2", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData join testData2 ON key = a where key=2", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData left join testData2 ON key = a", classOf[HashOuterJoin]),
      ("SELECT * FROM testData right join testData2 ON key = a where key=2", 
        classOf[HashOuterJoin]),
      ("SELECT * FROM testData right join testData2 ON key = a and key=2", 
        classOf[HashOuterJoin]),
      ("SELECT * FROM testData full outer join testData2 ON key = a", classOf[HashOuterJoin]),
      ("SELECT * FROM testData join testData2 ON key = a", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData join testData2 ON key = a and key=2", classOf[ShuffledHashJoin]),
      ("SELECT * FROM testData join testData2 ON key = a where key=2", classOf[ShuffledHashJoin])
    // TODO add BroadcastNestedLoopJoin
    )
    cases1.foreach { c => assertJoin(c._1, c._2) }
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
    lowerCaseData.registerAsTable("right")
    upperCaseData.registerAsTable("left")
    def run() {
      checkAnswer(
        left.join(right, LeftOuter, Some('n === 'N)),
        (1, "A", 1, "a") ::
        (2, "B", 2, "b") ::
        (3, "C", 3, "c") ::
        (4, "D", 4, "d") ::
        (5, "E", null, null) ::
        (6, "F", null, null) :: Nil)
  
      checkAnswer(
        left.join(right, LeftOuter, Some('n === 'N && 'n > 1)),
        (1, "A", null, null) ::
        (2, "B", 2, "b") ::
        (3, "C", 3, "c") ::
        (4, "D", 4, "d") ::
        (5, "E", null, null) ::
        (6, "F", null, null) :: Nil)
  
      checkAnswer(
        left.join(right, LeftOuter, Some('n === 'N && 'N > 1)),
        (1, "A", null, null) ::
        (2, "B", 2, "b") ::
        (3, "C", 3, "c") ::
        (4, "D", 4, "d") ::
        (5, "E", null, null) ::
        (6, "F", null, null) :: Nil)
  
      checkAnswer(
        left.join(right, LeftOuter, Some('n === 'N && 'l > 'L)),
        (1, "A", 1, "a") ::
        (2, "B", 2, "b") ::
        (3, "C", 3, "c") ::
        (4, "D", 4, "d") ::
        (5, "E", null, null) ::
        (6, "F", null, null) :: Nil)
    }

    check(run)
  }

  test("right outer join") {
    lowerCaseData.registerAsTable("left")
    upperCaseData.registerAsTable("right")

    val left = UnresolvedRelation(None, "left", None)
    val right = UnresolvedRelation(None, "right", None)

    def run() {
      checkAnswer(
        left.join(right, RightOuter, Some('n === 'N)),
        (1, "a", 1, "A") ::
        (2, "b", 2, "B") ::
        (3, "c", 3, "C") ::
        (4, "d", 4, "D") ::
        (null, null, 5, "E") ::
        (null, null, 6, "F") :: Nil)
      checkAnswer(
        left.join(right, RightOuter, Some('n === 'N && 'n > 1)),
        (null, null, 1, "A") ::
        (2, "b", 2, "B") ::
        (3, "c", 3, "C") ::
        (4, "d", 4, "D") ::
        (null, null, 5, "E") ::
        (null, null, 6, "F") :: Nil)
      checkAnswer(
        left.join(right, RightOuter, Some('n === 'N && 'N > 1)),
        (null, null, 1, "A") ::
        (2, "b", 2, "B") ::
        (3, "c", 3, "C") ::
        (4, "d", 4, "D") ::
        (null, null, 5, "E") ::
        (null, null, 6, "F") :: Nil)
      checkAnswer(
        left.join(right, RightOuter, Some('n === 'N && 'l > 'L)),
        (1, "a", 1, "A") ::
        (2, "b", 2, "B") ::
        (3, "c", 3, "C") ::
        (4, "d", 4, "D") ::
        (null, null, 5, "E") ::
        (null, null, 6, "F") :: Nil)
    }

    check(run)
  }

  test("full outer join") {
    upperCaseData.where('N <= 4).registerAsTable("left")
    upperCaseData.where('N >= 3).registerAsTable("right")

    def run() {
      checkAnswer(
        left.join(right, FullOuter, Some("left.N".attr === "right.N".attr)),
        (1, "A", null, null) ::
        (2, "B", null, null) ::
        (3, "C", 3, "C") ::
        (4, "D", 4, "D") ::
        (null, null, 5, "E") ::
        (null, null, 6, "F") :: Nil)
  
      checkAnswer(
        left.join(right, FullOuter, 
            Some(("left.N".attr === "right.N".attr) && ("left.N".attr !== 3))),
        (1, "A", null, null) ::
        (2, "B", null, null) ::
        (3, "C", null, null) ::
        (null, null, 3, "C") ::
        (4, "D", 4, "D") ::
        (null, null, 5, "E") ::
        (null, null, 6, "F") :: Nil)
  
      checkAnswer(
        left.join(right, FullOuter, 
            Some(("left.N".attr === "right.N".attr) && ("right.N".attr !== 3))),
        (1, "A", null, null) ::
        (2, "B", null, null) ::
        (3, "C", null, null) ::
        (null, null, 3, "C") ::
        (4, "D", 4, "D") ::
        (null, null, 5, "E") ::
        (null, null, 6, "F") :: Nil)
    }

    check(run)
  }
}

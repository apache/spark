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

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import catalyst.analysis._
import catalyst.expressions._
import catalyst.plans._
import catalyst.plans.logical.LogicalPlan
import catalyst.types._

/* Implicits */
import TestSQLContext._

object TestData {
  val testData =
    logical.LocalRelation('key.int, 'value.string)
      .loadData((1 to 100).map(i => (i, i.toString)))

  val testData2 =
    logical.LocalRelation('a.int, 'b.int).loadData(
      (1, 1) ::
        (1, 2) ::
        (2, 1) ::
        (2, 2) ::
        (3, 1) ::
        (3, 2) :: Nil
    )

  val testData3 =
    logical.LocalRelation('a.int, 'b.int).loadData(
      (1, null) ::
        (2, 2) :: Nil
    )

  val upperCaseData =
    logical.LocalRelation('N.int, 'L.string).loadData(
        (1, "A") ::
        (2, "B") ::
        (3, "C") ::
        (4, "D") ::
        (5, "E") ::
        (6, "F") :: Nil
    )

  val lowerCaseData =
    logical.LocalRelation('n.int, 'l.string).loadData(
        (1, "a") ::
        (2, "b") ::
        (3, "c") ::
        (4, "d") :: Nil
    )
}

class DslQueryTest extends FunSuite {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param plan the query to be executed
   * @param expectedAnswer the expected result, can either be an Any, Seq[Product], or Seq[ Seq[Any] ].
   */
  protected def checkAnswer(plan: LogicalPlan, expectedAnswer: Any): Unit = {
    val convertedAnswer = expectedAnswer match {
      case s: Seq[_] if s.isEmpty => s
      case s: Seq[_] if s.head.isInstanceOf[Product] &&
        !s.head.isInstanceOf[Seq[_]] => s.map(_.asInstanceOf[Product].productIterator.toIndexedSeq)
      case s: Seq[_] => s
      case singleItem => Seq(Seq(singleItem))
    }

    val isSorted = plan.collect { case s: logical.Sort => s}.nonEmpty
    def prepareAnswer(answer: Seq[Any]) = if (!isSorted) answer.sortBy(_.toString) else answer
    val sparkAnswer = try plan.toRdd.collect().toSeq catch {
      case e: Exception =>
        fail(
          s"""
            |Exception thrown while executing query:
            |$plan
            |== Physical Plan ==
            |${plan.executedPlan}
            |== Exception ==
            |$e
          """.stripMargin)
    }
    assert(prepareAnswer(convertedAnswer) === prepareAnswer(sparkAnswer))
  }
}

class BasicQuerySuite extends DslQueryTest {
  import TestSQLContext._
  import TestData._

  test("table scan") {
    checkAnswer(
      testData,
      testData.data)
  }

  test("agg") {
    checkAnswer(
      testData2.groupBy('a)('a, Sum('b)),
      Seq((1,3),(2,3),(3,3))
    )
  }

  test("select *") {
    checkAnswer(
      testData.select(Star(None)),
      testData.data)
  }

  test("simple select") {
    checkAnswer(
      testData.where('key === 1).select('value),
      Seq(Seq("1")))
  }

  test("random sample") {
    testData.sample(0.5).toRdd.collect()
  }

  test("sorting") {
    checkAnswer(
      testData2.orderBy('a.asc, 'b.asc),
      Seq((1,1), (1,2), (2,1), (2,2), (3,1), (3,2)))

    checkAnswer(
      testData2.orderBy('a.asc, 'b.desc),
      Seq((1,2), (1,1), (2,2), (2,1), (3,2), (3,1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.desc),
      Seq((3,2), (3,1), (2,2), (2,1), (1,2), (1,1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.asc),
      Seq((3,1), (3,2), (2,1), (2,2), (1,1), (1,2)))
  }

  test("average") {
    checkAnswer(
      testData2.groupBy()(Average('a)),
      2.0)
  }

  test("count") {
    checkAnswer(
      testData2.groupBy()(Count(1)),
      testData2.data.size
    )
  }

  test("null count") {
    checkAnswer(
      testData3.groupBy('a)('a, Count('b)),
      Seq((1,0), (2, 1))
    )

    checkAnswer(
      testData3.groupBy()(Count('a), Count('b), Count(1), CountDistinct('a :: Nil), CountDistinct('b :: Nil)),
      (2, 1, 2, 2, 1) :: Nil
    )
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
    val x = testData2.where('a === 1).subquery('x)
    val y = testData2.where('a === 1).subquery('y)
    checkAnswer(
      x.join(y).where("x.a".attr === "y.a".attr),
      (1,1,1,1) ::
      (1,1,1,2) ::
      (1,2,1,1) ::
      (1,2,1,2) :: Nil
    )
  }

  test("inner join, no matches") {
    val x = testData2.where('a === 1).subquery('x)
    val y = testData2.where('a === 2).subquery('y)
    checkAnswer(
      x.join(y).where("x.a".attr === "y.a".attr),
      Nil)
  }

  test("big inner join, 4 matches per row") {
    val bigData = testData.unionAll(testData).unionAll(testData).unionAll(testData)
    val bigDataX = bigData.subquery('x)
    val bigDataY = bigData.subquery('y)

    checkAnswer(
      bigDataX.join(bigDataY).where("x.key".attr === "y.key".attr),
      testData.data.flatMap(row => Seq.fill(16)((row.productIterator ++ row.productIterator).toSeq)))
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
    val left = upperCaseData.where('N <= 4).subquery('left)
    val right = upperCaseData.where('N >= 3).subquery('right)

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
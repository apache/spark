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

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.test._

/* Implicits */
import TestSQLContext._

class DslQuerySuite extends QueryTest {
  import TestData._

  test("table scan") {
    checkAnswer(
      testData,
      testData.collect().toSeq)
  }

  test("agg") {
    checkAnswer(
      testData2.groupBy('a)('a, Sum('b)),
      Seq((1,3),(2,3),(3,3))
    )
    checkAnswer(
      testData2.groupBy('a)('a, Sum('b) as 'totB).aggregate(Sum('totB)),
      9
    )
    checkAnswer(
      testData2.aggregate(Sum('b)),
      9
    )
  }

  test("select *") {
    checkAnswer(
      testData.select(Star(None)),
      testData.collect().toSeq)
  }

  test("simple select") {
    checkAnswer(
      testData.where('key === 1).select('value),
      Seq(Seq("1")))
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

    checkAnswer(
      arrayData.orderBy(GetItem('data, 0).asc),
      arrayData.collect().sortBy(_.data(0)).toSeq)

    checkAnswer(
      arrayData.orderBy(GetItem('data, 0).desc),
      arrayData.collect().sortBy(_.data(0)).reverse.toSeq)

    checkAnswer(
      mapData.orderBy(GetItem('data, 1).asc),
      mapData.collect().sortBy(_.data(1)).toSeq)

    checkAnswer(
      mapData.orderBy(GetItem('data, 1).desc),
      mapData.collect().sortBy(_.data(1)).reverse.toSeq)
  }

  test("limit") {
    checkAnswer(
      testData.limit(10),
      testData.take(10).toSeq)

    checkAnswer(
      arrayData.limit(1),
      arrayData.take(1).toSeq)

    checkAnswer(
      mapData.limit(1),
      mapData.take(1).toSeq)
  }

  test("average") {
    checkAnswer(
      testData2.groupBy()(Average('a)),
      2.0)
  }

  test("count") {
    assert(testData2.count() === testData2.map(_ => 1).count())
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

  test("zero count") {
    assert(emptyTableData.count() === 0)
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

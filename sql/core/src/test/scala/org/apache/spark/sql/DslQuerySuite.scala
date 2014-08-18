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

  test("repartition") {
    checkAnswer(
      testData.select('key).repartition(10).select('key),
      testData.select('key).collect().toSeq)
  }

  test("agg") {
    checkAnswer(
      testData2.groupBy('a)('a, sum('b)),
      Seq((1,3),(2,3),(3,3))
    )
    checkAnswer(
      testData2.groupBy('a)('a, sum('b) as 'totB).aggregate(sum('totB)),
      9
    )
    checkAnswer(
      testData2.aggregate(sum('b)),
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

  test("select with functions") {
    checkAnswer(
      testData.select(sum('value), avg('value), count(1)),
      Seq(Seq(5050.0, 50.5, 100)))

    checkAnswer(
      testData2.select('a + 'b, 'a < 'b),
      Seq(
        Seq(2, false),
        Seq(3, true),
        Seq(3, false),
        Seq(4, false),
        Seq(4, false),
        Seq(5, false)))

    checkAnswer(
      testData2.select(sumDistinct('a)),
      Seq(Seq(6)))
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
      arrayData.orderBy('data.getItem(0).asc),
      arrayData.collect().sortBy(_.data(0)).toSeq)

    checkAnswer(
      arrayData.orderBy('data.getItem(0).desc),
      arrayData.collect().sortBy(_.data(0)).reverse.toSeq)

    checkAnswer(
      mapData.orderBy('data.getItem(1).asc),
      mapData.collect().sortBy(_.data(1)).toSeq)

    checkAnswer(
      mapData.orderBy('data.getItem(1).desc),
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
      testData2.groupBy()(avg('a)),
      2.0)
  }

  test("null average") {
    checkAnswer(
      testData3.groupBy()(avg('b)),
      2.0)

    checkAnswer(
      testData3.groupBy()(avg('b), countDistinct('b)),
      (2.0, 1) :: Nil)
  }

  test("count") {
    assert(testData2.count() === testData2.map(_ => 1).count())
  }

  test("null count") {
    checkAnswer(
      testData3.groupBy('a)('a, count('b)),
      Seq((1,0), (2, 1))
    )

    checkAnswer(
      testData3.groupBy('a)('a, count('a + 'b)),
      Seq((1,0), (2, 1))
    )

    checkAnswer(
      testData3.groupBy()(count('a), count('b), count(1), countDistinct('a), countDistinct('b)),
      (2, 1, 2, 2, 1) :: Nil
    )
  }

  test("zero count") {
    assert(emptyTableData.count() === 0)
  }

  test("except") {
    checkAnswer(
      lowerCaseData.except(upperCaseData),
      (1, "a") ::
      (2, "b") ::
      (3, "c") ::
      (4, "d") :: Nil)
    checkAnswer(lowerCaseData.except(lowerCaseData), Nil)
    checkAnswer(upperCaseData.except(upperCaseData), Nil)
  }

  test("intersect") {
    checkAnswer(
      lowerCaseData.intersect(lowerCaseData),
      (1, "a") ::
      (2, "b") ::
      (3, "c") ::
      (4, "d") :: Nil)
    checkAnswer(lowerCaseData.intersect(upperCaseData), Nil)
  }
}

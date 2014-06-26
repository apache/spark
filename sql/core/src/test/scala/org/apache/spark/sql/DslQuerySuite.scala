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

  test("null average") {
    checkAnswer(
      testData3.groupBy()(Average('b)),
      2.0)

    checkAnswer(
      testData3.groupBy()(Average('b), CountDistinct('b :: Nil)),
      (2.0, 1) :: Nil)
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
      testData3.groupBy('a)('a, Count('a + 'b)),
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
}

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

/* Implicits */
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.test.TestSQLContext._

class DslQuerySuite extends QueryTest {
  import org.apache.spark.sql.TestData._

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

  test("SPARK-3395 limit distinct") {
    val filtered = TestData.testData2
      .distinct()
      .orderBy(SortOrder('a, Ascending), SortOrder('b, Ascending))
      .limit(1)
      .registerTempTable("onerow")
    checkAnswer(
      sql("select * from onerow inner join testData2 on onerow.a = testData2.a"),
      (1, 1, 1, 1) ::
      (1, 1, 1, 2) :: Nil)
  }

  test("SPARK-3858 generator qualifiers are discarded") {
    checkAnswer(
      arrayData.as('ad)
        .generate(Explode("data" :: Nil, 'data), alias = Some("ex"))
        .select("ex.data".attr),
      Seq(1, 2, 3, 2, 3, 4).map(Seq(_)))
  }

  test("average") {
    checkAnswer(
      testData2.aggregate(avg('a)),
      2.0)

    checkAnswer(
      testData2.aggregate(avg('a), sumDistinct('a)), // non-partial
      (2.0, 6.0) :: Nil)

    checkAnswer(
      decimalData.aggregate(avg('a)),
      BigDecimal(2.0))
    checkAnswer(
      decimalData.aggregate(avg('a), sumDistinct('a)), // non-partial
      (BigDecimal(2.0), BigDecimal(6)) :: Nil)

    checkAnswer(
      decimalData.aggregate(avg('a cast DecimalType(10, 2))),
      BigDecimal(2.0))
    checkAnswer(
      decimalData.aggregate(avg('a cast DecimalType(10, 2)), sumDistinct('a cast DecimalType(10, 2))), // non-partial
      (BigDecimal(2.0), BigDecimal(6)) :: Nil)
  }

  test("null average") {
    checkAnswer(
      testData3.aggregate(avg('b)),
      2.0)

    checkAnswer(
      testData3.aggregate(avg('b), countDistinct('b)),
      (2.0, 1) :: Nil)

    checkAnswer(
      testData3.aggregate(avg('b), sumDistinct('b)), // non-partial
      (2.0, 2.0) :: Nil)
  }

  test("zero average") {
    checkAnswer(
      emptyTableData.aggregate(avg('a)),
      null)

    checkAnswer(
      emptyTableData.aggregate(avg('a), sumDistinct('b)), // non-partial
      (null, null) :: Nil)
  }

  test("count") {
    assert(testData2.count() === testData2.map(_ => 1).count())

    checkAnswer(
      testData2.aggregate(count('a), sumDistinct('a)), // non-partial
      (6, 6.0) :: Nil)
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
      testData3.aggregate(count('a), count('b), count(1), countDistinct('a), countDistinct('b)),
      (2, 1, 2, 2, 1) :: Nil
    )

    checkAnswer(
      testData3.aggregate(count('b), countDistinct('b), sumDistinct('b)), // non-partial
      (1, 1, 2) :: Nil
    )
  }

  test("zero count") {
    assert(emptyTableData.count() === 0)

    checkAnswer(
      emptyTableData.aggregate(count('a), sumDistinct('a)), // non-partial
      (0, null) :: Nil)
  }

  test("zero sum") {
    checkAnswer(
      emptyTableData.aggregate(sum('a)),
      null)
  }

  test("zero sum distinct") {
    checkAnswer(
      emptyTableData.aggregate(sumDistinct('a)),
      null)
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

  test("udf") {
    val foo = (a: Int, b: String) => a.toString + b

    checkAnswer(
      // SELECT *, foo(key, value) FROM testData
      testData.select(Star(None), foo.call('key, 'value)).limit(3),
      (1, "1", "11") :: (2, "2", "22") :: (3, "3", "33") :: Nil
    )
  }

  test("sqrt") {
    checkAnswer(
      testData.select(sqrt('key)).orderBy('key asc),
      (1 to 100).map(n => Seq(math.sqrt(n)))
    )

    checkAnswer(
      testData.select(sqrt('value), 'key).orderBy('key asc, 'value asc),
      (1 to 100).map(n => Seq(math.sqrt(n), n))
    )

    checkAnswer(
      testData.select(sqrt(Literal(null))),
      (1 to 100).map(_ => Seq(null))
    )
  }

  test("abs") {
    checkAnswer(
      testData.select(abs('key)).orderBy('key asc),
      (1 to 100).map(n => Seq(n))
    )

    checkAnswer(
      negativeData.select(abs('key)).orderBy('key desc),
      (1 to 100).map(n => Seq(n))
    )

    checkAnswer(
      testData.select(abs(Literal(null))),
      (1 to 100).map(_ => Seq(null))
    )
  }

  test("upper") {
    checkAnswer(
      lowerCaseData.select(upper('l)),
      ('a' to 'd').map(c => Seq(c.toString.toUpperCase()))
    )

    checkAnswer(
      testData.select(upper('value), 'key),
      (1 to 100).map(n => Seq(n.toString, n))
    )

    checkAnswer(
      testData.select(upper(Literal(null))),
      (1 to 100).map(n => Seq(null))
    )
  }

  test("lower") {
    checkAnswer(
      upperCaseData.select(lower('L)),
      ('A' to 'F').map(c => Seq(c.toString.toLowerCase()))
    )

    checkAnswer(
      testData.select(lower('value), 'key),
      (1 to 100).map(n => Seq(n.toString, n))
    )

    checkAnswer(
      testData.select(lower(Literal(null))),
      (1 to 100).map(n => Seq(null))
    )
  }
}

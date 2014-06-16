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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test._

/* Implicits */
import TestSQLContext._
import TestData._

class SQLQuerySuite extends QueryTest {
  // Make sure the tables are loaded.
  TestData

  test("SPARK-2041 column name equals tablename") {
    checkAnswer(
      sql("SELECT tableName FROM tableName"),
      "test")
  }

  test("index into array") {
    checkAnswer(
      sql("SELECT data, data[0], data[0] + data[1], data[0 + 1] FROM arrayData"),
      arrayData.map(d => (d.data, d.data(0), d.data(0) + d.data(1), d.data(1))).collect().toSeq)
  }

  test("left semi greater than predicate") {
    checkAnswer(
      sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.a >= y.a + 2"),
      Seq((3,1), (3,2))
    )
  }

  test("index into array of arrays") {
    checkAnswer(
      sql(
        "SELECT nestedData, nestedData[0][0], nestedData[0][0] + nestedData[0][1] FROM arrayData"),
      arrayData.map(d =>
        (d.nestedData,
         d.nestedData(0)(0),
         d.nestedData(0)(0) + d.nestedData(0)(1))).collect().toSeq)
  }

  test("agg") {
    checkAnswer(
      sql("SELECT a, SUM(b) FROM testData2 GROUP BY a"),
      Seq((1,3),(2,3),(3,3)))
  }

  test("aggregates with nulls") {
    checkAnswer(
      sql("SELECT MIN(a), MAX(a), AVG(a), SUM(a), COUNT(a) FROM nullInts"),
      (1, 3, 2, 6, 3) :: Nil
    )
  }

  test("select *") {
    checkAnswer(
      sql("SELECT * FROM testData"),
      testData.collect().toSeq)
  }

  test("simple select") {
    checkAnswer(
      sql("SELECT value FROM testData WHERE key = 1"),
      Seq(Seq("1")))
  }

  test("sorting") {
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b ASC"),
      Seq((1,1), (1,2), (2,1), (2,2), (3,1), (3,2)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b DESC"),
      Seq((1,2), (1,1), (2,2), (2,1), (3,2), (3,1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b DESC"),
      Seq((3,2), (3,1), (2,2), (2,1), (1,2), (1,1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b ASC"),
      Seq((3,1), (3,2), (2,1), (2,2), (1,1), (1,2)))

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] ASC"),
      arrayData.collect().sortBy(_.data(0)).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] DESC"),
      arrayData.collect().sortBy(_.data(0)).reverse.toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] ASC"),
      mapData.collect().sortBy(_.data(1)).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] DESC"),
      mapData.collect().sortBy(_.data(1)).reverse.toSeq)
  }

  test("limit") {
    checkAnswer(
      sql("SELECT * FROM testData LIMIT 10"),
      testData.take(10).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData LIMIT 1"),
      arrayData.collect().take(1).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData LIMIT 1"),
      mapData.collect().take(1).toSeq)
  }

  test("average") {
    checkAnswer(
      sql("SELECT AVG(a) FROM testData2"),
      2.0)
  }

  test("average overflow") {
    checkAnswer(
      sql("SELECT AVG(a),b FROM largeAndSmallInts group by b"),
      Seq((2147483645.0,1),(2.0,2)))
  }

  test("count") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM testData2"),
      testData2.count())
  }

  test("count distinct") {
    checkAnswer(
      sql("SELECT COUNT(DISTINCT b) FROM testData2"),
      2)
  }

  test("approximate count distinct") {
    checkAnswer(
      sql("SELECT APPROXIMATE COUNT(DISTINCT a) FROM testData2"),
      3)
  }

  test("approximate count distinct with user provided standard deviation") {
    checkAnswer(
      sql("SELECT APPROXIMATE(0.04) COUNT(DISTINCT a) FROM testData2"),
      3)
  }

  // No support for primitive nulls yet.
  ignore("null count") {
    checkAnswer(
      sql("SELECT a, COUNT(b) FROM testData3"),
      Seq((1,0), (2, 1)))

    checkAnswer(
      testData3.groupBy()(Count('a), Count('b), Count(1), CountDistinct('a :: Nil), CountDistinct('b :: Nil)),
      (2, 1, 2, 2, 1) :: Nil)
  }

  test("inner join where, one match per row") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData WHERE n = N"),
      Seq(
        (1, "A", 1, "a"),
        (2, "B", 2, "b"),
        (3, "C", 3, "c"),
        (4, "D", 4, "d")))
  }

  test("inner join ON, one match per row") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON n = N"),
      Seq(
        (1, "A", 1, "a"),
        (2, "B", 2, "b"),
        (3, "C", 3, "c"),
        (4, "D", 4, "d")))
  }

  test("inner join, where, multiple matches") {
    checkAnswer(
      sql("""
        |SELECT * FROM
        |  (SELECT * FROM testData2 WHERE a = 1) x JOIN
        |  (SELECT * FROM testData2 WHERE a = 1) y
        |WHERE x.a = y.a""".stripMargin),
      (1,1,1,1) ::
      (1,1,1,2) ::
      (1,2,1,1) ::
      (1,2,1,2) :: Nil)
  }

  test("inner join, no matches") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData2 WHERE a = 1) x JOIN
          |  (SELECT * FROM testData2 WHERE a = 2) y
          |WHERE x.a = y.a""".stripMargin),
      Nil)
  }

  test("big inner join, 4 matches per row") {


    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) x JOIN
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) y
          |WHERE x.key = y.key""".stripMargin),
      testData.flatMap(
        row => Seq.fill(16)((row ++ row).toSeq)).collect().toSeq)
  }

  ignore("cartesian product join") {
    checkAnswer(
      testData3.join(testData3),
      (1, null, 1, null) ::
      (1, null, 2, 2) ::
      (2, 2, 1, null) ::
      (2, 2, 2, 2) :: Nil)
  }

  test("left outer join") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData LEFT OUTER JOIN lowerCaseData ON n = N"),
      (1, "A", 1, "a") ::
      (2, "B", 2, "b") ::
      (3, "C", 3, "c") ::
      (4, "D", 4, "d") ::
      (5, "E", null, null) ::
      (6, "F", null, null) :: Nil)
  }

  test("right outer join") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData RIGHT OUTER JOIN upperCaseData ON n = N"),
      (1, "a", 1, "A") ::
      (2, "b", 2, "B") ::
      (3, "c", 3, "C") ::
      (4, "d", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)
  }

  test("full outer join") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM upperCaseData WHERE N <= 4) leftTable FULL OUTER JOIN
          |  (SELECT * FROM upperCaseData WHERE N >= 3) rightTable
          |    ON leftTable.N = rightTable.N
        """.stripMargin),
      (1, "A", null, null) ::
      (2, "B", null, null) ::
      (3, "C", 3, "C") ::
      (4, "D", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)
  }

  test("mixed-case keywords") {
    checkAnswer(
      sql(
        """
          |SeleCT * from
          |  (select * from upperCaseData WherE N <= 4) leftTable fuLL OUtER joiN
          |  (sElEcT * FROM upperCaseData whERe N >= 3) rightTable
          |    oN leftTable.N = rightTable.N
        """.stripMargin),
      (1, "A", null, null) ::
      (2, "B", null, null) ::
      (3, "C", 3, "C") ::
      (4, "D", 4, "D") ::
      (null, null, 5, "E") ::
      (null, null, 6, "F") :: Nil)
  }

  test("select with table name as qualifier") {
    checkAnswer(
      sql("SELECT testData.value FROM testData WHERE testData.key = 1"),
      Seq(Seq("1")))
  }

  test("inner join ON with table name as qualifier") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        (1, "A", 1, "a"),
        (2, "B", 2, "b"),
        (3, "C", 3, "c"),
        (4, "D", 4, "d")))
  }

  test("qualified select with inner join ON with table name as qualifier") {
    checkAnswer(
      sql("SELECT upperCaseData.N, upperCaseData.L FROM upperCaseData JOIN lowerCaseData " +
        "ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        (1, "A"),
        (2, "B"),
        (3, "C"),
        (4, "D")))
  }

  test("system function upper()") {
    checkAnswer(
      sql("SELECT n,UPPER(l) FROM lowerCaseData"),
      Seq(
        (1, "A"),
        (2, "B"),
        (3, "C"),
        (4, "D")))

    checkAnswer(
      sql("SELECT n, UPPER(s) FROM nullStrings"),
      Seq(
        (1, "ABC"),
        (2, "ABC"),
        (3, null)))
  }

  test("system function lower()") {
    checkAnswer(
      sql("SELECT N,LOWER(L) FROM upperCaseData"),
      Seq(
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
        (5, "e"),
        (6, "f")))

    checkAnswer(
      sql("SELECT n, LOWER(s) FROM nullStrings"),
      Seq(
        (1, "abc"),
        (2, "abc"),
        (3, null)))
  }

  test("SET commands semantics using sql()") {
    clear()
    val testKey = "test.key.0"
    val testVal = "test.val.0"
    val nonexistentKey = "nonexistent"

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(sql("SET").collect().size == 0)

    // "set key=val"
    sql(s"SET $testKey=$testVal")
    checkAnswer(
      sql("SET"),
      Seq(Seq(testKey, testVal))
    )

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    checkAnswer(
      sql("set"),
      Seq(
        Seq(testKey, testVal),
        Seq(testKey + testKey, testVal + testVal))
    )

    // "set key"
    checkAnswer(
      sql(s"SET $testKey"),
      Seq(Seq(testKey, testVal))
    )
    checkAnswer(
      sql(s"SET $nonexistentKey"),
      Seq(Seq(nonexistentKey, "<undefined>"))
    )
    clear()
  }

}

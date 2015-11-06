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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{Row, DataFrame, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.scalatest.BeforeAndAfterAll

// TODO ideally we should put the test suite into the package `sql`, as
// `hive` package is optional in compiling, however, `SQLContext.sql` doesn't
// support the `cube` or `rollup` yet.
class HiveDataFrameAnalyticsSuite extends QueryTest with TestHiveSingleton with BeforeAndAfterAll {
  import hiveContext.implicits._
  import hiveContext.sql

  private var testData: DataFrame = _

  override def beforeAll() {
    testData = Seq((1, 2), (2, 4), (2, 9)).toDF("a", "b")
    hiveContext.registerDataFrameAsTable(testData, "mytable")
  }

  override def afterAll(): Unit = {
    hiveContext.dropTempTable("mytable")
  }

  test("rollup: aggregation input parameters overlap with the non-attribute expressions in group by") {
    val sqlRollUp = sql(
      """
        SELECT a + b, b, sum(a + b) as ab, (a + b) as c FROM mytable GROUP BY a + b, b WITH ROLLUP
      """.stripMargin)

    val res = sqlRollUp.collect()

    val expected =
      Row ( null, null, 20,  null) ::
      Row ( 3,    null,  3,  3   ) ::
      Row ( 6,    null,  6,  6   ) ::
      Row (11,    null, 11, 11   ) ::
      Row ( 3,    2,     3,  3   ) ::
      Row ( 6,    4,     6,  6   ) ::
      Row (11,    9,    11, 11   ) :: Nil

    checkAnswer(sqlRollUp, expected)

    checkAnswer(
      testData.rollup($"a" + $"b", $"b").agg(sum($"a" + $"b"), $"a" + $"b"),
      expected
    )
  }

  test("rollup: group by function") {
    val sqlRollUp = sql(
      """
        |SELECT a + b, b, sum(a - b) as ab
        |FROM mytable
        |GROUP BY a + b, b WITH ROLLUP
      """.stripMargin)

    val expected =
      Row (null, null, -10) ::
      Row (3,    null, -1)  ::
      Row (6,    null, -2)  ::
      Row (11,   null, -7)  ::
      Row (3,    2,    -1)  ::
      Row (6,    4,    -2)  ::
      Row (11,   9,    -7)  :: Nil

    checkAnswer(sqlRollUp, expected)

    checkAnswer(
      testData.rollup($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      expected
    )
  }

  test("rollup: aggregation function parameters overlap with the group by columns") {
    val sqlRollUp = sql(
      """
        |SELECT a, b, sum(b), max(b), min(b+b)
        |FROM mytable
        |GROUP BY a, b WITH ROLLUP
      """.stripMargin)

    val expected =
      Row (null, null, 15, 9, 4)  ::
      Row (1,    null, 2,  2, 4)  ::
      Row (2,    null, 13, 9, 8)  ::
      Row (1,    2,    2,  2, 4)  ::
      Row (2,    4,    4,  4, 8)  ::
      Row (2,    9,    9,  9, 18) :: Nil

    checkAnswer(sqlRollUp, expected)

    checkAnswer(
      testData.rollup("a", "b").agg(sum("b"), max("b"), min($"b" + $"b")),
      expected
    )
  }

  test("cube: aggregation input parameters overlap with the non-attribute expressions in group by") {
    val sqlCube = sql(
      """
        SELECT a + b, b, sum(a + b) as ab, (a + b) as c FROM mytable GROUP BY a + b, b WITH CUBE
      """.stripMargin)

    val res = sqlCube.collect()

    val expected =
      Row ( null, 2,     3,  null) ::
      Row ( null, 4,     6,  null) ::
      Row ( null, 9,    11,  null) ::
      Row ( null, null, 20,  null) ::
      Row ( 3,    null,  3,  3   ) ::
      Row ( 6,    null,  6,  6   ) ::
      Row (11,    null, 11, 11   ) ::
      Row ( 3,    2,     3,  3   ) ::
      Row ( 6,    4,     6,  6   ) ::
      Row (11,    9,    11, 11   ) :: Nil

    checkAnswer(sqlCube, expected)

    checkAnswer(
      testData.cube($"a" + $"b", $"b").agg(sum($"a" + $"b"), $"a" + $"b"),
      expected
    )
  }

  test("cube: group by function") {
    val sqlCube = sql(
      """
        |SELECT a + b, b, sum(a - b) as ab
        |FROM mytable
        |GROUP BY a + b, b WITH CUBE
      """.stripMargin)

    val expected =
      Row (null, 2,     -1) ::
      Row (null, 4,     -2) ::
      Row (null, 9,     -7) ::
      Row (null, null, -10) ::
      Row (3,    null,  -1) ::
      Row (6,    null,  -2) ::
      Row (11,   null,  -7) ::
      Row (3,    2,     -1) ::
      Row (6,    4,     -2) ::
      Row (11,   9,     -7) :: Nil

    checkAnswer(sqlCube, expected)

    checkAnswer(
      testData.cube($"a" + $"b", $"b").agg(sum($"a" - $"b")),
      expected
    )
  }

  test("cube: aggregation function parameters overlap with the group by columns") {
    val sqlCube = sql(
      """
        |SELECT a, b, sum(b), max(b), min(b+b)
        |FROM mytable
        |GROUP BY a, b WITH CUBE
      """.stripMargin)

    val expected =
      Row (null, 2,     2, 2,  4) ::
      Row (null, 4,     4, 4,  8) ::
      Row (null, 9,     9, 9, 18) ::
      Row (null, null, 15, 9,  4) ::
      Row (1,    null,  2, 2,  4) ::
      Row (2,    null, 13, 9,  8) ::
      Row (1,    2,     2, 2,  4) ::
      Row (2,    4,     4, 4,  8) ::
      Row (2,    9,     9, 9, 18) :: Nil

    checkAnswer(sqlCube, expected)

    checkAnswer(
      testData.cube("a", "b").agg(sum("b"), max("b"), min($"b" + $"b")),
      expected
    )
  }
}

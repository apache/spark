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

import org.apache.spark.sql.test.SharedSQLContext

class SubquerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  val row = identity[(java.lang.Integer, java.lang.Double)](_)

  lazy val l = Seq(
    row(1, 2.0),
    row(1, 2.0),
    row(2, 1.0),
    row(2, 1.0),
    row(3, 3.0),
    row(null, null),
    row(null, 5.0),
    row(6, null)).toDF("a", "b")

  lazy val r = Seq(
    row(2, 3.0),
    row(2, 3.0),
    row(3, 2.0),
    row(4, 1.0),
    row(null, null),
    row(null, 5.0),
    row(6, null)).toDF("c", "d")

  lazy val t = r.filter($"c".isNotNull && $"d".isNotNull)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    l.registerTempTable("l")
    r.registerTempTable("r")
    t.registerTempTable("t")
  }

  test("simple uncorrelated scalar subquery") {
    checkAnswer(
      sql("select (select 1 as b) as b"),
      Array(Row(1))
    )

    checkAnswer(
      sql("select (select (select 1) + 1) + 1"),
      Array(Row(3))
    )

    // string type
    checkAnswer(
      sql("select (select 's' as s) as b"),
      Array(Row("s"))
    )
  }

  test("uncorrelated scalar subquery in CTE") {
    checkAnswer(
      sql("with t2 as (select 1 as b, 2 as c) " +
        "select a from (select 1 as a union all select 2 as a) t " +
        "where a = (select max(b) from t2) "),
      Array(Row(1))
    )
  }

  test("uncorrelated scalar subquery should return null if there is 0 rows") {
    checkAnswer(
      sql("select (select 's' as s limit 0) as b"),
      Array(Row(null))
    )
  }

  test("runtime error when the number of rows is greater than 1") {
    val error2 = intercept[RuntimeException] {
      sql("select (select a from (select 1 as a union all select 2 as a) t) as b").collect()
    }
    assert(error2.getMessage.contains(
      "more than one row returned by a subquery used as an expression")
    )
  }

  test("uncorrelated scalar subquery on a DataFrame generated query") {
    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
    df.registerTempTable("subqueryData")

    checkAnswer(
      sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1"),
      Array(Row(4))
    )

    checkAnswer(
      sql("select -(select max(key) from subqueryData)"),
      Array(Row(-3))
    )

    checkAnswer(
      sql("select (select value from subqueryData limit 0)"),
      Array(Row(null))
    )

    checkAnswer(
      sql("select (select min(value) from subqueryData" +
        " where key = (select max(key) from subqueryData) - 1)"),
      Array(Row("two"))
    )
  }

  test("SPARK-14791: scalar subquery inside broadcast join") {
    val df = sql("select a, sum(b) as s from l group by a having a > (select avg(a) from l)")
    val expected = Row(3, 2.0, 3, 3.0) :: Row(6, null, 6, null) :: Nil
    (1 to 10).foreach { _ =>
      checkAnswer(r.join(df, $"c" === $"a"), expected)
    }
  }

  test("EXISTS predicate subquery") {
    checkAnswer(
      sql("select * from l where exists (select * from r where l.a = r.c)"),
      Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) :: Row(6, null) :: Nil)

    checkAnswer(
      sql("select * from l where exists (select * from r where l.a = r.c) and l.a <= 2"),
      Row(2, 1.0) :: Row(2, 1.0) :: Nil)
  }

  test("NOT EXISTS predicate subquery") {
    checkAnswer(
      sql("select * from l where not exists (select * from r where l.a = r.c)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(null, null) :: Row(null, 5.0) :: Nil)

    checkAnswer(
      sql("select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(3, 3.0) ::
      Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)
  }

  test("IN predicate subquery") {
    checkAnswer(
      sql("select * from l where l.a in (select c from r)"),
      Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) :: Row(6, null) :: Nil)

    checkAnswer(
      sql("select * from l where l.a in (select c from r where l.b < r.d)"),
      Row(2, 1.0) :: Row(2, 1.0) :: Nil)

    checkAnswer(
      sql("select * from l where l.a in (select c from r) and l.a > 2 and l.b is not null"),
      Row(3, 3.0) :: Nil)
  }

  test("NOT IN predicate subquery") {
    checkAnswer(
      sql("select * from l where a not in (select c from r)"),
      Nil)

    checkAnswer(
      sql("select * from l where a not in (select c from r where c is not null)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Nil)

    checkAnswer(
      sql("select * from l where a not in (select c from t where b < d)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(3, 3.0) :: Nil)

    // Empty sub-query
    checkAnswer(
      sql("select * from l where a not in (select c from r where c > 10 and b < d)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(2, 1.0) :: Row(2, 1.0) ::
      Row(3, 3.0) :: Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)

  }

  test("complex IN predicate subquery") {
    checkAnswer(
      sql("select * from l where (a, b) not in (select c, d from r)"),
      Nil)

    checkAnswer(
      sql("select * from l where (a, b) not in (select c, d from t) and (a + b) is not null"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) :: Nil)
  }

  test("same column in subquery and outer table") {
    checkAnswer(
      sql("select a from l l1 where a in (select a from l where a < 3 group by a)"),
      Row(1) :: Row(1) :: Row(2) :: Row(2) :: Nil
    )
  }
}

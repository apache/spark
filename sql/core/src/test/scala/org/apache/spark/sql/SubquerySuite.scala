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
    l.createOrReplaceTempView("l")
    r.createOrReplaceTempView("r")
    t.createOrReplaceTempView("t")
  }

  test("SPARK-18854 numberedTreeString for subquery") {
    val df = sql("select * from range(10) where id not in " +
      "(select id from range(2) union all select id from range(2))")

    // The depth first traversal of the plan tree
    val dfs = Seq("Project", "Filter", "Union", "Project", "Range", "Project", "Range", "Range")
    val numbered = df.queryExecution.analyzed.numberedTreeString.split("\n")

    // There should be 8 plan nodes in total
    assert(numbered.size == dfs.size)

    for (i <- dfs.indices) {
      val node = df.queryExecution.analyzed(i)
      assert(node.nodeName == dfs(i))
      assert(numbered(i).contains(node.nodeName))
    }
  }

  test("rdd deserialization does not crash [SPARK-15791]") {
    sql("select (select 1 as b) as b").rdd.count()
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

  test("define CTE in CTE subquery") {
    checkAnswer(
      sql(
        """
          | with t2 as (with t1 as (select 1 as b, 2 as c) select b, c from t1)
          | select a from (select 1 as a union all select 2 as a) t
          | where a = (select max(b) from t2)
        """.stripMargin),
      Array(Row(1))
    )
    checkAnswer(
      sql(
        """
          | with t2 as (with t1 as (select 1 as b, 2 as c) select b, c from t1),
          | t3 as (
          |   with t4 as (select 1 as d, 3 as e)
          |   select * from t4 cross join t2 where t2.b = t4.d
          | )
          | select a from (select 1 as a union all select 2 as a)
          | where a = (select max(d) from t3)
        """.stripMargin),
      Array(Row(1))
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
    df.createOrReplaceTempView("subqueryData")

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

  test("SPARK-15677: Queries against local relations with scalar subquery in Select list") {
    withTempView("t1", "t2") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")

      checkAnswer(
        sql("SELECT (select 1 as col) from t1"),
        Row(1) :: Row(1) :: Nil)

      checkAnswer(
        sql("SELECT (select max(c1) from t2) from t1"),
        Row(2) :: Row(2) :: Nil)

      checkAnswer(
        sql("SELECT 1 + (select 1 as col) from t1"),
        Row(2) :: Row(2) :: Nil)

      checkAnswer(
        sql("SELECT c1, (select max(c1) from t2) + c2 from t1"),
        Row(1, 3) :: Row(2, 4) :: Nil)

      checkAnswer(
        sql("SELECT c1, (select max(c1) from t2 where t1.c2 = t2.c2) from t1"),
        Row(1, 1) :: Row(2, 2) :: Nil)
    }
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

  test("EXISTS predicate subquery within OR") {
    checkAnswer(
      sql("select * from l where exists (select * from r where l.a = r.c)" +
        " or exists (select * from r where l.a = r.c)"),
      Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) :: Row(6, null) :: Nil)

    checkAnswer(
      sql("select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)" +
        " or not exists (select * from r where l.a = r.c)"),
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
      sql("select * from l where (a, b) not in (select c, d from t) and a < 4"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) :: Nil)

    // Empty sub-query
    checkAnswer(
      sql("select * from l where (a, b) not in (select c, d from r where c > 10)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(2, 1.0) :: Row(2, 1.0) ::
      Row(3, 3.0) :: Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)

  }

  test("IN predicate subquery within OR") {
    checkAnswer(
      sql("select * from l where l.a in (select c from r)" +
        " or l.a in (select c from r where l.b < r.d)"),
      Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) :: Row(6, null) :: Nil)

    intercept[AnalysisException] {
      sql("select * from l where a not in (select c from r)" +
        " or a not in (select c from r where c is not null)")
    }
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

  test("having with function in subquery") {
    checkAnswer(
      sql("select a from l group by 1 having exists (select 1 from r where d < min(b))"),
      Row(null) :: Row(1) :: Row(3) :: Nil)
  }

  test("SPARK-15832: Test embedded existential predicate sub-queries") {
    withTempView("t1", "t2", "t3", "t4", "t5") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")
      Seq((1, 1), (2, 2), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t3")

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where c2 IN (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where c2 NOT IN (select c2 from t2)
            |
          """.stripMargin),
       Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where EXISTS (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

       checkAnswer(
        sql(
          """
            | select c1 from t1
            | where NOT EXISTS (select c2 from t2)
            |
          """.stripMargin),
      Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where NOT EXISTS (select c2 from t2) and
            |       c2 IN (select c2 from t3)
            |
          """.stripMargin),
        Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select 1 as one) then 1
            |             else 2 end) = c1
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select 1 as one) then 1
            |             else 2 end)
            |        IN (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select c2 from t2) then 1
            |             else 2 end)
            |       IN (select c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select c2 from t2) then 1
            |             when c2 IN (select c2 from t3) then 2
            |             else 3 end)
            |       IN (select c2 from t1)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (c1, (case when c2 IN (select c2 from t2) then 1
            |                  when c2 IN (select c2 from t3) then 2
            |                  else 3 end))
            |       IN (select c1, c2 from t1)
            |
          """.stripMargin),
        Row(1) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t3
            | where ((case when c2 IN (select c2 from t2) then 1 else 2 end),
            |        (case when c2 IN (select c2 from t3) then 2 else 3 end))
            |     IN (select c1, c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Row(1) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where ((case when EXISTS (select c2 from t2) then 1 else 2 end),
            |        (case when c2 IN (select c2 from t3) then 2 else 3 end))
            |     IN (select c1, c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select c2 from t2) then 3
            |             else 2 end)
            |       NOT IN (select c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where ((case when c2 IN (select c2 from t2) then 1 else 2 end),
            |        (case when NOT EXISTS (select c2 from t3) then 2
            |              when EXISTS (select c2 from t2) then 3
            |              else 3 end))
            |     NOT IN (select c1, c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (select max(c1) from t2 where c2 IN (select c2 from t3))
            |       IN (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)
    }
  }

  test("correlated scalar subquery in where") {
    checkAnswer(
      sql("select * from l where b < (select max(d) from r where a = c)"),
      Row(2, 1.0) :: Row(2, 1.0) :: Nil)
  }

  test("correlated scalar subquery in select") {
    checkAnswer(
      sql("select a, (select sum(b) from l l2 where l2.a = l1.a) sum_b from l l1"),
      Row(1, 4.0) :: Row(1, 4.0) :: Row(2, 2.0) :: Row(2, 2.0) :: Row(3, 3.0) ::
      Row(null, null) :: Row(null, null) :: Row(6, null) :: Nil)
  }

  test("correlated scalar subquery in select (null safe)") {
    checkAnswer(
      sql("select a, (select sum(b) from l l2 where l2.a <=> l1.a) sum_b from l l1"),
      Row(1, 4.0) :: Row(1, 4.0) :: Row(2, 2.0) :: Row(2, 2.0) :: Row(3, 3.0) ::
        Row(null, 5.0) :: Row(null, 5.0) :: Row(6, null) :: Nil)
  }

  test("correlated scalar subquery in aggregate") {
    checkAnswer(
      sql("select a, (select sum(d) from r where a = c) sum_d from l l1 group by 1, 2"),
      Row(1, null) :: Row(2, 6.0) :: Row(3, 2.0) :: Row(null, null) :: Row(6, null) :: Nil)
  }

  test("SPARK-18504 extra GROUP BY column in correlated scalar subquery is not permitted") {
    withTempView("t") {
      Seq((1, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")

      val errMsg = intercept[AnalysisException] {
        sql("select (select sum(-1) from t t2 where t1.c2 = t2.c1 group by t2.c2) sum from t t1")
      }
      assert(errMsg.getMessage.contains(
        "A GROUP BY clause in a scalar correlated subquery cannot contain non-correlated columns:"))
    }
  }

  test("non-aggregated correlated scalar subquery") {
    val msg1 = intercept[AnalysisException] {
      sql("select a, (select b from l l2 where l2.a = l1.a) sum_b from l l1")
    }
    assert(msg1.getMessage.contains("Correlated scalar subqueries must be Aggregated"))

    val msg2 = intercept[AnalysisException] {
      sql("select a, (select b from l l2 where l2.a = l1.a group by 1) sum_b from l l1")
    }
    assert(msg2.getMessage.contains(
      "The output of a correlated scalar subquery must be aggregated"))
  }

  test("non-equal correlated scalar subquery") {
    val msg1 = intercept[AnalysisException] {
      sql("select a, (select sum(b) from l l2 where l2.a < l1.a) sum_b from l l1")
    }
    assert(msg1.getMessage.contains(
      "Correlated column is not allowed in a non-equality predicate:"))
  }

  test("disjunctive correlated scalar subquery") {
    checkAnswer(
      sql("""
        |select a
        |from   l
        |where  (select count(*)
        |        from   r
        |        where (a = c and d = 2.0) or (a = c and d = 1.0)) > 0
        """.stripMargin),
      Row(3) :: Nil)
  }

  test("SPARK-15370: COUNT bug in WHERE clause (Filter)") {
    // Case 1: Canonical example of the COUNT bug
    checkAnswer(
      sql("select l.a from l where (select count(*) from r where l.a = r.c) < l.a"),
      Row(1) :: Row(1) :: Row(3) :: Row(6) :: Nil)
    // Case 2: count(*) = 0; could be rewritten to NOT EXISTS but currently uses
    // a rewrite that is vulnerable to the COUNT bug
    checkAnswer(
      sql("select l.a from l where (select count(*) from r where l.a = r.c) = 0"),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
    // Case 3: COUNT bug without a COUNT aggregate
    checkAnswer(
      sql("select l.a from l where (select sum(r.d) is null from r where l.a = r.c)"),
      Row(1) :: Row(1) ::Row(null) :: Row(null) :: Row(6) :: Nil)
  }

  test("SPARK-15370: COUNT bug in SELECT clause (Project)") {
    checkAnswer(
      sql("select a, (select count(*) from r where l.a = r.c) as cnt from l"),
      Row(1, 0) :: Row(1, 0) :: Row(2, 2) :: Row(2, 2) :: Row(3, 1) :: Row(null, 0)
        :: Row(null, 0) :: Row(6, 1) :: Nil)
  }

  test("SPARK-15370: COUNT bug in HAVING clause (Filter)") {
    checkAnswer(
      sql("select l.a as grp_a from l group by l.a " +
        "having (select count(*) from r where grp_a = r.c) = 0 " +
        "order by grp_a"),
      Row(null) :: Row(1) :: Nil)
  }

  test("SPARK-15370: COUNT bug in Aggregate") {
    checkAnswer(
      sql("select l.a as aval, sum((select count(*) from r where l.a = r.c)) as cnt " +
        "from l group by l.a order by aval"),
      Row(null, 0) :: Row(1, 0) :: Row(2, 4) :: Row(3, 1) :: Row(6, 1)  :: Nil)
  }

  test("SPARK-15370: COUNT bug negative examples") {
    // Case 1: Potential COUNT bug case that was working correctly prior to the fix
    checkAnswer(
      sql("select l.a from l where (select sum(r.d) from r where l.a = r.c) is null"),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Row(6) :: Nil)
    // Case 2: COUNT aggregate but no COUNT bug due to > 0 test.
    checkAnswer(
      sql("select l.a from l where (select count(*) from r where l.a = r.c) > 0"),
      Row(2) :: Row(2) :: Row(3) :: Row(6) :: Nil)
    // Case 3: COUNT inside aggregate expression but no COUNT bug.
    checkAnswer(
      sql("select l.a from l where (select count(*) + sum(r.d) from r where l.a = r.c) = 0"),
      Nil)
  }

  test("SPARK-15370: COUNT bug in subquery in subquery in subquery") {
    checkAnswer(
      sql("""select l.a from l
            |where (
            |    select cntPlusOne + 1 as cntPlusTwo from (
            |        select cnt + 1 as cntPlusOne from (
            |            select sum(r.c) s, count(*) cnt from r where l.a = r.c having cnt = 0
            |        )
            |    )
            |) = 2""".stripMargin),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
  }

  test("SPARK-15370: COUNT bug with nasty predicate expr") {
    checkAnswer(
      sql("select l.a from l where " +
        "(select case when count(*) = 1 then null else count(*) end as cnt " +
        "from r where l.a = r.c) = 0"),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
  }

  test("SPARK-15370: COUNT bug with attribute ref in subquery input and output ") {
    checkAnswer(
      sql("select l.b, (select (r.c + count(*)) is null from r where l.a = r.c) from l"),
      Row(1.0, false) :: Row(1.0, false) :: Row(2.0, true) :: Row(2.0, true) ::
        Row(3.0, false) :: Row(5.0, true) :: Row(null, false) :: Row(null, true) :: Nil)
  }

  test("SPARK-16804: Correlated subqueries containing LIMIT - 1") {
    withTempView("onerow") {
      Seq(1).toDF("c1").createOrReplaceTempView("onerow")

      checkAnswer(
        sql(
          """
            | select c1 from onerow t1
            | where exists (select 1 from onerow t2 where t1.c1=t2.c1)
            | and   exists (select 1 from onerow LIMIT 1)""".stripMargin),
        Row(1) :: Nil)
    }
  }

  test("SPARK-16804: Correlated subqueries containing LIMIT - 2") {
    withTempView("onerow") {
      Seq(1).toDF("c1").createOrReplaceTempView("onerow")

      checkAnswer(
        sql(
          """
            | select c1 from onerow t1
            | where exists (select 1
            |               from   (select 1 from onerow t2 LIMIT 1)
            |               where  t1.c1=t2.c1)""".stripMargin),
        Row(1) :: Nil)
    }
  }

  test("SPARK-17337: Incorrect column resolution leads to incorrect results") {
    withTempView("t1", "t2") {
      Seq(1, 2).toDF("c1").createOrReplaceTempView("t1")
      Seq(1).toDF("c2").createOrReplaceTempView("t2")

      checkAnswer(
        sql(
          """
            | select *
            | from   (select t2.c2+1 as c3
            |         from   t1 left join t2 on t1.c1=t2.c2) t3
            | where  c3 not in (select c2 from t2)""".stripMargin),
        Row(2) :: Nil)
     }
   }

   test("SPARK-17348: Correlated subqueries with non-equality predicate (good case)") {
     withTempView("t1", "t2") {
       Seq((1, 1)).toDF("c1", "c2").createOrReplaceTempView("t1")
       Seq((1, 1), (2, 0)).toDF("c1", "c2").createOrReplaceTempView("t2")

       // Simple case
       checkAnswer(
         sql(
           """
             | select c1
             | from   t1
             | where  c1 in (select t2.c1
             |               from   t2
             |               where  t1.c2 >= t2.c2)""".stripMargin),
         Row(1) :: Nil)

       // More complex case with OR predicate
       checkAnswer(
         sql(
           """
             | select t1.c1
             | from   t1, t1 as t3
             | where  t1.c1 = t3.c1
             | and    (t1.c1 in (select t2.c1
             |                   from   t2
             |                   where  t1.c2 >= t2.c2
             |                          or t3.c2 < t2.c2)
             |         or t1.c2 >= 0)""".stripMargin),
         Row(1) :: Nil)
    }
  }

  test("SPARK-17348: Correlated subqueries with non-equality predicate (error case)") {
    withTempView("t1", "t2", "t3", "t4") {
      Seq((1, 1)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 0)).toDF("c1", "c2").createOrReplaceTempView("t2")
      Seq((2, 1)).toDF("c1", "c2").createOrReplaceTempView("t3")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t4")

      // Simplest case
      intercept[AnalysisException] {
        sql(
          """
            | select t1.c1
            | from   t1
            | where  t1.c1 in (select max(t2.c1)
            |                  from   t2
            |                  where  t1.c2 >= t2.c2)""".stripMargin).collect()
      }

      // Add a HAVING on top and augmented within an OR predicate
      intercept[AnalysisException] {
        sql(
          """
            | select t1.c1
            | from   t1
            | where  t1.c1 in (select max(t2.c1)
            |                  from   t2
            |                  where  t1.c2 >= t2.c2
            |                  having count(*) > 0 )
            |         or t1.c2 >= 0""".stripMargin).collect()
      }

      // Add a HAVING on top and augmented within an OR predicate
      intercept[AnalysisException] {
        sql(
          """
            | select t1.c1
            | from   t1, t1 as t3
            | where  t1.c1 = t3.c1
            | and    (t1.c1 in (select max(t2.c1)
            |                   from   t2
            |                   where  t1.c2 = t2.c2
            |                          or t3.c2 = t2.c2)
            |        )""".stripMargin).collect()
      }

      // In Window expression: changing the data set to
      // demonstrate if this query ran, it would return incorrect result.
      intercept[AnalysisException] {
        sql(
          """
          | select c1
          | from   t3
          | where  c1 in (select max(t4.c1) over ()
          |               from   t4
          |               where t3.c2 >= t4.c2)""".stripMargin).collect()
      }
    }
  }
  // This restriction applies to
  // the permutation of { LOJ, ROJ, FOJ } x { EXISTS, IN, scalar subquery }
  // where correlated predicates appears in right operand of LOJ,
  // or in left operand of ROJ, or in either operand of FOJ.
  // The test cases below cover the representatives of the patterns
  test("Correlated subqueries in outer joins") {
    withTempView("t1", "t2", "t3") {
      Seq(1).toDF("c1").createOrReplaceTempView("t1")
      Seq(2).toDF("c1").createOrReplaceTempView("t2")
      Seq(1).toDF("c1").createOrReplaceTempView("t3")

      // Left outer join (LOJ) in IN subquery context
      intercept[AnalysisException] {
        sql(
          """
            | select t1.c1
            | from   t1
            | where  1 IN (select 1
            |              from   t3 left outer join
            |                     (select c1 from t2 where t1.c1 = 2) t2
            |                     on t2.c1 = t3.c1)""".stripMargin).collect()
      }
      // Right outer join (ROJ) in EXISTS subquery context
      intercept[AnalysisException] {
        sql(
          """
            | select t1.c1
            | from   t1
            | where  exists (select 1
            |                from   (select c1 from t2 where t1.c1 = 2) t2
            |                       right outer join t3
            |                       on t2.c1 = t3.c1)""".stripMargin).collect()
      }
      // SPARK-18578: Full outer join (FOJ) in scalar subquery context
      intercept[AnalysisException] {
        sql(
          """
            | select (select max(1)
            |         from   (select c1 from  t2 where t1.c1 = 2 and t1.c1=t2.c1) t2
            |                full join t3
            |                on t2.c1=t3.c1)
            | from   t1""".stripMargin).collect()
      }
    }
  }

  // Generate operator
  test("Correlated subqueries in LATERAL VIEW") {
    withTempView("t1", "t2") {
      Seq((1, 1), (2, 0)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq[(Int, Array[Int])]((1, Array(1, 2)), (2, Array(-1, -3)))
        .toDF("c1", "arr_c2").createTempView("t2")
      checkAnswer(
        sql(
          """
          | select c2
          | from t1
          | where exists (select *
          |               from t2 lateral view explode(arr_c2) q as c2
                          where t1.c1 = t2.c1)""".stripMargin),
        Row(1) :: Row(0) :: Nil)
    }
  }

  test("SPARK-19933 Do not eliminate top-level aliases in sub-queries") {
    withTempView("t1", "t2") {
      spark.range(4).createOrReplaceTempView("t1")
      checkAnswer(
        sql("select * from t1 where id in (select id as id from t1)"),
        Row(0) :: Row(1) :: Row(2) :: Row(3) :: Nil)

      spark.range(2).createOrReplaceTempView("t2")
      checkAnswer(
        sql("select * from t1 where id in (select id as id from t2)"),
        Row(0) :: Row(1) :: Nil)
    }
  }
}

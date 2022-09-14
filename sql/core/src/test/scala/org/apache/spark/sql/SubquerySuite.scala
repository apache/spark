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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project, Sort}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecution}
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SubquerySuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  setupTestData()

  val row = identity[(java.lang.Integer, java.lang.Double)](_)

  lazy val l = Seq(
    row((1, 2.0)),
    row((1, 2.0)),
    row((2, 1.0)),
    row((2, 1.0)),
    row((3, 3.0)),
    row((null, null)),
    row((null, 5.0)),
    row((6, null))).toDF("a", "b")

  lazy val r = Seq(
    row((2, 3.0)),
    row((2, 3.0)),
    row((3, 2.0)),
    row((4, 1.0)),
    row((null, null)),
    row((null, 5.0)),
    row((6, null))).toDF("c", "d")

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

  test("SPARK-15791: rdd deserialization does not crash") {
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

  test("uncorrelated scalar subquery on a DataFrame generated query") {
    withTempView("subqueryData") {
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

    checkAnswer(
      sql("select * from l where a not in (select c from r)" +
        " or a not in (select c from r where c is not null)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Nil)
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

  test("SPARK-34269: correlated subquery with view in aggregate's grouping expression") {
    withTable("tr") {
      withView("vr") {
        r.write.saveAsTable("tr")
        sql("create view vr as select * from tr")
        checkAnswer(
          sql("select a, (select sum(d) from vr where a = c) sum_d from l l1 group by 1, 2"),
          Row(1, null) :: Row(2, 6.0) :: Row(3, 2.0) :: Row(null, null) :: Row(6, null) :: Nil)
      }
    }
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
    assert(msg1.getMessage.contains("Correlated scalar subqueries must be aggregated"))

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
      "Correlated column is not allowed in predicate (l2.a < outer(l1.a))"))
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
      sql(
        """
          |select l.b, (select (r.c + count(*)) is null
          |from r
          |where l.a = r.c group by r.c) from l
        """.stripMargin),
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
            |               from   (select c1 from onerow t2 LIMIT 1) t2
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
          | SELECT c2
          | FROM t1
          | WHERE EXISTS (SELECT *
          |               FROM t2 LATERAL VIEW explode(arr_c2) q AS c2
                          WHERE t1.c1 = t2.c1)""".stripMargin),
        Row(1) :: Row(0) :: Nil)

      val msg1 = intercept[AnalysisException] {
        sql(
          """
            | SELECT c1
            | FROM t2
            | WHERE EXISTS (SELECT *
            |               FROM t1 LATERAL VIEW explode(t2.arr_c2) q AS c2
            |               WHERE t1.c1 = t2.c1)
          """.stripMargin)
      }
      assert(msg1.getMessage.contains(
        "Expressions referencing the outer query are not supported outside of WHERE/HAVING"))
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

  test("ListQuery and Exists should work even no correlated references") {
    checkAnswer(
      sql("select * from l, r where l.a = r.c AND (r.d in (select d from r) OR l.a >= 1)"),
      Row(2, 1.0, 2, 3.0) :: Row(2, 1.0, 2, 3.0) :: Row(2, 1.0, 2, 3.0) ::
        Row(2, 1.0, 2, 3.0) :: Row(3.0, 3.0, 3, 2.0) :: Row(6, null, 6, null) :: Nil)
    checkAnswer(
      sql("select * from l, r where l.a = r.c + 1 AND (exists (select * from r) OR l.a = r.c)"),
      Row(3, 3.0, 2, 3.0) :: Row(3, 3.0, 2, 3.0) :: Nil)
  }

  test("SPARK-20688: correctly check analysis for scalar sub-queries") {
    withTempView("t") {
      Seq(1 -> "a").toDF("i", "j").createOrReplaceTempView("t")
      val query = "SELECT (SELECT count(*) FROM t WHERE a = 1)"
      checkError(
        exception =
          intercept[AnalysisException](sql(query)),
        errorClass = "UNRESOLVED_COLUMN",
        errorSubClass = "WITH_SUGGESTION",
        sqlState = None,
        parameters = Map(
          "objectName" -> "`a`",
          "proposal" -> "`t`.`i`, `t`.`j`"),
        context = ExpectedContext(
          fragment = "a",
          start = 37,
          stop = 37))
    }
  }

  test("SPARK-21835: Join in correlated subquery should be duplicateResolved: case 1") {
    withTable("t1") {
      withTempPath { path =>
        Seq(1 -> "a").toDF("i", "j").write.parquet(path.getCanonicalPath)
        sql(s"CREATE TABLE t1 USING parquet LOCATION '${path.toURI}'")

        val sqlText =
          """
            |SELECT * FROM t1 a
            |WHERE
            |NOT EXISTS (SELECT * FROM t1 b WHERE a.i = b.i)
          """.stripMargin
        val optimizedPlan = sql(sqlText).queryExecution.optimizedPlan
        val join = optimizedPlan.collectFirst { case j: Join => j }.get
        assert(join.duplicateResolved)
        assert(optimizedPlan.resolved)
      }
    }
  }

  test("SPARK-21835: Join in correlated subquery should be duplicateResolved: case 2") {
    withTable("t1", "t2", "t3") {
      withTempPath { path =>
        val data = Seq((1, 1, 1), (2, 0, 2))

        data.toDF("t1a", "t1b", "t1c").write.parquet(path.getCanonicalPath + "/t1")
        data.toDF("t2a", "t2b", "t2c").write.parquet(path.getCanonicalPath + "/t2")
        data.toDF("t3a", "t3b", "t3c").write.parquet(path.getCanonicalPath + "/t3")

        sql(s"CREATE TABLE t1 USING parquet LOCATION '${path.toURI}/t1'")
        sql(s"CREATE TABLE t2 USING parquet LOCATION '${path.toURI}/t2'")
        sql(s"CREATE TABLE t3 USING parquet LOCATION '${path.toURI}/t3'")

        val sqlText =
          s"""
             |SELECT *
             |FROM   (SELECT *
             |        FROM   t2
             |        WHERE  t2c IN (SELECT t1c
             |                       FROM   t1
             |                       WHERE  t1a = t2a)
             |        UNION
             |        SELECT *
             |        FROM   t3
             |        WHERE  t3a IN (SELECT t2a
             |                       FROM   t2
             |                       UNION ALL
             |                       SELECT t1a
             |                       FROM   t1
             |                       WHERE  t1b > 0)) t4
             |WHERE  t4.t2b IN (SELECT Min(t3b)
             |                          FROM   t3
             |                          WHERE  t4.t2a = t3a)
           """.stripMargin
        val optimizedPlan = sql(sqlText).queryExecution.optimizedPlan
        val joinNodes = optimizedPlan.collect { case j: Join => j }
        joinNodes.foreach(j => assert(j.duplicateResolved))
        assert(optimizedPlan.resolved)
      }
    }
  }

  test("SPARK-21835: Join in correlated subquery should be duplicateResolved: case 3") {
    val sqlText =
      """
        |SELECT * FROM l, r WHERE l.a = r.c + 1 AND
        |(EXISTS (SELECT * FROM r) OR l.a = r.c)
      """.stripMargin
    val optimizedPlan = sql(sqlText).queryExecution.optimizedPlan
    val join = optimizedPlan.collectFirst { case j: Join => j }.get
    assert(join.duplicateResolved)
    assert(optimizedPlan.resolved)
  }

  test("SPARK-23316: AnalysisException after max iteration reached for IN query") {
    // before the fix this would throw AnalysisException
    spark.range(10).where("(id,id) in (select id, null from range(3))").count
  }

  test("SPARK-24085 scalar subquery in partitioning expression") {
    withTable("parquet_part") {
      Seq("1" -> "a", "2" -> "a", "3" -> "b", "4" -> "b")
        .toDF("id_value", "id_type")
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("id_type")
        .format("parquet")
        .saveAsTable("parquet_part")
      checkAnswer(
        sql("SELECT * FROM parquet_part WHERE id_type = (SELECT 'b')"),
        Row("3", "b") :: Row("4", "b") :: Nil)
    }
  }

  private def getNumSortsInQuery(query: String): Int = {
    val plan = sql(query).queryExecution.optimizedPlan
    getNumSorts(plan) + getSubqueryExpressions(plan).map{s => getNumSorts(s.plan)}.sum
  }

  private def getSubqueryExpressions(plan: LogicalPlan): Seq[SubqueryExpression] = {
    val subqueryExpressions = ArrayBuffer.empty[SubqueryExpression]
    plan transformAllExpressions {
      case s: SubqueryExpression =>
        subqueryExpressions ++= (getSubqueryExpressions(s.plan) :+ s)
        s
    }
    subqueryExpressions.toSeq
  }

  private def getNumSorts(plan: LogicalPlan): Int = {
    plan.collect { case s: Sort => s }.size
  }

  test("SPARK-23957 Remove redundant sort from subquery plan(in subquery)") {
    withTempView("t1", "t2", "t3") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")
      Seq((1, 1, 1), (2, 2, 2)).toDF("c1", "c2", "c3").createOrReplaceTempView("t3")

      // Simple order by
      val query1 =
        """
           |SELECT c1 FROM t1
           |WHERE
           |c1 IN (SELECT c1 FROM t2 ORDER BY c1)
        """.stripMargin
      assert(getNumSortsInQuery(query1) == 0)

      // Nested order bys
      val query2 =
        """
           |SELECT c1
           |FROM   t1
           |WHERE  c1 IN (SELECT c1
           |              FROM   (SELECT *
           |                      FROM   t2
           |                      ORDER  BY c2)
           |              ORDER  BY c1)
        """.stripMargin
      assert(getNumSortsInQuery(query2) == 0)


      // nested IN
      val query3 =
        """
           |SELECT c1
           |FROM   t1
           |WHERE  c1 IN (SELECT c1
           |              FROM   t2
           |              WHERE  c1 IN (SELECT c1
           |                            FROM   t3
           |                            WHERE  c1 = 1
           |                            ORDER  BY c3)
           |              ORDER  BY c2)
        """.stripMargin
      assert(getNumSortsInQuery(query3) == 0)

      // Complex subplan and multiple sorts
      val query4 =
        """
           |SELECT c1
           |FROM   t1
           |WHERE  c1 IN (SELECT c1
           |              FROM   (SELECT c1, c2, count(*)
           |                      FROM   t2
           |                      GROUP BY c1, c2
           |                      HAVING count(*) > 0
           |                      ORDER BY c2)
           |              ORDER  BY c1)
        """.stripMargin
      assert(getNumSortsInQuery(query4) == 0)

      // Join in subplan
      val query5 =
        """
           |SELECT c1 FROM t1
           |WHERE
           |c1 IN (SELECT t2.c1 FROM t2, t3
           |       WHERE t2.c1 = t3.c1
           |       ORDER BY t2.c1)
        """.stripMargin
      assert(getNumSortsInQuery(query5) == 0)

      val query6 =
        """
           |SELECT c1
           |FROM   t1
           |WHERE  (c1, c2) IN (SELECT c1, max(c2)
           |                    FROM   (SELECT c1, c2, count(*)
           |                            FROM   t2
           |                            GROUP BY c1, c2
           |                            HAVING count(*) > 0
           |                            ORDER BY c2)
           |                    GROUP BY c1
           |                    HAVING max(c2) > 0
           |                    ORDER  BY c1)
        """.stripMargin

      assert(getNumSortsInQuery(query6) == 0)

      // Cases when sort is not removed from the plan
      // Limit on top of sort
      val query7 =
        """
           |SELECT c1 FROM t1
           |WHERE
           |c1 IN (SELECT c1 FROM t2 ORDER BY c1 limit 1)
        """.stripMargin
      assert(getNumSortsInQuery(query7) == 1)

      // Sort below a set operations (intersect, union)
      val query8 =
        """
           |SELECT c1 FROM t1
           |WHERE
           |c1 IN ((
           |        SELECT c1 + 1 AS c1 FROM t2
           |        ORDER BY c1
           |       )
           |       UNION
           |       (
           |         SELECT c1 + 2 AS c1 FROM t2
           |         ORDER BY c1
           |       ))
        """.stripMargin
      assert(getNumSortsInQuery(query8) == 2)
    }
  }

  test("SPARK-23957 Remove redundant sort from subquery plan(exists subquery)") {
    withTempView("t1", "t2", "t3") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")
      Seq((1, 1, 1), (2, 2, 2)).toDF("c1", "c2", "c3").createOrReplaceTempView("t3")

      // Simple order by exists correlated
      val query1 =
        """
           |SELECT c1 FROM t1
           |WHERE
           |EXISTS (SELECT t2.c1 FROM t2 WHERE t1.c1 = t2.c1 ORDER BY t2.c1)
        """.stripMargin
      assert(getNumSortsInQuery(query1) == 0)

      // Nested order by and correlated.
      val query2 =
        """
           |SELECT c1
           |FROM   t1
           |WHERE  EXISTS (SELECT c1
           |               FROM (SELECT *
           |                     FROM   t2
           |                     WHERE t2.c1 = t1.c1
           |                     ORDER  BY t2.c2) t2
           |               ORDER BY t2.c1)
        """.stripMargin
      assert(getNumSortsInQuery(query2) == 0)

      // nested EXISTS
      val query3 =
        """
           |SELECT c1
           |FROM   t1
           |WHERE  EXISTS (SELECT c1
           |               FROM t2
           |               WHERE EXISTS (SELECT c1
           |                             FROM   t3
           |                             WHERE  t3.c1 = t2.c1
           |                             ORDER  BY c3)
           |               AND t2.c1 = t1.c1
           |               ORDER BY c2)
        """.stripMargin
      assert(getNumSortsInQuery(query3) == 0)

      // Cases when sort is not removed from the plan
      // Limit on top of sort
      val query4 =
        """
           |SELECT c1 FROM t1
           |WHERE
           |EXISTS (SELECT t2.c1 FROM t2 WHERE t2.c1 = 1 ORDER BY t2.c1 limit 1)
        """.stripMargin
      assert(getNumSortsInQuery(query4) == 1)

      // Sort below a set operations (intersect, union)
      val query5 =
        """
           |SELECT c1 FROM t1
           |WHERE
           |EXISTS ((
           |        SELECT c1 FROM t2
           |        WHERE t2.c1 = 1
           |        ORDER BY t2.c1
           |        )
           |        UNION
           |        (
           |         SELECT c1 FROM t2
           |         WHERE t2.c1 = 2
           |         ORDER BY t2.c1
           |        ))
        """.stripMargin
      assert(getNumSortsInQuery(query5) == 2)
    }
  }

  ignore("SPARK-23957 Remove redundant sort from subquery plan(scalar subquery)") {
    withTempView("t1", "t2", "t3") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")
      Seq((1, 1, 1), (2, 2, 2)).toDF("c1", "c2", "c3").createOrReplaceTempView("t3")

      // Two scalar subqueries in OR
      val query1 =
        """
          |SELECT * FROM t1
          |WHERE  c1 = (SELECT max(t2.c1)
          |             FROM   t2
          |             ORDER BY max(t2.c1))
          |OR     c2 = (SELECT min(t3.c2)
          |             FROM   t3
          |             WHERE  t3.c1 = 1
          |             ORDER BY min(t3.c2))
        """.stripMargin
      assert(getNumSortsInQuery(query1) == 0)

      // scalar subquery - groupby and having
      val query2 =
        """
          |SELECT *
          |FROM   t1
          |WHERE  c1 = (SELECT   max(t2.c1)
          |             FROM     t2
          |             GROUP BY t2.c1
          |             HAVING   count(*) >= 1
          |             ORDER BY max(t2.c1))
        """.stripMargin
      assert(getNumSortsInQuery(query2) == 0)

      // nested scalar subquery
      val query3 =
        """
          |SELECT *
          |FROM   t1
          |WHERE  c1 = (SELECT   max(t2.c1)
          |             FROM     t2
          |             WHERE c1 = (SELECT max(t3.c1)
          |                         FROM t3
          |                         WHERE t3.c1 = 1
          |                         GROUP BY t3.c1
          |                         ORDER BY max(t3.c1)
          |                        )
          |              GROUP BY t2.c1
          |              HAVING   count(*) >= 1
          |              ORDER BY max(t2.c1))
        """.stripMargin
      assert(getNumSortsInQuery(query3) == 0)

      // Scalar subquery in projection
      val query4 =
        """
          |SELECT (SELECT min(c1) from t1 group by c1 order by c1)
          |FROM t1
          |WHERE t1.c1 = 1
        """.stripMargin
      assert(getNumSortsInQuery(query4) == 0)

      // Limit on top of sort prevents it from being pruned.
      val query5 =
        """
          |SELECT *
          |FROM   t1
          |WHERE  c1 = (SELECT   max(t2.c1)
          |             FROM     t2
          |             WHERE c1 = (SELECT max(t3.c1)
          |                         FROM t3
          |                         WHERE t3.c1 = 1
          |                         GROUP BY t3.c1
          |                         ORDER BY max(t3.c1)
          |                         )
          |             GROUP BY t2.c1
          |             HAVING   count(*) >= 1
          |             ORDER BY max(t2.c1)
          |             LIMIT 1)
        """.stripMargin
      assert(getNumSortsInQuery(query5) == 1)
    }
  }

  test("Cannot remove sort for floating-point order-sensitive aggregates from subquery") {
    Seq("float", "double").foreach { typeName =>
      Seq("SUM", "AVG", "KURTOSIS", "SKEWNESS", "STDDEV_POP", "STDDEV_SAMP",
          "VAR_POP", "VAR_SAMP").foreach { aggName =>
        val query =
          s"""
            |SELECT k, $aggName(v) FROM (
            |  SELECT k, v
            |  FROM VALUES (1, $typeName(2.0)), (2, $typeName(1.0)) t(k, v)
            |  ORDER BY v)
            |GROUP BY k
          """.stripMargin
        assert(getNumSortsInQuery(query) == 1)
      }
    }
  }

  test("SPARK-25482: Forbid pushdown to datasources of filters containing subqueries") {
    withTempView("t1", "t2") {
      sql("create temporary view t1(a int) using parquet")
      sql("create temporary view t2(b int) using parquet")
      val plan = sql("select * from t2 where b > (select max(a) from t1)")
      val subqueries = stripAQEPlan(plan.queryExecution.executedPlan).collect {
        case p => p.subqueries
      }.flatten
      assert(subqueries.length == 1)
    }
  }

  test("SPARK-26893: Allow pushdown of partition pruning subquery filters to file source") {
    withTable("a", "b") {
      spark.range(4).selectExpr("id", "id % 2 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(2).write.saveAsTable("b")

      val df = sql("SELECT * FROM a WHERE p <= (SELECT MIN(id) FROM b)")
      checkAnswer(df, Seq(Row(0, 0), Row(2, 0)))
      // need to execute the query before we can examine fs.inputRDDs()
      assert(stripAQEPlan(df.queryExecution.executedPlan) match {
        case WholeStageCodegenExec(ColumnarToRowExec(InputAdapter(
            fs @ FileSourceScanExec(_, _, _, partitionFilters, _, _, _, _, _)))) =>
          partitionFilters.exists(ExecSubqueryExpression.hasSubquery) &&
            fs.inputRDDs().forall(
              _.asInstanceOf[FileScanRDD].filePartitions.forall(
                _.files.forall(_.filePath.contains("p=0"))))
        case _ => false
      })
    }
  }

  test("SPARK-26078: deduplicate fake self joins for IN subqueries") {
    withTempView("a", "b") {
      Seq("a" -> 2, "b" -> 1).toDF("id", "num").createTempView("a")
      Seq("a" -> 2, "b" -> 1).toDF("id", "num").createTempView("b")

      val df1 = spark.sql(
        """
          |SELECT id,num,source FROM (
          |  SELECT id, num, 'a' as source FROM a
          |  UNION ALL
          |  SELECT id, num, 'b' as source FROM b
          |) AS c WHERE c.id IN (SELECT id FROM b WHERE num = 2)
        """.stripMargin)
      checkAnswer(df1, Seq(Row("a", 2, "a"), Row("a", 2, "b")))
      val df2 = spark.sql(
        """
          |SELECT id,num,source FROM (
          |  SELECT id, num, 'a' as source FROM a
          |  UNION ALL
          |  SELECT id, num, 'b' as source FROM b
          |) AS c WHERE c.id NOT IN (SELECT id FROM b WHERE num = 2)
        """.stripMargin)
      checkAnswer(df2, Seq(Row("b", 1, "a"), Row("b", 1, "b")))
      val df3 = spark.sql(
        """
          |SELECT id,num,source FROM (
          |  SELECT id, num, 'a' as source FROM a
          |  UNION ALL
          |  SELECT id, num, 'b' as source FROM b
          |) AS c WHERE c.id IN (SELECT id FROM b WHERE num = 2) OR
          |c.id IN (SELECT id FROM b WHERE num = 3)
        """.stripMargin)
      checkAnswer(df3, Seq(Row("a", 2, "a"), Row("a", 2, "b")))
    }
  }

  test("SPARK-27279: Reuse Subquery", DisableAdaptiveExecution("reuse is dynamic in AQE")) {
    Seq(true, false).foreach { reuse =>
      withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> reuse.toString) {
        val df = sql(
          """
            |SELECT (SELECT avg(key) FROM testData) + (SELECT avg(key) FROM testData)
            |FROM testData
            |LIMIT 1
          """.stripMargin)

        var countSubqueryExec = 0
        var countReuseSubqueryExec = 0
        df.queryExecution.executedPlan.transformAllExpressions {
          case s @ ScalarSubquery(_: SubqueryExec, _) =>
            countSubqueryExec = countSubqueryExec + 1
            s
          case s @ ScalarSubquery(_: ReusedSubqueryExec, _) =>
            countReuseSubqueryExec = countReuseSubqueryExec + 1
            s
        }

        if (reuse) {
          assert(countSubqueryExec == 1, "Subquery reusing not working correctly")
          assert(countReuseSubqueryExec == 1, "Subquery reusing not working correctly")
        } else {
          assert(countSubqueryExec == 2, "expect 2 SubqueryExec when not reusing")
          assert(countReuseSubqueryExec == 0,
            "expect 0 ReusedSubqueryExec when not reusing")
        }
      }
    }
  }

  test("Scalar subquery name should start with scalar-subquery#") {
    val df = sql("SELECT a FROM l WHERE a = (SELECT max(c) FROM r WHERE c = 1)".stripMargin)
    val subqueryExecs: ArrayBuffer[SubqueryExec] = ArrayBuffer.empty
    df.queryExecution.executedPlan.transformAllExpressions {
      case s @ ScalarSubquery(p: SubqueryExec, _) =>
        subqueryExecs += p
        s
    }
    assert(subqueryExecs.forall(_.name.startsWith("scalar-subquery#")),
          "SubqueryExec name should start with scalar-subquery#")
  }

  test("SPARK-28441: COUNT bug in WHERE clause (Filter) with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    // Case 1: Canonical example of the COUNT bug
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT udf(count(*)) FROM r WHERE l.a = r.c) < l.a"),
      Row(1) :: Row(1) :: Row(3) :: Row(6) :: Nil)
    // Case 2: count(*) = 0; could be rewritten to NOT EXISTS but currently uses
    // a rewrite that is vulnerable to the COUNT bug
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT udf(count(*)) FROM r WHERE l.a = r.c) = 0"),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
    // Case 3: COUNT bug without a COUNT aggregate
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT udf(sum(r.d)) is null FROM r WHERE l.a = r.c)"),
      Row(1) :: Row(1) ::Row(null) :: Row(null) :: Row(6) :: Nil)
  }

  test("SPARK-28441: COUNT bug in SELECT clause (Project) with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    checkAnswer(
      sql("SELECT a, (SELECT udf(count(*)) FROM r WHERE l.a = r.c) AS cnt FROM l"),
      Row(1, 0) :: Row(1, 0) :: Row(2, 2) :: Row(2, 2) :: Row(3, 1) :: Row(null, 0)
        :: Row(null, 0) :: Row(6, 1) :: Nil)
  }

  test("SPARK-28441: COUNT bug in HAVING clause (Filter) with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    checkAnswer(
      sql("""
            |SELECT
            |  l.a AS grp_a
            |FROM l GROUP BY l.a
            |HAVING
            |  (
            |    SELECT udf(count(*)) FROM r WHERE grp_a = r.c
            |  ) = 0
            |ORDER BY grp_a""".stripMargin),
      Row(null) :: Row(1) :: Nil)
  }

  test("SPARK-28441: COUNT bug in Aggregate with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    checkAnswer(
      sql("""
            |SELECT
            |  l.a AS aval,
            |  sum(
            |    (
            |      SELECT udf(count(*)) FROM r WHERE l.a = r.c
            |    )
            |  ) AS cnt
            |FROM l GROUP BY l.a ORDER BY aval""".stripMargin),
      Row(null, 0) :: Row(1, 0) :: Row(2, 4) :: Row(3, 1) :: Row(6, 1)  :: Nil)
  }

  test("SPARK-28441: COUNT bug negative examples with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    // Case 1: Potential COUNT bug case that was working correctly prior to the fix
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT udf(sum(r.d)) FROM r WHERE l.a = r.c) is null"),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Row(6) :: Nil)
    // Case 2: COUNT aggregate but no COUNT bug due to > 0 test.
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT udf(count(*)) FROM r WHERE l.a = r.c) > 0"),
      Row(2) :: Row(2) :: Row(3) :: Row(6) :: Nil)
    // Case 3: COUNT inside aggregate expression but no COUNT bug.
    checkAnswer(
      sql("""
            |SELECT
            |  l.a
            |FROM l
            |WHERE
            |  (
            |    SELECT udf(count(*)) + udf(sum(r.d))
            |    FROM r WHERE l.a = r.c
            |  ) = 0""".stripMargin),
      Nil)
  }

  test("SPARK-28441: COUNT bug in nested subquery with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    checkAnswer(
      sql("""
            |SELECT l.a FROM l
            |WHERE (
            |    SELECT cntPlusOne + 1 AS cntPlusTwo FROM (
            |        SELECT cnt + 1 AS cntPlusOne FROM (
            |            SELECT udf(sum(r.c)) s, udf(count(*)) cnt FROM r WHERE l.a = r.c
            |                   HAVING cnt = 0
            |        )
            |    )
            |) = 2""".stripMargin),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
  }

  test("SPARK-28441: COUNT bug with nasty predicate expr with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    checkAnswer(
      sql("""
            |SELECT
            |  l.a
            |FROM l WHERE
            |  (
            |    SELECT CASE WHEN udf(count(*)) = 1 THEN null ELSE udf(count(*)) END AS cnt
            |    FROM r WHERE l.a = r.c
            |  ) = 0""".stripMargin),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
  }

  test("SPARK-28441: COUNT bug with attribute ref in subquery input and output with PythonUDF") {
    import IntegratedUDFTestUtils._

    assume(shouldTestPythonUDFs)

    val pythonTestUDF = TestPythonUDF(name = "udf")
    registerTestUDF(pythonTestUDF, spark)

    checkAnswer(
      sql(
        """
          |SELECT
          |  l.b,
          |  (
          |    SELECT (r.c + udf(count(*))) is null
          |    FROM r
          |    WHERE l.a = r.c GROUP BY r.c
          |  )
          |FROM l
        """.stripMargin),
      Row(1.0, false) :: Row(1.0, false) :: Row(2.0, true) :: Row(2.0, true) ::
        Row(3.0, false) :: Row(5.0, true) :: Row(null, false) :: Row(null, true) :: Nil)
  }

  test("SPARK-28441: COUNT bug with non-foldable expression") {
    // Case 1: Canonical example of the COUNT bug
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT count(*) + cast(rand() as int) FROM r " +
        "WHERE l.a = r.c) < l.a"),
      Row(1) :: Row(1) :: Row(3) :: Row(6) :: Nil)
    // Case 2: count(*) = 0; could be rewritten to NOT EXISTS but currently uses
    // a rewrite that is vulnerable to the COUNT bug
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT count(*) + cast(rand() as int) FROM r " +
        "WHERE l.a = r.c) = 0"),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
    // Case 3: COUNT bug without a COUNT aggregate
    checkAnswer(
      sql("SELECT l.a FROM l WHERE (SELECT sum(r.d) is null from r " +
        "WHERE l.a = r.c)"),
      Row(1) :: Row(1) ::Row(null) :: Row(null) :: Row(6) :: Nil)
  }

  test("SPARK-28441: COUNT bug in nested subquery with non-foldable expr") {
    checkAnswer(
      sql("""
            |SELECT l.a FROM l
            |WHERE (
            |  SELECT cntPlusOne + 1 AS cntPlusTwo FROM (
            |    SELECT cnt + 1 AS cntPlusOne FROM (
            |      SELECT sum(r.c) s, (count(*) + cast(rand() as int)) cnt FROM r
            |        WHERE l.a = r.c HAVING cnt = 0
            |      )
            |  )
            |) = 2""".stripMargin),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
  }

  test("SPARK-28441: COUNT bug with non-foldable expression in Filter condition") {
    val df = sql("""
                   |SELECT
                   |  l.a
                   |FROM l WHERE
                   |  (
                   |    SELECT cntPlusOne + 1 as cntPlusTwo FROM
                   |    (
                   |      SELECT cnt + 1 as cntPlusOne FROM
                   |      (
                   |        SELECT sum(r.c) s, count(*) cnt FROM r WHERE l.a = r.c HAVING cnt > 0
                   |      )
                   |    )
                   |  ) = 2""".stripMargin)
    val df2 = sql("""
                    |SELECT
                    |  l.a
                    |FROM l WHERE
                    |  (
                    |    SELECT cntPlusOne + 1 AS cntPlusTwo
                    |    FROM
                    |      (
                    |        SELECT cnt + 1 AS cntPlusOne
                    |        FROM
                    |          (
                    |            SELECT sum(r.c) s, count(*) cnt FROM r
                    |            WHERE l.a = r.c HAVING (cnt + cast(rand() as int)) > 0
                    |          )
                    |       )
                    |   ) = 2""".stripMargin)
    checkAnswer(df, df2)
    checkAnswer(df, Nil)
  }

  test("SPARK-32290: SingleColumn Null Aware Anti Join Optimize") {
    Seq(true, false).foreach { enableNAAJ =>
      Seq(true, false).foreach { enableAQE =>
        Seq(true, false).foreach { enableCodegen =>
          withSQLConf(
            SQLConf.OPTIMIZE_NULL_AWARE_ANTI_JOIN.key -> enableNAAJ.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> enableCodegen.toString) {

            def findJoinExec(df: DataFrame): BaseJoinExec = {
              df.queryExecution.sparkPlan.collectFirst {
                case j: BaseJoinExec => j
              }.get
            }

            var df: DataFrame = null
            var joinExec: BaseJoinExec = null

            // single column not in subquery -- empty sub-query
            df = sql("select * from l where a not in (select c from r where c > 10)")
            checkAnswer(df, spark.table("l"))
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- sub-query include null
            df = sql("select * from l where a not in (select c from r where d < 6.0)")
            checkAnswer(df, Seq.empty)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- streamedSide row is null
            df =
              sql("select * from l where b = 5.0 and a not in(select c from r where c is not null)")
            checkAnswer(df, Seq.empty)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- streamedSide row is not null, match found
            df =
              sql("select * from l where a = 6 and a not in (select c from r where c is not null)")
            checkAnswer(df, Seq.empty)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- streamedSide row is not null, match not found
            df =
              sql("select * from l where a = 1 and a not in (select c from r where c is not null)")
            checkAnswer(df, Row(1, 2.0) :: Row(1, 2.0) :: Nil)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- d = b + 10 joinKey found, match ExtractEquiJoinKeys
            df = sql("select * from l where a not in (select c from r where d = b + 10)")
            checkAnswer(df, spark.table("l"))
            joinExec = findJoinExec(df)
            assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
            assert(!joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)

            // single column not in subquery -- d = b + 10 and b = 5.0 => d = 15, joinKey not found
            // match ExtractSingleColumnNullAwareAntiJoin
            df =
              sql("select * from l where b = 5.0 and a not in (select c from r where d = b + 10)")
            checkAnswer(df, Row(null, 5.0) :: Nil)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // multi column not in subquery
            df = sql("select * from l where (a, b) not in (select c, d from r where c > 10)")
            checkAnswer(df, spark.table("l"))
            assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
          }
        }
      }
    }
  }

  test("SPARK-28379: non-aggregated zero row scalar subquery") {
    checkAnswer(
      sql("select a, (select id from range(0) where id = a) from l where a = 3"),
      Row(3, null))
    checkAnswer(
      sql("select a, (select c from (select * from r limit 0) where c = a) from l where a = 3"),
      Row(3, null))
  }

  test("SPARK-28379: non-aggregated single row correlated scalar subquery") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      // inline table
      checkAnswer(
        sql("select c1, c2, (select col1 from values (0, 1) where col2 = c2) from t"),
        Row(0, 1, 0) :: Row(1, 2, null) :: Nil)
      // one row relation
      checkAnswer(
        sql("select c1, c2, (select a from (select 1 as a) where a = c2) from t"),
        Row(0, 1, 1) :: Row(1, 2, null) :: Nil)
      // limit 1 with order by
      checkAnswer(
        sql(
          """
            |select c1, c2, (
            |  select b from (select * from l order by a asc nulls last limit 1) where a = c2
            |) from t
            |""".stripMargin),
        Row(0, 1, 2.0) :: Row(1, 2, null) :: Nil)
      // limit 1 with window
      checkAnswer(
        sql(
          """
            |select c1, c2, (
            |  select w from (
            |    select a, sum(b) over (partition by a) w from l order by a asc nulls last limit 1
            |  ) where a = c1 + c2
            |) from t
            |""".stripMargin),
        Row(0, 1, 4.0) :: Row(1, 2, null) :: Nil)
      // set operations
      checkAnswer(
        sql(
          """
            |select c1, c2, (
            |  select a from ((select 1 as a) intersect (select 1 as a)) where a = c2
            |) from t
            |""".stripMargin),
        Row(0, 1, 1) :: Row(1, 2, null) :: Nil)
      // join
      checkAnswer(
        sql(
          """
            |select c1, c2, (
            |  select a from (select * from (select 1 as a) join (select 1 as b) on a = b)
            |  where a = c2
            |) from t
            |""".stripMargin),
        Row(0, 1, 1) :: Row(1, 2, null) :: Nil)
    }
  }

  test("SPARK-35080: correlated equality predicates contain only outer references") {
    withTempView("t") {
      Seq((0, 1), (1, 1)).toDF("c1", "c2").createOrReplaceTempView("t")
      checkAnswer(
        sql("select c1, c2, (select count(*) from l where c1 = c2) from t"),
        Row(0, 1, 0) :: Row(1, 1, 8) :: Nil)
    }
  }

  test("Subquery reuse across the whole plan") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY.key -> "false") {
      val df = sql(
        """
          |SELECT (SELECT avg(key) FROM testData), (SELECT (SELECT avg(key) FROM testData))
          |FROM testData
          |LIMIT 1
      """.stripMargin)

      // scalastyle:off
      // CollectLimit 1
      // +- *(1) Project [Subquery scalar-subquery#240, [id=#112] AS scalarsubquery()#248, Subquery scalar-subquery#242, [id=#183] AS scalarsubquery()#249]
      //    :  :- Subquery scalar-subquery#240, [id=#112]
      //    :  :  +- *(2) HashAggregate(keys=[], functions=[avg(cast(key#13 as bigint))])
      //    :  :     +- Exchange SinglePartition, true, [id=#108]
      //    :  :        +- *(1) HashAggregate(keys=[], functions=[partial_avg(cast(key#13 as bigint))])
      //    :  :           +- *(1) SerializeFromObject [knownnotnull(assertnotnull(input[0, org.apache.spark.sql.test.SQLTestData$TestData, true])).key AS key#13]
      //    :  :              +- Scan[obj#12]
      //    :  +- Subquery scalar-subquery#242, [id=#183]
      //    :     +- *(1) Project [ReusedSubquery Subquery scalar-subquery#240, [id=#112] AS scalarsubquery()#247]
      //    :        :  +- ReusedSubquery Subquery scalar-subquery#240, [id=#112]
      //    :        +- *(1) Scan OneRowRelation[]
      //    +- *(1) SerializeFromObject
      //      +- Scan[obj#12]
      // scalastyle:on

      val plan = df.queryExecution.executedPlan

      val subqueryIds = plan.collectWithSubqueries { case s: SubqueryExec => s.id }
      val reusedSubqueryIds = plan.collectWithSubqueries {
        case rs: ReusedSubqueryExec => rs.child.id
      }

      assert(subqueryIds.size == 2, "Whole plan subquery reusing not working correctly")
      assert(reusedSubqueryIds.size == 1, "Whole plan subquery reusing not working correctly")
      assert(reusedSubqueryIds.forall(subqueryIds.contains(_)),
        "ReusedSubqueryExec should reuse an existing subquery")
    }
  }

  test("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 USING parquet AS SELECT id AS a, id AS b, id AS c FROM range(10)")
      sql("CREATE TABLE t2 USING parquet AS SELECT id AS x, id AS y FROM range(8)")
      val df = sql(
        """
          |SELECT *
          |FROM   t1
          |WHERE  a IN (SELECT x
          |             FROM   (SELECT x AS x,
          |                            RANK() OVER (PARTITION BY x ORDER BY SUM(y) DESC) AS ranking
          |                     FROM   t2
          |                     GROUP  BY x) tmp1
          |             WHERE  ranking <= 5)
          |""".stripMargin)

      df.collect()
      val exchanges = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      assert(exchanges.size === 1)
    }
  }

  test("SPARK-36747: should not combine Project with Aggregate") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      checkAnswer(
        sql("""
              |SELECT m, (SELECT SUM(c2) FROM t WHERE c1 = m)
              |FROM (SELECT MIN(c2) AS m FROM t)
              |""".stripMargin),
        Row(1, 2) :: Nil)
      checkAnswer(
        sql("""
              |SELECT c, (SELECT SUM(c2) FROM t WHERE c1 = c)
              |FROM (SELECT c1 AS c FROM t GROUP BY c1)
              |""".stripMargin),
        Row(0, 1) :: Row(1, 2) :: Nil)
    }
  }

  test("SPARK-36656: Do not collapse projects with correlate scalar subqueries") {
    withTempView("t1", "t2") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((0, 2), (0, 3)).toDF("c1", "c2").createOrReplaceTempView("t2")
      val correctAnswer = Row(0, 2, 20) :: Row(1, null, null) :: Nil
      checkAnswer(
        sql(
          """
            |SELECT c1, s, s * 10 FROM (
            |  SELECT c1, (SELECT FIRST(c2) FROM t2 WHERE t1.c1 = t2.c1) s FROM t1)
            |""".stripMargin),
        correctAnswer)
      checkAnswer(
        sql(
          """
            |SELECT c1, s, s * 10 FROM (
            |  SELECT c1, SUM((SELECT FIRST(c2) FROM t2 WHERE t1.c1 = t2.c1)) s
            |  FROM t1 GROUP BY c1
            |)
            |""".stripMargin),
        correctAnswer)
    }
  }

  test("SPARK-37199: deterministic in QueryPlan considers subquery") {
    val deterministicQueryPlan = sql("select (select 1 as b) as b")
      .queryExecution.executedPlan
    assert(deterministicQueryPlan.deterministic)

    val nonDeterministicQueryPlan = sql("select (select rand(1) as b) as b")
      .queryExecution.executedPlan
    assert(!nonDeterministicQueryPlan.deterministic)
  }

  test("SPARK-38132: Not IN subquery correctness checks") {
    val t = "test_table"
    withTable(t) {
      Seq[(Integer, Integer)](
        (1, 1),
        (2, 2),
        (3, 3),
        (4, null),
        (null, 0))
        .toDF("c1", "c2").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) = true"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) = true"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) <=> true"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) <=> true"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) != false"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) != false"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"NOT((c1 NOT IN (SELECT c2 FROM $t)) <=> false)"), Seq.empty)
      checkAnswer(df.where(s"NOT((c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) <=> false)"),
        Row(4, null) :: Nil)
    }
  }

  test("SPARK-38155: disallow distinct aggregate in lateral subqueries") {
    withTempView("t1", "t2") {
      Seq((0, 1)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 2), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")
      assert(intercept[AnalysisException] {
        sql("SELECT * FROM t1 JOIN LATERAL (SELECT DISTINCT c2 FROM t2 WHERE c1 > t1.c1)")
      }.getMessage.contains("Correlated column is not allowed in predicate"))
    }
  }

  test("SPARK-38180: allow safe cast expressions in correlated equality conditions") {
    withTempView("t1", "t2") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((0, 2), (0, 3)).toDF("c1", "c2").createOrReplaceTempView("t2")
      checkAnswer(sql(
        """
          |SELECT (SELECT SUM(c2) FROM t2 WHERE c1 = a)
          |FROM (SELECT CAST(c1 AS DOUBLE) a FROM t1)
          |""".stripMargin),
        Row(5) :: Row(null) :: Nil)
      checkAnswer(sql(
        """
          |SELECT (SELECT SUM(c2) FROM t2 WHERE CAST(c1 AS STRING) = a)
          |FROM (SELECT CAST(c1 AS STRING) a FROM t1)
          |""".stripMargin),
        Row(5) :: Row(null) :: Nil)
      assert(intercept[AnalysisException] {
        sql(
          """
            |SELECT (SELECT SUM(c2) FROM t2 WHERE CAST(c1 AS SHORT) = a)
            |FROM (SELECT CAST(c1 AS SHORT) a FROM t1)
            |""".stripMargin)
      }.getMessage.contains("Correlated column is not allowed in predicate"))
    }
  }

  test("Merge non-correlated scalar subqueries") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) FROM testData),
            |  (SELECT sum(key) FROM testData),
            |  (SELECT count(distinct key) FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries in a subquery") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT (
            |  SELECT
            |    SUM(
            |      (SELECT avg(key) FROM testData) +
            |      (SELECT sum(key) FROM testData) +
            |      (SELECT count(distinct key) FROM testData))
            |   FROM testData
            |)
          """.stripMargin)

        checkAnswer(df, Row(520050.0) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        if (enableAQE) {
          assert(subqueryIds.size == 3, "Missing or unexpected SubqueryExec in the plan")
          assert(reusedSubqueryIds.size == 4,
            "Missing or unexpected reused ReusedSubqueryExec in the plan")
        } else {
          assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
          assert(reusedSubqueryIds.size == 5,
            "Missing or unexpected reused ReusedSubqueryExec in the plan")
        }
      }
    }
  }

  test("Merge non-correlated scalar subqueries from different levels") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) FROM testData),
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT sum(key) FROM testData)
            |      )
            |    FROM testData
            |  )
          """.stripMargin)

        checkAnswer(df, Row(50.5, 505000) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries from different parent plans") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT avg(key) FROM testData)
            |      )
            |    FROM testData
            |  ),
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT sum(key) FROM testData)
            |      )
            |    FROM testData
            |  )
          """.stripMargin)

        checkAnswer(df, Row(5050.0, 505000) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        if (enableAQE) {
          assert(subqueryIds.size == 3, "Missing or unexpected SubqueryExec in the plan")
          assert(reusedSubqueryIds.size == 3,
            "Missing or unexpected reused ReusedSubqueryExec in the plan")
        } else {
          assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
          assert(reusedSubqueryIds.size == 4,
            "Missing or unexpected reused ReusedSubqueryExec in the plan")
        }
      }
    }
  }

  test("Merge non-correlated scalar subqueries with conflicting names") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) AS key FROM testData),
            |  (SELECT sum(key) AS key FROM testData),
            |  (SELECT count(distinct key) AS key FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("SPARK-39355: Single column uses quoted to construct UnresolvedAttribute") {
    checkAnswer(
      sql("""
            |SELECT *
            |FROM (
            |    SELECT '2022-06-01' AS c1
            |) a
            |WHERE c1 IN (
            |     SELECT date_add('2022-06-01', 0)
            |)
            |""".stripMargin),
      Row("2022-06-01"))
    checkAnswer(
      sql("""
            |SELECT *
            |FROM (
            |    SELECT '2022-06-01' AS c1
            |) a
            |WHERE c1 IN (
            |    SELECT date_add(a.c1.k1, 0)
            |    FROM (
            |        SELECT named_struct('k1', '2022-06-01') AS c1
            |    ) a
            |)
            |""".stripMargin),
      Row("2022-06-01"))
  }

  test("SPARK-39511: Push limit 1 to right side if join type is Left Semi/Anti") {
    withTable("t1", "t2") {
      withTempView("v1") {
        spark.sql("CREATE TABLE t1(id int) using parquet")
        spark.sql("CREATE TABLE t2(id int, type string) using parquet")
        spark.sql("CREATE TEMP VIEW v1 AS SELECT id, 't' AS type FROM t1")
        val df = spark.sql("SELECT * FROM v1 WHERE type IN (SELECT type FROM t2)")
        val join =
          df.queryExecution.sparkPlan.collectFirst { case b: BroadcastNestedLoopJoinExec => b }
        assert(join.nonEmpty)
        assert(join.head.right.isInstanceOf[LocalLimitExec])
        assert(join.head.right.asInstanceOf[LocalLimitExec].limit === 1)
      }
    }
  }

  test("SPARK-39672: Fix removing project before filter with correlated subquery") {
    withTempView("v1", "v2") {
      Seq((1, 2, 3), (4, 5, 6)).toDF("a", "b", "c").createTempView("v1")
      Seq((1, 3, 5), (4, 5, 6)).toDF("a", "b", "c").createTempView("v2")

      def findProject(df: DataFrame): Seq[Project] = {
        df.queryExecution.optimizedPlan.collect {
          case p: Project => p
        }
      }

      // project before filter cannot be removed since subquery has conflicting attributes
      // with outer reference
      val df1 = sql(
        """
         |select * from
         |(
         |select
         |v1.a,
         |v1.b,
         |v2.c
         |from v1
         |inner join v2
         |on v1.a=v2.a) t3
         |where not exists (
         |  select 1
         |  from v2
         |  where t3.a=v2.a and t3.b=v2.b and t3.c=v2.c
         |)
         |""".stripMargin)
      checkAnswer(df1, Row(1, 2, 5))
      assert(findProject(df1).size == 4)

      // project before filter can be removed when there are no conflicting attributes
      val df2 = sql(
        """
         |select * from
         |(
         |select
         |v1.b,
         |v2.c
         |from v1
         |inner join v2
         |on v1.b=v2.c) t3
         |where not exists (
         |  select 1
         |  from v2
         |  where t3.b=v2.b and t3.c=v2.c
         |)
         |""".stripMargin)

      checkAnswer(df2, Row(5, 5))
      assert(findProject(df2).size == 3)
    }
  }
}

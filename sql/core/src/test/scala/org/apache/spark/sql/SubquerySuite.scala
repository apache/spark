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

import org.apache.spark.{SparkArithmeticException, SparkConf, SparkRuntimeException}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, EqualTo, ExprId,
  GreaterThanOrEqual, Literal, NamedExpression, OuterReference, Rand, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Sort, Union}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecution}
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BroadcastHashJoinExec, BroadcastNestedLoopJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class SubquerySuite extends SharedSparkSession
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

  private def checkNumJoins(plan: LogicalPlan, numJoins: Int): Unit = {
    val joins = plan.collect { case j: Join => j }
    assert(joins.size == numJoins)
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

  test("IN predicate subquery preserves its broadcast when replacing its plan") {
    val subqueryPlan = SubqueryExec("subquery", spark.range(3).queryExecution.executedPlan)
    val subquery = InSubqueryExec(
      Literal(1L), subqueryPlan, NamedExpression.newExprId, isDynamicPruning = false)
    subquery.updateResult()

    val updatedSubquery = subquery.withNewPlan(subqueryPlan)
    assert(updatedSubquery.values().isEmpty)
    assert(updatedSubquery.eval(InternalRow.empty) == true)
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
    withTempView("v") {
      Seq((1, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("v")
      val exception = intercept[SparkRuntimeException] {
        sql("select (select sum(-1) from v t2 where t1.c2 = t2.c1 group by t2.c2) sum from v t1").
          collect()
      }
      checkError(
        exception,
        condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS"
      )
    }
  }

  test("non-aggregated correlated scalar subquery") {
    val exception1 = intercept[SparkRuntimeException] {
      sql("select a, (select b from l l2 where l2.a = l1.a) sum_b from l l1").collect()
    }
    checkError(
      exception1,
      condition = "SCALAR_SUBQUERY_TOO_MANY_ROWS"
    )
    checkAnswer(
      sql("select a, (select b from l l2 where l2.a = l1.a group by 1) sum_b from l l1"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) ::
        Row(null, null) :: Row(null, null) :: Row(6, null) :: Nil
    )
  }

  test("non-equal correlated scalar subquery") {
    checkAnswer(
      sql("select a, (select sum(b) from l l2 where l2.a < l1.a) sum_b from l l1"),
      Seq(Row(1, null), Row(1, null), Row(2, 4), Row(2, 4), Row(3, 6), Row(null, null),
        Row(null, null), Row(6, 9)))
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
          |select l.b, (select (min(r.c) + count(*)) is null
          |from r
          |where l.a = r.c) from l
        """.stripMargin),
      Row(1.0, false) :: Row(1.0, false) :: Row(2.0, true) :: Row(2.0, true) ::
        Row(3.0, false) :: Row(5.0, true) :: Row(null, false) :: Row(null, true) :: Nil)
  }

  test("SPARK-43098: no COUNT bug with group-by") {
    checkAnswer(
      sql(
        """
          |select l.b, (select (r.c + count(*)) is null
          |from r
          |where l.a = r.c group by r.c) from l
        """.stripMargin),
      Row(1.0, false) :: Row(1.0, false) :: Row(2.0, null) :: Row(2.0, null) ::
        Row(3.0, false) :: Row(5.0, null) :: Row(null, false) :: Row(null, null) :: Nil)
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

      checkAnswer(
        sql(
          """
            | select t1.c1
            | from   t1
            | where  t1.c1 in (select max(t2.c1)
            |                  from   t2
            |                  where  t1.c2 >= t2.c2)""".stripMargin),
        Nil)

      // Same but with a COUNT aggregate
      checkAnswer(
        sql(
          """
            | select t1.c1
            | from   t1
            | where  t1.c1 in (select count(t2.c1)
            |                  from   t2
            |                  where  t1.c2 <= t2.c2)""".stripMargin),
        Row(1) :: Nil)


      // Add a HAVING on top and augmented within an OR predicate
      checkAnswer(
        sql(
          """
            | select t1.c1
            | from   t1
            | where  t1.c1 in (select max(t2.c1)
            |                  from   t2
            |                  where  t1.c2 >= t2.c2
            |                  having count(*) > 0 )
            |         or t1.c2 >= 0""".stripMargin),
        Row(1) :: Nil)

      checkAnswer(
        sql(
          """
            | select t1.c1
            | from   t1, t1 as t3
            | where  t1.c1 = t3.c1
            | and    (t1.c1 in (select max(t2.c1)
            |                   from   t2
            |                   where  t1.c2 = t2.c2
            |                          or t3.c2 = t2.c2)
            |        )""".stripMargin),
        Row(1) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1
            | from   t3
            | where  c1 in (select max(t4.c1) over ()
            |               from   t4
            |               where t3.c2 <= t4.c2)""".stripMargin),
        Row(2) :: Nil)
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
      val exception1 = intercept[AnalysisException] {
        sql(
          """
            | select t1.c1
            | from   t1
            | where  1 IN (select 1
            |              from   t3 left outer join
            |                     (select c1 from t2 where t1.c1 = 2) t2
            |                     on t2.c1 = t3.c1)""".stripMargin).collect()
      }
      checkErrorMatchPVals(
        exception1,
        condition = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
          "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED",
        parameters = Map("treeNode" -> "(?s).*"),
        sqlState = None,
        context = ExpectedContext(
          fragment = "select c1 from t2 where t1.c1 = 2",
          start = 111,
          stop = 143))

      // Right outer join (ROJ) in EXISTS subquery context
      val exception2 = intercept[AnalysisException] {
        sql(
          """
            | select t1.c1
            | from   t1
            | where  exists (select 1
            |                from   (select c1 from t2 where t1.c1 = 2) t2
            |                       right outer join t3
            |                       on t2.c1 = t3.c1)""".stripMargin).collect()
      }
      checkErrorMatchPVals(
        exception2,
        condition = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
          "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED",
        parameters = Map("treeNode" -> "(?s).*"),
        sqlState = None,
        context = ExpectedContext(
          fragment = "select c1 from t2 where t1.c1 = 2",
          start = 75,
          stop = 107))

      // SPARK-18578: Full outer join (FOJ) in scalar subquery context
      val exception3 = intercept[AnalysisException] {
        sql(
          """
            | select (select max(1)
            |         from   (select c1 from  t2 where t1.c1 = 2 and t1.c1=t2.c1) t2
            |                full join t3
            |                on t2.c1=t3.c1)
            | from   t1""".stripMargin).collect()
      }
      checkErrorMatchPVals(
        exception3,
        condition = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
          "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED",
        parameters = Map("treeNode" -> "(?s).*"),
        sqlState = None,
        context = ExpectedContext(
          fragment = "select c1 from  t2 where t1.c1 = 2 and t1.c1=t2.c1",
          start = 41,
          stop = 90))
    }
  }

  test("SPARK-36124: Correlated subqueries with union") {
    withTempView("t0", "t1", "t2") {
      Seq((1, 1), (2, 0)).toDF("t0a", "t0b").createOrReplaceTempView("t0")
      Seq((1, 1, 3)).toDF("t1a", "t1b", "t1c").createOrReplaceTempView("t1")
      Seq((1, 1, 5), (2, 2, 7)).toDF("t2a", "t2b", "t2c").createOrReplaceTempView("t2")

      // Union with different outer refs
      val query =
        """
          | SELECT t0a, (SELECT sum(t1c) FROM
          |   (SELECT t1c
          |   FROM   t1
          |   WHERE  t1a = t0a
          |   UNION ALL
          |   SELECT t2c
          |   FROM   t2
          |   WHERE  t2b = t0b)
          | )
          | FROM t0""".stripMargin

      val df = sql(query)
      checkAnswer(df,
        Row(1, 8) :: Row(2, null) :: Nil)

      val optimizedPlan = df.queryExecution.optimizedPlan
      val aggregate = optimizedPlan.collectFirst { case a: Aggregate => a }.get
      assert(aggregate.groupingExpressions.size == 2)
      val union = optimizedPlan.collectFirst { case u: Union => u }.get
      assert(union.output.size == 3)
      assert(optimizedPlan.resolved)

      withSQLConf(SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false") {
        val error = intercept[AnalysisException] { sql(query) }
        assert(error.getCondition == "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
          "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED")
      }
      withSQLConf(SQLConf.DECORRELATE_SET_OPS_ENABLED.key -> "false") {
        val error = intercept[AnalysisException] { sql(query) }
        assert(error.getCondition == "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
          "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED")
      }

      {
        // Union with same outer refs
        val df = sql(
            """
              | SELECT t0a, (SELECT sum(t1c) FROM
              |   (SELECT t1c
              |   FROM   t1
              |   WHERE  t1a = t0a
              |   UNION ALL
              |   SELECT t2c
              |   FROM   t2
              |   WHERE  t2a = t0a)
              | )
              | FROM t0""".stripMargin)
        checkAnswer(df,
          Row(1, 8) :: Row(2, 7) :: Nil)

        val optimizedPlan = df.queryExecution.optimizedPlan
        val aggregate = optimizedPlan.collectFirst { case a: Aggregate => a }.get
        assert(aggregate.groupingExpressions.size == 1)
        val union = optimizedPlan.collectFirst { case u: Union => u }.get
        assert(union.output.size == 2)
        assert(optimizedPlan.resolved)
      }
    }
  }

  test("SPARK-36124: Correlated subqueries with set ops") {
    withTempView("t0", "t1", "t2") {
      Seq((1, 1), (2, 0)).toDF("t0a", "t0b").createOrReplaceTempView("t0")
      Seq((1, 1, 3)).toDF("t1a", "t1b", "t1c").createOrReplaceTempView("t1")
      Seq((1, 1, 5), (2, 2, 7)).toDF("t2a", "t2b", "t2c").createOrReplaceTempView("t2")

      // Union with different outer refs
      for (setopType <- Seq("INTERSECT", "EXCEPT")) {
        for (distinctness <- Seq("ALL", "DISTINCT")) {
          val query =
            s"""
              | SELECT t0a, (SELECT sum(t1c) FROM
              |   (SELECT t1c
              |   FROM   t1
              |   WHERE  t1a = t0a
              |   ${setopType} ${distinctness}
              |   SELECT t2c
              |   FROM   t2
              |   WHERE  t2b = t0b)
              | )
              | FROM t0""".stripMargin

          val df = sql(query)
          val optimizedPlan = df.queryExecution.optimizedPlan
          val aggregate = optimizedPlan.collectFirst { case a: Aggregate => a }.get
          assert(aggregate.groupingExpressions.size == 2)
          if (distinctness == "DISTINCT") {
            if (setopType == "INTERSECT") {
              val join = optimizedPlan.collectFirst {
                case j @ Join(_, _, LeftSemi, _, _) => j
              }.get
              assert(splitConjunctivePredicates(join.condition.get).size == 3)
            } else {
              val join = optimizedPlan.collectFirst {
                case j @ Join(_, _, LeftAnti, _, _) => j
              }.get
              assert(splitConjunctivePredicates(join.condition.get).size == 3)
            }
          }
          assert(optimizedPlan.resolved)

          withSQLConf(SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false") {
            val error = intercept[AnalysisException] { sql(query) }
            assert(error.getCondition == "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
              "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED")
          }
          withSQLConf(SQLConf.DECORRELATE_SET_OPS_ENABLED.key -> "false") {
            val error = intercept[AnalysisException] { sql(query) }
            assert(error.getCondition == "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
              "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED")
          }
        }
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

      val exception1 = intercept[AnalysisException] {
        sql(
          """
            | SELECT c1
            | FROM t2
            | WHERE EXISTS (SELECT *
            |               FROM t1 LATERAL VIEW explode(t2.arr_c2) q AS c2
            |               WHERE t1.c1 = t2.c1)
          """.stripMargin)
      }
      checkError(
        exception1,
        condition = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE",
        parameters = Map("sqlExprs" -> "\"explode(arr_c2)\""),
        context = ExpectedContext(
          fragment = "LATERAL VIEW explode(t2.arr_c2) q AS c2",
          start = 68,
          stop = 106)
      )
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
    withTempView("v") {
      Seq(1 -> "a").toDF("i", "j").createOrReplaceTempView("v")
      val query = "SELECT (SELECT count(*) FROM v WHERE a = 1)"
      checkError(
        exception =
          intercept[AnalysisException](sql(query)),
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = None,
        parameters = Map(
          "objectName" -> "`a`",
          "proposal" -> "`i`, `j`"),
        context = ExpectedContext(
          fragment = "a",
          start = 37,
          stop = 37))
    }
  }

  test("SPARK-41912: Subquery does not validate CTE") {
    val df = sql("""
                   |    WITH
                   |    cte1 as (SELECT 1 col1),
                   |    cte2 as (SELECT (SELECT MAX(col1) FROM cte1))
                   |    SELECT * FROM cte1
                   |""".stripMargin
    )
    checkAnswer(df, Row(1) :: Nil)
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
    spark.range(10).where("(id,id) in (select id, null from range(3))").count()
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

  test("SPARK-26893: Allow pushdown of partition pruning subquery filters to file source") {
    withTable("a", "b") {
      spark.range(4).selectExpr("id", "id % 2 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(2).write.saveAsTable("b")

      val df = sql("SELECT * FROM a WHERE p <= (SELECT MIN(id) FROM b)")
      checkAnswer(df, Seq(Row(0, 0), Row(2, 0)))
      // need to execute the query before we can examine fs.inputRDDs()
      assert(stripAQEPlan(df.queryExecution.executedPlan) match {
        case WholeStageCodegenExec(ColumnarToRowExec(InputAdapter(
            fs @ FileSourceScanExec(_, _, _, _, partitionFilters, _, _, _, _, _, _)))) =>
          partitionFilters.exists(ExecSubqueryExpression.hasSubquery) &&
            fs.inputRDDs().forall(
              _.asInstanceOf[FileScanRDD].filePartitions.forall(
                _.files.forall(_.urlEncodedPath.contains("p=0"))))
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
      Row(1.0, false) :: Row(1.0, false) :: Row(2.0, null) :: Row(2.0, null) ::
        Row(3.0, false) :: Row(5.0, null) :: Row(null, false) :: Row(null, null) :: Nil)
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
    withTempView("v") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("v")
      // inline table
      checkAnswer(
        sql("select c1, c2, (select col1 from values (0, 1) where col2 = c2) from v"),
        Row(0, 1, 0) :: Row(1, 2, null) :: Nil)
      // one row relation
      checkAnswer(
        sql("select c1, c2, (select a from (select 1 as a) where a = c2) from v"),
        Row(0, 1, 1) :: Row(1, 2, null) :: Nil)
      // limit 1 with order by
      checkAnswer(
        sql(
          """
            |select c1, c2, (
            |  select b from (select * from l order by a asc nulls last limit 1) where a = c2
            |) from v
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
            |) from v
            |""".stripMargin),
        Row(0, 1, 4.0) :: Row(1, 2, null) :: Nil)
      // set operations
      checkAnswer(
        sql(
          """
            |select c1, c2, (
            |  select a from ((select 1 as a) intersect (select 1 as a)) where a = c2
            |) from v
            |""".stripMargin),
        Row(0, 1, 1) :: Row(1, 2, null) :: Nil)
      // join
      checkAnswer(
        sql(
          """
            |select c1, c2, (
            |  select a from (select * from (select 1 as a) join (select 1 as b) on a = b)
            |  where a = c2
            |) from v
            |""".stripMargin),
        Row(0, 1, 1) :: Row(1, 2, null) :: Nil)
    }
  }

  test("SPARK-35080: correlated equality predicates contain only outer references") {
    withTempView("v") {
      Seq((0, 1), (1, 1)).toDF("c1", "c2").createOrReplaceTempView("v")
      checkAnswer(
        sql("select c1, c2, (select count(*) from l where c1 = c2) from v"),
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
    withTempView("v") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("v")
      checkAnswer(
        sql("""
              |SELECT m, (SELECT SUM(c2) FROM v WHERE c1 = m)
              |FROM (SELECT MIN(c2) AS m FROM v)
              |""".stripMargin),
        Row(1, 2) :: Nil)
      checkAnswer(
        sql("""
              |SELECT c, (SELECT SUM(c2) FROM v WHERE c1 = c)
              |FROM (SELECT c1 AS c FROM v GROUP BY c1)
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

  test("SPARK-49819: Do not collapse projects with exist subqueries") {
    withTempView("v") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("v")
      checkAnswer(
        sql("""
              |SELECT m, CASE WHEN EXISTS (SELECT SUM(c2) FROM v WHERE c1 = m) THEN 1 ELSE 0 END
              |FROM (SELECT MIN(c2) AS m FROM v)
              |""".stripMargin),
        Row(1, 1) :: Nil)
      checkAnswer(
        sql("""
              |SELECT c, CASE WHEN EXISTS (SELECT SUM(c2) FROM v WHERE c1 = c) THEN 1 ELSE 0 END
              |FROM (SELECT c1 AS c FROM v GROUP BY c1)
              |""".stripMargin),
        Row(0, 1) :: Row(1, 1) :: Nil)
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

  test("SPARK-36114: distinct aggregate in lateral subqueries") {
    withTempView("t1", "t2") {
      Seq((0, 1)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 2), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")
      checkAnswer(
        sql("SELECT * FROM t1 JOIN LATERAL (SELECT DISTINCT c2 FROM t2 WHERE c1 > t1.c1)"),
        Row(0, 1, 2) :: Nil)
    }
  }

  test("SPARK-38180, SPARK-36114: allow safe cast expressions in correlated equality conditions") {
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
      // SPARK-36114: we now allow non-safe cast expressions in correlated predicates.
      val df = sql(
        """SELECT (SELECT SUM(c2) FROM t2 WHERE CAST(c1 AS SHORT) = a)
          |FROM (SELECT CAST(c1 AS SHORT) a FROM t1)
          |""".stripMargin)
      checkAnswer(df, Row(5) :: Row(null) :: Nil)
      // The optimized plan should have one left outer join and one domain (inner) join.
      checkNumJoins(df.queryExecution.optimizedPlan, 2)
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

  test("SPARK-40615: Check unsupported data type when decorrelating subqueries") {
    withTempView("v1", "v2") {
      sql(
        """
          |create temp view v1(x) as values
          |from_json('{"a":1, "b":2}', 'map<string,int>') t(x)
          |""".stripMargin)

      // Can use non-orderable data type in one row subquery that can be collapsed.
      checkAnswer(
        sql("select (select a + a from (select x['a'] as a)) from v1"),
        Row(2))

      // Cannot use non-orderable data type in one row subquery that cannot be collapsed.
      // However, this case is handled by rule PullOutNestedDataOuterRefExpressions.
      // We test a non-deterministic function to prevent the expression from being collapsed, so
      // we can't checkAnswer.
      assert(sql(
        """select (
          |  select concat(a, a) from
          |  (select upper(x['a'] + rand()) as a)
          |) from v1
          |""".stripMargin).collect().length == 1)

      // With PullOutNestedDataOuterRefExpressions disabled, this query should fail.
      withSQLConf(SQLConf.PULL_OUT_NESTED_DATA_OUTER_REF_EXPRESSIONS_ENABLED.key -> "false") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(
              """select (
                |  select concat(a, a) from
                |  (select upper(x['a'] + rand()) as a)
                |) from v1
                |""".stripMargin
            ).collect()
          },
          condition = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "UNSUPPORTED_CORRELATED_REFERENCE_DATA_TYPE",
          parameters = Map("expr" -> "v1.x", "dataType" -> "map"),
          context = ExpectedContext(
            fragment = "(\n  select concat(a, a) from\n  (select upper(x['a'] + rand()) as a)\n)",
            start = 7,
            stop = 75)
        )
      }
    }
  }

  test("SPARK-40800: always inline expressions in OptimizeOneRowRelationSubquery") {
    withTempView("t1") {
      sql("CREATE TEMP VIEW t1 AS SELECT ARRAY('a', 'b') a")
      Seq(true, false).foreach { enabled =>
        withSQLConf(SQLConf.ALWAYS_INLINE_ONE_ROW_RELATION_SUBQUERY.key -> enabled.toString) {
          // Scalar subquery.
          checkAnswer(sql(
            """
              |SELECT (
              |  SELECT array_sort(a, (i, j) -> rank[i] - rank[j])[0] AS sorted
              |  FROM (SELECT MAP('a', 1, 'b', 2) rank)
              |) FROM t1
              |""".stripMargin),
            Row("a"))
          // Lateral subquery.
          checkAnswer(
            sql("""
                  |SELECT sorted[0] FROM t1
                  |JOIN LATERAL (
                  |  SELECT array_sort(a, (i, j) -> rank[i] - rank[j]) AS sorted
                  |  FROM (SELECT MAP('a', 1, 'b', 2) rank)
                  |)
                  |""".stripMargin),
            Row("a"))
        }
      }
    }
  }

  test("SPARK-40862: correlated one-row subquery with non-deterministic expressions") {
    import org.apache.spark.sql.functions.udf
    withTempView("t1") {
      sql("CREATE TEMP VIEW t1 AS SELECT ARRAY('a', 'b') a")
      val func = udf(() => "a")
      spark.udf.register("func", func.asNondeterministic())
      checkAnswer(sql(
        """
          |SELECT (
          |  SELECT array_sort(a, (i, j) -> rank[i] - rank[j])[0] || str AS sorted
          |  FROM (SELECT MAP('a', 1, 'b', 2) rank, func() AS str)
          |) FROM t1
          |""".stripMargin),
        Row("aa"))
    }
  }

  test("SPARK-42745: Improved AliasAwareOutputExpression works with DSv2") {
    withSQLConf(
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(0)
          .write
          .mode("overwrite")
          .parquet(path.getCanonicalPath)
        withTempView("t1") {
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")
          checkAnswer(sql("select (select sum(id) from t1)"), Row(null))
        }
      }
    }
  }

  test("SPARK-42937: Outer join with subquery in condition") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val expected = Row(1, 2.0d, null, null) :: Row(1, 2.0d, null, null) ::
        Row(3, 3.0d, 3, 2.0d) :: Row(null, 5.0d, null, null) :: Nil
      checkAnswer(sql(
        """
          |select *
          |from l
          |left outer join r
          |on a = c
          |and a in (select c from t where d in (1.0, 2.0))
          |where b > 1.0""".stripMargin),
        expected)
    }
  }

  test("SPARK-43402: FileSourceScanExec supports push down data filter with scalar subquery") {
    def checkFileSourceScan(query: String, answer: Seq[Row]): Unit = {
      val df = sql(query)
      checkAnswer(df, answer)
      val fileSourceScanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExec => f
      }
      sparkContext.listenerBus.waitUntilEmpty()
      assert(fileSourceScanExec.size === 1)
      val scalarSubquery = fileSourceScanExec.head.dataFilters.flatMap(_.collect {
        case s: ScalarSubquery => s
      })
      assert(scalarSubquery.length === 1)
      assert(scalarSubquery.head.plan.isInstanceOf[ReusedSubqueryExec])
      assert(fileSourceScanExec.head.metrics("numFiles").value === 1)
      assert(fileSourceScanExec.head.metrics("numOutputRows").value === answer.size)
    }

    withTable("t1", "t2") {
      withSQLConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1") {
        Seq(1, 2, 3).toDF("c1").write.format("parquet").saveAsTable("t1")
        Seq(4, 5, 6).toDF("c2").write.format("parquet").saveAsTable("t2")

        checkFileSourceScan(
          "SELECT * FROM t1 WHERE c1 > (SELECT min(c2) FROM t2)",
          Seq.empty)
        checkFileSourceScan(
          "SELECT * FROM t1 WHERE c1 < (SELECT min(c2) FROM t2)",
          Row(1) :: Row(2) :: Row(3) :: Nil)
      }
    }
  }

  test("SPARK-44562: Add OptimizeOneRowRelationSubquery in batch of Subquery") {
    withTempView("v1", "v2") {
      sql(
        """
          |CREATE temporary VIEW v1
          |AS
          |SELECT id, 'foo' AS kind FROM (SELECT 1 AS id) t
          |""".stripMargin)
      sql(
        """
          |CREATE temporary VIEW v2
          |AS
          |SELECT * FROM v1 WHERE kind = (SELECT kind FROM v1 WHERE kind = 'foo')
          |""".stripMargin)
      val df = sql("SELECT * FROM v1 JOIN v2 ON v1.id = v2.id")
      val filter = df.queryExecution.optimizedPlan.collectFirst {
        case f: Filter => f
      }
      assert(filter.isEmpty,
        "Filter should be removed after OptimizeSubqueries and OptimizeOneRowRelationSubquery")
      checkAnswer(df, Row(1, "foo", 1, "foo"))
    }
  }

  test("SPARK-45584: subquery execution should not fail with ORDER BY and LIMIT") {
    withTable("t1") {
      sql(
        """
          |CREATE TABLE t1 USING PARQUET
          |AS SELECT * FROM VALUES
          |(1, "a"),
          |(2, "a"),
          |(3, "a") t(id, value)
          |""".stripMargin)
      val df = sql(
        """
          |WITH t2 AS (
          |  SELECT * FROM t1 ORDER BY id
          |)
          |SELECT *, (SELECT COUNT(*) FROM t2) FROM t2 LIMIT 10
          |""".stripMargin)
      // This should not fail with IllegalArgumentException.
      checkAnswer(
        df,
        Row(1, "a", 3) :: Row(2, "a", 3) :: Row(3, "a", 3) :: Nil)
    }
  }

  test("SPARK-45580: Handle case where a nested subquery becomes an existence join") {
    withTempView("t1", "t2", "t3") {
      Seq((1), (2), (3), (7)).toDF("a").persist().createOrReplaceTempView("t1")
      Seq((1), (2), (3)).toDF("c1").persist().createOrReplaceTempView("t2")
      Seq((3), (9)).toDF("col1").persist().createOrReplaceTempView("t3")

      val query1 =
        """
          |SELECT *
          |FROM t1
          |WHERE EXISTS (
          |  SELECT c1
          |  FROM t2
          |  WHERE a = c1
          |  OR a IN (SELECT col1 FROM t3)
          |)""".stripMargin
      val df1 = sql(query1)
      checkAnswer(df1, Row(1) :: Row(2) :: Row(3) :: Nil)

      val query2 =
        """
          |SELECT *
          |FROM t1
          |WHERE a IN (
          |  SELECT c1
          |  FROM t2
          |  where a IN (SELECT col1 FROM t3)
          |)""".stripMargin
      val df2 = sql(query2)
      checkAnswer(df2, Row(3))

      val query3 =
        """
          |SELECT *
          |FROM t1
          |WHERE NOT EXISTS (
          |  SELECT c1
          |  FROM t2
          |  WHERE a = c1
          |  OR a IN (SELECT col1 FROM t3)
          |)""".stripMargin
      val df3 = sql(query3)
      checkAnswer(df3, Row(7))
    }
  }

  test("SPARK-50091: Handle aggregates in left-hand operand of IN-subquery") {
    withView("v1", "v2") {
      Seq((1, 2, 2), (1, 5, 3), (2, 0, 4), (3, 7, 7), (3, 8, 8))
        .toDF("c1", "c2", "c3")
        .createOrReplaceTempView("v1")
      Seq((1, 2, 2), (1, 3, 3), (2, 2, 4), (3, 7, 7), (3, 1, 1))
        .toDF("col1", "col2", "col3")
        .createOrReplaceTempView("v2")

      val df1 = sql("SELECT col1, SUM(col2) IN (SELECT c3 FROM v1) FROM v2 GROUP BY col1")
      checkAnswer(df1,
        Row(1, false) :: Row(2, true) :: Row(3, true) :: Nil)

      val df2 = sql("""SELECT
                      |  col1,
                      |  SUM(col2) IN (SELECT c3 FROM v1) and SUM(col3) IN (SELECT c2 FROM v1) AS x
                      |FROM v2 GROUP BY col1
                      |ORDER BY col1""".stripMargin)
      checkAnswer(df2,
        Row(1, false) :: Row(2, false) :: Row(3, true) :: Nil)

      val df3 = sql("""SELECT col1, (SUM(col2), SUM(col3)) IN (SELECT c3, c2 FROM v1) AS x
                      |FROM v2
                      |GROUP BY col1
                      |ORDER BY col1""".stripMargin)
      checkAnswer(df3,
        Row(1, false) :: Row(2, false) :: Row(3, true) :: Nil)
    }
  }

  test("SPARK-51738: IN subquery with struct type") {
    checkAnswer(
      sql("SELECT foo IN (SELECT struct(1 a)) FROM (SELECT struct(1 b) foo)"),
      Row(true)
    )

    checkAnswer(
      sql("""
            |SELECT foo IN (SELECT struct(c, d) FROM r)
            |FROM (SELECT struct(a, b) foo FROM l)
            |""".stripMargin),
      Row(false) :: Row(false) :: Row(false) :: Row(false) :: Row(false)
        :: Row(true) :: Row(true) :: Row(true) :: Nil
    )
  }


  test("SPARK-52896: Outer reference ExprId should match exposed attribute") {
    val plan =
      sql(
        """
          | SELECT col1
          | FROM VALUES(1,2)
          | GROUP BY col1
          | HAVING MAX(col2) == (SELECT 1 WHERE MAX(col2) = 1)
          |
      """.stripMargin).queryExecution.analyzed

    // Expected plan:
    // Project
    // +- Filter (scalar-subquery)
    // :  +- Project
    // :     +- Filter
    // :        +- OneRowRelation
    // +- Aggregate
    //   +- LocalRelation

    val havingNode = plan.asInstanceOf[Project].child.asInstanceOf[Filter]
    val subquery =
      havingNode.condition.asInstanceOf[EqualTo].right.asInstanceOf[SubqueryExpression]
    val subqueryFilter = subquery.plan.asInstanceOf[Project].child.asInstanceOf[Filter]

    val exposedAttribute = subquery.getOuterAttrs.head.asInstanceOf[NamedExpression]
    val outerReferenceAttribute = subqueryFilter.condition.asInstanceOf[EqualTo].collectFirst {
      case outerReference: OuterReference => outerReference.e
    }.get

    assert(exposedAttribute.exprId == outerReferenceAttribute.exprId)
  }

  test("SPARK-58481: InSubqueryExec nullable correctly accounts for subquery output nullability") {
    // 5 NOT IN (99, NULL) is UNKNOWN, not TRUE or FALSE.  A join condition that is not TRUE
    // matches no rows, so a FULL OUTER JOIN must emit null-padded rows for every row in each
    // side -- 3 + 3 = 6 null-padded rows -- not the full cross product (9 rows).
    //
    // The predicate 5 NOT IN (...) is a constant that references neither join input, so
    // RewritePredicateSubquery returns the original Join unchanged and PlanSubqueries always
    // constructs InSubqueryExec here regardless of any optimizer config.
    withTable("t0", "t1", "t3") {
      sql("CREATE TABLE t0(c0 INT) USING PARQUET")
      sql("INSERT INTO t0 VALUES (1), (2), (3)")
      sql("CREATE TABLE t1(c0 INT) USING PARQUET")
      sql("INSERT INTO t1 VALUES (10), (20), (30)")
      sql("CREATE TABLE t3(c0 INT) USING PARQUET")
      sql("INSERT INTO t3 VALUES (99), (CAST(NULL AS INT))")

      val query = sql(
        "SELECT t0.c0, t1.c0 FROM t1 FULL OUTER JOIN t0" +
        " ON (5 NOT IN (SELECT t3.c0 FROM t3))")
      // Verify the physical plan contains InSubqueryExec so the nullability fix is
      // actually exercised and not silently bypassed by an optimizer path.
      // Use AdaptiveSparkPlanHelper.find to traverse AQE wrapper nodes, and recurse
      // into expression subtrees via Expression.exists to find the wrapped InSubqueryExec.
      assert(
        find(query.queryExecution.executedPlan) { p =>
          p.expressions.exists(_.exists {
            case _: InSubqueryExec => true
            case _ => false
          })
        }.isDefined,
        "Expected InSubqueryExec in the physical plan")
      // Unmatched t1 rows null-pad t0; unmatched t0 rows null-pad t1.
      checkAnswer(query, Seq(
        Row(null, 10), Row(null, 20), Row(null, 30),
        Row(1, null), Row(2, null), Row(3, null)))
    }
  }

  test("SPARK-58481: multi-column IN subquery with nullable non-head output is nullable") {
    // Disable the optimizer's join-condition IN rewrite so the query exercises InSubqueryExec.
    // Use VALUES-derived temp views: their nullability is inferred from the literals (no NULL
    // literal => non-nullable), rather than declared and then widened. Parquet file-source
    // analysis applies dataSchema.asNullable regardless of DDL NOT NULL, which would defeat
    // the nullability control this test relies on.
    withSQLConf(
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false"
    ) {
      // Case A: NULL after a definitive match (null in non-head position after matching head).
      // RHS: (99,99) and (1,NULL).
      //   (1,1) vs (99,99): first field 1!=99 => FALSE.
      //   (1,1) vs (1,NULL): first fields equal, second null => UNKNOWN.
      //   Overall for (1,1): UNKNOWN => NOT IN = null-padded.
      //   (2,2) vs both: all FALSE => NOT IN = TRUE => joins with both rhs rows.
      withTempView("lhs", "rhs") {
        sql("CREATE TEMPORARY VIEW lhs AS SELECT * FROM VALUES (1, 1), (2, 2) AS t(a, b)")
        sql(
          """CREATE TEMPORARY VIEW rhs AS
            |SELECT * FROM VALUES (99, 99), (1, CAST(NULL AS INT)) AS t(a, b)""".stripMargin)
        checkAnswer(
          sql(
            """SELECT lhs.a, rhs.a FROM lhs FULL OUTER JOIN rhs
              |ON ((lhs.a, lhs.b) NOT IN (SELECT a, b FROM rhs))""".stripMargin),
          Seq(Row(1, null), Row(2, 99), Row(2, 1)))
      }

      // Case B: NULL in head position followed by a definitive mismatch in a later field.
      // RHS: (NULL, 99).
      //   (1,1) vs (NULL,99): first field null => UNKNOWN so far; second field 1!=99 => FALSE.
      //   A later definitive mismatch must override the earlier UNKNOWN: result is FALSE,
      //   NOT IN = TRUE. A field-order regression would leave (1,1) as UNKNOWN instead.
      withTempView("lhs2", "rhs2") {
        sql("CREATE TEMPORARY VIEW lhs2 AS SELECT * FROM VALUES (1, 1) AS t(a, b)")
        sql(
          """CREATE TEMPORARY VIEW rhs2 AS
            |SELECT * FROM VALUES (CAST(NULL AS INT), 99) AS t(a, b)""".stripMargin)
        // (1,1) NOT IN ((NULL,99)): second field 1!=99 makes the candidate FALSE =>
        // NOT IN = TRUE => inner join returns the single matching row.
        checkAnswer(
          sql(
            """SELECT lhs2.a FROM lhs2 JOIN (SELECT 1 AS a)
              |ON ((lhs2.a, lhs2.b) NOT IN (SELECT a, b FROM rhs2))""".stripMargin),
          Seq(Row(1)))
      }

      // Case C: UNKNOWN candidate followed by an exact-match candidate => IN = TRUE.
      // RHS: (1,NULL) and (1,1).
      //   (1,1) vs (1,NULL): first fields equal, second null => UNKNOWN.
      //   (1,1) vs (1,1): exact match => TRUE.
      //   The exact match must dominate the UNKNOWN: IN = TRUE, NOT IN = FALSE.
      //   A candidate-order regression would short-circuit on UNKNOWN and miss the TRUE.
      withTempView("lhs3", "rhs3") {
        sql("CREATE TEMPORARY VIEW lhs3 AS SELECT * FROM VALUES (1, 1) AS t(a, b)")
        sql(
          """CREATE TEMPORARY VIEW rhs3 AS
            |SELECT * FROM VALUES (1, CAST(NULL AS INT)), (1, 1) AS t(a, b)""".stripMargin)
        // (1,1) IN ((1,NULL),(1,1)): exact match exists => IN = TRUE => inner join returns row.
        checkAnswer(
          sql(
            """SELECT lhs3.a FROM lhs3 JOIN (SELECT 1 AS a)
              |ON ((lhs3.a, lhs3.b) IN (SELECT a, b FROM rhs3))""".stripMargin),
          Seq(Row(1)))
      }
    }
  }

  test("SPARK-58481: multi-column IN subquery uses Catalyst ordering for BinaryType fields") {
    // Object.equals on Array[Byte] compares by identity, not value; Catalyst ordering compares
    // by content. A multi-column IN where one field is BinaryType would incorrectly return FALSE
    // (no match) with JVM equality even when the bytes are equal. Use an inner join to keep the
    // assertion simple: the join condition is TRUE iff the IN match succeeds.
    withSQLConf(
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false"
    ) {
      withTable("lbin", "rbin") {
        sql("CREATE TABLE lbin(id INT NOT NULL, b BINARY NOT NULL) USING PARQUET")
        sql("INSERT INTO lbin VALUES (1, X'01')")
        sql("CREATE TABLE rbin(id INT NOT NULL, b BINARY NOT NULL) USING PARQUET")
        sql("INSERT INTO rbin VALUES (1, X'01')")
        // (1, 0x01) IN ((1, 0x01)) must be TRUE; the join should return one row.
        checkAnswer(
          sql(
            """SELECT lbin.id FROM lbin JOIN rbin
              |ON ((lbin.id, lbin.b) IN (SELECT id, b FROM rbin))""".stripMargin),
          Seq(Row(1)))
      }
    }
  }

  test("SPARK-58481: multi-column NOT IN with nullable LHS and non-nullable RHS is nullable") {
    // CreateNamedStruct.nullable is always false, so child.nullable would return false for a
    // multi-column LHS even when individual fields are nullable. The generated NOT IN code
    // would then suppress null handling and turn UNKNOWN into TRUE, producing wrong results.
    withSQLConf(
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false"
    ) {
      // Case A: RHS is fully non-null (stored in nonNullSet). LHS (NULL, 2) vs RHS (99, 2):
      // second fields equal, first field null => UNKNOWN; exercises the nonNullSet scan with
      // a nullable LHS. Fixture: lhs.a is nullable; rhs columns are NOT NULL (VALUES-derived
      // to avoid Parquet dataSchema.asNullable widening that would defeat the RHS control).
      withTable("lhs") {
        withTempView("rhs") {
          sql("CREATE TABLE lhs(a INT, b INT NOT NULL) USING PARQUET")
          sql("INSERT INTO lhs VALUES (1, 1), (NULL, 2)")
          // rhs as VALUES view: both columns inferred non-nullable from all-literal rows.
          sql("CREATE TEMPORARY VIEW rhs AS SELECT * FROM VALUES (99, 2) AS t(a, b)")
          // (NULL, 2) NOT IN ((99,2)): second fields match, first is null => UNKNOWN
          //   => join condition not TRUE => (NULL,2) is null-padded: Row(null, null).
          // (1, 1)   NOT IN ((99,2)): first field 1!=99 => FALSE => NOT IN = TRUE
          //   => (1,1) joins with rhs(99,2): Row(1, 99). rhs matched; no null-padded rhs.
          checkAnswer(
            sql(
              """SELECT lhs.a, rhs.a FROM lhs FULL OUTER JOIN rhs
                |ON ((lhs.a, lhs.b) NOT IN (SELECT a, b FROM rhs))""".stripMargin),
            Seq(Row(1, 99), Row(null, null)))
        }
      }

      // Case B: RHS is fully non-null (goes into nonNullSet, not nullRows). LHS (NULL, 1) vs
      // RHS (99, 2): first field null, but second field 1 != 2 is a definitive mismatch that
      // makes the candidate FALSE. An implementation that stops at the first UNKNOWN instead
      // of continuing to check later fields would return UNKNOWN here instead of FALSE, causing
      // NOT IN to be UNKNOWN and the inner join to return no rows.
      withTempView("lhs_nn", "rhs_nn") {
        sql("CREATE TEMPORARY VIEW lhs_nn AS " +
          "SELECT * FROM VALUES (CAST(NULL AS INT), 1) AS t(a, b)")
        sql("CREATE TEMPORARY VIEW rhs_nn AS " +
          "SELECT * FROM VALUES (99, 2) AS t(a, b)")
        // (NULL, 1) NOT IN ((99, 2)): second field 1 != 2 => candidate FALSE => NOT IN TRUE.
        // Inner join returns the single lhs row.
        checkAnswer(
          sql(
            """SELECT lhs_nn.b FROM lhs_nn JOIN (SELECT 1 AS x)
              |ON ((lhs_nn.a, lhs_nn.b) NOT IN (SELECT a, b FROM rhs_nn))
              |""".stripMargin),
          Seq(Row(1)))
      }
    }
  }

  test("SPARK-58481: LEGACY_IN_SUBQUERY_NULLABILITY suppresses RHS-only nullability") {
    // Legacy mode suppresses only RHS-derived nullability (plan.output nullable).
    // A non-nullable scalar LHS (Literal 5) has lhsNullable=false; with RHS suppressed,
    // nullable=false. The generated code omits null handling and NOT IN on a subquery that
    // returns NULL evaluates to TRUE -- the pre-fix single-column behaviour the flag preserves.
    // Note: intentionally codegen-specific. The interpreted path correctly returns UNKNOWN
    // regardless of nullable (6 rows); the assertion of 9 verifies codegen ran.
    withSQLConf(
      SQLConf.LEGACY_IN_SUBQUERY_NULLABILITY.key -> "true",
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false"
    ) {
      withTable("t0", "t1", "t3") {
        sql("CREATE TABLE t0(c0 INT) USING PARQUET")
        sql("INSERT INTO t0 VALUES (1), (2), (3)")
        sql("CREATE TABLE t1(c0 INT) USING PARQUET")
        sql("INSERT INTO t1 VALUES (10), (20), (30)")
        sql("CREATE TABLE t3(c0 INT) USING PARQUET")
        sql("INSERT INTO t3 VALUES (99), (CAST(NULL AS INT))")

        // Legacy: lhsNullable=false (Literal 5), rhsNullable suppressed => nullable=false.
        // Generated code suppresses null; 5 NOT IN (99, NULL) evaluates to TRUE.
        // FULL OUTER JOIN condition is TRUE => full cross product of 3 x 3 = 9 rows.
        assert(sql(
          "SELECT t0.c0, t1.c0 FROM t1 FULL OUTER JOIN t0 ON (5 NOT IN (SELECT t3.c0 FROM t3))")
          .count() === 9)
      }
    }
  }

  test("SPARK-58481: LEGACY_IN_SUBQUERY_NULLABILITY preserves nullable LHS fields " +
      "for multi-column IN subqueries") {
    // Legacy mode suppresses RHS nullability but preserves LHS field nullability.
    // With a nullable LHS field, lhsNullable=true even in legacy mode, so nullable=true.
    // Generated NOT IN code propagates UNKNOWN correctly; result is identical to non-legacy.
    withSQLConf(
      SQLConf.LEGACY_IN_SUBQUERY_NULLABILITY.key -> "true",
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false"
    ) {
      withTable("lhs", "rhs") {
        sql("CREATE TABLE lhs(a INT, b INT NOT NULL) USING PARQUET")
        sql("INSERT INTO lhs VALUES (1, 1), (NULL, 2)")
        sql("CREATE TABLE rhs(a INT NOT NULL, b INT NOT NULL) USING PARQUET")
        sql("INSERT INTO rhs VALUES (99, 2)")
        // (NULL, 2) NOT IN ((99,2)): first field null => UNKNOWN => null-padded: Row(null, null).
        // (1, 1)   NOT IN ((99,2)): 1!=99 => FALSE => NOT IN=TRUE => joins: Row(1, 99).
        checkAnswer(
          sql(
            """SELECT lhs.a, rhs.a FROM lhs FULL OUTER JOIN rhs
              |ON ((lhs.a, lhs.b) NOT IN (SELECT a, b FROM rhs))""".stripMargin),
          Seq(Row(1, 99), Row(null, null)))
      }
    }
  }

  test("SPARK-58481: InSubqueryExec.doGenCode registers Nondeterministic children " +
      "for partition-level initialization") {
    // A join-condition IN subquery with a nondeterministic LHS is rejected by CheckAnalysis,
    // so this direct-construction test provides a focused unit check: it constructs
    // InSubqueryExec directly (bypassing analysis) to verify that doGenCode correctly
    // registers each Nondeterministic descendant of the LHS child for partition-level
    // initialization. Without that registration, a Rand node's eval() would throw
    // IllegalArgumentException (via require(initialized, ...)) because initialize() was
    // never called.
    //
    // Two output columns force plan.output.length > 1, taking the multi-column fallback
    // branch (with the explicit foreach loop), not the single-column InSet path where Rand
    // would register itself through ordinary child codegen traversal.
    //
    // The test compiles the expression to bytecode via GeneratePredicate and executes it,
    // so a missing initialize() call causes an actual failure rather than just a missing
    // entry in partitionInitializationStatements.
    val subplanOutput = spark
      .range(0)
      .select($"id".cast(IntegerType).as("a"), $"id".cast(IntegerType).as("b"))
      .queryExecution.executedPlan.output
    val subplan = SubqueryExec(
      "test_subquery",
      LocalTableScanExec(subplanOutput, Nil, None))
    // LHS: struct of (Literal(1), Rand(42)>=0.0). Rand is always non-negative so the struct
    // always equals the single RHS row (1, true), making IN = true.
    val lhs = CreateNamedStruct(Seq(
      Literal("a"), Literal(1),
      Literal("b"), GreaterThanOrEqual(Rand(42L), Literal(0.0))))
    val expr = InSubqueryExec(
      lhs, subplan, ExprId(99),
      isDynamicPruning = false,
      result = Array[InternalRow](InternalRow(1, true)).asInstanceOf[Array[Any]])
    // Compile to bytecode, initialize (seeds the Rand RNG), and evaluate.
    // If initialize() were skipped the Rand eval() would throw IllegalArgumentException.
    val pred = GeneratePredicate.generate(expr, useSubexprElimination = false)
    pred.initialize(0)
    assert(pred.eval(InternalRow.empty) === true,
      "Expected IN to be TRUE: (1, rand()>=0.0) IN ((1, true))")
  }

  test("SPARK-58481: MultiColumnInSubqueryEvaluator preserves shared Nondeterministic " +
      "identity after Java serialization round-trip") {
    // In doGenCode, ctx.references receives both the MultiColumnInSubqueryEvaluator (slot 0)
    // and the bare Nondeterministic node (slot 1) for partition initialization. On the driver
    // both slots point to the same Rand instance (evaluator.child embeds it). The risk is that
    // Java serialization could write two independent copies, causing initialize() called on
    // references[1] to seed a different instance than eval() uses inside the evaluator.
    //
    // Java's ObjectOutputStream tracks shared references within a single graph (via its
    // internal handle table), so a single serialize/deserialize round-trip of the references
    // array as one object produces back-references rather than copies. This test verifies
    // that invariant explicitly using the same JavaSerializer Spark uses for task closures.
    val rand = Rand(42L)
    val lhs = CreateNamedStruct(Seq(
      Literal("a"), Literal(1),
      Literal("b"), GreaterThanOrEqual(rand, Literal(0.0))))
    val evaluator = new MultiColumnInSubqueryEvaluator(
      lhs,
      Array(IntegerType, IntegerType),
      Array(InternalRow(1, true)),
      legacyNullInEmptyBehavior = false)
    // Simulate what doGenCode does: put the evaluator and the bare Nondeterministic into
    // the references array as separate slots (as WholeStageCodegenEvaluatorFactory would).
    val references: Array[Any] = Array(evaluator, rand)
    // Round-trip through JavaSerializer -- the same serializer Spark uses for task closures
    // (SparkEnv always sets closureSerializer = new JavaSerializer(...)).
    // Serializing the array directly is equivalent to serializing it as a field of
    // WholeStageCodegenEvaluatorFactory: Java's ObjectOutputStream maintains a handle table
    // across the entire stream, so shared-reference tracking is independent of nesting depth.
    val ser = new JavaSerializer(new SparkConf()).newInstance()
    val rt = ser.deserialize[Array[Any]](ser.serialize(references))
    val rtEvaluator = rt(0).asInstanceOf[MultiColumnInSubqueryEvaluator]
    val rtRand = rt(1).asInstanceOf[Rand]
    // Find the Rand inside the deserialized evaluator's child expression tree.
    var embeddedRand: Rand = null
    rtEvaluator.child.foreach {
      case n: Rand => embeddedRand = n
      case _ =>
    }
    assert(embeddedRand ne null,
      "Expected a Rand node inside the evaluator's child after round-trip")
    // The critical identity check: the Rand node that would receive initialize(partitionIndex)
    // must be the same instance eval() uses inside the evaluator -- if Java serialization wrote
    // two independent copies, initialize() would seed a different instance than eval() uses.
    assert(embeddedRand eq rtRand,
      "Rand inside evaluator and separately-registered Rand must be the same instance " +
      "after serialization round-trip")
  }

  test("SPARK-58481: scalar subquery in multi-column LHS is literalized before capture") {
    // If the multi-column LHS contains a nested scalar subquery (e.g. (col, (SELECT max(x)
    // FROM t)) IN (...)), PlanSubqueries embeds an executable ScalarSubquery whose non-transient
    // BaseSubqueryExec would otherwise remain reachable through evaluator.child in generated task
    // closures. MultiColumnInSubqueryEvaluator replaces each ScalarSubquery with its already-
    // evaluated Literal before capturing child, so no BaseSubqueryExec crosses the boundary.
    //
    // Verify via a direct-construction InSubqueryExec (same pattern as the nondeterministic test)
    // that after a JavaSerializer round-trip the evaluator's child contains only a Literal where
    // the scalar subquery was, with no ScalarSubquery or BaseSubqueryExec reachable.
    val scalarSubplanOutput = spark
      .range(0).select($"id".cast(IntegerType).as("v"))
      .queryExecution.executedPlan.output
    val scalarSubplan = SubqueryExec(
      "scalar_subquery",
      LocalTableScanExec(scalarSubplanOutput, Seq(InternalRow(42)), None))
    // Build the scalar subquery expression, pre-update it with its result (42).
    val scalarSub = ScalarSubquery(scalarSubplan, ExprId(77))
    scalarSub.updateResult()
    // Outer subplan for the IN clause: two-column output so plan.output.length > 1.
    val outerSubplanOutput = spark
      .range(0)
      .select($"id".cast(IntegerType).as("a"), $"id".cast(IntegerType).as("b"))
      .queryExecution.executedPlan.output
    val outerSubplan = SubqueryExec(
      "outer_subquery",
      LocalTableScanExec(outerSubplanOutput, Nil, None))
    // LHS: (scalarSub, Literal(1)) -- the scalar subquery is the first field.
    val lhs = CreateNamedStruct(Seq(
      Literal("a"), scalarSub,
      Literal("b"), Literal(1)))
    val expr = InSubqueryExec(
      lhs, outerSubplan, ExprId(88),
      isDynamicPruning = false,
      result = Array[InternalRow](InternalRow(42, 1)).asInstanceOf[Array[Any]])
    // Access multiColEvaluator to trigger literalization (normally done in doGenCode/eval).
    val evaluator = expr.multiColEvaluator
    // Serialize and deserialize the evaluator via JavaSerializer -- the task-closure serializer.
    val ser = new JavaSerializer(new SparkConf()).newInstance()
    val rtEvaluator = ser.deserialize[MultiColumnInSubqueryEvaluator](
      ser.serialize(evaluator))
    // The deserialized child must contain no ScalarSubquery or BaseSubqueryExec.
    var foundScalarSubquery = false
    rtEvaluator.child.foreach {
      case _: ScalarSubquery => foundScalarSubquery = true
      case _ =>
    }
    assert(!foundScalarSubquery,
      "ScalarSubquery (and thus BaseSubqueryExec) must not be reachable from the " +
      "deserialized evaluator's child -- scalar subqueries must be literalized first")
    // The evaluator must still produce the correct result: (42, 1) IN ((42, 1)) = true.
    assert(evaluator.eval(InternalRow.empty) === true,
      "Expected IN to be TRUE: (42, 1) IN ((42, 1))")
  }

  test("SPARK-58481: multi-column IN with empty subquery short-circuits before evaluating LHS") {
    // When legacyNullInEmptyBehavior=false, IN (empty subquery) is always FALSE regardless
    // of the LHS value. MultiColumnInSubqueryEvaluator.eval must return false before calling
    // child.eval so that a LHS expression containing a runtime error (e.g. division by zero)
    // does not throw when the subquery is empty. Mirrors InSet.eval's guard (SPARK-44550).
    //
    // A plain WHERE IN is rewritten to LeftSemi by RewritePredicateSubquery before
    // PlanSubqueries runs, which would make the test vacuous. Force the InSubqueryExec path
    // via a FULL OUTER JOIN ON condition with the opt disabled, matching other tests here.
    withSQLConf(
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false",
      SQLConf.ANSI_ENABLED.key -> "true"
    ) {
      // lhs.id=0: (1 / lhs.id) would throw DIVIDE_BY_ZERO at runtime under ANSI if
      // child.eval were called. The subquery is empty so the short-circuit must fire first.
      withTable("lhs58481", "rhs58481") {
        sql("CREATE TABLE lhs58481(id INT NOT NULL) USING PARQUET")
        sql("INSERT INTO lhs58481 VALUES (0)")
        sql("CREATE TABLE rhs58481(id INT NOT NULL) USING PARQUET")
        sql("INSERT INTO rhs58481 VALUES (1)")
        checkAnswer(
          sql(
            """SELECT lhs58481.id FROM lhs58481 FULL OUTER JOIN rhs58481
              |ON ((1 / lhs58481.id, lhs58481.id) IN (SELECT 1, 0 WHERE false))
              |""".stripMargin),
          // FULL OUTER JOIN with FALSE condition: lhs row (id=0) plus null-padded rhs
          Seq(Row(0), Row(null)))
      }
    }
  }

  test("SPARK-58481: legacy empty-subquery branch evaluates the LHS expression") {
    // With legacyNullInEmptyBehavior=true, IN (empty set) must NOT skip child.eval: it returns
    // NULL when the LHS is null and FALSE otherwise, so the LHS must be evaluated.
    // Verify that MultiColumnInSubqueryEvaluator.eval does not short-circuit before child.eval
    // in this branch: a DIVIDE_BY_ZERO LHS must throw when the subquery is empty.
    withSQLConf(
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false",
      SQLConf.LEGACY_NULL_IN_EMPTY_LIST_BEHAVIOR.key -> "true",
      SQLConf.ANSI_ENABLED.key -> "true"
    ) {
      withTable("lhs58481b", "rhs58481b") {
        sql("CREATE TABLE lhs58481b(id INT NOT NULL) USING PARQUET")
        sql("INSERT INTO lhs58481b VALUES (0)")
        sql("CREATE TABLE rhs58481b(id INT NOT NULL) USING PARQUET")
        sql("INSERT INTO rhs58481b VALUES (1)")
        val ex = intercept[SparkArithmeticException] {
          sql(
            """SELECT lhs58481b.id FROM lhs58481b FULL OUTER JOIN rhs58481b
              |ON ((1 / lhs58481b.id, lhs58481b.id) IN (SELECT 1, 0 WHERE false))
              |""".stripMargin).collect()
        }
        assert(ex.getCondition === "DIVIDE_BY_ZERO")
      }
    }
  }

  test("SPARK-58481: multi-column IN with null LHS fields against null RHS fields") {
    // (NULL, 1) IN ((NULL, 1)): both first fields null => UNKNOWN (never TRUE); result UNKNOWN.
    // (NULL, 1) IN ((NULL, 2)): both first fields null so far UNKNOWN, but second field
    //   1 != 2 is a definitive mismatch => the candidate is FALSE. An implementation that
    //   short-circuits to UNKNOWN on the first null comparison would miss this mismatch and
    //   return UNKNOWN instead of FALSE for that candidate, producing wrong results.
    withSQLConf(
      "spark.sql.optimizer.optimizeUncorrelatedInSubqueriesInJoinCondition.enabled" -> "false"
    ) {
      withTempView("lhs_null", "rhs_null1", "rhs_null2") {
        sql("CREATE TEMPORARY VIEW lhs_null AS " +
          "SELECT * FROM VALUES (CAST(NULL AS INT), 1) AS t(a, b)")

        // (NULL,1) IN ((NULL,1)): UNKNOWN => NOT IN is UNKNOWN => FULL OUTER JOIN null-pads
        // both sides: the lhs row (b=1) is unmatched and the rhs row yields a null lhs.
        sql("CREATE TEMPORARY VIEW rhs_null1 AS " +
          "SELECT * FROM VALUES (CAST(NULL AS INT), 1) AS t(a, b)")
        checkAnswer(
          sql(
            """SELECT lhs_null.b FROM lhs_null FULL OUTER JOIN (SELECT 1 AS x)
              |ON ((lhs_null.a, lhs_null.b) NOT IN (SELECT a, b FROM rhs_null1))
              |""".stripMargin),
          Seq(Row(1), Row(null)))

        // (NULL,1) IN ((NULL,2)): second field 1 != 2 => candidate FALSE => NOT IN TRUE => joins.
        sql("CREATE TEMPORARY VIEW rhs_null2 AS " +
          "SELECT * FROM VALUES (CAST(NULL AS INT), 2) AS t(a, b)")
        checkAnswer(
          sql(
            """SELECT lhs_null.b FROM lhs_null JOIN (SELECT 1 AS x)
              |ON ((lhs_null.a, lhs_null.b) NOT IN (SELECT a, b FROM rhs_null2))
              |""".stripMargin),
          Seq(Row(1)))
      }
    }
  }
}

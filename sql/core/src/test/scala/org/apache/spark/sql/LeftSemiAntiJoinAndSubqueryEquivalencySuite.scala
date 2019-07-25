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

/**
 * This suite verifies that correlated subqueries and similar queries written
 * directly using left semi and left anti join converges to the same plan
 * which ensures that the same optimization rules are applied to both form
 * of queries.
 */

class LeftSemiAntiJoinAndSubqueryEquivalencySuite extends QueryTest with SharedSQLContext {

  import testImplicits._
  import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Join}
  import org.apache.spark.sql.catalyst.plans.LeftSemiOrAnti

  setupTestData()

  val row = identity[(java.lang.Integer, java.lang.Integer, java.lang.Integer)](_)

  lazy val t1 = Seq(
    row((1, 1, 1)),
    row((1, 2, 2)),
    row((2, 1, null)),
    row((3, 1, 2)),
    row((null, 0, 3)),
    row((4, null, 2)),
    row((0, -1, null))).toDF("t1a", "t1b", "t1c")

  lazy val t2 = Seq(
    row((1, 1, 1)),
    row((2, 1, 1)),
    row((2, 1, null)),
    row((3, 3, 3)),
    row((3, 1, 0)),
    row((null, null, 1)),
    row((0, 0, -1))).toDF("t2a", "t2b", "t2c")

  lazy val t3 = Seq(
    row((1, 1, 1)),
    row((2, 1, 0)),
    row((2, 1, null)),
    row((10, 4, -1)),
    row((3, 2, 0)),
    row((-2, 1, -1)),
    row((null, null, null))).toDF("t3a", "t3b", "t3c")

  lazy val t4 = Seq(
    row((1, 1, 2)),
    row((1, 2, 1)),
    row((2, 1, null))).toDF("t4a", "t4b", "t4c")

  lazy val t5 = Seq(
    row((1, 1, 1)),
    row((2, null, 0)),
    row((2, 1, null))).toDF("t5a", "t5b", "t5c")

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    t1.createOrReplaceTempView("t1")
    t2.createOrReplaceTempView("t2")
    t3.createOrReplaceTempView("t3")
    t4.createOrReplaceTempView("t4")
    t5.createOrReplaceTempView("t5")
  }

  test("LeftAnti over Project") {
    val plan1 =
      sql(
        """
          | select *
          | from   (select t1a+1 t1a1, t1b
          |         from   t1
          |         where  t1a > 2) tx
          | where  t1a1 not in (select t2a from   t2)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | select *
          | from   (select t1a+1 t1a1, t1b
          |         from   t1
          |         where  t1a > 2) tx
          | left anti join t2 on t1a1 = t2a or isnull(t1a1 = t2a)
        """.stripMargin)

    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftSemi over Aggregate") {
    val plan1 =
      sql(
        """
          | select *
          | from   (select   sum(t1a), coalesce(t1c, 0) t1c_expr
          |         from     t1
          |         group by coalesce(t1c, 0)) tx
          | where  t1c_expr in (select t2b
          |                     from   t2, t3
          |                     where  t2a = t3a)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | select *
          | from   (select   sum(t1a), coalesce(t1c, 0) t1c_expr
          |         from     t1
          |         group by coalesce(t1c, 0)) tx
          | left semi join (select t2b from t2, t3 where t2a = t3a) tx1
          | on t1c_expr = tx1.t2b
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftSemi over Window") {
    val plan1 =
      sql(
        """
          | select *
          | from   (select t1b, sum(t1b * t1a) over (partition by t1b) sum
          |         from   t1) tx
          | where  tx.t1b in (select t2b from t2)
        """.stripMargin)

     val plan2 =
       sql(
         """
           | select *
           | from   (select t1b, sum(t1b * t1a) over (partition by t1b) sum
           |         from   t1) tx
           | left semi join t2
           | on t2b = tx.t1b
         """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftAnti over Union") {
    val plan1 =
      sql(
        """
          | select *
          | from   (select t1a, t1b, t1c
          |         from   t1, t3
          |         where  t1a = t3a
          |         union all
          |         select t2a, t2b, t2c
          |         from   t2, t3
          |         where  t2a = t3a) ua
          | where  t1c not in (select t4c
          |                    from   t5, t4
          |                    where  t5.t5b = t4.t4b)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | select *
          | from   (select t1a, t1b, t1c
          |         from   t1, t3
          |         where  t1a = t3a
          |         union all
          |         select t2a, t2b, t2c
          |         from   t2, t3
          |         where  t2a = t3a) ua
          | left anti join (select t4c
          |                 from t5, t4
          |                 where t5.t5b = t4.t4b) ub
          | on ua.t1c = ub.t4c or isnull(ua.t1c = ub.t4c)
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftAnti over other UnaryNode") {
    val plan1 =
      sql(
        """
          | select *
          | from   (select   t1a+1 t1a1, t1b, t3c
          |         from     t1, t3
          |         where    t1b = t3b
          |         and      t1a < 3
          |         order by t1b) tx
          | where  tx.t1a1 not in (select t2a
          |                        from   t2
          |                        where  t2b < 3
          |                        and    tx.t3c >= 0)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | select *
          | from   (select   t1a+1 t1a1, t1b, t3c
          |         from     t1, t3
          |         where    t1b = t3b
          |         and      t1a < 3
          |         order by t1b) tx
          | left anti join (select t2a
          |                 from t2
          |                 where t2b < 3) tx2
          | on (tx.t1a1 = tx2.t2a or isnull(tx.t1a1 = tx2.t2a)) and tx.t3c >= 0
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftSemi over inner join") {
    val plan1 =
      sql(
        """
          | with cte as
          |   (select * from t1 inner join t2 on t1b = t2b and t2a >= 2)
          | select *
          | from   cte
          | where  t1a in (select t3a from t3 where t3b >= 1)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | with cte as
          |   (select * from t1 inner join t2 on t1b = t2b and t2a >= 2)
          | select *
          | from   cte
          | left semi join (select t3a from t3 where t3b >= 1) cte2
          | on t1a = cte2.t3a
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftSemi over left outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with cte1 as
          |   (select * from t1 left join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   cte1
          | where  t1a in (select t3a from t3 where t3b >= 1)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | with cte1 as
          |   (select * from t1 left join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   cte1 left semi join t3
          | on t1a = t3a and t3b >=1
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftAnti over left outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with cte1 as
          |   (select * from t1 left join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   cte1
          | where  t1a not in (select t3a from t3 where t3b >= 1)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | with cte1 as
          |   (select * from t1 left join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from cte1 left anti join t3
          | on (t1a = t3a or isnull(t1a = t3a)) and t3b >= 1
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftSemi over right outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with cte1 as
          |   (select * from t1 right join t2 on t1b = t2b and t2c is null)
          | select *
          | from   cte1
          | where  t1a in (select t3a from t3 where t3b >= 1)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | with cte1 as
          |   (select * from t1 right join t2 on t1b = t2b and t2c is null)
          | select *
          | from   cte1 left semi join t3
          | on t1a = t3a and t3b >= 1
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftAnti over right outer join with correlated columns on the right table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 right join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  t2a not in (select t3a from t3 where t3b >= 1)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | with join as
          |   (select * from t1 right join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from
          | join left anti join t3
          | on (t2a = t3a or isnull(t2a = t3a)) and t3b >= 1
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }


  test("LeftAnti over full outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  t1a not in (select t3a from t3 where t3b >= 1)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from
          | join left anti join t3
          | on (t1a = t3a  or isnull(t1a = t3a)) and t3b >=1
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftAnti over full outer join with correlated columns on the right table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  t2b not in (select t3b from t3 where t3a >= 1)
        """.stripMargin)

    val plan2 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          |        left anti join
          |        ( select *
          |          from t3) t3
          |          on ((t2b = t3b  or isnull(t2b = t3b)) and t3a >=1
          |        )
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftAnti over right outer join with no correlated columns") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 right join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  not exists (select 1 from t3 where t3b < -1)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select *
          | from   t1
          |        right outer join
          |        (select *
          |         from   t2
          |         where  not exists (select 1 from t3 where t3b < -1)) t2
          |        on t1b = t2b and t2c >= 2
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }

  test("LeftAnti over full outer join with no correlated columns") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 0)
          | select *
          | from   join
          | where  not exists (select 1 from t3 where t3b < -1)
          | and    (t1c = 1 or t1c is null)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 0)
          | select *
          | from
          | join left anti join t3
          | on t3b < -1
          | where  (t1c = 1 or t1c is null)
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
  }
}


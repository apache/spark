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

/*
 * Writing test cases using combinatorial testing technique
 * Dimension 1: (A) Exists or (B) In
 * Dimension 2: (A) LeftSemi, (B) LeftAnti, or (C) ExistenceJoin
 * Dimension 3: (A) Join over Project, (B) Join over Agg, (C) Join over Window,
 *              (D) Join over Union, or (E) Join over other UnaryNode
 * Dimension 4: (A) join condition is column or (B) expression
 * Dimension 5: Subquery is (A) a single table, or (B) more than one table
 * Dimension 6: Parent side is (A) a single table, or (B) more than one table
 */
class LeftSemiOrAntiPushdownSuite extends QueryTest with SharedSQLContext {
  import testImplicits._
  import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Join}
  import org.apache.spark.sql.catalyst.plans.LeftSemiOrAnti

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

  private def checkLeftSemiOrAntiPlan(plan: LogicalPlan): Unit = {
    plan match {
      case j @ Join(_, _, LeftSemiOrAnti(_), _) =>
      // This is the expected result.
      case _ =>
        fail(
          s"""
           |== FAIL: Top operator must be a LeftSemi or LeftAnti ===
           |${plan.toString}
           """.stripMargin)
    }
  }

  /**
   * TC 1.1: 1A-2B-3A-4B-5A-6A
   * Expected result: LeftAnti below Project
   * Note that the expression T1A+1 is evaluated twice in Join and Project
   *
   * TC 1.1.1: Comparing to Inner, we do not push down Inner join under Project
   *
   * SELECT TX.*
   * FROM   (SELECT T1A+1 T1A1, T1B
   *         FROM   T1
   *         WHERE  T1A > 2) TX, T2
   * WHERE  T2A = T1A1
   */
  test("TC 1.1: LeftSemi/LeftAnti over Project") {
    val plan1 =
      sql(
        """
          | select *
          | from   (select t1a+1 t1a1, t1b
          |         from   t1
          |         where  t1a > 2) tx
          | where  not exists (select 1
          |                    from   t2
          |                    where  t2a = t1a1)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select t1a+1 t1a1, t1b
          | from   t1
          | where  t1a > 2
          | and    not exists (select 1
          |                    from   t2
          |                    where  t2a = t1a+1)
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }

  /**
   * TC 1.4: 1B-2B-3D-4A-5B-6B
   * Expected result: LeftAnti below Union
   */
  test("TC 1.4: LeftSemi/LeftAnti over Union") {
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
          |         and    t1c not in (select t4c
          |                            from   t5, t4
          |                            where  t5.t5b = t4.t4b)
          |         union all
          |         select t2a, t2b, t2c
          |         from   t2, t3
          |         where  t2a = t3a
          |         and    t2c not in (select t4c
          |                            from   t5, t4
          |                            where  t5.t5b = t4.t4b)
          |        ) ua
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }

  /**
   * TC 1.5: 1B-2B-3E-4B-5A-6B
   * Expected result: LeftAnti below Sort
   */
  test("TC 1.5: LeftSemi/LeftAnti over other UnaryNode") {
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
          | from   (select t1a+1 t1a1, t1b, t3c
          |         from   t1, t3
          |         where  t1b = t3b
          |         and    t1a < 3
          |         and    t1.t1a+1 not in (select t2a
          |                                 from   t2
          |                                 where  t2b < 3
          |                                 and    t3c >= 0)
          |         order by t1b) tx
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }

  /**
   * LeftSemi/LeftAnti over join
   *
   * Dimension 1: (A) LeftSemi or (B) LeftAnti
   * Dimension 2: Join below is (A) Inner (B) LeftOuter (C) RightOuter (D) FullOuter, or,
   *              (E) LeftSemi/LeftAnti
   * Dimension 3: Subquery correlated to (A) left table (B) right table, (C) both tables,
   *              or, (D) no correlated predicate
   */
  /**
   * TC 2.1: 1A-2A-3A
   * Expected result: LeftSemi join below Inner join
   */
    test("TC 2.1: LeftSemi over inner join") {
      val plan1 =
        sql(
          """
            | with join as
            |   (select * from t1 inner join t2 on t1b = t2b and t2a >= 2)
            | select *
            | from   join
            | where  t1a in (select t3a from t3 where t3b >= 1)
          """.stripMargin)
      val plan2 =
        sql(
          """
            | select *
            | from   (select *
            |         from   t1
            |         where  t1a in (select t3a from t3 where t3b >= 1)) t1
            |        inner join t2
            |        on t1b = t2b and t2a >= 2
          """.stripMargin)
      checkAnswer(plan1, plan2)
      comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
      plan1.show
    }
  /**
   * TC 2.2: 1A-2B-3A
   * Expected result: LeftSemi join below LeftOuter join
   */
  test("TC 2.2: LeftSemi over left outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 left join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  exists (select 1 from t3 where t3a = t1a and t3b >= 1)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select *
          | from   (select *
          |         from   t1
          |         where  exists (select 1 from t3 where t3a = t1a and t3b >= 1)) t1
          |        left join t2
          |        on t1b = t2b and t2c >= 2
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }
  /**
   * TC 2.3: 1B-2B-3A
   * Expected result: LeftAnti join below LeftOuter join
   */
  test("TC 2.3: LeftAnti over left outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 left join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  not exists (select 1 from t3 where t3a = t1a and t3b >= 1)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select *
          | from   (select *
          |         from t1
          |         where  not exists (select 1 from t3 where t3a = t1a and t3b >= 1)) t1
          |        left join t2
          |        on t1b = t2b and t2c >= 2
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }
  /**
   * TC 2.4: 1A-2C-3A
   * Expected result: LeftSemi join below Inner join
   */
  test("TC 2.4: LeftSemi over right outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 right join t2 on t1b = t2b and t2c is null)
          | select *
          | from   join
          | where  exists (select 1 from t3 where t3a = t1a and t3b >= 1)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select *
          | from   (select *
          |         from   t1
          |         where  exists (select 1 from t3 where t3a = t1a and t3b >= 1)) t1
          |        inner join t2
          |        on t1b = t2b and t2c is null
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }
  /**
   * TC 2.5: 1B-2C-3B
   * Expected result: LeftAnti join below RightOuter join
   * RightOuter does not convert to Inner because NOT IN can return null.
   */
  test("TC 2.5: LeftAnti over right outer join with correlated columns on the right table") {
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
          | select *
          | from   t1
          |        right join
          |        (select *
          |         from   t2
          |         where  t2a not in (select t3a from t3 where t3b >= 1)) t2
          |        on t1b = t2b and t2c >= 2
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }
  /**
   * TC 2.6: 1B-2C-3C
   * Expected result: No push down
   */
  test("TC 2.6: LeftAnti over right outer join with correlated cols on both left and right tbls") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 right join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  not exists (select 1 from t3 where t3a = t1a and t3b > t2b)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | with join as
          |   (select * from t1 right join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          |        left anti join
          |        (select t3a, t3b
          |         from   t3
          |         where  t3a is not null
          |         and    t3b is not null) t3
          |        on t3a = t1a and t3b > t2b
        """.stripMargin)
    checkAnswer(plan1, plan2)
    val optPlan = plan1.queryExecution.optimizedPlan
    checkLeftSemiOrAntiPlan(optPlan)
    plan1.show
  }
  /**
   * TC 2.7: 1B-2D-3A
   * Expected result: LeftAnti join below LeftOuter join
   */
  test("TC 2.7: LeftAnti over full outer join with correlated columns on the left table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  not exists (select 1 from t3 where t3a = t1a and t3b >= 1)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select *
          | from   (select *
          |         from   t1
          |         where  not exists (select 1 from t3 where t3a = t1a and t3b >= 1)) t1
          |        left join t2
          |        on t1b = t2b and t2c >= 2
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }
  /**
   * TC 2.8: 1A-2D-3B
   * Expected result: LeftSemi join below RightOuter join
   */
  test("TC 2.8: LeftSemi over full outer join with correlated columns on the right table") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 full join t2 on t1b = t2b and t2c >= 2)
          | select *
          | from   join
          | where  exists (select 1 from t3 where t3a = t2a and t3b >= 1)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select *
          | from   t1
          |        right join
          |        (select *
          |         from   t2
          |         where  exists (select 1 from t3 where t3a = t2a and t3b >= 1)) t2
          |        on t1b = t2b and t2c >= 2
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }
  /**
   * TC 2.9: 1A-2E-3A
   * Expected result: No push down
   */
  test("TC 2.9: LeftSemi over left semi join with correlated columns on the left table") {
    import org.apache.spark.sql.catalyst.plans.logical.Union
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 left semi join t2 on t1b = t2b and t2c >= 0)
          | select *
          | from   join
          | where  exists (select 1
          |                from   (select * from t3
          |                        union all
          |                        select * from t4) t3
          |                where  t3a = t1a and t3c is not null)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | with join as
          |   (select *
          |    from   t1
          |           left semi join t2
          |           on t1b = t2b and t2c >= 0)
          | select *
          | from   join
          |        left semi join
          |        (select * from t3
          |         union all
          |         select * from t4) t3
          |        on t3a = t1a and t3c is not null
        """.stripMargin)
    checkAnswer(plan1, plan2)
    val optPlan = plan1.queryExecution.optimizedPlan
    optPlan match {
      case j @ Join(_, _: Union, LeftSemiOrAnti(_), _) =>
      // This is the expected result.
      case _ =>
        fail(
          s"""
             |== FAIL: The right operand of the top operator must be a Union ===
             |${optPlan.toString}
           """.stripMargin)
    }
    plan1.show
  }
  /**
   * TC 2.10: 1A-2A-3C
   * Expected result: No push down
   */
  test("TC 2.10: LeftSemi over inner join with correlated columns on both left and right tables") {
    val plan1 =
      sql(
        """
          | with join as
          |   (select * from t1 inner join t2 on t1b = t2b and t2c is null)
          | select *
          | from   join
          | where  exists (select 1 from t3 where t3a = t1a and t3a = t2a)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | with join as
          |   (select *
          |    from   t1
          |           inner join t2
          |           on t1b = t2b and t2c is null)
          | select *
          | from   join
          |        left semi join t3
          |        on t3a = t1a and t3a = t2a
        """.stripMargin)
    checkAnswer(plan1, plan2)
    val optPlan = plan1.queryExecution.optimizedPlan
    checkLeftSemiOrAntiPlan(optPlan)
    plan1.show
  }
  /**
   * TC 2.11: 1B-2C-3D
   * Expected result: LeftSemi join below RightOuter join
   */
  test("TC 2.11: LeftAnti over right outer join with no correlated columns") {
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
    plan1.show
  }
  /**
   * TC 2.12: 1B-2D-3D
   * Expected result: LeftSemi join below RightOuter join
   */
  test("TC 2.12: LeftAnti over full outer join with no correlated columns") {
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
          | from   join
          |        left anti join t3
          |        on t3b < -1
          | where  (t1c = 1 or t1c is null)
        """.stripMargin)
    checkAnswer(plan1, plan2)
    comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    plan1.show
  }
  /**
   * TC 3.1: Negative case - LeftSemi over Aggregate
   * Expected result: No push down
   */
  test("TC 3.1: Negative case - LeftSemi over Aggregate") {
    val plan1 =
      sql(
        """
          | select   t1b, min(t1a) as min
          | from     t1 b
          | group by t1b
          | having   t1b in (select t1b+1
          |                  from   t1 a
          |                  where  a.t1a = min(b.t1a) )
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select   b.*
          | from     (select   t1b, min(t1a) as min
          |           from     t1
          |           group by t1b) b
          |          left semi join t1
          |          on  b.t1b = t1.t1b+1
          |          and b.min = t1.t1a
          |          and t1.t1a is not null
        """.stripMargin)
    checkAnswer(plan1, plan2)
    val optPlan = plan1.queryExecution.optimizedPlan
    checkLeftSemiOrAntiPlan(optPlan)
    plan1.show
  }
  /**
   * TC 3.2: Negative case - LeftAnti over Window
   * Expected result: No push down
   */
  test("TC 3.2: Negative case - LeftAnti over Window") {
    val plan1 =
      sql(
        """
          | select   b.t1b, b.min
          | from     (select t1b, min(t1a) over (partition by t1b) min
          |           from   t1) b
          | where    not exists (select 1
          |                      from   t1 a
          |                      where  a.t1a = b.min
          |                      and    a.t1b = b.t1b)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select   b.t1b, b.min
          | from     (select t1b, min(t1a) over (partition by t1b) min
          |           from   t1) b
          |          left anti join t1 a
          |          on  a.t1a = b.min
          |          and a.t1b = b.t1b
        """.stripMargin)
    checkAnswer(plan1, plan2)
    val optPlan = plan1.queryExecution.optimizedPlan
    checkLeftSemiOrAntiPlan(optPlan)
    plan1.show
  }
  /**
   * TC 3.3: Negative case - LeftSemi over Union
   * Expected result: No push down
   */
  test("TC 3.3: Negative case - LeftSemi over Union") {
    val plan1 =
      sql(
        """
          | select   un.t2b, un.t2a
          | from     (select t2b, t2a
          |           from   t2
          |           union all
          |           select t3b, t3a
          |           from   t3) un
          | where    exists (select 1
          |                  from   t1 a
          |                  where  a.t1b = un.t2b
          |                  and    a.t1a = un.t2a + case when rand() < 0 then 1 else 0 end)
        """.stripMargin)
    val plan2 =
      sql(
        """
          | select   un.t2b, un.t2a
          | from     (select t2b, t2a
          |           from   t2
          |           union all
          |           select t3b, t3a
          |           from   t3) un
          |          left semi join t1 a
          |          on  a.t1b = un.t2b
          |          and a.t1a = un.t2a
        """.stripMargin)
    checkAnswer(plan1, plan2)
    val optPlan = plan1.queryExecution.optimizedPlan
    checkLeftSemiOrAntiPlan(optPlan)
    plan1.show
  }
}

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

import org.apache.spark.sql.catalyst.expressions.{And, GreaterThan, LessThan, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class CTEInlineSuiteBase
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("SPARK-36447: non-deterministic CTE dedup") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() from t
           |)
           |select * from v except select * from v
         """.stripMargin)
      checkAnswer(df, Nil)

      val r = df.queryExecution.optimizedPlan.find {
        case RepartitionByExpression(p, _, None, _) => p.isEmpty
        case _ => false
      }
      assert(
        r.isDefined,
        "Non-deterministic With-CTE with multiple references should be not inlined.")
    }
  }

  test("SPARK-36447: non-deterministic CTE in subquery") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() c3 from t
           |)
           |select * from v where c3 not in (select c3 from v)
         """.stripMargin)
      checkAnswer(df, Nil)
      assert(
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "Non-deterministic With-CTE with multiple references should be not inlined.")
    }
  }

  test("SPARK-36447: non-deterministic CTE with one reference should be inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() c3 from t
           |)
           |select c1, c2 from v where c3 > 0
         """.stripMargin)
      checkAnswer(df, Row(0, 1) :: Row(1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.exists(_.isInstanceOf[WithCTE]),
        "With-CTE should not be inlined in analyzed plan.")
      assert(
        !df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "With-CTE with one reference should be inlined in optimized plan.")
    }
  }

  test("SPARK-36447: nested non-deterministic CTEs referenced more than once are not inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select c1, c2, rand() c4 from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case r: RepartitionOperation => r
        }.length == 6,
        "With-CTE should contain 2 CTE def after optimization.")
    }
  }

  test("SPARK-36447: nested CTEs only the deterministic is inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case r: RepartitionOperation => r
        }.length == 4,
        "One CTE def should be inlined after optimization.")
    }
  }

  test("SPARK-36447: nested non-deterministic CTEs referenced only once are inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select c1, c2, c3, rand() c4 from v1
           |)
           |select c1, c2 from v2 where c3 > 0 and c4 > 0
         """.stripMargin)
      checkAnswer(df, Row(0, 1) :: Row(1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case r: RepartitionOperation => r
        }.isEmpty,
        "CTEs with one reference should all be inlined after optimization.")
    }
  }

  test("SPARK-36447: With in subquery of main query") {
    withSQLConf(
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      withTempView("t") {
        Seq((2, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
        val df = sql(
          s"""with v as (
             |  select c1, c2, rand() c3 from t
             |)
             |select * from v except
             |select * from v where c1 = (
             |  with v2 as (
             |    select c1, c2, rand() c3 from t
             |  )
             |  select count(*) from v where c2 not in (
             |    select c2 from v2 where c3 not in (select c3 from v2)
             |  )
             |)
           """.stripMargin)
        checkAnswer(df, Nil)
        assert(
          collectWithSubqueries(df.queryExecution.executedPlan) {
            case r: ReusedExchangeExec => r
          }.length == 3,
          "Non-deterministic CTEs are reused shuffles.")
      }
    }
  }

  test("SPARK-36447: With in subquery of CTE def") {
    withTempView("t") {
      Seq((2, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with v as (
           |  select c1, c2, rand() c3 from t where c1 = (
           |    with v2 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select count(*) from (
           |      select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |    )
           |  )
           |)
           |select count(*) from (
           |  select * from v where c1 > 0 union select * from v where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 2,
        "Non-deterministic CTEs are reused shuffles.")
    }
  }

  test("SPARK-36447: nested deterministic CTEs are inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, c1 + c2 c3 from t
           |),
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case r: RepartitionOperation => r
        }.isEmpty,
        "Deterministic CTEs should all be inlined after optimization.")
    }
  }

  test("SPARK-36447: invalid nested CTEs") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val ex = intercept[AnalysisException](sql(
        s"""with
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |),
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin))
      checkErrorTableNotFound(ex, "`v1`",
        ExpectedContext("v1", 29, 30))
    }
  }

  test("CTE Predicate push-down and column pruning") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, 's' c3, rand() c4 from t
           |),
           |vv as (
           |  select v1.c1, v1.c2, rand() c5 from v v1, v v2
           |  where v1.c1 > 0 and v1.c3 = 's' and v1.c2 = v2.c2
           |)
           |select vv1.c1, vv1.c2, vv2.c1, vv2.c2 from vv vv1, vv vv2
           |where vv1.c2 > 0 and vv2.c2 > 0 and vv1.c1 = vv2.c1
         """.stripMargin)
      checkAnswer(df, Row(1, 2, 1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.length == 6,
        "CTE should not be inlined after optimization.")
      val distinctCteRepartitions = cteRepartitions.map(_.canonicalized).distinct
      // Check column pruning and predicate push-down.
      assert(distinctCteRepartitions.length == 2)
      assert(distinctCteRepartitions(1).collectFirst {
        case p: Project if p.projectList.length == 3 => p
      }.isDefined, "CTE columns should be pruned.")
      assert(distinctCteRepartitions(1).collectFirst {
        case f: Filter if f.condition.semanticEquals(GreaterThan(f.output(1), Literal(0))) => f
      }.isDefined, "Predicate 'c2 > 0' should be pushed down to the CTE def 'v'.")
      assert(distinctCteRepartitions(0).collectFirst {
        case f: Filter if f.condition.find(_.semanticEquals(f.output(0))).isDefined => f
      }.isDefined, "CTE 'vv' definition contains predicate 'c1 > 0'.")
      assert(distinctCteRepartitions(1).collectFirst {
        case f: Filter if f.condition.find(_.semanticEquals(f.output(0))).isDefined => f
      }.isEmpty, "Predicate 'c1 > 0' should be not pushed down to the CTE def 'v'.")
      // Check runtime repartition reuse.
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 2,
        "CTE repartition is reused.")
    }
  }

  test("CTE Predicate push-down and column pruning - combined predicate") {
    withTempView("t") {
      Seq((0, 1, 2), (1, 2, 3)).toDF("c1", "c2", "c3").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, c3, rand() c4 from t
           |),
           |vv as (
           |  select v1.c1, v1.c2, rand() c5 from v v1, v v2
           |  where v1.c1 > 0 and v2.c3 < 5 and v1.c2 = v2.c2
           |)
           |select vv1.c1, vv1.c2, vv2.c1, vv2.c2 from vv vv1, vv vv2
           |where vv1.c2 > 0 and vv2.c2 > 0 and vv1.c1 = vv2.c1
         """.stripMargin)
      checkAnswer(df, Row(1, 2, 1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.length == 6,
        "CTE should not be inlined after optimization.")
      val distinctCteRepartitions = cteRepartitions.map(_.canonicalized).distinct
      // Check column pruning and predicate push-down.
      assert(distinctCteRepartitions.length == 2)
      assert(distinctCteRepartitions(1).collectFirst {
        case p: Project if p.projectList.length == 3 => p
      }.isDefined, "CTE columns should be pruned.")
      assert(
        distinctCteRepartitions(1).collectFirst {
          case f: Filter
              if f.condition.semanticEquals(
                And(
                  GreaterThan(f.output(1), Literal(0)),
                  Or(
                    GreaterThan(f.output(0), Literal(0)),
                    LessThan(f.output(2), Literal(5))))) =>
            f
        }.isDefined,
        "Predicate 'c2 > 0 AND (c1 > 0 OR c3 < 5)' should be pushed down to the CTE def 'v'.")
      // Check runtime repartition reuse.
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 2,
        "CTE repartition is reused.")
    }
  }

  test("Views with CTEs - 1 temp view") {
    withTempView("t", "t2") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      sql(
        s"""with
           |v as (
           |  select c1 + c2 c3 from t
           |)
           |select sum(c3) s from v
         """.stripMargin).createOrReplaceTempView("t2")
      val df = sql(
        s"""with
           |v as (
           |  select c1 * c2 c3 from t
           |)
           |select sum(c3) from v except select s from t2
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
    }
  }

  test("Views with CTEs - 2 temp views") {
    withTempView("t", "t2", "t3") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      sql(
        s"""with
           |v as (
           |  select c1 + c2 c3 from t
           |)
           |select sum(c3) s from v
         """.stripMargin).createOrReplaceTempView("t2")
      sql(
        s"""with
           |v as (
           |  select c1 * c2 c3 from t
           |)
           |select sum(c3) s from v
         """.stripMargin).createOrReplaceTempView("t3")
      val df = sql("select s from t3 except select s from t2")
      checkAnswer(df, Row(2) :: Nil)
    }
  }

  test("Views with CTEs - temp view + sql view") {
    withTable("t") {
      withTempView ("t2", "t3") {
        Seq((0, 1), (1, 2)).toDF("c1", "c2").write.saveAsTable("t")
        sql(
          s"""with
             |v as (
             |  select c1 + c2 c3 from t
             |)
             |select sum(c3) s from v
           """.stripMargin).createOrReplaceTempView("t2")
        sql(
          s"""create view t3 as
             |with
             |v as (
             |  select c1 * c2 c3 from t
             |)
             |select sum(c3) s from v
           """.stripMargin)
        val df = sql("select s from t3 except select s from t2")
        checkAnswer(df, Row(2) :: Nil)
      }
    }
  }

  test("Union of Dataframes with CTEs") {
    val a = spark.sql("with t as (select 1 as n) select * from t ")
    val b = spark.sql("with t as (select 2 as n) select * from t ")
    val df = a.union(b)
    checkAnswer(df, Row(1) :: Row(2) :: Nil)
  }

  test("CTE definitions out of original order when not inlined") {
    withTempView("issue_current") {
      Seq((1, 2, 10, 100), (2, 3, 20, 200)).toDF("workspace_id", "issue_id", "shard_id", "field_id")
        .createOrReplaceTempView("issue_current")
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.catalyst.optimizer.InlineCTE") {
        val df = sql(
          """
            |WITH cte_0 AS (
            |  SELECT workspace_id, issue_id, shard_id, field_id FROM issue_current
            |),
            |cte_1 AS (
            |  WITH filtered_source_table AS (
            |    SELECT * FROM cte_0 WHERE shard_id in ( 10 )
            |  )
            |  SELECT source_table.workspace_id, field_id FROM cte_0 source_table
            |  INNER JOIN (
            |    SELECT workspace_id, issue_id FROM filtered_source_table GROUP BY 1, 2
            |  ) target_table
            |  ON source_table.issue_id = target_table.issue_id
            |  AND source_table.workspace_id = target_table.workspace_id
            |  WHERE source_table.shard_id IN ( 10 )
            |)
            |SELECT * FROM cte_1
        """.stripMargin)
        checkAnswer(df, Row(1, 100) :: Nil)
      }
    }
  }

  test("Make sure CTESubstitution places WithCTE back in the plan correctly.") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")

      // CTE on both sides of join - WithCTE placed over first common parent, i.e., the join.
      val df1 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  with
           |  v1 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v1
           |) v1 join (
           |  with
           |  v2 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v2
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df1, Row(2, 2) :: Nil)
      df1.queryExecution.analyzed match {
        case Aggregate(_, _, WithCTE(_, cteDefs)) => assert(cteDefs.length == 2)
        case other => fail(s"Expect pattern Aggregate(WithCTE(_)) but got $other")
      }

      // CTE on one side of join - WithCTE placed back where it was.
      val df2 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  select c1, c2, rand() c3 from t
           |) v1 join (
           |  with
           |  v2 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v2
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df2, Row(2, 2) :: Nil)
      df2.queryExecution.analyzed match {
        case Aggregate(_, _, Join(_, SubqueryAlias(_, WithCTE(_, cteDefs)), _, _, _)) =>
          assert(cteDefs.length == 1)
        case other => fail(s"Expect pattern Aggregate(Join(_, WithCTE(_))) but got $other")
      }

      // CTE on one side of join and both sides of union - WithCTE placed on first common parent.
      val df3 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  select c1, c2, rand() c3 from t
           |) v1 join (
           |  select * from (
           |    with
           |    v1 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v1
           |  )
           |  union all
           |  select * from (
           |    with
           |    v2 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v2
           |  )
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df3, Row(4, 4) :: Nil)
      df3.queryExecution.analyzed match {
        case Aggregate(_, _, Join(_, SubqueryAlias(_, WithCTE(_: Union, cteDefs)), _, _, _)) =>
          assert(cteDefs.length == 2)
        case other => fail(
          s"Expect pattern Aggregate(Join(_, (WithCTE(Union(_, _))))) but got $other")
      }

      // CTE on one side of join and one side of union - WithCTE placed back where it was.
      val df4 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  select c1, c2, rand() c3 from t
           |) v1 join (
           |  select * from (
           |    with
           |    v1 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v1
           |  )
           |  union all
           |  select c1, c2, rand() c3 from t
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df4, Row(4, 4) :: Nil)
      df4.queryExecution.analyzed match {
        case Aggregate(_, _, Join(_, SubqueryAlias(_, Union(children, _, _)), _, _, _))
          if children.head.find(_.isInstanceOf[WithCTE]).isDefined =>
          assert(
            children.head.collect {
              case w: WithCTE => w
            }.head.cteDefs.length == 1)
        case other => fail(
          s"Expect pattern Aggregate(Join(_, (WithCTE(Union(_, _))))) but got $other")
      }

      // CTE on both sides of join and one side of union - WithCTE placed on first common parent.
      val df5 = sql(
        s"""
           |select count(v1.c3), count(v2.c3) from (
           |  with
           |  v1 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v1
           |) v1 join (
           |  select c1, c2, rand() c3 from t
           |  union all
           |  select * from (
           |    with
           |    v2 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select * from v2
           |  )
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df5, Row(4, 4) :: Nil)
      df5.queryExecution.analyzed match {
        case Aggregate(_, _, WithCTE(_, cteDefs)) => assert(cteDefs.length == 2)
        case other => fail(s"Expect pattern Aggregate(WithCTE(_)) but got $other")
      }

      // CTE as root node - WithCTE placed back where it was.
      val df6 = sql(
        s"""
           |with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |)
           |select count(v1.c3), count(v2.c3) from
           |v1 join (
           |  with
           |  v2 as (
           |    select c1, c2, rand() c3 from t
           |  )
           |  select * from v2
           |) v2 on v1.c1 = v2.c1
         """.stripMargin)
      checkAnswer(df6, Row(2, 2) :: Nil)
      df6.queryExecution.analyzed match {
        case WithCTE(_, cteDefs) => assert(cteDefs.length == 2)
        case other => fail(s"Expect pattern WithCTE(_) but got $other")
      }
    }
  }

  test("SPARK-44934: CTE column pruning handles duplicate exprIds in CTE") {
    withTempView("t") {
      Seq((0, 1, 2), (1, 2, 3)).toDF("c1", "c2", "c3").createOrReplaceTempView("t")
      val query =
        """
          |with cte as (
          |  select c1, c1, c2, c3 from t where random() > 0
          |)
          |select cte.c1, cte2.c1, cte.c2, cte2.c3 from
          |  (select c1, c2 from cte) cte
          |    inner join
          |  (select c1, c3 from cte) cte2
          |    on cte.c1 = cte2.c1
          """.stripMargin

      val df = sql(query)
      checkAnswer(df, Row(0, 0, 1, 2) :: Row(1, 1, 2, 3) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 1,
        "With-CTE should contain 1 CTE def after analysis.")
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.length == 2,
        "CTE should not be inlined after optimization.")
      assert(cteRepartitions.head.collectFirst {
        case p: Project if p.projectList.length == 4 => p
      }.isDefined, "CTE columns should not be pruned.")
    }
  }

  test("SPARK-49816: should only update out-going-ref-count for referenced outer CTE relation") {
    withView("v") {
      sql(
        """
          |WITH
          |t1 AS (SELECT 1 col),
          |t2 AS (SELECT * FROM t1)
          |SELECT * FROM t2
          |""".stripMargin).createTempView("v")
      // r1 is un-referenced, but it should not decrease the ref count of t2 inside view v.
      val df = sql(
        """
          |WITH
          |r1 AS (SELECT * FROM v),
          |r2 AS (SELECT * FROM v)
          |SELECT * FROM r2
          |""".stripMargin)
      checkAnswer(df, Row(1))
    }
  }
}

class CTEInlineSuiteAEOff extends CTEInlineSuiteBase with DisableAdaptiveExecutionSuite

class CTEInlineSuiteAEOn extends CTEInlineSuiteBase with EnableAdaptiveExecutionSuite {
  import testImplicits._

  test("SPARK-40105: Improve repartition in ReplaceCTERefWithRepartition") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select /*+ rebalance(c1) */ c1, c2, rand() from t
           |)
           |select * from v except select * from v
         """.stripMargin)
      checkAnswer(df, Nil)

      assert(!df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]))
      assert(df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RebalancePartitions]))
    }
  }
}

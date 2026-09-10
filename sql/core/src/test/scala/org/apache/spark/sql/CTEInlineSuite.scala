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

import org.apache.spark.sql.catalyst.analysis.{CurrentNamespace, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, GreaterThan, LessThan, Literal, Or, Rand}
import org.apache.spark.sql.catalyst.optimizer.InlineCTE
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class CTEInlineSuiteBase
  extends SharedSparkSession
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
        df.queryExecution.optimizedPlan.collectFirst {
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
        df.queryExecution.optimizedPlan.collectFirst {
          case r: RepartitionOperation => r
        }.isEmpty,
        "Deterministic CTEs should all be inlined after optimization.")
    }
  }

  test("SPARK-56921: plan normalization handles nested CTEs under union") {
    withTempView("input", "common") {
      Seq((1, 1, 10), (1, 2, 20), (2, 1, 30))
        .toDF("a", "b", "value")
        .createOrReplaceTempView("input")

      sql(
        s"""with cte_common as (
           |  select a, b, sum(value) as value
           |  from input
           |  group by a, b
           |)
           |select * from cte_common
         """.stripMargin).createOrReplaceTempView("common")

      val left = sql(
        s"""with cte_a as (
           |  select a, sum(value) as value
           |  from common
           |  group by a
           |)
           |select a as id, value from cte_a
         """.stripMargin)

      val right = sql(
        s"""with cte_b as (
           |  select b, sum(value) as value
           |  from common
           |  group by b
           |)
           |select b as id, value from cte_b
         """.stripMargin)

      checkAnswer(
        left.union(right),
        Row(1, 30) :: Row(2, 30) :: Row(1, 40) :: Row(2, 20) :: Nil)
    }
  }

  test("SPARK-56921: plan normalization preserves recursive CTE loop refs") {
    val df = sql(
      s"""with recursive t(n) as (
         |  select 1
         |  union all
         |  select n + 1 from t where n < 3
         |)
         |select * from t
       """.stripMargin)

    val normalized = df.queryExecution.normalized
    val unionLoops = normalized.collect { case unionLoop: UnionLoop => unionLoop }

    assert(unionLoops.nonEmpty, "Recursive CTE should normalize with a UnionLoop.")
    unionLoops.foreach { unionLoop =>
      val unionLoopRefs = unionLoop.recursion.collect {
        case unionLoopRef: UnionLoopRef => unionLoopRef
      }

      assert(unionLoopRefs.nonEmpty, "Recursive CTE should normalize with a UnionLoopRef.")
      assert(
        unionLoopRefs.forall(_.loopId == unionLoop.id),
        "UnionLoopRef loop IDs should match the normalized UnionLoop ID.")
    }

    checkAnswer(df, Row(1) :: Row(2) :: Row(3) :: Nil)
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
      Seq((0, 1, 2, 3), (1, 2, 3, 4)).toDF("c1", "c2", "c3", "c4").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, c3, c4, rand() c5 from t
           |),
           |vv as (
           |  select v1.c1, v1.c2, rand() c6 from v v1, v v2
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

  test("SPARK-59434: non-deterministic predicates are not pushed into a CTE def") {
    withTempView("t") {
      Seq(0, 1, 2).toDF("c1").createOrReplaceTempView("t")
      // The CTE def is non-deterministic and referenced twice, so it is not inlined and the
      // references' predicates get OR-merged into the shared def. A reference keeps its own
      // predicate, so a non-deterministic one must not be pushed down as well.
      val df = sql(
        """with v as (select c1, rand(1) r from t)
          |select c1 from v where rand(2) < 0.5
          |union all
          |select c1 from v where rand(3) < 0.5
          |""".stripMargin)
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.nonEmpty,
        "Non-deterministic With-CTE with multiple references should not be inlined.")
      assert(
        cteRepartitions.forall(_.collectFirst {
          case f: Filter if f.condition.exists(_.isInstanceOf[Rand]) => f
        }.isEmpty),
        "Non-deterministic predicate should not be pushed down to the CTE def 'v'.")
      val randFilters = df.queryExecution.optimizedPlan.collect {
        case f: Filter if f.condition.exists(_.isInstanceOf[Rand]) => f
      }
      assert(randFilters.length == 2,
        "Each reference's non-deterministic predicate should be evaluated once.")
    }
  }

  test("SPARK-59434: deterministic conjuncts are still pushed into a CTE def") {
    withTempView("t") {
      Seq((0, 1), (1, 2), (2, 3)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        """with v as (select c1, c2, rand(1) r from t)
          |select c1 from v where c1 > 0 and rand(2) < 0.5
          |union all
          |select c1 from v where c1 < 2
          |""".stripMargin)
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.nonEmpty, "CTE should not be inlined after optimization.")
      // The non-deterministic conjunct stays at the reference, the deterministic ones are
      // still OR-merged and pushed into the definition.
      val distinctCteRepartitions = cteRepartitions.map(_.canonicalized).distinct
      assert(distinctCteRepartitions.length == 1)
      assert(
        distinctCteRepartitions.head.collectFirst {
          case f: Filter if f.condition.semanticEquals(
            Or(GreaterThan(f.output(0), Literal(0)), LessThan(f.output(0), Literal(2)))) => f
        }.isDefined,
        "Predicate 'c1 > 0 OR c1 < 2' should be pushed down to the CTE def 'v'.")
      assert(
        distinctCteRepartitions.head.collectFirst {
          case f: Filter if f.condition.exists(_.isInstanceOf[Rand]) => f
        }.isEmpty,
        "Non-deterministic predicate should not be pushed down to the CTE def 'v'.")
    }
  }

  test("SPARK-59434: a non-deterministic reference blocks push-down for its siblings") {
    withTempView("t") {
      Seq((0, 1), (1, 2), (2, 3)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        """with v as (select c1, c2, rand(1) r from t)
          |select c1 from v where rand(2) < 0.5
          |union all
          |select c1 from v where c1 > 0
          |""".stripMargin)
      val cteRepartitions = df.queryExecution.optimizedPlan.collect {
        case r: RepartitionOperation => r
      }
      assert(cteRepartitions.nonEmpty, "CTE should not be inlined after optimization.")
      // The first reference has no pushable predicate, so the combined predicate is TRUE and
      // the definition gets no filter. The sibling's 'c1 > 0' must not be pushed on its own,
      // which would drop rows the first reference needs.
      assert(
        cteRepartitions.forall(_.collectFirst { case f: Filter => f }.isEmpty),
        "CTE def 'v' should get no pushed-down filter.")
    }
  }

  test("SPARK-59434: a non-deterministic predicate is evaluated once per CTE reference") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempView("t") {
        spark.range(0, 6, 1, 1).selectExpr("cast(id as int) c1").createOrReplaceTempView("t")
        // `monotonically_increasing_id` counts the rows it sees within a partition, so each
        // reference drops exactly its own first row. A second evaluation in the shared def
        // would drop a row there as well, leaving fewer. `rand(1)` only keeps the def from
        // being inlined; column pruning removes it, so it never reaches a filter.
        val df = sql(
          """with v as (select c1, rand(1) r from t)
            |select c1 from v where monotonically_increasing_id() > 0
            |union all
            |select c1 from v where monotonically_increasing_id() > 0
            |""".stripMargin)
        assert(
          df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
          "Non-deterministic With-CTE with multiple references should not be inlined.")
        assert(df.count() === 10, "Each reference should drop only its own first row.")
      }
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
        case Aggregate(_, _, WithCTE(_, cteDefs), _) => assert(cteDefs.length == 2)
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
        case Aggregate(_, _, Join(_, SubqueryAlias(_, WithCTE(_, cteDefs)), _, _, _), _) =>
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
        case Aggregate(_, _, Join(_, SubqueryAlias(_, WithCTE(_: Union, cteDefs)), _, _, _), _) =>
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
        case Aggregate(_, _, Join(_, SubqueryAlias(_, Union(children, _, _)), _, _, _), _)
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
        case Aggregate(_, _, WithCTE(_, cteDefs), _) => assert(cteDefs.length == 2)
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

  test("SPARK-45752: Unreferenced CTE should all be checked by CheckAnalysis0") {
    val e = intercept[AnalysisException](sql(
      s"""
        |with
        |a as (select * from tab_non_exists),
        |b as (select * from a)
        |select 2
        |""".stripMargin))
    checkErrorTableNotFound(e, "`tab_non_exists`", ExpectedContext("tab_non_exists", 26, 39))

    withTable("tab_exists") {
      spark.sql("CREATE TABLE tab_exists(id INT) using parquet")
      val e = intercept[AnalysisException](sql(
        s"""
           |with
           |a as (select * from tab_exists),
           |b as (select * from a),
           |c as (select * from tab_non_exists),
           |d as (select * from c)
           |select 2
           |""".stripMargin))
      checkErrorTableNotFound(e, "`tab_non_exists`", ExpectedContext("tab_non_exists", 83, 96))
    }
  }

  test("SPARK-48307: not-inlined CTE references sibling") {
    val df = sql(
      """
        |WITH
        |v1 AS (SELECT 1 col),
        |v2 AS (SELECT col, rand() FROM v1)
        |SELECT l.col FROM v2 l JOIN v2 r ON l.col = r.col
        |""".stripMargin)
    checkAnswer(df, Row(1))
  }

  test("SPARK-49816: detect self-contained WithCTE nodes") {
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

  test("SPARK-49816: complicated reference count") {
    // Manually build the logical plan for
    // WITH
    //  r1 AS (SELECT random()),
    //  r2 AS (
    //    WITH
    //      t1 AS (SELECT * FROM r1),
    //      t2 AS (SELECT * FROM r1)
    //    SELECT * FROM t2
    //  )
    // SELECT * FROM r2
    // r1 should be inlined as it's only referenced once: main query -> r2 -> t2 -> r1
    val r1 = CTERelationDef(Project(Seq(Alias(Rand(Literal(0)), "r")()), OneRowRelation()))
    val r1Ref = CTERelationRef(r1.id, r1.resolved, r1.output, r1.isStreaming)
    val t1 = CTERelationDef(Project(r1.output, r1Ref))
    val t2 = CTERelationDef(Project(r1.output, r1Ref))
    val t2Ref = CTERelationRef(t2.id, t2.resolved, t2.output, t2.isStreaming)
    val r2 = CTERelationDef(WithCTE(Project(t2.output, t2Ref), Seq(t1, t2)))
    val r2Ref = CTERelationRef(r2.id, r2.resolved, r2.output, r2.isStreaming)
    val query = WithCTE(Project(r2.output, r2Ref), Seq(r1, r2))
    val inlined = InlineCTE().apply(query)
    assert(!inlined.exists(_.isInstanceOf[WithCTE]))
  }

  test("SPARK-49816: complicated reference count 2") {
    // Manually build the logical plan for
    // WITH
    //  r1 AS (SELECT random()),
    //  r2 AS (
    //    WITH
    //      t1 AS (SELECT * FROM r1),
    //      t2 AS (SELECT * FROM t1)
    //    SELECT * FROM t2
    //  )
    // SELECT * FROM r1
    // This is similar to the previous test case, but t2 reference t1 instead of r1, and the main
    // query references r1. r1 should be inlined as r2 is not referenced at all.
    val r1 = CTERelationDef(Project(Seq(Alias(Rand(Literal(0)), "r")()), OneRowRelation()))
    val r1Ref = CTERelationRef(r1.id, r1.resolved, r1.output, r1.isStreaming)
    val t1 = CTERelationDef(Project(r1.output, r1Ref))
    val t1Ref = CTERelationRef(t1.id, t1.resolved, t1.output, t1.isStreaming)
    val t2 = CTERelationDef(Project(t1.output, t1Ref))
    val t2Ref = CTERelationRef(t2.id, t2.resolved, t2.output, t2.isStreaming)
    val r2 = CTERelationDef(WithCTE(Project(t2.output, t2Ref), Seq(t1, t2)))
    val query = WithCTE(Project(r1.output, r1Ref), Seq(r1, r2))
    val inlined = InlineCTE().apply(query)
    assert(!inlined.exists(_.isInstanceOf[WithCTE]))
  }

  test("SPARK-49816: complicated reference count 3") {
    // Manually build the logical plan for
    // WITH
    //  r1 AS (
    //    WITH
    //      t1 AS (SELECT random()),
    //      t2 AS (SELECT * FROM t1)
    //    SELECT * FROM t2
    //  ),
    //  r2 AS (
    //    WITH
    //      t1 AS (SELECT random()),
    //      t2 AS (SELECT * FROM r1)
    //    SELECT * FROM t2
    //  )
    // SELECT * FROM r1 UNION ALL SELECT * FROM r2
    // The inner WITH in r1 and r2 should become `SELECT random()` and r1/r2 should be inlined.
    val t1 = CTERelationDef(Project(Seq(Alias(Rand(Literal(0)), "r")()), OneRowRelation()))
    val t1Ref = CTERelationRef(t1.id, t1.resolved, t1.output, t1.isStreaming)
    val t2 = CTERelationDef(Project(t1.output, t1Ref))
    val t2Ref = CTERelationRef(t2.id, t2.resolved, t2.output, t2.isStreaming)
    val cte = WithCTE(Project(t2.output, t2Ref), Seq(t1, t2))
    val r1 = CTERelationDef(cte)
    val r1Ref = CTERelationRef(r1.id, r1.resolved, r1.output, r1.isStreaming)
    val r2 = CTERelationDef(cte)
    val r2Ref = CTERelationRef(r2.id, r2.resolved, r2.output, r2.isStreaming)
    val query = WithCTE(Union(r1Ref, r2Ref), Seq(r1, r2))
    val inlined = InlineCTE().apply(query)
    assert(!inlined.exists(_.isInstanceOf[WithCTE]))
  }

  test("SPARK-51109: CTE in subquery expression as grouping column") {
    withTable("t") {
      Seq(1 -> 1).toDF("c1", "c2").write.saveAsTable("t")
      withView("v") {
        sql(
          """
            |CREATE VIEW v AS
            |WITH r AS (SELECT c1 + c2 AS c FROM t)
            |SELECT * FROM r
            |""".stripMargin)
        checkAnswer(
          sql("SELECT (SELECT max(c) FROM v WHERE c > id) FROM range(1) GROUP BY 1"),
          Row(2)
        )
      }
    }
  }

  test("SPARK-51625: command in CTE relations should trigger inline") {
    val plan = UnresolvedWith(
      child = UnresolvedRelation(Seq("t")),
      cteRelations = Seq(UnresolvedCTERelation(
        "t", SubqueryAlias("t", ShowTables(CurrentNamespace, pattern = None))))
    )
    assert(!spark.sessionState.analyzer.execute(plan).exists {
      case _: WithCTE => true
      case _ => false
    })
  }

  test("SPARK-52818: MergeSubplans should not create nested WithCTE with cross-scope refs") {
    // A non-deterministic CTE referenced in multiple scalar subqueries is not inlined, leaving
    // a WithCTE. .show() adds a Limit, so the top node is not WithCTE and MergeSubplans runs,
    // merging scalar subqueries into a new outer WithCTE whose CTE defs reference the inner
    // WithCTE's defs. ReplaceCTERefWithRepartition then crashes processing the outer defs
    // before the inner ones are in the map.
    withTempView("t") {
      Seq(("a", "b"), ("c", "d")).toDF("c1", "c2").createOrReplaceTempView("t")
      sql(
        """WITH cte AS (
          |  SELECT c1, c2, monotonically_increasing_id() AS id FROM t
          |),
          |agg1 AS (
          |  SELECT c1, count(*) / (SELECT count(c1) FROM cte) AS r
          |  FROM cte WHERE c1 IS NOT NULL GROUP BY c1
          |),
          |agg2 AS (
          |  SELECT c2, count(*) / (SELECT count(c2) FROM cte) AS r
          |  FROM cte WHERE c2 IS NOT NULL GROUP BY c2
          |)
          |SELECT b.c1, a1.r, a2.r FROM cte b
          |LEFT JOIN agg1 a1 ON b.c1 = a1.c1
          |LEFT JOIN agg2 a2 ON b.c2 = a2.c2
          |""".stripMargin).show()
    }
  }

  test("MATERIALIZED CTE referenced multiple times is evaluated once") {
    withTempView("t") {
      Seq((0, 1), (1, 2), (2, 3)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as materialized (
           |  select c1, c2 from t
           |)
           |select * from v where c1 = 0 union all select * from v where c2 > 2
         """.stripMargin)
      checkAnswer(df, Row(0, 1) :: Row(2, 3) :: Nil)
      assert(
        df.queryExecution.optimizedPlan.collect {
          case r: RepartitionOperation => r
        }.length == 2,
        "MATERIALIZED CTE should not be inlined.")
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 1,
        "MATERIALIZED CTE should be evaluated once and reused.")
    }
  }

  test("MATERIALIZED CTE referenced once is not inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as materialized (
           |  select c1, c2 from t
           |)
           |select c1 from v where c2 > 1
         """.stripMargin)
      checkAnswer(df, Row(1) :: Nil)
      assert(
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "MATERIALIZED CTE should not be inlined even if it is referenced once.")
    }
  }

  test("MATERIALIZED CTE in subquery expression") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""select c1 from t where c2 in (
           |  with v as materialized (select c2 from t) select c2 from v where c2 > 1
           |)
         """.stripMargin)
      checkAnswer(df, Row(1) :: Nil)
      assert(
        df.queryExecution.optimizedPlan.collectWithSubqueries {
          case r: RepartitionOperation => r
        }.nonEmpty,
        "MATERIALIZED CTE in subquery should not be inlined.")
    }
  }

  test("NOT MATERIALIZED non-deterministic CTE referenced multiple times is inlined") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as not materialized (
           |  select c1, c2, rand() c3 from t
           |)
           |select count(*) from (select c1 from v union all select c1 from v)
         """.stripMargin)
      checkAnswer(df, Row(4) :: Nil)
      assert(
        !df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "NOT MATERIALIZED CTE should be inlined even if it is non-deterministic and " +
          "referenced multiple times.")
    }
  }

  test("MATERIALIZED CTE cannot reference the outer query") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      def assertMaterializedCTEError(query: String): Unit = {
        checkError(
          exception = intercept[AnalysisException](sql(query)),
          condition = "UNSUPPORTED_FEATURE.MATERIALIZED_CTE_WITH_OUTER_REFERENCE",
          parameters = Map("colName" -> "`c1`"),
          context = ExpectedContext(
            fragment = "t.c1",
            start = query.lastIndexOf("t.c1"),
            stop = query.lastIndexOf("t.c1") + 3))
      }
      def query(option: String): String = {
        s"""select * from t where exists (
           |  with v as $option (select 1 from t t2 where t2.c1 = t.c1) select * from v
           |)""".stripMargin
      }
      // A correlated CTE is fine when it is inlined.
      checkAnswer(sql(query("")), Row(0, 1) :: Row(1, 2) :: Nil)
      checkAnswer(sql(query("not materialized")), Row(0, 1) :: Row(1, 2) :: Nil)
      assertMaterializedCTEError(query("materialized"))
      // The outer reference can also come from another CTE that the MATERIALIZED CTE references,
      // defined in the same WITH clause or in an enclosing query.
      assertMaterializedCTEError(
        """select * from t where exists (
          |  with v1 as (select 1 from t t2 where t2.c1 = t.c1),
          |       v2 as materialized (select * from v1)
          |  select * from v2
          |)""".stripMargin)
      assertMaterializedCTEError(
        """select * from t where exists (
          |  with v1 as (select 1 from t t2 where t2.c1 = t.c1)
          |  select * from v1 where exists (
          |    with v2 as materialized (select * from v1) select * from v2
          |  )
          |)""".stripMargin)
    }
  }

  test("MATERIALIZED CTE with an inner CTE correlated to its own relations") {
    withTempView("t", "t2") {
      Seq((0, 1), (1, 2), (2, 3)).toDF("c1", "c2").createOrReplaceTempView("t")
      Seq((0, 10), (1, 20), (1, 30)).toDF("c1", "c2").createOrReplaceTempView("t2")
      // The inner CTE references `a`, a relation of the MATERIALIZED CTE definition itself, so the
      // outer reference does not cross the materialized boundary.
      Seq(
        """with v as materialized (
          |  select a.c1, (with u as (select max(c2) m from t2 where t2.c1 = a.c1)
          |                select m from u) as m
          |  from t a)
          |select * from v""".stripMargin,
        """with v as materialized (
          |  select a.c1, l.m
          |  from t a, lateral (with u as (select max(c2) m from t2 where t2.c1 = a.c1)
          |                     select m from u) l)
          |select * from v""".stripMargin).foreach { query =>
        val df = sql(query)
        checkAnswer(df, Row(0, 10) :: Row(1, 30) :: Row(2, null) :: Nil)
        assert(
          df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
          "MATERIALIZED CTE should not be inlined.")
      }
    }
  }

  test("MATERIALIZED CTE referencing a CTE with an inner correlated CTE") {
    withTempView("t", "t2") {
      Seq((0, 1), (1, 2), (2, 3)).toDF("c1", "c2").createOrReplaceTempView("t")
      Seq((0, 10), (1, 20), (1, 30)).toDF("c1", "c2").createOrReplaceTempView("t2")
      // `v1` is inlined into the MATERIALIZED `v2`, and its inner CTE `u` is correlated to `v1`'s
      // own relation `a`, which is within the materialized boundary.
      val df = sql(
        """with v1 as (
          |  select a.c1, (with u as (select max(c2) m from t2 where t2.c1 = a.c1)
          |                select m from u) as m
          |  from t a),
          |v2 as materialized (select * from v1)
          |select * from v2""".stripMargin)
      checkAnswer(df, Row(0, 10) :: Row(1, 30) :: Row(2, null) :: Nil)
      assert(
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "MATERIALIZED CTE should not be inlined.")
    }
  }

  test("MATERIALIZED CTE correlated through a CTE shared with the enclosing query") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      // The enclosing query and the MATERIALIZED CTE both read `s`, so the outer reference and
      // the definition may share attribute ids. The correlation still crosses the boundary.
      val query =
        """with s as (select c1, c2 from t)
          |select * from s o where exists (
          |  with v as materialized (select i.c1 from s i where i.c1 = o.c1)
          |  select * from v
          |)""".stripMargin
      checkError(
        exception = intercept[AnalysisException](sql(query)),
        condition = "UNSUPPORTED_FEATURE.MATERIALIZED_CTE_WITH_OUTER_REFERENCE",
        parameters = Map("colName" -> "`c1`"),
        context = ExpectedContext(
          fragment = "o.c1",
          start = query.lastIndexOf("o.c1"),
          stop = query.lastIndexOf("o.c1") + 3))
    }
  }

  test("MATERIALIZED CTE referencing a correlated CTE through a subquery") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      // `s` is correlated to the enclosing query and inlined into the MATERIALIZED `v`, which
      // references it only inside a subquery.
      Seq(
        "select a.c1, (select count(*) from s) as n from t a",
        "select a.c1, l.n from t a, lateral (select count(*) n from s) l",
        "select a.c1 from t a where a.c1 in (select c1 from s)").foreach { body =>
        val query =
          s"""select * from t o where exists (
             |  with s as (select i.c1 from t i where i.c1 = o.c1),
             |       v as materialized ($body)
             |  select * from v
             |)""".stripMargin
        checkError(
          exception = intercept[AnalysisException](sql(query)),
          condition = "UNSUPPORTED_FEATURE.MATERIALIZED_CTE_WITH_OUTER_REFERENCE",
          parameters = Map("colName" -> "`c1`"),
          context = ExpectedContext(
            fragment = "o.c1",
            start = query.indexOf("o.c1"),
            stop = query.indexOf("o.c1") + 3))
      }
    }
  }

  test("MATERIALIZED CTE check does not preempt resolution errors") {
    withTempView("t", "t2") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      Seq((0, 10), (1, 20)).toDF("c1", "c2").createOrReplaceTempView("t2")
      // An unresolved column inside the definition, and one elsewhere in the query.
      Seq(
        """select * from t o where exists (
          |  with v as materialized (select no_such_col from t2 i where i.c1 = o.c1)
          |  select * from v
          |)""".stripMargin,
        """select * from t o where exists (
          |  with v as materialized (select 1 from t2 i where i.c1 = o.c1)
          |  select * from v
          |) and no_such_col = 1""".stripMargin).foreach { query =>
        val e = intercept[AnalysisException](sql(query))
        assert(e.getCondition == "UNRESOLVED_COLUMN.WITH_SUGGESTION")
      }
    }
  }

  test("non-deterministic predicates are not pushed into a MATERIALIZED CTE") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        "with v as materialized (select c1 from t) select count(*) from v where rand() < 0.5")
      // The reference keeps the predicate, so pushing it into the definition as well would
      // evaluate it twice.
      val randFilters = df.queryExecution.optimizedPlan.collect {
        case f: Filter if f.condition.exists(_.isInstanceOf[Rand]) => f
      }
      assert(randFilters.length == 1, "Non-deterministic predicate should be evaluated once.")
      assert(
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
        "MATERIALIZED CTE should not be inlined.")
    }
  }

  test("MATERIALIZED CTE in a correlated subquery") {
    withTempView("t", "t2") {
      Seq((0, 1), (1, 2), (2, 3)).toDF("c1", "c2").createOrReplaceTempView("t")
      Seq((0, 10), (1, 20), (1, 30)).toDF("c1", "c2").createOrReplaceTempView("t2")
      def assertCorrelatedSubqueryError(query: String): Unit = {
        val definition = "v as materialized (select c1 from t2)"
        checkError(
          exception = intercept[AnalysisException](sql(query)),
          condition = "UNSUPPORTED_FEATURE.MATERIALIZED_CTE_IN_CORRELATED_SUBQUERY",
          parameters = Map("cteName" -> "`v`"),
          context = ExpectedContext(
            fragment = definition,
            start = query.indexOf(definition),
            stop = query.indexOf(definition) + definition.length - 1))
      }
      // The query of the WITH clause references the outer query, so the `WithCTE` sits on the
      // correlated path that decorrelation cannot pass through.
      assertCorrelatedSubqueryError(
        """select * from t o where exists (
          |  with v as materialized (select c1 from t2) select * from v where v.c1 = o.c1
          |)""".stripMargin)
      assertCorrelatedSubqueryError(
        """select o.c1, (
          |  with v as materialized (select c1 from t2) select count(*) from v where v.c1 = o.c1
          |) from t o""".stripMargin)
      assertCorrelatedSubqueryError(
        """select * from t o, lateral (
          |  with v as materialized (select c1 from t2) select count(*) n from v where v.c1 = o.c1
          |) l""".stripMargin)
      // A correlation above a derived table holding the WITH clause, or no correlation at all.
      checkAnswer(
        sql("""select * from t o where exists (
              |  select * from (with v as materialized (select c1 from t2) select * from v) x
              |  where x.c1 = o.c1
              |)""".stripMargin),
        Row(0, 1) :: Row(1, 2) :: Nil)
      checkAnswer(
        sql("""select * from t o where o.c1 in (
              |  with v as materialized (select c1 from t2) select c1 from v
              |)""".stripMargin),
        Row(0, 1) :: Row(1, 2) :: Nil)
    }
  }

  test("RECURSIVE CTE with MATERIALIZED and NOT MATERIALIZED") {
    def query(option: String): String = {
      s"""with recursive r(n) as $option (select 1 union all select n + 1 from r where n < 5)
         |select * from r a join r b on a.n = b.n""".stripMargin
    }
    val expected = (1 to 5).map(n => Row(n, n))
    val materialized = sql(query("materialized"))
    checkAnswer(materialized, expected)
    assert(
      collectWithSubqueries(materialized.queryExecution.executedPlan) {
        case r: ReusedExchangeExec => r
      }.length == 1,
      "MATERIALIZED recursive CTE should be evaluated once and reused.")
    val inlined = sql(query("not materialized"))
    checkAnswer(inlined, expected)
    assert(
      !inlined.queryExecution.optimizedPlan.exists(_.isInstanceOf[RepartitionOperation]),
      "NOT MATERIALIZED recursive CTE should be inlined.")
  }

  test("MATERIALIZED CTE with an outer reference is rejected when creating a view") {
    withTempView("t", "mv") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val e = intercept[AnalysisException](sql(
        """create temporary view mv as
          |select * from t o where exists (
          |  with v as materialized (select 1 from t i where i.c1 = o.c1) select * from v
          |)""".stripMargin))
      assert(e.getCondition == "UNSUPPORTED_FEATURE.MATERIALIZED_CTE_WITH_OUTER_REFERENCE")
    }
  }

  test("MATERIALIZED CTE is rejected where CTEs are inlined during analysis") {
    withTempView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      def assertAlwaysInlinedError(query: String): Unit = {
        val definition = "v as materialized (select c1 from t)"
        checkError(
          exception = intercept[AnalysisException](sql(query)),
          condition = "UNSUPPORTED_FEATURE.MATERIALIZED_CTE_ALWAYS_INLINED",
          parameters = Map("cteName" -> "`v`"),
          context = ExpectedContext(
            fragment = definition,
            start = query.indexOf(definition),
            stop = query.indexOf(definition) + definition.length - 1))
      }
      withTable("a", "b") {
        sql("create table a(c1 int) using parquet")
        sql("create table b(c1 int) using parquet")
        // A multi-insert statement runs each insert as its own command.
        assertAlwaysInlinedError(
          """with v as materialized (select c1 from t)
            |from v
            |insert into a select c1
            |insert into b select c1""".stripMargin)
        withSQLConf(SQLConf.LEGACY_INLINE_CTE_IN_COMMANDS.key -> "true") {
          assertAlwaysInlinedError(
            "insert into a with v as materialized (select c1 from t) select c1 from v")
        }
      }
      withSQLConf(SQLConf.LEGACY_CTE_PRECEDENCE_POLICY.key -> "LEGACY") {
        assertAlwaysInlinedError("with v as materialized (select c1 from t) select * from v")
      }
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

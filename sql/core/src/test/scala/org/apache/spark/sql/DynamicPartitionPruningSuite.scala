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

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode._
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.connector.catalog.InMemoryTableCatalog
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingQueryWrapper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for the filtering ratio policy used to trigger dynamic partition pruning (DPP).
 */
abstract class DynamicPartitionPruningSuiteBase
    extends QueryTest
    with SharedSparkSession
    with GivenWhenThen
    with AdaptiveSparkPlanHelper {

  val tableFormat: String = "parquet"

  import testImplicits._

  protected def initState(): Unit = {}
  protected def runAnalyzeColumnCommands: Boolean = true

  override def beforeAll(): Unit = {
    super.beforeAll()

    initState()

    val factData = Seq[(Int, Int, Int, Int)](
      (1000, 1, 1, 10),
      (1010, 2, 1, 10),
      (1020, 2, 1, 10),
      (1030, 3, 2, 10),
      (1040, 3, 2, 50),
      (1050, 3, 2, 50),
      (1060, 3, 2, 50),
      (1070, 4, 2, 10),
      (1080, 4, 3, 20),
      (1090, 4, 3, 10),
      (1100, 4, 3, 10),
      (1110, 5, 3, 10),
      (1120, 6, 4, 10),
      (1130, 7, 4, 50),
      (1140, 8, 4, 50),
      (1150, 9, 1, 20),
      (1160, 10, 1, 20),
      (1170, 11, 1, 30),
      (1180, 12, 2, 20),
      (1190, 13, 2, 20),
      (1200, 14, 3, 40),
      (1200, 15, 3, 70),
      (1210, 16, 4, 10),
      (1220, 17, 4, 20),
      (1230, 18, 4, 20),
      (1240, 19, 5, 40),
      (1250, 20, 5, 40),
      (1260, 21, 5, 40),
      (1270, 22, 5, 50),
      (1280, 23, 1, 50),
      (1290, 24, 1, 50),
      (1300, 25, 1, 50)
    )

    val storeData = Seq[(Int, String, String)](
      (1, "North-Holland", "NL"),
      (2, "South-Holland", "NL"),
      (3, "Bavaria", "DE"),
      (4, "California", "US"),
      (5, "Texas", "US"),
      (6, "Texas", "US")
    )

    val storeCode = Seq[(Int, Int)](
      (1, 10),
      (2, 20),
      (3, 30),
      (4, 40),
      (5, 50),
      (6, 60)
    )

    spark.range(1000)
      .select($"id" as "product_id", ($"id" % 10) as "store_id", ($"id" + 1) as "code")
      .write
      .format(tableFormat)
      .mode("overwrite")
      .saveAsTable("product")

    factData.toDF("date_id", "store_id", "product_id", "units_sold")
      .write
      .format(tableFormat)
      .saveAsTable("fact_np")

    factData.toDF("date_id", "store_id", "product_id", "units_sold")
      .write
      .partitionBy("store_id")
      .format(tableFormat)
      .saveAsTable("fact_sk")

    factData.toDF("date_id", "store_id", "product_id", "units_sold")
      .write
      .partitionBy("store_id")
      .format(tableFormat)
      .saveAsTable("fact_stats")

    storeData.toDF("store_id", "state_province", "country")
      .write
      .format(tableFormat)
      .saveAsTable("dim_store")

    storeData.toDF("store_id", "state_province", "country")
      .write
      .format(tableFormat)
      .saveAsTable("dim_stats")

    storeCode.toDF("store_id", "code")
      .write
      .partitionBy("store_id")
      .format(tableFormat)
      .saveAsTable("code_stats")

    if (runAnalyzeColumnCommands) {
      sql("ANALYZE TABLE fact_stats COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE dim_stats COMPUTE STATISTICS FOR COLUMNS store_id")
      sql("ANALYZE TABLE code_stats COMPUTE STATISTICS FOR COLUMNS store_id")
    }
  }

  override def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS fact_np")
      sql("DROP TABLE IF EXISTS fact_sk")
      sql("DROP TABLE IF EXISTS product")
      sql("DROP TABLE IF EXISTS dim_store")
      sql("DROP TABLE IF EXISTS fact_stats")
      sql("DROP TABLE IF EXISTS dim_stats")
    } finally {
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY)
      super.afterAll()
    }
  }

  /**
   * Check if the query plan has a partition pruning filter inserted as
   * a subquery duplicate or as a custom broadcast exchange.
   */
  def checkPartitionPruningPredicate(
      df: DataFrame,
      withSubquery: Boolean,
      withBroadcast: Boolean): Unit = {
    df.collect()

    val plan = df.queryExecution.executedPlan
    val dpExprs = collectDynamicPruningExpressions(plan)
    val hasSubquery = dpExprs.exists {
      case InSubqueryExec(_, _: SubqueryExec, _, _) => true
      case _ => false
    }
    val subqueryBroadcast = dpExprs.collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _) => b
    }

    val hasFilter = if (withSubquery) "Should" else "Shouldn't"
    assert(hasSubquery == withSubquery,
      s"$hasFilter trigger DPP with a subquery duplicate:\n${df.queryExecution}")
    val hasBroadcast = if (withBroadcast) "Should" else "Shouldn't"
    assert(subqueryBroadcast.nonEmpty == withBroadcast,
      s"$hasBroadcast trigger DPP with a reused broadcast exchange:\n${df.queryExecution}")

    subqueryBroadcast.foreach { s =>
      s.child match {
        case _: ReusedExchangeExec => // reuse check ok.
        case BroadcastQueryStageExec(_, _: ReusedExchangeExec, _) => // reuse check ok.
        case b: BroadcastExchangeLike =>
          val hasReuse = plan.find {
            case ReusedExchangeExec(_, e) => e eq b
            case _ => false
          }.isDefined
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case a: AdaptiveSparkPlanExec =>
          val broadcastQueryStage = collectFirst(a) {
            case b: BroadcastQueryStageExec => b
          }
          val broadcastPlan = broadcastQueryStage.get.broadcast
          val hasReuse = find(plan) {
            case ReusedExchangeExec(_, e) => e eq broadcastPlan
            case b: BroadcastExchangeLike => b eq broadcastPlan
            case _ => false
          }.isDefined
          assert(hasReuse, s"$s\nshould have been reused in\n$plan")
        case _ =>
          fail(s"Invalid child node found in\n$s")
      }
    }

    val isMainQueryAdaptive = plan.isInstanceOf[AdaptiveSparkPlanExec]
    subqueriesAll(plan).filterNot(subqueryBroadcast.contains).foreach { s =>
      val subquery = s match {
        case r: ReusedSubqueryExec => r.child
        case o => o
      }
      assert(subquery.find(_.isInstanceOf[AdaptiveSparkPlanExec]).isDefined == isMainQueryAdaptive)
    }
  }

  /**
   * Check if the plan has the given number of distinct broadcast exchange subqueries.
   */
  def checkDistinctSubqueries(df: DataFrame, n: Int): Unit = {
    df.collect()

    val buf = collectDynamicPruningExpressions(df.queryExecution.executedPlan).collect {
      case InSubqueryExec(_, b: SubqueryBroadcastExec, _, _) =>
        b.index
    }
    assert(buf.distinct.size == n)
  }

  /**
   * Collect the children of all correctly pushed down dynamic pruning expressions in a spark plan.
   */
  private def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case s: FileSourceScanExec => s.partitionFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case s: BatchScanExec => s.runtimeFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case _ => Nil
    }
  }

  /**
   * Check if the plan contains unpushed dynamic pruning filters.
   */
  def checkUnpushedFilters(df: DataFrame): Boolean = {
    find(df.queryExecution.executedPlan) {
      case FilterExec(condition, _) =>
        splitConjunctivePredicates(condition).exists {
          case _: DynamicPruningExpression => true
          case _ => false
        }
      case _ => false
    }.isDefined
  }

  /**
   * Test the result of a simple join on mock-up tables
   */
  test("simple inner join triggers DPP with mock-up tables") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      val df = sql(
        """
          |SELECT f.date_id, f.store_id FROM fact_sk f
          |JOIN dim_store s ON f.store_id = s.store_id AND s.country = 'NL'
        """.stripMargin)

      checkPartitionPruningPredicate(df, true, false)

      checkAnswer(df, Row(1000, 1) :: Row(1010, 2) :: Row(1020, 2) :: Nil)
    }
  }

  /**
   * Test DPP is triggered by a self-join on a partitioned table
   */
  test("self-join on a partitioned table should not trigger DPP") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      withTable("fact") {
        sql(
          s"""
             |CREATE TABLE fact (
             |  col1 varchar(14), col2 bigint, col3 bigint, col4 decimal(18,8), partCol1 varchar(1)
             |) USING $tableFormat PARTITIONED BY (partCol1)
        """.stripMargin)

        val df = sql(
          """
            |SELECT b.col1 FROM fact a
            |JOIN
            |(SELECT * FROM (
            |    SELECT *, Lag(col4) OVER (PARTITION BY partCol1, col1 ORDER BY col2) prev_col4
            |    FROM (SELECT partCol1, col1, col2, col3, col4 FROM fact) subquery) subquery2
            |  WHERE  col3 = 0 AND col4 = prev_col4
            |) b
            |ON a.partCol1 = b.partCol1
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
      }
    }
  }

  test("DPP should not be rewritten as an existential join") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.5",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      val df = sql(
        s"""
           |SELECT * FROM product p WHERE p.store_id NOT IN
           | (SELECT f.store_id FROM fact_sk f JOIN dim_store d ON f.store_id = d.store_id
           |    WHERE d.state_province = 'NL'
           | )
       """.stripMargin)

      val found = df.queryExecution.executedPlan.find {
        case BroadcastHashJoinExec(_, _, p: ExistenceJoin, _, _, _, _, _) => true
        case _ => false
      }

      assert(found.isEmpty)
    }
  }

  /**
   * (1) DPP should be disabled when the large (fact) table isn't partitioned by the join key
   * (2) DPP should be triggered only for certain join types
   * (3) DPP should trigger only when we have attributes on both sides of the join condition
   */
  test("DPP triggers only for certain types of query") {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_PRUNING_SIDE_EXTRA_FILTER_RATIO.key -> "1") {
      Given("dynamic partition pruning disabled")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
        val df = sql(
          """
            |SELECT * FROM fact_sk f
            |LEFT SEMI JOIN dim_store s
            |ON f.store_id = s.store_id AND s.country = 'NL'
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
      }

      Given("not a partition column")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        val df = sql(
          """
            |SELECT * FROM fact_np f
            |JOIN dim_store s
            |ON f.date_id = s.store_id WHERE s.country = 'NL'
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
      }

      Given("no predicate on the dimension table")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        val df = sql(
          """
            |SELECT * FROM fact_sk f
            |JOIN dim_store s
            |ON f.store_id = s.store_id
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
      }

      Given("left-semi join with partition column on the left side")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        val df = sql(
          """
            |SELECT * FROM fact_sk f
            |LEFT SEMI JOIN dim_store s
            |ON f.store_id = s.store_id AND s.country = 'NL'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)
      }

      Given("left-semi join with partition column on the right side")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        val df = sql(
          """
            |SELECT * FROM dim_store s
            |LEFT SEMI JOIN fact_sk f
            |ON f.store_id = s.store_id AND s.country = 'NL'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)
      }

      Given("left outer with partition column on the left side")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
        val df = sql(
          """
            |SELECT * FROM fact_sk f
            |LEFT OUTER JOIN dim_store s
            |ON f.store_id = s.store_id WHERE f.units_sold = 10
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
      }

      Given("right outer join with partition column on the left side")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
        val df = sql(
          """
            |SELECT * FROM fact_sk f RIGHT OUTER JOIN dim_store s
            |ON f.store_id = s.store_id WHERE s.country = 'NL'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)
      }
    }
  }

  /**
   * The filtering policy has a fallback when the stats are unavailable
   */
  test("filtering ratio policy fallback") {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      Given("no stats and selective predicate")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
        val df = sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_sk f
            |JOIN dim_store s
            |ON f.store_id = s.store_id WHERE s.country LIKE '%C_%'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)
      }

      Given("no stats and selective predicate with the size of dim too large")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
        sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id
            |FROM fact_sk f WHERE store_id < 5
          """.stripMargin)
          .write
          .partitionBy("store_id")
          .saveAsTable("fact_aux")

        val df = sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id
            |FROM fact_aux f JOIN dim_store s
            |ON f.store_id = s.store_id WHERE s.country = 'US'
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)

        checkAnswer(df,
          Row(1070, 2, 10, 4) ::
          Row(1080, 3, 20, 4) ::
          Row(1090, 3, 10, 4) ::
          Row(1100, 3, 10, 4) :: Nil
        )
      }

      Given("no stats and selective predicate with the size of dim small")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
        val df = sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_sk f
            |JOIN dim_store s
            |ON f.store_id = s.store_id WHERE s.country = 'NL'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)

        checkAnswer(df,
          Row(1010, 1, 10, 2) ::
          Row(1020, 1, 10, 2) ::
          Row(1000, 1, 10, 1) :: Nil
        )
      }
    }
  }

  /**
   *  The filtering ratio policy performs best when it uses cardinality estimates
   */
  test("filtering ratio policy with stats when the broadcast pruning is disabled") {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      Given("disabling the use of stats in the DPP heuristic")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "false") {
        val df = sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
            |JOIN dim_stats s
            |ON f.store_id = s.store_id WHERE s.country = 'DE'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)
      }

      Given("filtering ratio with stats disables pruning")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
        val df = sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
            |JOIN dim_stats s
            |ON (f.store_id = s.store_id) WHERE s.store_id > 0 AND s.store_id IN
            |(SELECT p.store_id FROM product p)
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
      }

      Given("filtering ratio with stats enables pruning")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
        val df = sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
            |JOIN dim_stats s
            |ON f.store_id = s.store_id WHERE s.country = 'DE'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)

        checkAnswer(df,
          Row(1030, 2, 10, 3) ::
          Row(1040, 2, 50, 3) ::
          Row(1050, 2, 50, 3) ::
          Row(1060, 2, 50, 3) :: Nil
        )
      }

      Given("join condition more complex than fact.attr = dim.attr")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
        val df = sql(
          """
            |SELECT f.date_id, f.product_id, f.units_sold, f.store_id
            |FROM fact_stats f JOIN dim_stats s
            |ON f.store_id + 1 = s.store_id WHERE s.country = 'DE'
          """.stripMargin)

        checkPartitionPruningPredicate(df, true, false)

        checkAnswer(df,
          Row(1010, 1, 10, 2) ::
          Row(1020, 1, 10, 2) :: Nil
        )
      }
    }
  }

  test("partition pruning in broadcast hash joins with non-deterministic probe part") {
    Given("alias with simple join condition, and non-deterministic query")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.pid, f.sid FROM
          |(SELECT date_id, product_id AS pid, store_id AS sid
          |   FROM fact_stats WHERE RAND() > 0.5) AS f
          |JOIN dim_stats s
          |ON f.sid = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, false)
    }

    Given("alias over multiple sub-queries with simple join condition")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.pid, f.sid FROM
          |(SELECT date_id, pid_d AS pid, sid_d AS sid FROM
          |  (SELECT date_id, product_id AS pid_d, store_id AS sid_d FROM fact_stats
          |  WHERE RAND() > 0.5) fs
          |  JOIN dim_store ds ON fs.sid_d = ds.store_id) f
          |JOIN dim_stats s
          |ON f.sid  = s.store_id  WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, false)
    }
  }

  test("partition pruning in broadcast hash joins with aliases") {
    Given("alias with simple join condition, using attribute names only")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.pid, f.sid FROM
          |(select date_id, product_id as pid, store_id as sid from fact_stats) as f
          |JOIN dim_stats s
          |ON f.sid = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1030, 2, 3) ::
        Row(1040, 2, 3) ::
        Row(1050, 2, 3) ::
        Row(1060, 2, 3) :: Nil
      )
    }

    Given("alias with expr as join condition")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.pid, f.sid FROM
          |(SELECT date_id, product_id AS pid, store_id AS sid FROM fact_stats) AS f
          |JOIN dim_stats s
          |ON f.sid + 1 = s.store_id + 1 WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1030, 2, 3) ::
        Row(1040, 2, 3) ::
        Row(1050, 2, 3) ::
        Row(1060, 2, 3) :: Nil
      )
    }

    Given("alias over multiple sub-queries with simple join condition")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.pid, f.sid FROM
          |(SELECT date_id, pid_d AS pid, sid_d AS sid FROM
          |  (select date_id, product_id AS pid_d, store_id AS sid_d FROM fact_stats) fs
          |  JOIN dim_store ds ON fs.sid_d = ds.store_id) f
          |JOIN dim_stats s
          |ON f.sid  = s.store_id  WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1030, 2, 3) ::
          Row(1040, 2, 3) ::
          Row(1050, 2, 3) ::
          Row(1060, 2, 3) :: Nil
      )
    }

    Given("alias over multiple sub-queries with simple join condition")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.pid_d as pid, f.sid_d as sid FROM
          | (SELECT date_id, pid_dd AS pid_d, sid_dd AS sid_d FROM
          |  (
          |   (select date_id, product_id AS pid_dd, store_id AS sid_dd FROM fact_stats) fss
          |     JOIN dim_store ds ON fss.sid_dd = ds.store_id
          |   ) fs
          |  JOIN dim_store ds ON fs.sid_dd = ds.store_id
          | ) f
          |JOIN dim_stats s
          |ON f.sid_d  = s.store_id  WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1030, 2, 3) ::
          Row(1040, 2, 3) ::
          Row(1050, 2, 3) ::
          Row(1060, 2, 3) :: Nil
      )
    }
  }

  test("partition pruning in broadcast hash joins") {
    Given("disable broadcast pruning and disable subquery duplication")
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      val df = sql(
        """
          |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
          |JOIN dim_stats s
          |ON f.store_id = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, false)

      checkAnswer(df,
        Row(1030, 2, 10, 3) ::
        Row(1040, 2, 50, 3) ::
        Row(1050, 2, 50, 3) ::
        Row(1060, 2, 50, 3) :: Nil
      )
    }

    Given("disable reuse broadcast results and enable subquery duplication")
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "0.5",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      val df = sql(
        """
          |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
          |JOIN dim_stats s
          |ON f.store_id = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, true, false)

      checkAnswer(df,
        Row(1030, 2, 10, 3) ::
        Row(1040, 2, 50, 3) ::
        Row(1050, 2, 50, 3) ::
        Row(1060, 2, 50, 3) :: Nil
      )
    }

    Given("enable reuse broadcast results and disable query duplication")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
          |JOIN dim_stats s
          |ON f.store_id = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1030, 2, 10, 3) ::
        Row(1040, 2, 50, 3) ::
        Row(1050, 2, 50, 3) ::
        Row(1060, 2, 50, 3) :: Nil
      )
    }

    Given("disable broadcast hash join and disable query duplication")
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df = sql(
        """
          |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
          |JOIN dim_stats s
          |ON f.store_id = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, false)

      checkAnswer(df,
        Row(1030, 2, 10, 3) ::
        Row(1040, 2, 50, 3) ::
        Row(1050, 2, 50, 3) ::
        Row(1060, 2, 50, 3) :: Nil
      )
    }

    Given("disable broadcast hash join and enable query duplication")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
          |JOIN dim_stats s
          |ON f.store_id = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      checkPartitionPruningPredicate(df, true, false)

      checkAnswer(df,
        Row(1030, 2, 10, 3) ::
        Row(1040, 2, 50, 3) ::
        Row(1050, 2, 50, 3) ::
        Row(1060, 2, 50, 3) :: Nil
      )
    }
  }

  test("broadcast a single key in a HashedRelation") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("fact", "dim") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          (($"id" * 20) % 100).as("mod"),
          ($"id" + 1).cast("string").as("str"))
          .write.partitionBy("one", "two", "three", "str")
          .format(tableFormat).mode("overwrite").saveAsTable("fact")

        spark.range(10).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          ($"id" * 10).as("prod"),
          ($"id" + 1).cast("string").as("str"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dim")

        // broadcast a single Long key
        val dfLong = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.one = d.one)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfLong, Row(9, 10, 11, "10") :: Nil)

        // reuse a single Byte key
        val dfByte = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.two = d.two)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfByte, Row(9, 10, 11, "10") :: Nil)

        // reuse a single String key
        val dfStr = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.str = d.str)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfStr, Row(9, 10, 11, "10") :: Nil)
      }
    }
  }

  test("broadcast multiple keys in a LongHashedRelation") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("fact", "dim") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          (($"id" * 20) % 100).as("mod"),
          ($"id" % 10).cast("string").as("str"))
          .write.partitionBy("one", "two", "three")
          .format(tableFormat).mode("overwrite").saveAsTable("fact")

        spark.range(10).select(
          $"id",
          ($"id" + 1).cast("int").as("one"),
          ($"id" + 2).cast("byte").as("two"),
          ($"id" + 3).cast("short").as("three"),
          ($"id" * 10).as("prod"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dim")

        // broadcast multiple keys
        val dfLong = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.one = d.one and f.two = d.two and f.three = d.three)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(dfLong, Row(9, 10, 11, "9") :: Nil)
      }
    }
  }

  test("broadcast multiple keys in an UnsafeHashedRelation") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("fact", "dim") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).cast("string").as("one"),
          ($"id" + 2).cast("string").as("two"),
          ($"id" + 3).cast("string").as("three"),
          (($"id" * 20) % 100).as("mod"),
          ($"id" % 10).cast("string").as("str"))
          .write.partitionBy("one", "two", "three")
          .format(tableFormat).mode("overwrite").saveAsTable("fact")

        spark.range(10).select(
          $"id",
          ($"id" + 1).cast("string").as("one"),
          ($"id" + 2).cast("string").as("two"),
          ($"id" + 3).cast("string").as("three"),
          ($"id" * 10).as("prod"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dim")

        // broadcast multiple keys
        val df = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.one = d.one and f.two = d.two and f.three = d.three)
            |WHERE d.prod > 80
          """.stripMargin)

        checkAnswer(df, Row(9, "10", "11", "9") :: Nil)
      }
    }
  }

  test("different broadcast subqueries with identical children") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("fact", "dim") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).cast("string").as("one"),
          ($"id" + 2).cast("string").as("two"),
          ($"id" + 3).cast("string").as("three"),
          (($"id" * 20) % 100).as("mod"),
          ($"id" % 10).cast("string").as("str"))
          .write.partitionBy("one", "two", "three")
          .format(tableFormat).mode("overwrite").saveAsTable("fact")

        spark.range(10).select(
          $"id",
          ($"id" + 1).cast("string").as("one"),
          ($"id" + 2).cast("string").as("two"),
          ($"id" + 3).cast("string").as("three"),
          ($"id" * 10).as("prod"))
          .write.partitionBy("one", "two", "three", "prod")
          .format(tableFormat).mode("overwrite").saveAsTable("dim")

        // we are expecting three filters on different keys to be pushed down
        val df = sql(
          """
            |SELECT f.id, f.one, f.two, f.str FROM fact f
            |JOIN dim d
            |ON (f.one = d.one and f.two = d.two and f.three = d.three)
            |WHERE d.prod > 80
          """.stripMargin)

        checkDistinctSubqueries(df, 3)
        checkAnswer(df, Row(9, "10", "11", "9") :: Nil)
      }
    }
  }

  test("no partition pruning when the build side is a stream") {
    withTable("fact") {
      val input = MemoryStream[Int]
      val stream = input.toDF.select($"value" as "one", ($"value" * 3) as "code")
      spark.range(100).select(
        $"id",
        ($"id" + 1).as("one"),
        ($"id" + 2).as("two"),
        ($"id" + 3).as("three"))
        .write.partitionBy("one")
        .format(tableFormat).mode("overwrite").saveAsTable("fact")
      val table = sql("SELECT * from fact f")

      // join a partitioned table with a stream
      val joined = table.join(stream, Seq("one")).where("code > 40")
      val query = joined.writeStream.format("memory").queryName("test").start()
      input.addData(1, 10, 20, 40, 50)
      try {
        query.processAllAvailable()
      } finally {
        query.stop()
      }
      // search dynamic pruning predicates on the executed plan
      val plan = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
      val ret = plan.find {
        case s: FileSourceScanExec => s.partitionFilters.exists {
          case _: DynamicPruningExpression => true
          case _ => false
        }
        case _ => false
      }
      assert(ret.isDefined == false)
    }
  }

  test("avoid reordering broadcast join keys to match input hash partitioning") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("large", "dimTwo", "dimThree") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).as("A"),
          ($"id" + 2).as("B"))
          .write.partitionBy("A")
          .format(tableFormat).mode("overwrite").saveAsTable("large")

        spark.range(10).select(
          $"id",
          ($"id" + 1).as("C"),
          ($"id" + 2).as("D"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dimTwo")

        spark.range(10).select(
          $"id",
          ($"id" + 1).as("E"),
          ($"id" + 2).as("F"),
          ($"id" + 3).as("G"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dimThree")

        val fact = sql("SELECT * from large")
        val dim = sql("SELECT * from dimTwo")
        val prod = sql("SELECT * from dimThree")

        // The query below first joins table fact with table dim on keys (A, B), and then joins
        // table fact with table prod on keys (B, A). The join key reordering in EnsureRequirements
        // ensured that the order of the keys stays the same (A, B) in both joins. The keys in a
        // broadcast shuffle should not be reordered in order to trigger broadcast reuse.
        val df = fact.join(dim,
          fact.col("A") === dim.col("C") && fact.col("B") === dim.col("D"), "LEFT")
          .join(broadcast(prod),
            fact.col("B") === prod.col("F") && fact.col("A") === prod.col("E"))
          .where(prod.col("G") > 5)

        checkPartitionPruningPredicate(df, false, true)
      }
    }
  }

  /**
   * This test is a small reproduction of the Query-23 of the TPCDS benchmark.
   * The query employs an aggregation on the result of a join between a store table and a
   * date dimension table which is further joined with item, date, and store tables using
   * a disjoint filter. The outcome of this query is a sequence of nested joins that have
   * duplicated partitioning keys, also used to uniquely identify the dynamic pruning filters.
   */
  test("dynamic partition pruning ambiguity issue across nested joins") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("store", "date", "item") {
        spark.range(500)
          .select((($"id" + 30) % 50).as("ss_item_sk"),
            ($"id" % 20).as("ss_sold_date_sk"), ($"id" * 3).as("price"))
          .write.partitionBy("ss_sold_date_sk")
          .format("parquet").mode("overwrite").saveAsTable("store")

        spark.range(20)
          .select($"id".as("d_date_sk"), ($"id").as("d_year"))
          .write.format("parquet").mode("overwrite").saveAsTable("date")

        spark.range(20)
          .select(($"id" + 30).as("i_item_sk"))
          .write.format("parquet").mode("overwrite").saveAsTable("item")

        val df = sql(
          """
            |WITH aux AS
            |(SELECT i_item_sk as frequent_item_sk FROM store, item, date
            |WHERE ss_sold_date_sk = d_date_sk
            |AND ss_item_sk = i_item_sk
            |AND d_year IN (2, 4, 6, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
            |GROUP BY i_item_sk HAVING count(*) > 0)
            |SELECT sum(sales) a
            |    FROM (SELECT price sales FROM item, date, aux, store
            |    WHERE d_year IN (1, 3, 5, 7)
            |      AND ss_sold_date_sk = d_date_sk
            |      AND ss_item_sk = i_item_sk
            |      AND i_item_sk = frequent_item_sk) x
          """.stripMargin)

        checkAnswer(df, Row(28080) :: Nil)
      }
    }
  }

  test("cleanup any DPP filter that isn't pushed down due to expression id clashes") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withTable("fact", "dim") {
        spark.range(20).select($"id".as("A"), $"id".as("AA"))
          .write.partitionBy("A").format(tableFormat).mode("overwrite").saveAsTable("fact")
        spark.range(10).select($"id".as("B"), $"id".as("BB"))
          .write.format(tableFormat).mode("overwrite").saveAsTable("dim")
        val df = sql(
          """
            |SELECT A, AA FROM
            | (SELECT A, AA from fact
            | JOIN dim ON (A = B AND AA = BB) WHERE BB > 1)
            | JOIN dim ON (AA = BB AND A = B)
            |WHERE BB < 5
          """.stripMargin)

        assert(!checkUnpushedFilters(df))
      }
    }
  }

  test("cleanup any DPP filter that isn't pushed down due to non-determinism") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.pid, f.sid FROM
          |(SELECT date_id, product_id AS pid, store_id AS sid
          |   FROM fact_stats WHERE RAND() > 0.5) AS f
          |JOIN dim_stats s
          |ON f.sid = s.store_id WHERE s.country = 'DE'
        """.stripMargin)

      assert(!checkUnpushedFilters(df))
    }
  }

  test("join key with multiple references on the filtering plan") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName
    ) {
      // when enable AQE, the reusedExchange is inserted when executed.
      withTable("fact", "dim") {
        spark.range(100).select(
          $"id",
          ($"id" + 1).cast("string").as("a"),
          ($"id" + 2).cast("string").as("b"))
          .write.partitionBy("a", "b")
          .format(tableFormat).mode("overwrite").saveAsTable("fact")

        spark.range(10).select(
          $"id",
          ($"id" + 1).cast("string").as("x"),
          ($"id" + 2).cast("string").as("y"),
          ($"id" + 2).cast("string").as("z"),
          ($"id" + 2).cast("string").as("w"))
          .write
          .format(tableFormat).mode("overwrite").saveAsTable("dim")

        val df = sql(
          """
            |SELECT f.id, f.a, f.b FROM fact f
            |JOIN dim d
            |ON f.b + f.a = d.y + d.z
            |WHERE d.x = (SELECT avg(p.w) FROM dim p)
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, true)
      }
    }
  }

  test("Make sure dynamic pruning works on uncorrelated queries") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT d.store_id,
          |       SUM(f.units_sold),
          |       (SELECT SUM(f.units_sold)
          |        FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
          |        WHERE d.country = 'US') AS total_prod
          |FROM fact_stats f JOIN dim_stats d ON d.store_id = f.store_id
          |WHERE d.country = 'US'
          |GROUP BY 1
        """.stripMargin)
      checkAnswer(df, Row(4, 50, 70) :: Row(5, 10, 70) :: Row(6, 10, 70) :: Nil)

      val plan = df.queryExecution.executedPlan
      val countSubqueryBroadcasts =
        collectWithSubqueries(plan)({ case _: SubqueryBroadcastExec => 1 }).sum

      val countReusedSubqueryBroadcasts =
        collectWithSubqueries(plan)({ case ReusedSubqueryExec(_: SubqueryBroadcastExec) => 1}).sum

      assert(countSubqueryBroadcasts == 1)
      assert(countReusedSubqueryBroadcasts == 1)
    }
  }

  test("SPARK-32509: Unused Dynamic Pruning filter shouldn't affect " +
    "canonicalization and exchange reuse") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val df = sql(
          """ WITH view1 as (
            |   SELECT f.store_id FROM fact_stats f WHERE f.units_sold = 70
            | )
            |
            | SELECT * FROM view1 v1 join view1 v2 WHERE v1.store_id = v2.store_id
          """.stripMargin)

        checkPartitionPruningPredicate(df, false, false)
        val reuseExchangeNodes = collect(df.queryExecution.executedPlan) {
          case se: ReusedExchangeExec => se
        }
        assert(reuseExchangeNodes.size == 1, "Expected plan to contain 1 ReusedExchangeExec " +
          s"nodes. Found ${reuseExchangeNodes.size}")

        checkAnswer(df, Row(15, 15) :: Nil)
      }
    }
  }

  test("Plan broadcast pruning only when the broadcast can be reused") {
    Given("dynamic pruning filter on the build side")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT f.date_id, f.store_id, f.product_id, f.units_sold FROM fact_np f
          |JOIN code_stats s
          |ON f.store_id = s.store_id WHERE f.date_id <= 1030
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, false)

      checkAnswer(df,
        Row(1000, 1, 1, 10) ::
        Row(1010, 2, 1, 10) ::
        Row(1020, 2, 1, 10) ::
        Row(1030, 3, 2, 10) :: Nil
      )
    }

    Given("dynamic pruning filter on the probe side")
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """
          |SELECT /*+ BROADCAST(f)*/
          |f.date_id, f.store_id, f.product_id, f.units_sold FROM fact_np f
          |JOIN code_stats s
          |ON f.store_id = s.store_id WHERE f.date_id <= 1030
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1000, 1, 1, 10) ::
        Row(1010, 2, 1, 10) ::
        Row(1020, 2, 1, 10) ::
        Row(1030, 3, 2, 10) :: Nil
      )
    }
  }

  test("SPARK-32659: Fix the data issue when pruning DPP on non-atomic type") {
    Seq(NO_CODEGEN, CODEGEN_ONLY).foreach { mode =>
      Seq(true, false).foreach { pruning =>
        withSQLConf(
          SQLConf.CODEGEN_FACTORY_MODE.key -> mode.toString,
          SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> s"$pruning") {
          Seq("struct", "array").foreach { dataType =>
            val df = sql(
              s"""
                 |SELECT f.date_id, f.product_id, f.units_sold, f.store_id FROM fact_stats f
                 |JOIN dim_stats s
                 |ON $dataType(f.store_id) = $dataType(s.store_id) WHERE s.country = 'DE'
              """.stripMargin)

            if (pruning) {
              checkPartitionPruningPredicate(df, false, true)
            } else {
              checkPartitionPruningPredicate(df, false, false)
            }

            checkAnswer(df,
              Row(1030, 2, 10, 3) ::
              Row(1040, 2, 50, 3) ::
              Row(1050, 2, 50, 3) ::
              Row(1060, 2, 50, 3) :: Nil
            )
          }
        }
      }
    }
  }

  test("SPARK-32817: DPP throws error when the broadcast side is empty") {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      val df = sql(
        """
          |SELECT * FROM fact_sk f
          |JOIN dim_store s
          |ON f.store_id = s.store_id WHERE s.country = 'XYZ'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df, Nil)
    }
  }

  test("Subquery reuse across the whole plan",
    DisableAdaptiveExecution("DPP in AQE must reuse broadcast")) {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      withTable("df1", "df2") {
        spark.range(100)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df1")

        spark.range(10)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql(
          """
            |SELECT df1.id, df2.k
            |FROM df1 JOIN df2 ON df1.k = df2.k
            |WHERE df2.id < (SELECT max(id) FROM df2 WHERE id <= 2)
            |""".stripMargin)

        checkPartitionPruningPredicate(df, true, false)

        checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)

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
  }

  test("SPARK-34436: DPP support LIKE ANY/ALL expression") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
      val df = sql(
        """
          |SELECT date_id, product_id FROM fact_sk f
          |JOIN dim_store s
          |ON f.store_id = s.store_id WHERE s.country LIKE ANY ('%D%E%', '%A%B%')
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1030, 2) ::
        Row(1040, 2) ::
        Row(1050, 2) ::
        Row(1060, 2) :: Nil
      )
    }
  }

  test("SPARK-34595: DPP support RLIKE expression") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true") {
      val df = sql(
        """
          |SELECT date_id, product_id FROM fact_sk f
          |JOIN dim_store s
          |ON f.store_id = s.store_id WHERE s.country RLIKE '[DE|US]'
        """.stripMargin)

      checkPartitionPruningPredicate(df, false, true)

      checkAnswer(df,
        Row(1030, 2) ::
        Row(1040, 2) ::
        Row(1050, 2) ::
        Row(1060, 2) ::
        Row(1070, 2) ::
        Row(1080, 3) ::
        Row(1090, 3) ::
        Row(1100, 3) ::
        Row(1110, 3) ::
        Row(1120, 4) :: Nil
      )
    }
  }

  test("SPARK-32855: Filtering side can not broadcast by join type") {
    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "false",
      SQLConf.DYNAMIC_PARTITION_PRUNING_PRUNING_SIDE_EXTRA_FILTER_RATIO.key -> "1") {

      val sqlStr =
        """
          |SELECT s.store_id,f. product_id FROM dim_store s
          |LEFT JOIN fact_sk f
          |ON f.store_id = s.store_id WHERE s.country = 'NL'
          """.stripMargin

      // DPP will only apply if disable reuseBroadcastOnly
      Seq(true, false).foreach { reuseBroadcastOnly =>
        withSQLConf(
          SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> s"$reuseBroadcastOnly") {
          val df = sql(sqlStr)
          checkPartitionPruningPredicate(df, !reuseBroadcastOnly, false)
        }
      }

      // DPP will only apply if left side can broadcast by size
      Seq(1L, 100000L).foreach { threshold =>
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$threshold") {
          val df = sql(sqlStr)
          checkPartitionPruningPredicate(df, threshold > 10L, false)
        }
      }
    }
  }

  test("SPARK-34637: DPP side broadcast query stage is created firstly") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      val df = sql(
        """ WITH v as (
          |   SELECT f.store_id FROM fact_stats f WHERE f.units_sold = 70 group by f.store_id
          | )
          |
          | SELECT * FROM v v1 join v v2 WHERE v1.store_id = v2.store_id
        """.stripMargin)

      // A possible resulting query plan:
      // BroadcastHashJoin
      // +- HashAggregate
      //    +- ShuffleQueryStage
      //       +- Exchange
      //          +- HashAggregate
      //             +- Filter
      //                +- FileScan [PartitionFilters: dynamicpruning#3385]
      //                     +- SubqueryBroadcast dynamicpruning#3385
      //                        +- AdaptiveSparkPlan
      //                           +- BroadcastQueryStage
      //                              +- BroadcastExchange
      //
      // +- BroadcastQueryStage
      //    +- ReusedExchange

      checkPartitionPruningPredicate(df, false, true)
      checkAnswer(df, Row(15, 15) :: Nil)
    }
  }

  test("SPARK-35568: Fix UnsupportedOperationException when enabling both AQE and DPP") {
    val df = sql(
      """
        |SELECT s.store_id, f.product_id
        |FROM (SELECT DISTINCT * FROM fact_sk) f
        |  JOIN (SELECT
        |          *,
        |          ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY state_province DESC) AS rn
        |        FROM dim_store) s
        |   ON f.store_id = s.store_id
        |WHERE s.country = 'DE' AND s.rn = 1
        |""".stripMargin)

    checkAnswer(df, Row(3, 2) :: Row(3, 2) :: Row(3, 2) :: Row(3, 2) :: Nil)
  }
}

abstract class DynamicPartitionPruningV1Suite extends DynamicPartitionPruningSuiteBase {

  import testImplicits._

  /**
   * Check the static scan metrics with and without DPP
   */
  test("static scan metrics",
    DisableAdaptiveExecution("DPP in AQE must reuse broadcast")) {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false") {
      withTable("fact", "dim") {
        val numPartitions = 10

        spark.range(10)
          .map { x => Tuple3(x, x + 1, 0) }
          .toDF("did", "d1", "d2")
          .write
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("dim")

        spark.range(100)
          .map { x => Tuple2(x, x % numPartitions) }
          .toDF("f1", "fid")
          .write.partitionBy("fid")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("fact")

        def getFactScan(plan: SparkPlan): SparkPlan = {
          val scanOption =
            find(plan) {
              case s: FileSourceScanExec =>
                s.output.exists(_.find(_.argString(maxFields = 100).contains("fid")).isDefined)
              case s: BatchScanExec =>
                // we use f1 col for v2 tables due to schema pruning
                s.output.exists(_.find(_.argString(maxFields = 100).contains("f1")).isDefined)
              case _ => false
            }
          assert(scanOption.isDefined)
          scanOption.get
        }

        // No dynamic partition pruning, so no static metrics
        // All files in fact table are scanned
        val df1 = sql("SELECT sum(f1) FROM fact")
        df1.collect()
        val scan1 = getFactScan(df1.queryExecution.executedPlan)
        assert(!scan1.metrics.contains("staticFilesNum"))
        assert(!scan1.metrics.contains("staticFilesSize"))
        val allFilesNum = scan1.metrics("numFiles").value
        val allFilesSize = scan1.metrics("filesSize").value
        assert(scan1.metrics("numPartitions").value === numPartitions)
        assert(scan1.metrics("pruningTime").value === -1)

        // No dynamic partition pruning, so no static metrics
        // Only files from fid = 5 partition are scanned
        val df2 = sql("SELECT sum(f1) FROM fact WHERE fid = 5")
        df2.collect()
        val scan2 = getFactScan(df2.queryExecution.executedPlan)
        assert(!scan2.metrics.contains("staticFilesNum"))
        assert(!scan2.metrics.contains("staticFilesSize"))
        val partFilesNum = scan2.metrics("numFiles").value
        val partFilesSize = scan2.metrics("filesSize").value
        assert(0 < partFilesNum && partFilesNum < allFilesNum)
        assert(0 < partFilesSize && partFilesSize < allFilesSize)
        assert(scan2.metrics("numPartitions").value === 1)
        assert(scan2.metrics("pruningTime").value === -1)

        // Dynamic partition pruning is used
        // Static metrics are as-if reading the whole fact table
        // "Regular" metrics are as-if reading only the "fid = 5" partition
        val df3 = sql("SELECT sum(f1) FROM fact, dim WHERE fid = did AND d1 = 6")
        df3.collect()
        val scan3 = getFactScan(df3.queryExecution.executedPlan)
        assert(scan3.metrics("staticFilesNum").value == allFilesNum)
        assert(scan3.metrics("staticFilesSize").value == allFilesSize)
        assert(scan3.metrics("numFiles").value == partFilesNum)
        assert(scan3.metrics("filesSize").value == partFilesSize)
        assert(scan3.metrics("numPartitions").value === 1)
        assert(scan3.metrics("pruningTime").value !== -1)
      }
    }
  }
}

class DynamicPartitionPruningV1SuiteAEOff extends DynamicPartitionPruningV1Suite
  with DisableAdaptiveExecutionSuite

class DynamicPartitionPruningV1SuiteAEOn extends DynamicPartitionPruningV1Suite
  with EnableAdaptiveExecutionSuite

abstract class DynamicPartitionPruningV2Suite extends DynamicPartitionPruningSuiteBase {
  override protected def runAnalyzeColumnCommands: Boolean = false

  override protected def initState(): Unit = {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.defaultCatalog", "testcat")
  }
}

class DynamicPartitionPruningV2SuiteAEOff extends DynamicPartitionPruningV2Suite
  with DisableAdaptiveExecutionSuite

class DynamicPartitionPruningV2SuiteAEOn extends DynamicPartitionPruningV2Suite
  with EnableAdaptiveExecutionSuite

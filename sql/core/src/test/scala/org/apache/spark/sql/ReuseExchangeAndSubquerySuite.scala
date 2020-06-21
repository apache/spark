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

import scala.collection.mutable

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, Exchange, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession


class ReuseExchangeAndSubquerySuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()

    val factData = (0 to 100).map(i => (i%5, i%20, i))
    factData.toDF("store_id", "product_id", "units_sold")
      .write
      .partitionBy("store_id")
      .format("parquet")
      .saveAsTable("fact_stats")

    val dimData = Seq[(Int, String, String)](
      (1, "AU", "US"),
      (2, "CA", "US"),
      (3, "KA", "IN"),
      (4, "DL", "IN"),
      (5, "GA", "PA")
    )
    dimData.toDF("store_id", "state_province", "country")
      .write
      .format("parquet")
      .saveAsTable("dim_stats")
    sql("ANALYZE TABLE fact_stats COMPUTE STATISTICS FOR COLUMNS store_id")
    sql("ANALYZE TABLE dim_stats COMPUTE STATISTICS FOR COLUMNS store_id")
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS fact_stats")
    sql("DROP TABLE IF EXISTS dim_stats")
    super.afterAll()
  }

  private def getAllExchangesAndSubqueries(plan: SparkPlan): Seq[SparkPlan] = {
    val allExchangesAndSubqueries = new mutable.ListBuffer[SparkPlan]
    allExchangesAndSubqueries ++= plan.collect {
      case re: ReusedExchangeExec => re
      case rs: ReusedSubqueryExec => rs
      case e: Exchange => e
      case e: BaseSubqueryExec => e
    }
    plan.transformAllExpressions {
      case e: ExecSubqueryExpression =>
        allExchangesAndSubqueries ++= getAllExchangesAndSubqueries(e.plan)
        e
    }
    allExchangesAndSubqueries
  }

  private def validateReuseExchangesAndReuseSubqueries(
      plan: SparkPlan,
      expectedReuseBroadcastExchanges: Int,
      expectedReuseShuffleExchanges: Int,
      expectedReuseSubqueries: Int): Unit = {
    val plans = getAllExchangesAndSubqueries(plan)
    val shuffleExchangeIds = plans.filter(_.isInstanceOf[ShuffleExchangeExec]).map(_.id)
    val broadcastExchangeIds = plans.filter(_.isInstanceOf[BroadcastExchangeExec]).map(_.id)
    val referencedShuffleExchangeIds = plans.flatMap {
      case r: ReusedExchangeExec if r.child.isInstanceOf[ShuffleExchangeExec] => Some(r.child.id)
      case _ => None
    }
    val referencedBroadcastExchangeIds = plans.flatMap {
      case r: ReusedExchangeExec if r.child.isInstanceOf[BroadcastExchangeExec] => Some(r.child.id)
      case _ => None
    }
    val subqueryIds = plans.filter(_.isInstanceOf[BaseSubqueryExec]).map(_.id)
    val referencedSubqueryIds = plans.flatMap {
      case r: ReusedSubqueryExec => Some(r.child.id)
      case _ => None
    }

    val missingBroadcastExchangeIds = referencedBroadcastExchangeIds.toSet.diff(
      broadcastExchangeIds.toSet)
    assert(missingBroadcastExchangeIds.isEmpty, "ReusedExchangeExec pointing to incorrect " +
      s"BroadcastExchangeExec IDs [${missingBroadcastExchangeIds.mkString(",")}] in plan:\n $plan")

    val missingShuffleExchangeIds = referencedShuffleExchangeIds.toSet.diff(
      shuffleExchangeIds.toSet)
    assert(missingShuffleExchangeIds.isEmpty, "ReusedExchangeExec pointing to incorrect " +
      s"ShuffleExchangeExec IDs [${missingShuffleExchangeIds.mkString(",")}] in plan:\n $plan")

    val missingSubqueryExchanges = referencedSubqueryIds.toSet.diff(subqueryIds.toSet)
    assert(missingSubqueryExchanges.isEmpty, "ReusedSubqueryExec pointing to incorrect" +
      s" Subquery [${missingSubqueryExchanges.mkString(",")}] in plan:\n $plan")

    assert(referencedSubqueryIds.size == expectedReuseSubqueries, s"Expected " +
      s"$expectedReuseSubqueries ReusedSubqueryExec in Plan, " +
      s"found - ${referencedSubqueryIds.size}. Plan:\n $plan")

    assert(referencedBroadcastExchangeIds.size == expectedReuseBroadcastExchanges, s"Expected " +
      s"$expectedReuseBroadcastExchanges ReusedExchangeExec over BroadcastExchangeExec in Plan, " +
      s"found - ${referencedBroadcastExchangeIds.size}. Plan:\n $plan")

    assert(referencedShuffleExchangeIds.size == expectedReuseShuffleExchanges, s"Expected " +
      s"$expectedReuseShuffleExchanges ReusedExchangeExec over BroadcastExchangeExec in Plan, " +
      s"found - ${referencedShuffleExchangeIds.size}. Plan:\n $plan")
  }

  test("Inner ReusedExchangeExec inside InSubqueryExec should not make reference issues to " +
    "outer Exchange operators") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "500") {
        val df = sql(
          """
            | With view1 as (
            |   SELECT product_id, f.store_id
            |   FROM fact_stats f JOIN dim_stats
            |   ON f.store_id = dim_stats.store_id WHERE dim_stats.country = 'IN')
            |
            | SELECT * FROM view1 v1 join view1 v2 WHERE v1.product_id = v2.product_id
      """.stripMargin)
        // Here we are doing: `v1 join v1` where v1 = fact_stats join dim_stats
        // So v1 should be computed only once and We should have a ReusedExchangeExec
        // for ShuffleExchangeExec. Also DPP will trigger inside v1. So we should have
        // ReusedExchangeExec for BroadcastExchangeExec
        validateReuseExchangesAndReuseSubqueries(df.queryExecution.executedPlan, 1, 1, 0)
      }
    }
  }

  test("test reuse exchange inside a reuse subquery") {
    withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
      withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val df = sql(
            """
              | With v1 as (
              |   SELECT f1.store_id
              |   FROM fact_stats f1 join fact_stats f2
              |   WHERE f1.store_id = f2.store_id
              | )
              | SELECT (SELECT max(store_id) from v1), (SELECT max(store_id) from v1)
            """.stripMargin)
          validateReuseExchangesAndReuseSubqueries(df.queryExecution.executedPlan, 0, 1, 1)
        }
      }
    }
  }

  Seq(true, false).foreach { broadcastJoin =>
    test("test reuse exchange when child of reuse exchange contains " +
      s"reusable subquery (broadcast join: $broadcastJoin)") {
      withSQLConf(SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true") {
        withSQLConf(SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
          // withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false") {
          val autoBroadcastJoinThreshold = if (broadcastJoin) 1024 * 1024 * 1024 else -1
          withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> s"$autoBroadcastJoinThreshold") {
            val df = sql(
              """
                | With view1 as (
                |   SELECT product_id, units_sold
                |   FROM fact_stats
                |   WHERE store_id = (SELECT max(store_id) FROM dim_stats)
                |         and units_sold = 2
                | ), view2 as (
                |   SELECT product_id, units_sold
                |   FROM fact_stats
                |   WHERE store_id = (SELECT max(store_id) FROM dim_stats)
                |         and units_sold = 1
                | )
                |
                | SELECT *
                | FROM view1 v1 join view2 v2 join view2 v3
                | WHERE v1.product_id = v2.product_id and v2.product_id = v3.product_id
      """.stripMargin)
            // view2 is self joined. So one of them should be Exchange and other should
            // be ReusedExchangeExec.
            // Also the subquery is repeated thrice. But one of them is inside ReusedExchangeExec.
            // So out of remaining two, one will be ReusedSubqueryExec
            if (broadcastJoin) {
              validateReuseExchangesAndReuseSubqueries(df.queryExecution.executedPlan, 1, 0, 1)
            } else {
              validateReuseExchangesAndReuseSubqueries(df.queryExecution.executedPlan, 0, 1, 1)
            }
          }
        }
      }
    }
  }
}

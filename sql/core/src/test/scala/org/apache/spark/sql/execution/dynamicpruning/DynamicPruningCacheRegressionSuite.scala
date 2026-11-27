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

package org.apache.spark.sql.execution.dynamicpruning

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, DynamicPruningSubquery, Literal}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class DynamicPruningCacheRegressionSuite
    extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  private val fixtureTables = Seq("cache_dim", "cache_fact1", "cache_fact2")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Seq((0L, 1), (1L, 0), (2L, 0)).toDF("k", "flag")
      .coalesce(1).write.saveAsTable("cache_dim")
    for (table <- fixtureTables.tail) {
      spark.range(30).selectExpr("id % 3 AS k", "id AS payload")
        .coalesce(1).write.partitionBy("k").saveAsTable(table)
    }
  }

  override protected def afterAll(): Unit = {
    try {
      fixtureTables.foreach(table => sql(s"DROP TABLE IF EXISTS $table"))
    } finally {
      super.afterAll()
    }
  }

  for {
    adaptive <- Seq(false, true)
    reverse <- Seq(false, true)
    broadcastOnly <- if (reverse) Seq(true) else Seq(true, false)
  } {
    val settings = s"adaptive=$adaptive, broadcastOnly=$broadcastOnly, reverse=$reverse"

    test(s"DPP producer remains traversable across Project and Filter subqueries: $settings") {
      withDppConf(adaptive, broadcastOnly) {
        def scalar(table: String): String = s"""
          SELECT /*+ BROADCAST(d) */ sum(f.payload) FROM $table f
          JOIN cache_dim d ON f.k = d.k WHERE d.flag = 1
        """
        val tables = if (reverse) Seq("cache_fact2", "cache_fact1") else {
          Seq("cache_fact1", "cache_fact2")
        }
        val df = sql(s"SELECT (${scalar(tables.head)}) AS s FROM range(1) r " +
          s"WHERE r.id < (${scalar(tables.last)})")
        val logicalDpp = ArrayBuffer.empty[DynamicPruningSubquery]
        df.queryExecution.optimizedPlan.transformAllExpressionsWithSubqueries {
          case pruning: DynamicPruningSubquery =>
            logicalDpp += pruning
            pruning
        }
        assert(logicalDpp.size == 2)
        assert(logicalDpp.forall(_.onlyInBroadcast == broadcastOnly))
        checkAnswer(df, Row(135L))
        val plan = df.queryExecution.executedPlan
        val dpp = collectWithSubqueries(plan) {
          case scan: FileSourceScanExec => scan.partitionFilters.flatMap(_.collect {
            case DynamicPruningExpression(i: InSubqueryExec) => i.plan
          })
        }.flatten
        assert(dpp.size == 2)
        val producers = dpp.map(unwrap).map(_.asInstanceOf[SubqueryBroadcastExec])
        val joins = collectWithSubqueries(plan) { case join: BroadcastHashJoinExec => join }
        val joinExchanges = joins.flatMap(broadcasts)
        producers.foreach { producer =>
          val exchanges = broadcasts(producer.child)
          assert(exchanges.nonEmpty)
          assert(exchanges.forall(e => joinExchanges.exists(_ eq e)), plan.treeString)
        }
        val visible = collectWithSubqueries(plan) { case s: SubqueryBroadcastExec => s }
        assert(producers.forall(p => visible.exists(_ eq p)), plan.treeString)
        assert(!collectWithSubqueries(plan) { case r: ReusedSubqueryExec => r }
          .exists(_.child.isInstanceOf[ReusedSubqueryExec]))
        if (broadcastOnly && !reverse) {
          checkDisplay(df)
        }
      }
    }

    test(s"DPP planning caches distinguish internal join hints: $settings") {
      withDppConf(adaptive, broadcastOnly) {
        def scalar(table: String, hint: String): String = s"""
          SELECT /*+ BROADCAST(q) */ sum(f.payload) FROM $table f JOIN (
            SELECT /*+ $hint(r) */ d.k FROM cache_dim d
            JOIN range(3) r ON d.k = r.id WHERE d.flag = 1
          ) q ON f.k = q.k
        """
        val hints = if (reverse) Seq("BROADCAST", "MERGE") else Seq("MERGE", "BROADCAST")
        val df = sql(s"SELECT (${scalar("cache_fact1", hints.head)}) AS s FROM range(1) r " +
          s"WHERE r.id < (${scalar("cache_fact2", hints.last)})")
        checkAnswer(df, Row(135L))
        val filters = collectWithSubqueries(df.queryExecution.executedPlan) {
          case scan: FileSourceScanExec => scan.partitionFilters
        }.flatten
        assert(!filters.exists {
          case DynamicPruningExpression(Literal(true, _)) => true
          case _ => false
        })
        val producers = filters.collect {
          case DynamicPruningExpression(in: InSubqueryExec) => unwrap(in.plan)
        }
        assert(producers.size == 2)
        assert(producers.forall(_.isInstanceOf[SubqueryBroadcastExec]))
      }
    }
  }

  private def withDppConf(adaptive: Boolean, broadcastOnly: Boolean)(f: => Unit): Unit = {
    withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> broadcastOnly.toString,
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.0",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
        SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      f
    }
  }

  private def unwrap(plan: BaseSubqueryExec): BaseSubqueryExec = plan match {
    case r: ReusedSubqueryExec => unwrap(r.child)
    case other => other
  }

  private def broadcasts(plan: SparkPlan): Seq[SparkPlan] = collect(plan) {
    case exchange: BroadcastExchangeExec => exchange
    case reused: ReusedExchangeExec => reused.child
  }

  private def checkDisplay(df: DataFrame): Unit = {
    val formatted = df.queryExecution.explainString(FormattedMode)
    assert(formatted.contains("cache_fact1"))
    assert(formatted.contains("cache_fact2"))
    def nodes(info: SparkPlanInfo): Seq[SparkPlanInfo] = {
      info +: info.children.flatMap(nodes)
    }
    val planInfo = SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan)
    val described = nodes(planInfo)
    assert(described.exists(_.nodeName == "SubqueryBroadcast"))
    val ids = described.flatMap(_.metadata.get("spark.sql.dpp.planInfo.id")).toSet
    val refs = described.flatMap(_.metadata.get("spark.sql.dpp.planInfo.ref"))
    assert(refs.nonEmpty && refs.forall(ids.contains))
  }
}

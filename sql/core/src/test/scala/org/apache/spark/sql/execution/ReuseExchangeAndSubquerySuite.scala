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

package org.apache.spark.sql.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ReuseExchangeAndSubquerySuite extends SharedSparkSession with AdaptiveSparkPlanHelper {

  val tableFormat: String = "parquet"

  private def sortAggregateConf: Seq[(String, String)] = Seq(
    SQLConf.USE_HASH_AGG.key -> "false",
    SQLConf.USE_OBJECT_HASH_AGG.key -> "false")

  Seq(false, true).foreach { aqeEnabled =>
    test("SPARK-33744: exchange reuse across identical sort aggregates " +
        s"(AQE: $aqeEnabled)") {
      withSQLConf(sortAggregateConf ++ Seq(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqeEnabled.toString,
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true"): _*) {
        withTable("sort_agg_reuse") {
          spark.range(10).select(col("id").as("k"))
            .write.format(tableFormat).saveAsTable("sort_agg_reuse")
          val branch = "SELECT k, count(*) AS c FROM sort_agg_reuse GROUP BY k"
          val df = sql(s"SELECT * FROM ($branch) a JOIN ($branch) b ON a.k = b.k AND a.c = b.c")

          checkAnswer(df, (0L until 10L).map(i => Row(i, 1L, i, 1L)))
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          // The join-side exchange sits above the final aggregate, whose result expressions alias
          // the aggregate attributes. Its reuse is what a sort aggregate's canonicalization
          // decides; the pre-shuffle exchange below the partial aggregate is reused either way.
          val reused = plan.collect {
            case r: ReusedExchangeExec => r
            case ShuffleQueryStageExec(_, r: ReusedExchangeExec, _) => r
          }
          val reusedOverFinalAgg = reused.exists { r =>
            r.child.collect { case a: SortAggregateExec => a }
              .exists(_.aggregateExpressions.exists(_.mode == Final))
          }
          assert(reusedOverFinalAgg,
            s"expected the join-side exchange over a final sort aggregate to be reused:\n$plan")
        }
      }
    }
  }

  test("SPARK-33744: HashAggregate canonicalization") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      import testImplicits._
      val data = Seq((1, 1)).toDF("c0", "c1")
      val df1 = data.groupBy(col("c0")).agg(first("c1"))
      val df2 = data.groupBy(col("c0")).agg(first("c1"))
      assert(df1.queryExecution.executedPlan.collect {
        case _: HashAggregateExec => true
      }.nonEmpty, "setup: expected a hash aggregate")
      assert(df1.queryExecution.executedPlan.canonicalized ==
        df2.queryExecution.executedPlan.canonicalized)
    }
  }

  test("SPARK-33744: SortAggregate canonicalization") {
    withSQLConf(sortAggregateConf ++ Seq(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"): _*) {
      import testImplicits._
      val data = Seq(("a", "a")).toDF("c0", "c1")
      val df1 = data.groupBy(col("c0")).agg(first("c1"))
      val df2 = data.groupBy(col("c0")).agg(first("c1"))
      assert(df1.queryExecution.executedPlan.collect {
        case _: SortAggregateExec => true
      }.nonEmpty, "setup: expected a sort aggregate")
      assert(df1.queryExecution.executedPlan.canonicalized ==
        df2.queryExecution.executedPlan.canonicalized)
    }
  }

  test("SPARK-32041: No reuse interference inside ReuseExchange") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
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
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql(
          """
            |WITH t AS (
            |  SELECT df1.id, df2.k
            |  FROM df1 JOIN df2 ON df1.k = df2.k
            |  WHERE df2.id < 2
            |)
            |SELECT * FROM t AS a JOIN t AS b ON a.id = b.id
            |""".stripMargin)

        val plan = df.queryExecution.executedPlan

        val exchangeIds = plan.collectWithSubqueries { case e: Exchange => e.id }
        val reusedExchangeIds = plan.collectWithSubqueries {
          case re: ReusedExchangeExec => re.child.id
        }

        assert(reusedExchangeIds.forall(exchangeIds.contains(_)),
          "ReusedExchangeExec should reuse an existing exchange")
      }
    }
  }

  test("SPARK-32041: No reuse interference between ReuseExchange and ReuseSubquery") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
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
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql(
          """
            |WITH t AS (
            |  SELECT df1.id, df2.k
            |  FROM df1 JOIN df2 ON df1.k = df2.k
            |  WHERE df2.id < 2
            |),
            |t2 AS (
            |  SELECT * FROM t
            |  UNION
            |  SELECT * FROM t
            |)
            |SELECT * FROM t2 AS a JOIN t2 AS b ON a.id = b.id
            |""".stripMargin)

        val plan = df.queryExecution.executedPlan

        val exchangeIds = plan.collectWithSubqueries { case e: Exchange => e.id }
        val reusedExchangeIds = plan.collectWithSubqueries {
          case re: ReusedExchangeExec => re.child.id
        }

        assert(reusedExchangeIds.forall(exchangeIds.contains(_)),
          "ReusedExchangeExec should reuse an existing exchange")
      }
    }
  }

  test("SPARK-61574: A scan copied by ReuseExchangeAndSubquery keeps its columnar decision") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable("df1", "df2") {
        spark.range(100)
          .select(col("id"), (col("id") % 10).as("k"))
          .write
          .partitionBy("k")
          .format(tableFormat)
          .saveAsTable("df1")

        spark.range(10)
          .select(col("id"), col("id").as("k"))
          .write
          .format(tableFormat)
          .saveAsTable("df2")

        // The broadcast side comes first, so the dynamic pruning subquery of the `df1` scan reuses
        // its exchange, and the scan is copied after the columnar transitions are in place.
        val df = sql(
          """
            |SELECT /*+ BROADCAST(df2) */ df1.id
            |FROM df2 JOIN df1 ON df2.k = df1.k
            |WHERE df2.id = 3
            |""".stripMargin)

        val copiedScans = df.queryExecution.executedPlan.collect {
          case s: FileSourceScanExec if s.partitionFilters.exists(_.exists {
            case e: ExecSubqueryExpression => e.plan.exists(_.isInstanceOf[ReusedExchangeExec])
            case _ => false
          }) => s
        }
        assert(copiedScans.size == 1)

        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          assert(df.collect().map(_.getLong(0)).sorted.toSeq === (3L until 100L by 10))
        }
      }
    }
  }
}

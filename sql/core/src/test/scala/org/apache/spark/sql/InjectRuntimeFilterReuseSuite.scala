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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression, Literal}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class InjectRuntimeFilterReuseSuite
    extends QueryTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  val tableFormat: String = "parquet"

  val adaptiveExecutionOn: Boolean

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.sessionState.conf.setConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED, adaptiveExecutionOn)
    spark.sessionState.conf.setConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED, true)
    spark.sessionState.conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, 5000L)
    spark.sessionState.conf.setConf(SQLConf.PARQUET_COMPRESSION, "uncompressed")
    spark.sessionState.conf.setConf(SQLConf.CBO_ENABLED, true)

    spark
      .range(2000)
      .select(col("id").as("a"), col("id").as("b"))
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t1")

    spark
      .range(2000)
      .select(col("id").as("a"), col("id").as("b"), col("id").as("c"))
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t2")

    spark
      .range(100)
      .select(col("id").as("a"), col("id").as("b"), col("id").as("c"))
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t3")

    import testImplicits._
    val expr5 = when($"id" % 4 === lit(0), col("id").cast("string"))
      .otherwise(col("id").*(2).cast("string"))
      .as("c")

    val expr6 = when($"id" % 4 === lit(0), col("id").cast("string"))
      .otherwise(col("id").*(3).cast("string"))
      .as("c")

    spark
      .range(2000)
      .select(col("id").as("a"), col("id").as("b"), expr5)
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t5")

    spark
      .range(100)
      .select(col("id").as("a"), col("id").as("b"), expr6)
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("t6")

    Seq(13, 27, 8, 26, 15, 27, 14, 17, 24, 26, 17, 18, 4, 28, 16, 11, 17, 8, 19, 28, 4, 8, 2, 19,
      26, 30, 7, 14, 7, 17, 1, 5, 19, 21, 15, 20, 3, 19, 3, 4, 26, 25, 18, 30, 12, 22, 11, 24, 3,
      17, 16, 22, 26, 9, 28, 8, 18, 22, 11, 21, 5, 25, 1, 4, 28, 15, 28, 24, 18, 16, 24, 16, 27,
      27, 1, 23, 19, 9, 18, 21, 2, 8, 24, 5, 8, 23, 8, 19, 28, 20, 5, 4, 30, 23, 30, 12, 9, 10,
      23, 7).zipWithIndex
      .toDF("a", "b")
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("ct4")

    Seq(10, 7, 21, 13, 28, 11, 1, 2, 20, 24, 9, 18, 19, 17, 8, 6, 5, 30, 12, 23, 5, 21, 19, 18,
      21, 30, 8, 26, 24, 30, 5, 27, 13, 10, 14, 24, 5, 7, 1, 5, 16, 12, 5, 4, 1, 29, 9, 1, 6, 5,
      5, 14, 9, 29, 13, 8, 21, 2, 11, 5, 1, 7, 11, 30, 23, 2, 30, 10, 4, 22, 25, 21, 9, 6, 23, 6,
      19, 14, 1, 17, 8, 24, 30, 25, 20, 27, 11, 21, 4, 5, 3, 4, 4, 21, 28, 10, 28, 5, 11,
      25).zipWithIndex
      .toDF("a", "b")
      .write
      .format(tableFormat)
      .mode(SaveMode.Overwrite)
      .saveAsTable("ct3")

    spark.sql("analyze table t1 compute statistics for columns a, b").collect()
    spark.sql("analyze table t2 compute statistics for columns a, b").collect()
    spark.sql("analyze table t3 compute statistics for columns a, b").collect()
    spark.sql("analyze table ct3 compute statistics for columns a, b").collect()
    spark.sql("analyze table ct4 compute statistics for columns a, b").collect()
    spark.sql("analyze table t5 compute statistics for columns a, b, c").collect()
    spark.sql("analyze table t6 compute statistics for columns a, b, c").collect()
  }

  override def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS t1")
      sql("DROP TABLE IF EXISTS t2")
      sql("DROP TABLE IF EXISTS t3")
      sql("DROP TABLE IF EXISTS ct3")
      sql("DROP TABLE IF EXISTS ct4")
      sql("DROP TABLE IF EXISTS t5")
      sql("DROP TABLE IF EXISTS t6")
    } finally {
      spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
      spark.sessionState.conf.unsetConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED)
      spark.sessionState.conf.unsetConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD)
      spark.sessionState.conf.unsetConf(SQLConf.PARQUET_COMPRESSION)
      super.afterAll()
    }
  }

  def checkForReuseExchange(
      reuseExpected: Boolean,
      plan: SparkPlan,
      rootPlan: SparkPlan): Unit = {
    val reuseStr = if (reuseExpected) "Should" else "Shoudn't"
    plan match {
      case _: ReusedExchangeExec =>
      case ex: ShuffleExchangeExec =>
        val hasReuse = find(rootPlan) {
          case ReusedExchangeExec(_, e) => e eq ex
          case _ => false
        }.isDefined
        assert(hasReuse == reuseExpected, s"$plan\n${reuseStr} have been reused in\n$rootPlan")
      case _ => fail(s"Invalid child node found in\n$plan")
    }
  }

  def checkSubqueryForReuseExchange(reuseExpected: Boolean,
                                    plan: SparkPlan, rootPlan: SparkPlan): Unit = {
    plan match {
      case ObjectHashAggregateExec(_, _, _, _, _, _, _, _,
      ShuffleExchangeExec(_,
      ObjectHashAggregateExec(_, _, _, _, _, _, _, _, child), _)) =>
        checkForReuseExchange(reuseExpected, child, rootPlan)
      case a: AdaptiveSparkPlanExec =>
        checkSubqueryForReuseExchange(reuseExpected, a.executedPlan, rootPlan)
      case _ =>
    }
  }

  /**
   * Check if the query plan has a bloom filter inserted either
   * with Reusable Exchange or not
   */
  def checkBloomFilterPredicate(
                                 df: DataFrame,
                                 hasBloomFilter: Boolean,
                                 reuseExpected: Boolean = true): Unit = {
    df.collect()

    val plan = df.queryExecution.executedPlan
    val bloomExprs = collectBloomFilterExpressions(plan)
    val hasSubquery = bloomExprs.exists {
      case BloomFilterMightContain(_, _, false) => true
      case _ => false
    }

    val subqueryExecs = bloomExprs.collect {
      case BloomFilterMightContain(s, _, false) => s.asInstanceOf[ScalarSubquery].plan
    }

    val name = "trigger bloom filter"
    val hasFilter = if (hasBloomFilter) "Should" else "Shouldn't"
    assert(
      hasSubquery == hasBloomFilter,
      s"$hasFilter $name with a subquery duplicate:\n${df.queryExecution}")

    val reuseStr = if (reuseExpected) "Should" else "Shoudn't"
    subqueryExecs.foreach { s =>
      checkSubqueryForReuseExchange(reuseExpected, s.child, plan)
    }
  }

  private def collectBloomFilterExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case s: FilterExec =>
        s.condition.collect {
          case d: BloomFilterMightContain => d
        }
      case _ => Nil
    }
  }

  test("Bloom Filter: check table stats") {
    val catalog = spark.sessionState.catalog
    assert(catalog.getTableMetadata(TableIdentifier("t1")).stats.get.rowCount.get == 2000)
    assert(catalog.getTableMetadata(TableIdentifier("t2")).stats.get.rowCount.get == 2000)
    assert(catalog.getTableMetadata(TableIdentifier("t3")).stats.get.rowCount.get == 100)
    assert(catalog.getTableMetadata(TableIdentifier("t1")).stats.get.sizeInBytes > 10000L)
    assert(catalog.getTableMetadata(TableIdentifier("t2")).stats.get.sizeInBytes > 10000L)
    assert(catalog.getTableMetadata(TableIdentifier("t3")).stats.get.sizeInBytes < 5000L)
  }

  test("Bloom Filter with reuse exchange") {
    withSQLConf(
      SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "10000") {
      val df = sql("""
          |SELECT t1.a, t2.b
          |FROM t1
          |JOIN t2
          |ON t1.a = t2.a AND t2.b < 10
          |""".stripMargin)
      checkBloomFilterPredicate(df, true)
    }
  }

  test("Bloom Filter is not triggered if SMJ doesn't introduce Exchange") {
    withSQLConf(
      SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "10000") {
      val df = sql("""
          |SELECT t5.a,
          |       t5.b
          |FROM   t5
          |JOIN (SELECT t3.a, sum(t3.b) sumb
          |       FROM t3
          |       GROUP BY t3.a) t3new
          |ON t5.a = t3new.a
          |WHERE coalesce(t3new.sumb, 0) > 0
          |""".stripMargin)

      checkBloomFilterPredicate(df, false)
      val trueNode = find(df.queryExecution.executedPlan) {
        case fe: FilterExec => fe.condition.containsChild.contains(Literal.TrueLiteral)
        case _ => false
      }
      assert(trueNode.isDefined)
    }
  }

  test("Bloom Filter is not triggered with SMJ if Exchange reuse is disabled") {
    withSQLConf(
      SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1",
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "10000") {
      val df = sql("""
          |SELECT t11.a,
          |       t11.b
          |FROM   (SELECT t5.a,
          |               t6.b
          |        FROM t5
          |        JOIN t6
          |            ON t5.a = t6.a AND t5.c = t6.c AND t6.b < 10) t11
          |       JOIN t3
          |         ON t11.a = t3.a
          |""".stripMargin)

      checkBloomFilterPredicate(df, false)
    }
  }

  test("Bloom Filter reuses as much plan possible when AQE removes the" +
    "top Exchange use by Bloom filter") {
    withSQLConf(
      SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "400",
      SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "20000",
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "10000") {
      val df = sql(
        """
          |SELECT t1.a, t1.b
          |FROM t1
          |JOIN (
          | SELECT ct4.a ct4a, ct4.b ct4b
          | FROM ct4
          | JOIN ct3
          | ON ct4.a = ct3.a
          | AND ct4.a > ct4.b + 20) newt
          |ON t1.b = newt.ct4b
          |""".stripMargin)
      // With AQE
      // The initial plan will have SMJ whose right side is used as build plan for Bloom
      // But AQE will convert that SMJ to BHJ replacing ShuffleExchange with BroadcastExchange
      // Bloom build plan will try to reuse the plan till last exchanges

      // Initial Plan
      //   Project
      //   +- SortMergeJoin
      //      :- Sort
      //      :  +- Exchange
      //      :     +- Filter -> contains bloom filter
      //      :        :  +- Subquery
      //      :        :     +- AdaptiveSparkPlan
      //                        +- ObjectHashAggregate
      //                           +- Exchange
      //                              +- ObjectHashAggregate
      //                                 +- Exchange
      //                                    +- Project
      //                                       +- SortMergeJoin
      //                                          :- Sort
      //                                          :  +- Exchange
      //                                          :     +- Filter
      //                                          :        +- FileScan
      //                                          +- Sort
      //                                             +- Exchange
      //                                                +- Filter
      //                                                   +- FileScan
      //      :        +- FileScan
      //      +- Sort
      //         +- Exchange -> this exchanges is used by bloom initially and will be removed by AQE
      //            +- Project
      //               +- SortMergeJoin
      //                  :- Sort
      //                  :  +- Exchange
      //                  :     +- Filter
      //                  :        +- FileScan
      //                  +- Sort
      //                     +- Exchange
      //                        +- Filter
      //                           +- FileScan

      // Final Plan
      // Project
      // +- BroadcastHashJoin
      //   :- AQEShuffleRead
      //   :  +- ShuffleQueryStage
      //   :     +- Exchange
      //   :        +- Filter -> contains bloom filter
      //   :           :  +- Subquery
      //   :           :     +- AdaptiveSparkPlan
      //                        +- ObjectHashAggregate
      //                           +- ShuffleQueryStage
      //                              +- Exchange
      //                                  +- ObjectHashAggregate
      //                                     +- AQEShuffleRead coalesced
      //                                        +- ShuffleQueryStage
      //                                           +- Exchange
      //                                              +- Project
      //                                                 +- BroadcastHashJoin
      //                                                 :- BroadcastQueryStage
      //                                                 :  +- BroadcastExchange
      //                                                 :     +- AQEShuffleRead
      //                                                 :        +- ShuffleQueryStage
      //                                                 :           +- ReusedExchange
      //                                                 +- AQEShuffleRead
      //                                                    +- ShuffleQueryStage
      //                                                        +- ReusedExchange
      //   :           +- ColumnarToRow
      //   :              +- FileScan
      //   +- BroadcastQueryStage
      //      +- BroadcastExchange
      //         +- Project
      //            +- BroadcastHashJoin
      //               :- BroadcastQueryStage
      //               :  +- ReusedExchange
      //               +- AQEShuffleRead
      //                  +- ShuffleQueryStage
      //                     +- Exchange
      //                        +- Filter
      //                           +- ColumnarToRow
      //                              +- FileScan
      checkBloomFilterPredicate(df, true, !adaptiveExecutionOn)
    }
  }
}

class InjectRuntimeFilterReuseSuiteAEOff extends InjectRuntimeFilterReuseSuite {
  override val adaptiveExecutionOn: Boolean = false
}

class InjectRuntimeFilterReuseSuiteAEOn extends InjectRuntimeFilterReuseSuite {
  override val adaptiveExecutionOn: Boolean = true
}

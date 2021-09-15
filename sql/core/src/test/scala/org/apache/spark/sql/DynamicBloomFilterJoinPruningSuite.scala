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

import java.sql.{Date, Timestamp}

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.Decimal

/**
 * Test suite for the filtering ratio policy used to trigger dynamic bloom filter pruning.
 */
abstract class DynamicBloomFilterPruningSuiteBase
    extends QueryTest
    with SharedSparkSession
    with GivenWhenThen
    with AdaptiveSparkPlanHelper {

  private val tableFormat: String = "parquet"
  private var originalAutoBroadcastJoinThreshold: Long = _
  private var originalParquetCompressionCodec: String = _


  override def beforeAll(): Unit = {
    super.beforeAll()
    originalAutoBroadcastJoinThreshold = conf.autoBroadcastJoinThreshold
    originalParquetCompressionCodec = conf.parquetCompressionCodec
    conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
  }

  override def afterAll(): Unit = {
    spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED)
    spark.sessionState.conf.unsetConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY)
    conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, originalAutoBroadcastJoinThreshold)
    conf.setConf(SQLConf.PARQUET_COMPRESSION, originalParquetCompressionCodec)
    super.afterAll()
  }

  /**
   * Check if the query plan has a bloom filter pruning filter.
   */
  def checkBloomFilterPruningPredicate(df: DataFrame, withBloomFilter: Boolean): Unit = {
    df.collect()

    val plan = df.queryExecution.executedPlan
    val inBloomFilterExchange = collectDynamicPruningExpressions(plan).collect {
      case InBloomFilterSubqueryExec(_, s: SubqueryExec, _, _) =>
        collectFirst(s) {
          case c: CoalesceExec =>
            collectFirst(c) {
              case s: ShuffleExchangeExec => s
              case r: ReusedExchangeExec =>
                collectFirst(r.child) { case s: ShuffleExchangeExec => s }.getOrElse(Nil)
            }.getOrElse(Nil)
        }
    }.flatten
    val joinExchange = collect(plan) {
      case s: SortMergeJoinExec => s.children.flatMap { c =>
        collectFirst(c) {
          case s: ShuffleExchangeExec => s
          case r: ReusedExchangeExec =>
            collectFirst(r.child) { case s: ShuffleExchangeExec => s }.getOrElse(Nil)
        }
      }
    }.flatten

    // InBloomFilterSubqueryExec exist
    assert(inBloomFilterExchange.nonEmpty === withBloomFilter)
    // Can reuse the exchange
    assert(inBloomFilterExchange.forall(e => joinExchange.exists(_ eq e)))
  }

  /**
   * Collect the children of all correctly pushed down dynamic pruning expressions in a spark plan.
   */
  private def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case s: FilterExec => s.condition.collect {
        case d: DynamicPruningExpression => d.child
      }
      case _ => Nil
    }
  }

  test("Enable CBO to trigger bloom filter pruning") {
    withTable("t1", "t2") {
      spark.range(1000).selectExpr("id AS a", "id AS b", "id AS c")
        .write
        .format(tableFormat)
        .saveAsTable("t1")
      spark.range(10).selectExpr("id AS a", "id AS b", "id AS c")
        .write
        .format(tableFormat)
        .saveAsTable("t2")

      sql("ANALYZE TABLE t1 COMPUTE STATISTICS FOR ALL COLUMNS")
      sql("ANALYZE TABLE t2 COMPUTE STATISTICS FOR ALL COLUMNS")

      Seq(true, false).foreach { cboEnabled =>
        withSQLConf(
          SQLConf.DYNAMIC_BLOOM_FILTER_JOIN_PRUNING_ENABLED.key -> "true",
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
          SQLConf.CBO_ENABLED.key -> cboEnabled.toString) {
          val df = sql(
            """
              |SELECT f.a, f.b FROM t1 f
              |JOIN t2 s ON f.a = s.a AND s.b = 2
            """.stripMargin)

          checkBloomFilterPruningPredicate(df, cboEnabled)
          checkAnswer(df, Row(2, 2) :: Nil)
        }
      }
    }
  }

  test("Enable bloom filter pruning if one side is much larger than other side") {
    withTable("t1", "t2") {
      spark.range(200000).selectExpr("id AS a", "id AS b", "id AS c")
        .write
        .format(tableFormat)
        .saveAsTable("t1")
      spark.range(10).selectExpr("id AS a", "id AS b", "id AS c")
        .write
        .format(tableFormat)
        .saveAsTable("t2")

      withSQLConf(
        SQLConf.DYNAMIC_BLOOM_FILTER_JOIN_PRUNING_ENABLED.key -> "true",
        SQLConf.CBO_ENABLED.key -> "false") {
        val df = sql(
          """
            |SELECT f.a, f.b FROM t1 f
            |JOIN t2 s ON f.a = s.a AND s.b = 2
          """.stripMargin)

        checkBloomFilterPruningPredicate(df, true)
        checkAnswer(df, Row(2, 2) :: Nil)
      }
    }
  }

  private def checkSupportedDataTypes(value: Any): Unit = {
    test(s"Check support data type: ${value.getClass.getCanonicalName}") {
      withSQLConf(
        SQLConf.DYNAMIC_BLOOM_FILTER_JOIN_PRUNING_ENABLED.key -> "true",
        SQLConf.CBO_ENABLED.key -> "true") {
        withTable("t_dt1", "t_dt2") {
          spark.range(20000)
            .select(lit(Literal(value)).as("a"), col("id").as("b"))
            .write
            .format(tableFormat)
            .mode(SaveMode.Overwrite)
            .saveAsTable("t_dt1")

          spark.range(5)
            .select(lit(Literal(value)).as("a"), col("id").as("b"))
            .write
            .format(tableFormat)
            .mode(SaveMode.Overwrite)
            .saveAsTable("t_dt2")

          sql("ANALYZE TABLE t_dt1 COMPUTE STATISTICS FOR ALL COLUMNS")
          sql("ANALYZE TABLE t_dt2 COMPUTE STATISTICS FOR ALL COLUMNS")

          CodegenObjectFactoryMode.values.foreach { mode =>
            withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> mode.toString) {
              val df = sql(
                """
                  |SELECT f.a, f.b FROM t_dt1 f
                  |JOIN t_dt2 s ON f.a = s.a AND s.b = 2
                """.stripMargin)

              checkBloomFilterPruningPredicate(df, true)
              checkAnswer(df, spark.table("t_dt1"))
            }
          }
        }
      }
    }
  }

  checkSupportedDataTypes(false)
  checkSupportedDataTypes(1.toByte)
  checkSupportedDataTypes(2.toShort)
  checkSupportedDataTypes(3)
  checkSupportedDataTypes(4L)
  checkSupportedDataTypes(5.6.toFloat)
  checkSupportedDataTypes(7.8)
  checkSupportedDataTypes(new Decimal().set(9.0123).toPrecision(38, 18).toBigDecimal)
  checkSupportedDataTypes("str")
  checkSupportedDataTypes(Array[Byte](1, 2, 3, 4))
  checkSupportedDataTypes(Date.valueOf("2020-07-24"))
  checkSupportedDataTypes(Timestamp.valueOf("2020-07-24 14:01:11"))
}

class DynamicBloomFilterPruningSuiteAEOff extends DynamicBloomFilterPruningSuiteBase
  with DisableAdaptiveExecutionSuite

class DynamicBloomFilterPruningSuiteAEOn extends DynamicBloomFilterPruningSuiteBase
  with EnableAdaptiveExecutionSuite

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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEPropagateEmptyRelation, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

abstract class DynamicPartitionPruningHiveScanSuite
    extends DynamicPartitionPruningSuiteBase with TestHiveSingleton with SQLTestUtils {

  override val tableFormat: String = "hive"

  override protected def collectDynamicPruningExpressions(plan: SparkPlan): Seq[Expression] = {
    flatMap(plan) {
      case s: FileSourceScanExec => s.partitionFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case s: BatchScanExec => s.runtimeFilters.collect {
        case d: DynamicPruningExpression => d.child
      }
      case h: HiveTableScanExec => h.partitionPruningPred.collect {
        case d: DynamicPruningExpression => d.child
      }
      case _ => Nil
    }
  }

  test("SPARK-36876: simple inner join triggers DPP with mock-up tables") {
    withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
      SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
      SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
      HiveUtils.CONVERT_METASTORE_ORC.key -> "false",
      "hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable("df1", "df2") {
        spark.range(1000)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df1")

        spark.range(100)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format(tableFormat)
          .mode("overwrite")
          .saveAsTable("df2")

        val df = sql("SELECT df1.id, df2.k FROM df1 JOIN df2 ON df1.k = df2.k AND df2.id < 2")

        checkPartitionPruningPredicate(df, true, false)

        checkAnswer(df, Row(0, 0) :: Row(1, 1) :: Nil)
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
        withTable("fact_aux") {
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

          checkPartitionPruningPredicate(df, true, false)

          checkAnswer(df,
            Row(1070, 2, 10, 4) ::
              Row(1080, 3, 20, 4) ::
              Row(1090, 3, 10, 4) ::
              Row(1100, 3, 10, 4) :: Nil
          )
        }
      }

      Given("no stats and selective predicate with the size of dim too large but cached")
      withSQLConf(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "true") {
        withTable("fact_aux") {
          withTempView("cached_dim_store") {
            sql(
              """
                |SELECT f.date_id, f.product_id, f.units_sold, f.store_id
                |FROM fact_sk f WHERE store_id < 5
              """.stripMargin)
              .write
              .partitionBy("store_id")
              .saveAsTable("fact_aux")

            spark.table("dim_store").cache()
              .createOrReplaceTempView("cached_dim_store")

            val df = sql(
              """
                |SELECT f.date_id, f.product_id, f.units_sold, f.store_id
                |FROM fact_aux f JOIN cached_dim_store s
                |ON f.store_id = s.store_id WHERE s.country = 'US'
              """.stripMargin)

            checkPartitionPruningPredicate(df, true, false)

            checkAnswer(df,
              Row(1070, 2, 10, 4) ::
                Row(1080, 3, 20, 4) ::
                Row(1090, 3, 10, 4) ::
                Row(1100, 3, 10, 4) :: Nil
            )
          }
        }
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
}

class DynamicPartitionPruningHiveScanSuiteAEOff extends DynamicPartitionPruningHiveScanSuite
  with DisableAdaptiveExecutionSuite

class DynamicPartitionPruningHiveScanSuiteAEOn extends DynamicPartitionPruningHiveScanSuite
  with EnableAdaptiveExecutionSuite

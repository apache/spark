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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class TableCacheQueryStageExecSuite extends SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("SPARK-58767: re-use of exchange involving cached plans ") {
    withTempPaths(3) { paths =>
      // to be safe use 3 data paths so that the 3 tables are not in any situation canonicalized
      // same.
      val data = Seq((1, 1), (2, 2), (3, 3), (4, 4), (6, 6), (5, 5))
      paths.zipWithIndex.foreach{case(path, indx) =>
        val tableNumber = indx + 1
        spark.createDataFrame(data).toDF(s"c1_$tableNumber", s"c2_$tableNumber").write.mode(
          SaveMode.Overwrite).parquet(path.getAbsolutePath)}
      withTempView("v1") {
        withTable("t1", "t2", "t3") {
          withCache("v1") {
            spark.sql(
              s"""
                 |CREATE TABLE t1 USING PARQUET LOCATION '${paths(0).getAbsolutePath}'
                 |""".stripMargin
            )
            spark.sql(
              s"""
                 |CREATE TABLE t2 USING PARQUET LOCATION '${paths(1).getAbsolutePath}'
                 |""".stripMargin
            )
            spark.sql(
              s"""
                 |CREATE TABLE t3 USING PARQUET LOCATION '${paths(2).getAbsolutePath}'
                 |""".stripMargin
            )

            spark.table("t1").where($"c1_1" > 0).createTempView("v1")
            spark.catalog.cacheTable("v1")
            val testCode = () => {
              val al1 = spark.table("v1").as("al1")
              val al2 = spark.table("v1").as("al2")
              val t2 = spark.table("t2")
              val t3 = spark.table("t3")
              val j1 = t2.where($"c2_2" > 0).join(t3, t2.col("c1_2") === t3.col("c1_3"))
              assert(j1.collect().nonEmpty)
              val j2 = j1.join(
                broadcast(al1.where($"al1.c1_1" > 0)), $"al1.c1_1" === t2.col("c1_2"))
              assert(j2.collect().nonEmpty)
              val df = j2
                .join(broadcast(al2.where($"al2.c1_1" > 0)), $"al2.c1_1" === t3.col("c1_3"))
              val result = df.collect()
              assert(result.nonEmpty)
              df
            }

            val dfExchngReuse = withSQLConf(
              SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
              SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true") {
              testCode()
            }

            val dfExchngNoReuse = withSQLConf(
              SQLConf.EXCHANGE_REUSE_ENABLED.key -> "false",
              SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false") {
              testCode()
            }
            // There should be No re-use of exchange when reuse is disabled
            assert(
              collectAllBroadcastStageExec(dfExchngNoReuse.queryExecution.executedPlan).count(
                _.plan.isInstanceOf[ReusedExchangeExec]) == 0
            )

            // There should be 1 re-use of exchange when reuse is enabled
            assert(
              collectAllBroadcastStageExec(dfExchngReuse.queryExecution.executedPlan).count(
                _.plan.isInstanceOf[ReusedExchangeExec]) == 1
            )
            checkAnswer(dfExchngReuse, dfExchngNoReuse)
          }
        }
      }
    }

    def collectAllBroadcastStageExec(sp: SparkPlan): Seq[BroadcastQueryStageExec] = {
      sp match {
        case ap: AdaptiveSparkPlanExec => collectAllBroadcastStageExec(ap.finalPhysicalPlan)

        case b: BroadcastQueryStageExec => collectAllBroadcastStageExec(b.plan) :+ b

        case other => other.children.flatMap(collectAllBroadcastStageExec)
      }
    }
  }

  test("SPARK-58767: canonicalization check") {
    withTempPath { path =>
      // to be safe use 3 data paths so that the 3 tables are not in any situation canonicalized
      // same.
      val data = Seq((1, 1), (2, 2), (3, 3), (4, 4), (6, 6), (5, 5))
      spark.createDataFrame(data).toDF("c1", "c2").write.mode(
        SaveMode.Overwrite).parquet(path.getAbsolutePath)
      withTempView("v1") {
        withTable("t1") {
          withCache("v1") {
            spark.sql(
              s"""
                 |CREATE TABLE t1 USING PARQUET LOCATION '${path.getAbsolutePath}'
                 |""".stripMargin
            )
            val viewDf = spark.table("t1").where($"c1" > 0)
            viewDf.createTempView("v1")
            spark.catalog.cacheTable("v1")

            val imr1 = spark.sharedState.cacheManager.useCachedData(viewDf.logicalPlan)
              .collectLeaves().head.asInstanceOf[InMemoryRelation]
            val imr2 = spark.sharedState.cacheManager.useCachedData(spark.table("t1").where($"c1" >
              0).logicalPlan).collectLeaves().head.asInstanceOf[InMemoryRelation]
            val imrExec1 = InMemoryTableScanExec(imr1.output, Seq.empty, imr1)
            val imrExec2 = InMemoryTableScanExec(imr2.output, Seq.empty, imr2)
            val tqExec1 = TableCacheQueryStageExec(1, imrExec1, imrExec1.canonicalized)
            val tqExec2 = TableCacheQueryStageExec(2, imrExec2, imrExec2.canonicalized)
            assert(tqExec1.canonicalized === tqExec2.canonicalized)
          }
        }
      }
    }
  }
}

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

import java.io.File

import org.scalatest.concurrent.Eventually

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class InputGeneratedOutputMetricsSuite extends QueryTest with SharedSQLContext with Eventually {

  test("Range query input/output/generated metrics") {
    val numRows = 150L
    val numSelectedRows = 100L
    val res = MetricsTestHelper.runAndGetMetrics(spark.range(0, numRows, 1).
      filter(x => x < numSelectedRows).toDF())

    assert(res.recordsRead.sum === 0)
    assert(res.shuffleRecordsRead.sum === 0)
    assert(res.generatedRows === numRows :: Nil)
    assert(res.outputRows === numSelectedRows :: numRows :: Nil)
  }

  test("Input/output/generated metrics with repartitioning") {
    val numRows = 100L
    val res = MetricsTestHelper.runAndGetMetrics(
      spark.range(0, numRows).repartition(3).filter(x => x % 5 == 0).toDF())

    assert(res.recordsRead.sum === 0)
    assert(res.shuffleRecordsRead.sum === numRows)
    assert(res.generatedRows === numRows :: Nil)
    assert(res.outputRows === 20 :: numRows :: Nil)
  }

  test("Input/output/generated metrics with more repartitioning") {
    withTempDir { tempDir =>
      val dir = new File(tempDir, "pqS").getCanonicalPath

      spark.range(10).write.parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("pqS")

      val res = MetricsTestHelper.runAndGetMetrics(
        spark.range(0, 30).repartition(3).crossJoin(sql("select * from pqS")).repartition(2)
            .toDF()
      )

      assert(res.recordsRead.sum == 10)
      assert(res.shuffleRecordsRead.sum == 3 * 10 + 2 * 150)
      assert(res.generatedRows == 30 :: Nil)
      assert(res.outputRows == 10 :: 30 :: 300 :: Nil)
    }
  }
}

object MetricsTestHelper {
  case class AggregatedMetricsResult(
      recordsRead: List[Long],
      shuffleRecordsRead: List[Long],
      generatedRows: List[Long],
      outputRows: List[Long])

  private[this] def extractMetricValues(
      df: DataFrame,
      metricValues: Map[Long, String],
      metricName: String): List[Long] = {
    df.queryExecution.executedPlan.collect {
      case plan if plan.metrics.contains(metricName) =>
        metricValues(plan.metrics(metricName).id).toLong
    }.toList.sorted
  }

  def runAndGetMetrics(df: DataFrame, useWholeStageCodeGen: Boolean = false):
      AggregatedMetricsResult = {
    val spark = df.sparkSession
    val sparkContext = spark.sparkContext

    var recordsRead = List[Long]()
    var shuffleRecordsRead = List[Long]()
    val listener = new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        if (taskEnd.taskMetrics != null) {
          recordsRead = taskEnd.taskMetrics.inputMetrics.recordsRead ::
            recordsRead
          shuffleRecordsRead = taskEnd.taskMetrics.shuffleReadMetrics.recordsRead ::
            shuffleRecordsRead
        }
      }
    }

    val oldExecutionIds = spark.sharedState.listener.executionIdToData.keySet

    val prevUseWholeStageCodeGen =
      spark.sessionState.conf.getConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED)
    try {
      spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, useWholeStageCodeGen)
      sparkContext.listenerBus.waitUntilEmpty(10000)
      sparkContext.addSparkListener(listener)
      df.collect()
      sparkContext.listenerBus.waitUntilEmpty(10000)
    } finally {
      spark.sessionState.conf.setConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED, prevUseWholeStageCodeGen)
    }

    val executionId = spark.sharedState.listener.executionIdToData.keySet.diff(oldExecutionIds).head
    val metricValues = spark.sharedState.listener.getExecutionMetrics(executionId)
    val outputRes = extractMetricValues(df, metricValues, "numOutputRows")
    val generatedRes = extractMetricValues(df, metricValues, "numGeneratedRows")

    AggregatedMetricsResult(recordsRead.sorted, shuffleRecordsRead.sorted, generatedRes, outputRes)
  }
}

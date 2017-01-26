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

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkException, TaskOutputListener}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils


class TaskOutputListenerSuite extends QueryTest with SharedSQLContext with Eventually {

  // This helper is needed for tests doing file I/O. Otherwise a leaked file stream is reported.
  def waitForNoRunningTasks(): Unit = {
    eventually(timeout(60.seconds)) {
      assert(sparkContext.statusTracker.getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  val CartProdInputSize = 100000L
  var listener: SparkListener = null
  val tempDir = Utils.createTempDir().getCanonicalFile
  val tables = List[(String, Long)](("pqS", 10L), ("pqM", 1000L), ("pqL", CartProdInputSize))

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    listener = new TaskOutputListener(sparkContext, 100)
    sparkContext.addSparkListener(listener)

    tables.foreach { tab =>
      val dir = new File(tempDir, tab._1).getCanonicalPath
      spark.range(tab._2).write.parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView(tab._1)
    }
  }

  protected override def afterAll(): Unit = {
    sparkContext.removeSparkListener(listener)
    Utils.deleteRecursively(tempDir)
    super.afterAll()
  }

  test("Range query input/output/generated metrics") {
    val NumRows = 150L
    val NumSelectedRows = 100L
    val res = MetricsTestHelper.runAndGetMetrics(spark.range(0, NumRows, 1).
      filter(x => x < NumSelectedRows).toDF())

    assert(res.recordsRead.sum === 0)
    assert(res.shuffleRecordsRead.sum === 0)
    assert(res.generatedRows === NumRows :: Nil)
    assert(res.outputRows === NumSelectedRows :: NumRows :: Nil)
  }

  test("Input/output/generated metrics with repartitioning") {
    val NumRows = 100L
    val res = MetricsTestHelper.runAndGetMetrics(
      spark.range(0, NumRows).repartition(3).filter(x => x % 5 == 0).toDF())

    assert(res.recordsRead.sum === 0)
    assert(res.shuffleRecordsRead.sum === NumRows)
    assert(res.generatedRows === NumRows :: Nil)
    assert(res.outputRows === 20 :: NumRows :: Nil)
  }

  test("Input/output/generated metrics with more repartitioning") {
    val NumRows = 30L
    val res = MetricsTestHelper.runAndGetMetrics(
      spark.range(0, NumRows).repartition(3).crossJoin(sql("select * from pqS")).repartition(2)
          .toDF()
    )

    assert(res.recordsRead.sum === 10)
    assert(res.shuffleRecordsRead.sum === 3 * 10 + 2 * 150)
    assert(res.generatedRows == NumRows :: Nil)
    assert(res.outputRows === 10 :: 30 :: 300 :: Nil)
  }

  test("Simple CartProd slow query termination.") {
    try {
      val ex = intercept[SparkException] {
        sql(s"select count(*) from pqL cross join pqL").show
      }
      assert(ex.getMessage().contains("spark.outputRatioKillThreshold"))
    } finally {
      waitForNoRunningTasks()
    }
  }

  test("Slow query termination with multiple shuffles.") {
    try {
      val ex = intercept[SparkException] {
        sql("select 1 as A from pqM").repartition(3)
            .crossJoin(sql("select 2 as B from pqM")).repartition(2)
            .crossJoin(sql("select 3 as C from pqL")).repartition(3)
            .filter("A = 1 AND B = 2 AND C = 3").collect()
      }
      assert(ex.getMessage().contains("spark.outputRatioKillThreshold"))
    } finally {
      waitForNoRunningTasks()
    }
  }

  /* This does not work perfectly. The kill request is issued, but the query doesn't die. */
  test("Dataset Range slow query termination.") {
    val ex = intercept[SparkException](
      spark.range(0, CartProdInputSize, 1).crossJoin(spark.range(0, CartProdInputSize, 1)).count()
    )
    assert(ex.getMessage().contains("spark.outputRatioKillThreshold"))
  }
}


object MetricsTestHelper {
  case class AggregatedMetricsResult(recordsRead: List[Long], shuffleRecordsRead: List[Long],
                                     generatedRows: List[Long], outputRows: List[Long])

  def runAndGetMetrics(df: DataFrame, useWholeStageCodeGen: Boolean = false):
      AggregatedMetricsResult = {
    val spark = df.sparkSession
    val sparkContext = spark.sparkContext

    var recordsRead = List[Long]()
    var shuffleRecordsRead = List[Long]()
    val listener = new SparkListener() {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        val metrics = taskEnd.taskMetrics
        recordsRead = taskEnd.taskMetrics.inputMetrics.recordsRead :: recordsRead
        shuffleRecordsRead = taskEnd.taskMetrics.shuffleReadMetrics.recordsRead ::
          shuffleRecordsRead
      }
    }

    val oldExecutionIds = spark.sharedState.listener.executionIdToData.keySet
    spark.conf.set("spark.sql.codegen.wholeStage", useWholeStageCodeGen.toString())

    sparkContext.listenerBus.waitUntilEmpty(10000)
    sparkContext.addSparkListener(listener)
    df.collect()
    sparkContext.listenerBus.waitUntilEmpty(10000)
    sparkContext.removeSparkListener(listener)

    val executionId = spark.sharedState.listener.executionIdToData.keySet.diff(oldExecutionIds).head
    val jobs = spark.sharedState.listener.getExecution(executionId).get.jobs
    val metricValues = spark.sharedState.listener.getExecutionMetrics(executionId)

    val planNodes = SparkPlanGraph(
      SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan)).allNodes

    val outputRes = planNodes.flatMap { node =>
      node.metrics.filter(metric => metric.name == "number of output rows").map { metric =>
        metricValues(metric.accumulatorId)
      }
    }.map(x => x.toLong).sorted.toList
    val generatedRes = planNodes.flatMap { node =>
      node.metrics.filter(metric => metric.name == "number of generated rows").map { metric =>
        metricValues(metric.accumulatorId)
      }
    }.map(x => x.toLong).sorted.toList

    AggregatedMetricsResult(recordsRead.sorted, shuffleRecordsRead.sorted, generatedRes, outputRes)
  }
}

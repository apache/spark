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

import java.util.Properties

import org.apache.spark.{SparkException, Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.{SparkListenerJobStart, SparkListenerStageSubmitted,
  SparkListenerTaskEnd, StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class MaxShuffleWriteBytesListenerSuite extends SharedSparkSession {

  private def shuffleDf(numPartitions: Int = 100) =
    spark.range(0, 1000000L, 1, numPartitions).groupBy(col("id") % 100).count()

  private def shuffleQuery(numPartitions: Int = 100): Array[Row] =
    shuffleDf(numPartitions).collect()

  private def newStageInfo(stageId: Int, attempt: Int): StageInfo =
    new StageInfo(stageId, attempt, "shuffle-stage", 1, Nil, Nil, "",
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)

  private def newTaskEnd(stageId: Int, attempt: Int, bytes: Long): SparkListenerTaskEnd = {
    val taskMetrics = new TaskMetrics()
    taskMetrics.shuffleWriteMetrics.incBytesWritten(bytes)
    val taskInfo = new TaskInfo(0L, 0, attempt, 0, 0L, "localhost", "localhost",
      TaskLocality.NO_PREF, false)
    new SparkListenerTaskEnd(
      stageId, attempt, "ShuffleMapTask", Success, taskInfo, null, taskMetrics)
  }

  test("cancels the query when shuffle write exceeds the threshold") {
    withSQLConf(
      SQLConf.MAX_SHUFFLE_WRITE_BYTES.key -> "1",
      SQLConf.MAX_SHUFFLE_WRITE_BYTES_STOP_APPLICATION.key -> "false") {
      // Enough partitions that the listener's cancelJob lands while tasks are still running,
      // rather than racing the shuffle to completion.
      val e = intercept[SparkException] {
        shuffleQuery(numPartitions = 1000)
      }
      assert(e.getCondition == "SPARK_JOB_CANCELLED")
      assert(e.getMessage.contains("shuffle write size"))
    }
  }

  test("does not cancel the query when shuffle write is below the threshold") {
    withSQLConf(
      SQLConf.MAX_SHUFFLE_WRITE_BYTES.key -> "1g",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = shuffleDf()
      val rows = df.collect()
      assert(rows.length === 100)
      assert(rows.map(_.getLong(1)).sum === 1000000L)
      assert(df.queryExecution.executedPlan.collect { case _: ShuffleExchangeExec => () }.nonEmpty)
    }
  }

  test("is disabled when the threshold is not set") {
    val rows = shuffleQuery()
    assert(rows.length === 100)
    assert(rows.map(_.getLong(1)).sum === 1000000L)
  }

  test("does not double-count shuffle write bytes across a stage retry") {
    val listener = new MaxShuffleWriteBytesListener(spark.sparkContext)
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, "1")
    properties.setProperty(SQLExecution.EXECUTION_ROOT_ID_KEY, "1")
    properties.setProperty(SQLConf.MAX_SHUFFLE_WRITE_BYTES.key, "1000")
    val stageAttempt0 = newStageInfo(1, 0)
    val stageAttempt1 = newStageInfo(1, 1)

    listener.onJobStart(new SparkListenerJobStart(0, 0L, Seq(stageAttempt0), properties))
    listener.onStageSubmitted(new SparkListenerStageSubmitted(stageAttempt0, properties))
    listener.onTaskEnd(newTaskEnd(1, 0, 100L))

    listener.onStageSubmitted(new SparkListenerStageSubmitted(stageAttempt1, properties))
    listener.onTaskEnd(newTaskEnd(1, 1, 100L))
    // A late Success from the superseded attempt must not be counted a second time.
    listener.onTaskEnd(newTaskEnd(1, 0, 100L))

    assert(listener.rootShuffleWriteBytes(1L) === Some(100L))
  }

  test("accumulates shuffle-write bytes across multiple successful tasks") {
    val listener = new MaxShuffleWriteBytesListener(spark.sparkContext)
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, "1")
    properties.setProperty(SQLExecution.EXECUTION_ROOT_ID_KEY, "1")
    properties.setProperty(SQLConf.MAX_SHUFFLE_WRITE_BYTES.key, "1000")
    val stage = newStageInfo(1, 0)
    listener.onJobStart(new SparkListenerJobStart(0, 0L, Seq(stage), properties))
    listener.onStageSubmitted(new SparkListenerStageSubmitted(stage, properties))
    listener.onTaskEnd(newTaskEnd(1, 0, 100L))
    listener.onTaskEnd(newTaskEnd(1, 0, 400L))
    listener.onTaskEnd(newTaskEnd(1, 0, 500L))
    assert(listener.rootShuffleWriteBytes(1L) === Some(1000L))
  }
}

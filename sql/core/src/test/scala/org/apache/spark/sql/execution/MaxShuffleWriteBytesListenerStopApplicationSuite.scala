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

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite,
  Success}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerJobStart,
  SparkListenerStageSubmitted, SparkListenerTaskEnd, StageInfo, TaskInfo, TaskLocality}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

class MaxShuffleWriteBytesListenerStopApplicationSuite
    extends SparkFunSuite with Eventually with LocalSparkContext {

  override def afterEach(): Unit = {
    try {
      SparkSession.getActiveSession.foreach(_.stop())
      SparkSession.clearActiveSession()
      SparkSession.getDefaultSession.foreach(_.stop())
      SparkSession.clearDefaultSession()
    } finally {
      super.afterEach()
    }
  }

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

  test("stopApplication=true stops the application when the threshold is exceeded") {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("max-shuffle-write-bytes-stop-application")
      .set("spark.ui.enabled", "false")
      .set(SQLConf.MAX_SHUFFLE_WRITE_BYTES.key, "1")
      .set(SQLConf.MAX_SHUFFLE_WRITE_BYTES_STOP_APPLICATION.key, "true")
    sc = new SparkContext(conf)
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

    // The offending job is cancelled with the descriptive reason before the application is
    // stopped, so the failure the driver sees carries the friendly reason rather than the generic
    // "SparkContext was shut down" message.
    val e = intercept[SparkException] {
      spark.range(0, 1000000L, 1, 100).groupBy(col("id") % 100).count().collect()
    }
    assert(e.getMessage.contains("shuffle write size"))

    eventually(timeout(30.seconds), interval(200.millis)) {
      assert(sc.isStopped,
        "application should be stopped once shuffle write exceeds the threshold")
      // isStopped flips at the start of SparkContext.stop(), and a later stop()
      // returns immediately. The context stays active until stop() finishes;
      // wait for that so the next suite in this JVM can start (SPARK-2243).
      assert(SparkContext.getActive.isEmpty,
        "stop() should finish and clear the active SparkContext")
    }
  }

  test("waits for all offending jobs to end before stopping the application") {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("max-shuffle-write-bytes-stop-application-multi-job")
      .set("spark.ui.enabled", "false")
    sc = new SparkContext(conf)

    val listener = new MaxShuffleWriteBytesListener(sc)
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ID_KEY, "1")
    properties.setProperty(SQLExecution.EXECUTION_ROOT_ID_KEY, "1")
    properties.setProperty(SQLConf.MAX_SHUFFLE_WRITE_BYTES.key, "1")
    properties.setProperty(SQLConf.MAX_SHUFFLE_WRITE_BYTES_STOP_APPLICATION.key, "true")

    val stage1 = newStageInfo(1, 0)
    val stage2 = newStageInfo(2, 0)
    listener.onJobStart(new SparkListenerJobStart(0, 0L, Seq(stage1), properties))
    listener.onJobStart(new SparkListenerJobStart(1, 0L, Seq(stage2), properties))
    listener.onStageSubmitted(new SparkListenerStageSubmitted(stage1, properties))
    listener.onStageSubmitted(new SparkListenerStageSubmitted(stage2, properties))

    // Exceeding the threshold cancels both jobs, but the application must not stop yet.
    listener.onTaskEnd(newTaskEnd(1, 0, 100L))
    assert(!sc.isStopped)

    // Wait for every offending job to end, not just the first one.
    listener.onJobEnd(new SparkListenerJobEnd(0, 0L, JobSucceeded))
    Thread.sleep(1000)
    assert(!sc.isStopped, "application must not stop until all offending jobs have ended")

    listener.onJobEnd(new SparkListenerJobEnd(1, 0L, JobSucceeded))
    eventually(timeout(30.seconds), interval(200.millis)) {
      assert(sc.isStopped)
      // isStopped flips at the start of SparkContext.stop(), which runs on a daemon thread, so a
      // later stop() returns immediately. The context stays active until stop() finishes; wait for
      // that so the next suite in this JVM can start (SPARK-2243).
      assert(SparkContext.getActive.isEmpty)
    }
  }
}

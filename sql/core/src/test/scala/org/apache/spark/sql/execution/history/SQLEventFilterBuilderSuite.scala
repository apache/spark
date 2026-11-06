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

package org.apache.spark.sql.execution.history

import java.util.Properties

import scala.io.{Codec, Source}

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.{CompactionResultCode, EventLogFileCompactor, EventLogFileReader, EventLogFileWriter, EventLogTestHelper}
import org.apache.spark.internal.config.History
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ListenerEventsTestHelper
import org.apache.spark.util.Utils

class SQLEventFilterBuilderSuite extends SparkFunSuite {
  import ListenerEventsTestHelper._

  override protected def beforeEach(): Unit = {
    ListenerEventsTestHelper.reset()
  }

  test("track live SQL executions") {
    var time = 0L

    val listener = new SQLEventFilterBuilder

    listener.onOtherEvent(SparkListenerLogStart("TestSparkVersion"))

    // Start the application.
    time += 1
    listener.onApplicationStart(SparkListenerApplicationStart(
      "name",
      Some("id"),
      time,
      "user",
      Some("attempt"),
      None))

    // Start a couple of executors.
    time += 1
    val execIds = Array("1", "2")
    execIds.foreach { id =>
      listener.onExecutorAdded(createExecutorAddedEvent(id, time))
    }

    // Start SQL Execution
    listener.onOtherEvent(
      SparkListenerSQLExecutionStart(1, Some(1), "desc1", "details1", "plan",
      new SparkPlanInfo("node", "str", Seq.empty, Map.empty, Seq.empty), time, Map.empty))

    time += 1

    // job 1, 2: coupled with SQL execution 1, finished
    val jobProp = createJobProps()
    val jobPropWithSqlExecution = new Properties(jobProp)
    jobPropWithSqlExecution.setProperty(SQLExecution.EXECUTION_ID_KEY, "1")
    val jobInfoForJob1 = pushJobEventsWithoutJobEnd(listener, 1, jobPropWithSqlExecution,
      execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    val jobInfoForJob2 = pushJobEventsWithoutJobEnd(listener, 2, jobPropWithSqlExecution,
      execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(2, time, JobSucceeded))

    // job 3: not coupled with SQL execution 1, finished
    pushJobEventsWithoutJobEnd(listener, 3, jobProp, execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(3, time, JobSucceeded))

    // job 4: not coupled with SQL execution 1, not finished
    pushJobEventsWithoutJobEnd(listener, 4, jobProp, execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(4, time, JobSucceeded))

    assert(listener.liveSQLExecutions === Set(1))

    // only SQL executions related jobs are tracked
    assert(listener.liveJobs === Set(1, 2))
    assert(listener.liveStages ===
      (jobInfoForJob1.stageIds ++ jobInfoForJob2.stageIds).toSet)
    assert(listener.liveTasks ===
      (jobInfoForJob1.stageToTaskIds.values.flatten ++
        jobInfoForJob2.stageToTaskIds.values.flatten).toSet)
    assert(listener.liveRDDs ===
      (jobInfoForJob1.stageToRddIds.values.flatten ++
        jobInfoForJob2.stageToRddIds.values.flatten).toSet)

    // End SQL execution
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(1, 0))

    assert(listener.liveSQLExecutions.isEmpty)
    assert(listener.liveJobs.isEmpty)
    assert(listener.liveStages.isEmpty)
    assert(listener.liveTasks.isEmpty)
    assert(listener.liveRDDs.isEmpty)
  }

  test("SPARK-60110: Don't compact files if a live SQL execution start is skipped") {
    withTempDir { dir =>
      val sparkConf = new SparkConf()
      val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)
      val fs = new Path(dir.getAbsolutePath).getFileSystem(hadoopConf)
      val planInfo = new SparkPlanInfo("node", "str", Seq.empty, Map.empty, Seq.empty)
      val maxLineLength = 8 * 1024

      // SQL execution 1 is still live and has no jobs yet, and its start line exceeds the
      // line-length limit. SQL execution 2 is finished.
      val sqlStart1 = SparkListenerSQLExecutionStart(1, Some(1), "desc1", "details1",
        "x" * maxLineLength, planInfo, 0L)
      val sqlStart2 = SparkListenerSQLExecutionStart(2, Some(2), "desc2", "details2", "plan",
        planInfo, 0L)
      val appStart = SparkListenerApplicationStart("app", Some("app"), 0, "user", None)

      // 1~2 are candidates to compact, 3~5 are dummies to ensure max files to retain
      val fileStatuses = EventLogTestHelper.writeEventsToRollingWriter(fs, "app", dir,
        sparkConf, hadoopConf,
        Seq(sqlStart1),
        Seq(sqlStart2, SparkListenerSQLExecutionEnd(2, 0L)),
        Seq(appStart),
        Seq(appStart),
        Seq(appStart))
      val maxFilesToRetain = 3

      // Replay skips the start of SQL execution 1, so the filters would treat it as finished
      // and drop it. Compaction must not proceed.
      val limitedConf = sparkConf.clone()
        .set(History.EVENT_LOG_MAX_LINE_LENGTH, maxLineLength.toLong)
      val limitedCompactor = new EventLogFileCompactor(limitedConf, hadoopConf, fs,
        maxFilesToRetain, 0.0d)
      val result = limitedCompactor.compact(fileStatuses)
      assert(result.code === CompactionResultCode.INCOMPLETE_REPLAY)
      fileStatuses.foreach { status => assert(fs.exists(status.getPath)) }

      // With the default limit, the start of live SQL execution 1 is kept in the compact file.
      val compactor = new EventLogFileCompactor(sparkConf, hadoopConf, fs, maxFilesToRetain, 0.0d)
      assert(compactor.compact(fileStatuses).code === CompactionResultCode.SUCCESS)
      val compactFilePath = new Path(fileStatuses(1).getPath.getParent,
        fileStatuses(1).getPath.getName + EventLogFileWriter.COMPACTED)
      Utils.tryWithResource(EventLogFileReader.openEventLog(compactFilePath, fs)) { is =>
        val lines = Source.fromInputStream(is)(Codec.UTF8).getLines().toList
        assert(lines === Seq(EventLogTestHelper.convertEvent(sqlStart1)))
      }
    }
  }
}

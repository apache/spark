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

package org.apache.spark.sql.execution.streaming

import java.io.File

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.execution.streaming.runtime.{AsyncProgressTrackingMicroBatchExecution, StreamExecution}
import org.apache.spark.sql.execution.streaming.runtime.AsyncProgressTrackingMicroBatchExecution.{ASYNC_PROGRESS_TRACKING_CHECKPOINTING_INTERVAL_MS, ASYNC_PROGRESS_TRACKING_ENABLED}
import org.apache.spark.sql.execution.streaming.runtime.MicroBatchExecution
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemorySink
import org.apache.spark.sql.execution.streaming.sources.LowLatencyMemoryStream
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.execution.streaming.state.{FailureInjectionCheckpointFileManager, FailureInjectionFileSystem}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamRealTimeModeManualClockSuiteBase}
import org.apache.spark.util.Utils


private class UnsupportedSink extends MemorySink {
  override def name(): String = "UnsupportedSink"
}

class AsyncProgressTrackingRealTimeModeSuite
  extends StreamRealTimeModeManualClockSuiteBase with BeforeAndAfter with Matchers {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  def waitPendingOffsetWrites(streamExecution: StreamExecution): Unit = {
    assert(streamExecution.isInstanceOf[AsyncProgressTrackingMicroBatchExecution])
    eventually(timeout(Span(60, Seconds))) {
      streamExecution
        .asInstanceOf[AsyncProgressTrackingMicroBatchExecution]
        .areWritesPendingOrInProgress() should be(false)
    }
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getBatchIdsSortedFromLog(logPath: String): List[Int] = {
    getListOfFiles(logPath)
      .filter(file => !file.isHidden)
      .map(file => file.getName.toInt)
      .sorted
  }

  test("async progress tracking happy path") {
    val checkpointLocation = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    val inputData = LowLatencyMemoryStream[Int]
    val df = inputData.toDF()

    testStream(
      df,
      outputMode = OutputMode.Update(),
      sink = new ContinuousMemorySink(),
      extraOptions = Map(
        ASYNC_PROGRESS_TRACKING_ENABLED -> "true",
        ASYNC_PROGRESS_TRACKING_CHECKPOINTING_INTERVAL_MS -> "0"
      ))(
      StartStream(checkpointLocation = checkpointLocation),
      AddData(inputData, 0 until 10: _*),
      CheckAnswerWithTimeout(60000, 0 until 10: _*),
      advanceRealTimeClock,
      AddData(inputData, 10 until 20: _*),
      CheckAnswerWithTimeout(60000, 0 until 20: _*),
      advanceRealTimeClock,
      AddData(inputData, 20 until 30: _*),
      CheckAnswerWithTimeout(60000, 0 until 30: _*),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(2),
      Execute { query =>
        waitPendingOffsetWrites(query)
        getBatchIdsSortedFromLog(checkpointLocation + "/offsets") should
          equal(Array(0, 1, 2))
        getBatchIdsSortedFromLog(checkpointLocation + "/commits") should
          equal(Array(0, 1, 2))
      },
    )
  }

  test("async progress tracking enabled by default in RTM for stateless queries") {
    val inputData = LowLatencyMemoryStream[Int]
    val df = inputData.toDF()
    testStream(df, outputMode = OutputMode.Update)(
      AssertOnQuery(_.getClass == classOf[AsyncProgressTrackingMicroBatchExecution]))
  }

  test("user setting for APT takes precedence over default behavior") {
    // We respect the user setting even if the query is APT-compatible
    {
      val inputData = LowLatencyMemoryStream[Int]
      val df = inputData.toDF()
      testStream(
        df,
        outputMode = OutputMode.Update,
        extraOptions = Map(ASYNC_PROGRESS_TRACKING_ENABLED -> "false"))(
        AssertOnQuery(_.getClass == classOf[MicroBatchExecution]))
    }

    {
      val inputData = LowLatencyMemoryStream[Int]
      val df = inputData.toDF()
      testStream(
        df,
        outputMode = OutputMode.Update,
        extraOptions = Map(ASYNC_PROGRESS_TRACKING_ENABLED -> "true"))(
        AssertOnQuery(_.getClass == classOf[AsyncProgressTrackingMicroBatchExecution]))
    }

    // Even if it would lead to an error
    {
      val inputData = LowLatencyMemoryStream[Int]
      val df = inputData.toDF()
      val e = intercept[IllegalArgumentException] {
        testStream(
          df,
          outputMode = OutputMode.Update,
          sink = new UnsupportedSink(),
          extraOptions = Map(ASYNC_PROGRESS_TRACKING_ENABLED -> "true"))()
      }
      assert(e.getMessage.contains("does not support async progress tracking"))
    }
  }

  test("we do not enable APT in RTM by default when the logical plan is stateful") {
    val inputData = LowLatencyMemoryStream[Int]
    val df = inputData.toDF().groupBy("value").count()
    testStream(df, outputMode = OutputMode.Update)(
      AssertOnQuery(_.getClass != classOf[AsyncProgressTrackingMicroBatchExecution]))
  }

  test("async progress tracking recovery") {
    val checkpointLocation = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    val inputData = LowLatencyMemoryStream[Int]
    val df = inputData.toDF()
    val sink = new ContinuousMemorySink()

    testStream(df, outputMode = OutputMode.Update(), sink = sink, extraOptions = Map(
      ASYNC_PROGRESS_TRACKING_ENABLED -> "true",
      ASYNC_PROGRESS_TRACKING_CHECKPOINTING_INTERVAL_MS -> "0"
    ))(
      StartStream(checkpointLocation = checkpointLocation),
      AddData(inputData, 0 until 10: _*),
      CheckAnswerWithTimeout(60000, 0 until 10: _*),
      advanceRealTimeClock,
      AddData(inputData, 10 until 20: _*),
      CheckAnswerWithTimeout(60000, 0 until 20: _*),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(1),
      StopStream, // Should automatically wait for pending writes.
      Execute { _ =>
        sink.clear()
        getBatchIdsSortedFromLog(checkpointLocation + "/offsets") should
          equal(Array(0, 1))
        getBatchIdsSortedFromLog(checkpointLocation + "/commits") should
          equal(Array(0, 1))
      },
      // Restart the query.
      StartStream(checkpointLocation = checkpointLocation),
      AddData(inputData, 100),
      CheckAnswerWithTimeout(60000, 100), // Should not reprocess old data.
    )
  }

  test("current batch wait for the previous batch to commit") {
    withSQLConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
        classOf[FailureInjectionCheckpointFileManager].getName) {
      withTempDir { checkpointDir =>
        val injectionState = FailureInjectionFileSystem.registerTempPath(checkpointDir.getPath)
        try {
          val inputData = LowLatencyMemoryStream[Int]
          val df = inputData.toDF()

          // This regex will delay the createAtomic() of commit file for batch 1.
          injectionState.delayCreateAtomicRegex = Seq(".*/commits/1")

          testStream(df, outputMode = OutputMode.Update(), sink = new ContinuousMemorySink(),
            extraOptions = Map(
              ASYNC_PROGRESS_TRACKING_ENABLED -> "true",
              ASYNC_PROGRESS_TRACKING_CHECKPOINTING_INTERVAL_MS -> "0"
            ))(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath),

            // Batch 0 - should complete normally.
            AddData(inputData, 0 until 10: _*),
            CheckAnswerWithTimeout(60000, 0 until 10: _*),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(0),

            // Batch 1 - the commit will be delayed!
            AddData(inputData, 10 until 20: _*),
            CheckAnswerWithTimeout(60000, 0 until 20: _*),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(1),

            // Batch 2 - should wait for batch 1's commit to complete.
            AddData(inputData, 20 until 30: _*),
            CheckAnswerWithTimeout(60000, 0 until 30: _*),
            advanceRealTimeClock,
            // At this point batch 2 should be blocked waiting for batch 1's commit.

            // Verify that batch 2 is waiting.
            Execute { query =>
              Thread.sleep(2000) // wait a bit to ensure batch 2 is blocked
              query.lastProgress.batchId should be(1) // still at batch 1
            },

            // Signal the delayed streams to allow batch 1's commit to finish, and unblock batch 2.
            Execute { q =>
              FailureInjectionFileSystem.createAtomicDelaySemaphore.release()
              // Clear the regex so subsequent batches aren't delayed.
              injectionState.delayCreateAtomicRegex = Seq.empty
            },
            WaitUntilBatchProcessed(2),
            Execute { query =>
              waitPendingOffsetWrites(query)
              getBatchIdsSortedFromLog(checkpointDir.getAbsolutePath + "/offsets") should
                equal(Array(0, 1, 2))
              getBatchIdsSortedFromLog(checkpointDir.getAbsolutePath + "/commits") should
                equal(Array(0, 1, 2))
            },
            StopStream
          )
        } finally {
          FailureInjectionFileSystem.removePathFromTempToInjectionState(checkpointDir.getPath)
        }
      }
    }
  }

  // Inject a close() failure on the batch-1 write of the given log ("commits" or "offsets") and
  // assert the stream fails fast with the matching categorized log-write-failure error, preserving
  // the original IOException as the cause. Covers both the commit-log and offset-log async write
  // failure paths in RTM.
  private def testAsyncLogWriteFailure(logType: String, expectedErrorClass: String): Unit = {
    withSQLConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
        classOf[FailureInjectionCheckpointFileManager].getName) {
      withTempDir { checkpointDir =>
        val injectionState = FailureInjectionFileSystem.registerTempPath(checkpointDir.getPath)
        try {
          val inputData = LowLatencyMemoryStream[Int]
          val df = inputData.toDF()

          // This regex will make the close() of the batch-1 log file for `logType` throw error.
          injectionState.createAtomicDelayCloseRegex = Seq(s".*/$logType/1")

          testStream(df, outputMode = OutputMode.Update(), sink = new ContinuousMemorySink(),
            extraOptions = Map(
              ASYNC_PROGRESS_TRACKING_ENABLED -> "true",
              ASYNC_PROGRESS_TRACKING_CHECKPOINTING_INTERVAL_MS -> "0"
            ))(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath),

            // Batch 0 - should complete normally.
            AddData(inputData, 0 until 10: _*),
            CheckAnswerWithTimeout(60000, 0 until 10: _*),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(0),

            // Batch 1 - The close() will throw IOException, and the stream should fail fast.
            AddData(inputData, 10 until 20: _*),
            CheckAnswerWithTimeout(60000, 0 until 20: _*),
            advanceRealTimeClock,
            ExpectFailure[Exception](
              assertFailure = e => {
                // The async log write failure surfaces in one of two valid ways, depending on a
                // benign race in how the execution thread observes the error: either wrapped in
                // STREAMING_ASYNC_OPERATION_FAILED (when the thread is interrupted mid-batch) or
                // thrown directly (when the post-batch error check picks it up first). Unwrap the
                // async wrapper if present, then assert on the categorized log write failure.
                val categorized = e match {
                  case sre: SparkRuntimeException
                      if sre.getCondition == "STREAMING_ASYNC_OPERATION_FAILED" => sre.getCause
                  case other => other
                }
                val sparkEx = categorized.asInstanceOf[SparkException]
                checkError(
                  sparkEx,
                  expectedErrorClass,
                  parameters = Map("batchId" -> "1", "checkpointLocation" -> ".*"),
                  matchPVals = true)
                // The original IOException must be preserved as the root cause.
                assert(sparkEx.getCause.isInstanceOf[java.io.IOException])
                assert(sparkEx.getCause.getMessage === "Fake File Stream Close Failure")
              },
              typeIsSuperClass = true),
          )
        } finally {
          FailureInjectionFileSystem.removePathFromTempToInjectionState(checkpointDir.getPath)
        }
      }
    }
  }

  test("throw exceptions immediately on commit log write failure") {
    testAsyncLogWriteFailure("commits", StreamingErrors.COMMIT_LOG_WRITE_FAILURE)
  }

  test("throw exceptions immediately on offset log write failure") {
    testAsyncLogWriteFailure("offsets", StreamingErrors.OFFSET_LOG_WRITE_FAILURE)
  }
}

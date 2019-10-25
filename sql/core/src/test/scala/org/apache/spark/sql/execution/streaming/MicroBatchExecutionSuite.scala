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
import java.util.UUID

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest

class MicroBatchExecutionSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("SPARK-24156: do not plan a no-data batch again after it has already been planned") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(df)(
      AddData(inputData, 10, 11, 12, 13, 14, 15), // Set watermark to 5
      CheckAnswer(),
      AddData(inputData, 25), // Set watermark to 15 to make MicroBatchExecution run no-data batch
      CheckAnswer((10, 5)),   // Last batch should be a no-data batch
      StopStream,
      Execute { q =>
        // Delete the last committed batch from the commit log to signify that the last batch
        // (a no-data batch) never completed
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purgeAfter(commit - 1)
      },
      // Add data before start so that MicroBatchExecution can plan a batch. It should not,
      // it should first re-run the incomplete no-data batch and then run a new batch to process
      // new data.
      AddData(inputData, 30),
      StartStream(),
      CheckNewAnswer((15, 1)),   // This should not throw the error reported in SPARK-24156
      StopStream,
      Execute { q =>
        // Delete the entire commit log
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purge(commit + 1)
      },
      AddData(inputData, 50),
      StartStream(),
      CheckNewAnswer((25, 1), (30, 1))   // This should not throw the error reported in SPARK-24156
    )
  }

  test("Add config to retry spark streaming's meta log when it met") {
    val s = MemoryStream[Int]
    // Specified checkpointLocation manually to init metadata file
    withTempDir { f =>
      val tmp = f.getAbsolutePath

      testStream(s.toDF())(
        StartStream(checkpointLocation = tmp)
      )

      // fail with less retries
      withSQLConf(
        SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
          classOf[FakeFileSystemBasedCheckpointFileManager].getName,
        SQLConf.STREAMING_META_DATA_NUM_RETRIES.key ->
          1.toString) {
        val e = intercept[Throwable] {
          testStream(s.toDF())(
            StartStream(checkpointLocation = tmp),
            AddData(s, 1),
            CheckAnswer(1)
          )
        }
        assert(e.getMessage.contains("Failed to write meta data log after retry"))
      }

      // ok with enough retries
      withSQLConf(
        SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
          classOf[FakeFileSystemBasedCheckpointFileManager].getName,
        SQLConf.STREAMING_META_DATA_NUM_RETRIES.key ->
          SQLConf.STREAMING_META_DATA_NUM_RETRIES.defaultValue.get.toString) {
        testStream(s.toDF())(
          StartStream(checkpointLocation = tmp),
          AddData(s, 1),
          CheckAnswer(1, 1)
        )
      }
    }
  }
}

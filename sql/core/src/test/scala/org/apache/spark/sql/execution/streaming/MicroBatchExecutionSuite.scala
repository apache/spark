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

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should._
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.SparkDataStream
import org.apache.spark.sql.functions.{count, timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest, Trigger}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.Utils

class MicroBatchExecutionSuite extends StreamTest with BeforeAndAfter with Matchers {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  test("async log purging") {
    withSQLConf(SQLConf.MIN_BATCHES_TO_RETAIN.key -> "2", SQLConf.ASYNC_LOG_PURGE.key -> "true") {
      withTempDir { checkpointLocation =>
        val inputData = new MemoryStream[Int](id = 0, sqlContext = sqlContext)
        val ds = inputData.toDS()
        testStream(ds)(
          StartStream(checkpointLocation = checkpointLocation.getCanonicalPath),
          AddData(inputData, 0),
          CheckNewAnswer(0),
          AddData(inputData, 1),
          CheckNewAnswer(1),
          Execute { q =>
            getListOfFiles(s"$checkpointLocation/offsets")
              .filter(file => !file.isHidden)
              .map(file => file.getName.toInt)
              .sorted should equal(Array(0, 1))
            getListOfFiles(s"$checkpointLocation/commits")
              .filter(file => !file.isHidden)
              .map(file => file.getName.toInt)
              .sorted should equal(Array(0, 1))
          },
          AddData(inputData, 2),
          CheckNewAnswer(2),
          AddData(inputData, 3),
          CheckNewAnswer(3),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }

            getListOfFiles(s"$checkpointLocation/offsets")
              .filter(file => !file.isHidden)
              .map(file => file.getName.toInt)
              .sorted should equal(Array(1, 2, 3))
            getListOfFiles(s"$checkpointLocation/commits")
              .filter(file => !file.isHidden)
              .map(file => file.getName.toInt)
              .sorted should equal(Array(1, 2, 3))
          },
          StopStream
        )
      }
    }
  }

  test("error notifier test") {
    withSQLConf(SQLConf.MIN_BATCHES_TO_RETAIN.key -> "2", SQLConf.ASYNC_LOG_PURGE.key -> "true") {
      withTempDir { checkpointLocation =>
        val inputData = new MemoryStream[Int](id = 0, sqlContext = sqlContext)
        val ds = inputData.toDS()
        val e = intercept[StreamingQueryException] {

          testStream(ds)(
            StartStream(checkpointLocation = checkpointLocation.getCanonicalPath),
            AddData(inputData, 0),
            CheckNewAnswer(0),
            AddData(inputData, 1),
            CheckNewAnswer(1),
            Execute { q =>
              q.asInstanceOf[MicroBatchExecution].errorNotifier.markError(new Exception("test"))
            },
            AddData(inputData, 2),
            CheckNewAnswer(2))
        }
        e.getCause.getMessage should include("test")
      }
    }
  }

  test("SPARK-24156: do not plan a no-data batch again after it has already been planned") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
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

  test("SPARK-38033: SS cannot be started because the commitId and offsetId are inconsistent") {
    val inputData = MemoryStream[Int]
    val streamEvent = inputData.toDF().select("value")

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-test-offsetId-commitId-inconsistent/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    testStream(streamEvent) (
      AddData(inputData, 1, 2, 3, 4, 5, 6),
      StartStream(Trigger.AvailableNow(), checkpointLocation = checkpointDir.getAbsolutePath),
      ExpectFailure[IllegalStateException] { e =>
        assert(e.getMessage.contains("batch 3 doesn't exist"))
      }
    )
  }

  test("no-data-batch re-executed after restart should call V1 source.getBatch()") {
    val testSource = ReExecutedBatchTestSource(spark)
    val df = testSource.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long])

    /** Reset this test source so that it appears to be a new source requiring initialization */
    def resetSource(): StreamAction = Execute("reset source") { _ =>
      testSource.reset()  // Make it look like a new source that needs to be re-initialized
      require(testSource.currentOffset === 0)
      require(testSource.getBatchCallCount === 0)
    }

    /** Add data to this test source by incrementing its available offset */
    def addData(numNewRows: Int): StreamAction = new AddData {
      override def addData(query: Option[StreamExecution]): (SparkDataStream, streaming.Offset) = {
        testSource.incrementAvailableOffset(numNewRows)
        (testSource, testSource.getOffset.get)
      }
    }

    testStream(df)(
      addData(numNewRows = 10),   // generate values 1...10, sets watermark to 0
      CheckAnswer(),
      addData(numNewRows = 10),   // generate values 11...20, sets watermark to 10
      ProcessAllAvailable(),      // let no-data-batch be executed
      CheckAnswer(0, 5),          // start time of windows closed and outputted
      Execute("verify source internal state before stop") { q =>
        // Last batch should be a no-data batch
        require(q.lastProgress.numInputRows === 0)
        // Source should have expected internal state
        require(testSource.currentOffset === 20)
        // getBatch should be called only for 2 batches with data, not for no-data-batches
        assert(testSource.getBatchCallCount === 2)
      },
      StopStream,

      /* Verify that if the last no-data-batch was incomplete, getBatch() is called only once */
      Execute("mark last batch as incomplete") { q =>
        // Delete the last committed batch from the commit log to signify that the last batch
        // (a no-data batch) did not complete and has to be re-executed on restart.
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purgeAfter(commit - 1)
      },
      resetSource(),
      StartStream(),
      ProcessAllAvailable(),  // allow initialization and re-execution
      Execute("verify source.getBatch() called after re-executed no-data-batch") { q =>
        // After restart, getBatch() should be called once even for no-data batch
        assert(testSource.getBatchCallCount === 1)
        assert(testSource.currentOffset === 20)
      },
      addData(numNewRows = 10),   // generate values 21...30, sets watermark to 20
      ProcessAllAvailable(),      // let no-data-batch be executed
      CheckAnswer(0, 5, 10, 15),
      StopStream,

      /* Verify that if the last no-data-batch was complete, getBatch() is still called only once */
      Execute("verify last batch was complete") { q =>
        // Verify that the commit log records the last batch as completed
        require(q.commitLog.getLatest().map(_._1).get === q.offsetLog.getLatest().map(_._1).get)
      },
      resetSource(),
      StartStream(),
      ProcessAllAvailable(),      // allow initialization to completed
      Execute("verify source.getBatch() called even if no-data-batch was not re-executed") { q =>
        // After restart, getBatch() should be called even for no-data batch, but only once
        assert(testSource.getBatchCallCount === 1)
        assert(testSource.currentOffset === 30)
      },
      addData(numNewRows = 10),   // generate values 31...40, sets watermark to 30
      ProcessAllAvailable(),      // let no-data-batch be executed
      CheckAnswer(0, 5, 10, 15, 20, 25)
    )
  }

  case class ReExecutedBatchTestSource(spark: SparkSession) extends Source {
    @volatile var currentOffset = 0L
    @volatile var getBatchCallCount = 0

    override def getOffset: Option[Offset] = {
      if (currentOffset <= 0) None else Some(LongOffset(currentOffset))
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
      getBatchCallCount = getBatchCallCount + 1
      if (currentOffset == 0) currentOffset = getOffsetValue(end)
      val plan = Range(
        start.map(getOffsetValue).getOrElse(0L) + 1L, getOffsetValue(end) + 1L, 1, None,
        isStreaming = true)
      Dataset.ofRows(spark, plan)
    }

    def incrementAvailableOffset(numNewRows: Int): Unit = {
      currentOffset = currentOffset + numNewRows
    }

    def reset(): Unit = {
      currentOffset = 0L
      getBatchCallCount = 0
    }
    def toDF(): DataFrame = Dataset.ofRows(spark, StreamingExecutionRelation(this, spark))
    override def schema: StructType = new StructType().add("value", LongType)
    override def stop(): Unit = {}
    private def getOffsetValue(offset: Offset): Long = {
      offset match {
        case s: SerializedOffset => LongOffset(s).offset
        case l: LongOffset => l.offset
        case _ => throw new IllegalArgumentException("incorrect offset type: " + offset)
      }
    }
  }
}

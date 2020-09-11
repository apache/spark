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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{LongType, StructType}

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

  test("no-data-batch re-executed after restart should call V1 source.getBatch()") {
    val testSource = ReExecutedBatchTestSource(spark)
    val df = testSource.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long])

    /** Reset this test source so that it appears to be a new source requiring initialization */
    def resetSource(): StreamAction = Execute("reset source") { _ =>
      testSource.reset()  // Make it look like a new source that needs to be re-initialized
      require(testSource.currentOffset === 0)
      require(testSource.getBatchCallCount === 0)
    }

    /** Add data to this test source by incrementing its available offset */
    def addData(numNewRows: Int): StreamAction = new AddData {
      override def addData(query: Option[StreamExecution]): (BaseStreamingSource, Offset) = {
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


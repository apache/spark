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

package org.apache.spark.sql.streaming

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{Encoder, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{AcceptsLatestSeenOffset, SparkDataStream}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemoryStream, ContinuousMemoryStreamOffset}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class AcceptsLatestSeenOffsetSuite extends StreamTest with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("DataSource V1 source with micro-batch is not supported") {
    val testSource = new TestSource(spark)
    val df = testSource.toDF()

    /** Add data to this test source by incrementing its available offset */
    def addData(numNewRows: Int): StreamAction = new AddData {
      override def addData(
          query: Option[StreamExecution]): (SparkDataStream, streaming.Offset) = {
        testSource.incrementAvailableOffset(numNewRows)
        (testSource, testSource.getOffset.get)
      }
    }

    addData(10)
    val query = df.writeStream.format("console").start()
    val exc = intercept[StreamingQueryException] {
      query.processAllAvailable()
    }
    assert(exc.getMessage.contains(
      "AcceptsLatestSeenOffset is not supported with DSv1 streaming source"))
  }

  test("DataSource V2 source with micro-batch") {
    val inputData = new TestMemoryStream[Long](0, spark.sqlContext)
    val df = inputData.toDF().select("value")

    /** Add data to this test source by incrementing its available offset */
    def addData(values: Array[Long]): StreamAction = new AddData {
      override def addData(
          query: Option[StreamExecution]): (SparkDataStream, streaming.Offset) = {
        (inputData, inputData.addData(values))
      }
    }

    testStream(df)(
      StartStream(),
      addData((1L to 10L).toArray),
      ProcessAllAvailable(),
      Execute("latest seen offset should be null") { _ =>
        // this verifies that the callback method is not called for the new query
        assert(inputData.latestSeenOffset === null)
      },
      StopStream,

      StartStream(),
      addData((11L to 20L).toArray),
      ProcessAllAvailable(),
      Execute("latest seen offset should be 0") { _ =>
        assert(inputData.latestSeenOffset === LongOffset(0))
      },
      StopStream,

      Execute("mark last batch as incomplete") { q =>
        // Delete the last committed batch from the commit log to signify that the last batch
        // (a no-data batch) did not complete and has to be re-executed on restart.
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purgeAfter(commit - 1)
      },
      StartStream(),
      addData((21L to 30L).toArray),
      ProcessAllAvailable(),
      Execute("latest seen offset should be 1") { _ =>
        assert(inputData.latestSeenOffset === LongOffset(1))
      }
    )
  }

  test("DataSource V2 source with micro-batch - rollback of microbatch 0") {
    //  Test case: when the query is restarted, we expect the execution to call `latestSeenOffset`
    //  first. Later as part of the execution, execution may call `initialOffset` if the previous
    //  run of the query had no committed batches.
    val inputData = new TestMemoryStream[Long](0, spark.sqlContext)
    val df = inputData.toDF().select("value")

    /** Add data to this test source by incrementing its available offset */
    def addData(values: Array[Long]): StreamAction = new AddData {
      override def addData(
        query: Option[StreamExecution]): (SparkDataStream, streaming.Offset) = {
        (inputData, inputData.addData(values))
      }
    }

    testStream(df)(
      StartStream(),
      addData((1L to 10L).toArray),
      ProcessAllAvailable(),
      Execute("latest seen offset should be null") { _ =>
        // this verifies that the callback method is not called for the new query
        assert(inputData.latestSeenOffset === null)
      },
      StopStream,

      Execute("mark last batch as incomplete") { q =>
        // Delete the last committed batch from the commit log to signify that the last batch
        // (a no-data batch) did not complete and has to be re-executed on restart.
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purgeAfter(commit - 1)
      },

      Execute("reset flag initial offset called flag") { q =>
        inputData.assertInitialOffsetIsCalledAfterLatestOffsetSeen = true
      },
      StartStream(),
      addData((11L to 20L).toArray),
      ProcessAllAvailable(),
      Execute("latest seen offset should be 0") { _ =>
        assert(inputData.latestSeenOffset === LongOffset(0))
      },
      StopStream
    )
  }

  test("DataSource V2 source with continuous mode") {
    val inputData = new TestContinuousMemoryStream[Long](0, spark.sqlContext, 1)
    val df = inputData.toDF().select("value")

    /** Add data to this test source by incrementing its available offset */
    def addData(values: Array[Long]): StreamAction = new AddData {
      override def addData(
          query: Option[StreamExecution]): (SparkDataStream, streaming.Offset) = {
        (inputData, inputData.addData(values))
      }
    }

    testStream(df)(
      StartStream(trigger = Trigger.Continuous("1 hour")),
      addData((1L to 10L).toArray),
      AwaitEpoch(0),
      Execute { _ =>
        assert(inputData.latestSeenOffset === null)
      },
      IncrementEpoch(),
      StopStream,

      StartStream(trigger = Trigger.Continuous("1 hour")),
      addData((11L to 20L).toArray),
      AwaitEpoch(2),
      Execute { _ =>
        assert(inputData.latestSeenOffset === ContinuousMemoryStreamOffset(Map(0 -> 10)))
      },
      IncrementEpoch(),
      StopStream,

      StartStream(trigger = Trigger.Continuous("1 hour")),
      addData((21L to 30L).toArray),
      AwaitEpoch(3),
      Execute { _ =>
        assert(inputData.latestSeenOffset === ContinuousMemoryStreamOffset(Map(0 -> 20)))
      }
    )
  }

  class TestSource(spark: SparkSession) extends Source with AcceptsLatestSeenOffset {

    @volatile var currentOffset = 0L

    override def getOffset: Option[Offset] = {
      if (currentOffset <= 0) None else Some(LongOffset(currentOffset))
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
      if (currentOffset == 0) currentOffset = getOffsetValue(end)
      val plan = Range(
        start.map(getOffsetValue).getOrElse(0L) + 1L, getOffsetValue(end) + 1L, 1, None,
        isStreaming = true)
      Dataset.ofRows(spark, plan)
    }

    def incrementAvailableOffset(numNewRows: Int): Unit = {
      currentOffset = currentOffset + numNewRows
    }

    override def setLatestSeenOffset(offset: streaming.Offset): Unit = {
      assert(false, "This method should not be called!")
    }

    def reset(): Unit = {
      currentOffset = 0L
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

  class TestMemoryStream[A : Encoder](
      _id: Int,
      _sqlContext: SQLContext,
      _numPartitions: Option[Int] = None)
    extends MemoryStream[A](_id, _sqlContext, _numPartitions)
    with AcceptsLatestSeenOffset {

    @volatile var latestSeenOffset: streaming.Offset = null

    // Flag to assert the sequence of calls in following scenario:
    //  When the query is restarted, we expect the execution to call `latestSeenOffset` first.
    //  Later as part of the execution, execution may call `initialOffset` if the previous
    //  run of the query had no committed batches.
    @volatile var assertInitialOffsetIsCalledAfterLatestOffsetSeen: Boolean = false

    override def setLatestSeenOffset(offset: streaming.Offset): Unit = {
      latestSeenOffset = offset
    }

    override def initialOffset: streaming.Offset = {
      if (assertInitialOffsetIsCalledAfterLatestOffsetSeen && latestSeenOffset == null) {
        fail("Expected the latest seen offset to be set.")
      }
      super.initialOffset
    }
  }

  class TestContinuousMemoryStream[A : Encoder](
      _id: Int,
      _sqlContext: SQLContext,
      _numPartitions: Int = 2)
    extends ContinuousMemoryStream[A](_id, _sqlContext, _numPartitions)
    with AcceptsLatestSeenOffset {

    @volatile var latestSeenOffset: streaming.Offset = _

    override def setLatestSeenOffset(offset: streaming.Offset): Unit = {
      latestSeenOffset = offset
    }
  }
}

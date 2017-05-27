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

import java.util.concurrent.CountDownLatch

import org.apache.commons.lang3.RandomStringUtils
import org.mockito.Mockito._
import org.scalactic.TolerantNumerics
import org.scalatest.concurrent.Eventually._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.mock.MockitoSugar

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.{BlockingSource, MockSourceProvider, StreamManualClock}
import org.apache.spark.util.ManualClock


class StreamingQuerySuite extends StreamTest with BeforeAndAfter with Logging with MockitoSugar {

  import AwaitTerminationTester._
  import testImplicits._

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("name unique in active queries") {
    withTempDir { dir =>
      def startQuery(name: Option[String]): StreamingQuery = {
        val writer = MemoryStream[Int].toDS.writeStream
        name.foreach(writer.queryName)
        writer
          .foreach(new TestForeachWriter)
          .start()
      }

      // No name by default, multiple active queries can have no name
      val q1 = startQuery(name = None)
      assert(q1.name === null)
      val q2 = startQuery(name = None)
      assert(q2.name === null)

      // Can be set by user
      val q3 = startQuery(name = Some("q3"))
      assert(q3.name === "q3")

      // Multiple active queries cannot have same name
      val e = intercept[IllegalArgumentException] {
        startQuery(name = Some("q3"))
      }

      q1.stop()
      q2.stop()
      q3.stop()
    }
  }

  test(
    "id unique in active queries + persists across restarts, runId unique across start/restarts") {
    val inputData = MemoryStream[Int]
    withTempDir { dir =>
      var cpDir: String = null

      def startQuery(restart: Boolean): StreamingQuery = {
        if (cpDir == null || !restart) cpDir = s"$dir/${RandomStringUtils.randomAlphabetic(10)}"
        MemoryStream[Int].toDS().groupBy().count()
          .writeStream
          .format("memory")
          .outputMode("complete")
          .queryName(s"name${RandomStringUtils.randomAlphabetic(10)}")
          .option("checkpointLocation", cpDir)
          .start()
      }

      // id and runId unique for new queries
      val q1 = startQuery(restart = false)
      val q2 = startQuery(restart = false)
      assert(q1.id !== q2.id)
      assert(q1.runId !== q2.runId)
      q1.stop()
      q2.stop()

      // id persists across restarts, runId unique across restarts
      val q3 = startQuery(restart = false)
      q3.stop()

      val q4 = startQuery(restart = true)
      q4.stop()
      assert(q3.id === q3.id)
      assert(q3.runId !== q4.runId)

      // Only one query with same id can be active
      val q5 = startQuery(restart = false)
      val e = intercept[IllegalStateException] {
        startQuery(restart = true)
      }
    }
  }

  testQuietly("isActive, exception, and awaitTermination") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map { 6 / _}

    testStream(mapped)(
      AssertOnQuery(_.isActive === true),
      AssertOnQuery(_.exception.isEmpty),
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3),
      TestAwaitTermination(ExpectBlocked),
      TestAwaitTermination(ExpectBlocked, timeoutMs = 2000),
      TestAwaitTermination(ExpectNotBlocked, timeoutMs = 10, expectedReturnValue = false),
      StopStream,
      AssertOnQuery(_.isActive === false),
      AssertOnQuery(_.exception.isEmpty),
      TestAwaitTermination(ExpectNotBlocked),
      TestAwaitTermination(ExpectNotBlocked, timeoutMs = 2000, expectedReturnValue = true),
      TestAwaitTermination(ExpectNotBlocked, timeoutMs = 10, expectedReturnValue = true),
      StartStream(),
      AssertOnQuery(_.isActive === true),
      AddData(inputData, 0),
      ExpectFailure[SparkException](),
      AssertOnQuery(_.isActive === false),
      TestAwaitTermination(ExpectException[SparkException]),
      TestAwaitTermination(ExpectException[SparkException], timeoutMs = 2000),
      TestAwaitTermination(ExpectException[SparkException], timeoutMs = 10),
      AssertOnQuery(q => {
        q.exception.get.startOffset ===
          q.committedOffsets.toOffsetSeq(Seq(inputData), OffsetSeqMetadata()).toString &&
          q.exception.get.endOffset ===
            q.availableOffsets.toOffsetSeq(Seq(inputData), OffsetSeqMetadata()).toString
      }, "incorrect start offset or end offset on exception")
    )
  }

  testQuietly("OneTime trigger, commit log, and exception") {
    import Trigger.Once
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map { 6 / _}

    testStream(mapped)(
      AssertOnQuery(_.isActive === true),
      StopStream,
      AddData(inputData, 1, 2),
      StartStream(trigger = Once),
      CheckAnswer(6, 3),
      StopStream, // clears out StreamTest state
      AssertOnQuery { q =>
        // both commit log and offset log contain the same (latest) batch id
        q.batchCommitLog.getLatest().map(_._1).getOrElse(-1L) ==
          q.offsetLog.getLatest().map(_._1).getOrElse(-2L)
      },
      AssertOnQuery { q =>
        // blow away commit log and sink result
        q.batchCommitLog.purge(1)
        q.sink.asInstanceOf[MemorySink].clear()
        true
      },
      StartStream(trigger = Once),
      CheckAnswer(6, 3), // ensure we fall back to offset log and reprocess batch
      StopStream,
      AddData(inputData, 3),
      StartStream(trigger = Once),
      CheckLastBatch(2), // commit log should be back in place
      StopStream,
      AddData(inputData, 0),
      StartStream(trigger = Once),
      ExpectFailure[SparkException](),
      AssertOnQuery(_.isActive === false),
      AssertOnQuery(q => {
        q.exception.get.startOffset ===
          q.committedOffsets.toOffsetSeq(Seq(inputData), OffsetSeqMetadata()).toString &&
          q.exception.get.endOffset ===
            q.availableOffsets.toOffsetSeq(Seq(inputData), OffsetSeqMetadata()).toString
      }, "incorrect start offset or end offset on exception")
    )
  }

  testQuietly("status, lastProgress, and recentProgress") {
    import StreamingQuerySuite._
    clock = new StreamManualClock

    /** Custom MemoryStream that waits for manual clock to reach a time */
    val inputData = new MemoryStream[Int](0, sqlContext) {
      // getOffset should take 50 ms the first time it is called
      override def getOffset: Option[Offset] = {
        val offset = super.getOffset
        if (offset.nonEmpty) {
          clock.waitTillTime(1050)
        }
        offset
      }

      // getBatch should take 100 ms the first time it is called
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        if (start.isEmpty) clock.waitTillTime(1150)
        super.getBatch(start, end)
      }
    }

    // query execution should take 350 ms the first time it is called
    val mapped = inputData.toDS.coalesce(1).as[Long].map { x =>
      clock.waitTillTime(1500)  // this will only wait the first time when clock < 1500
      10 / x
    }.agg(count("*")).as[Long]

    case class AssertStreamExecThreadIsWaitingForTime(targetTime: Long)
      extends AssertOnQuery(q => {
        eventually(Timeout(streamingTimeout)) {
          if (q.exception.isEmpty) {
            assert(clock.isStreamWaitingFor(targetTime))
          }
        }
        if (q.exception.isDefined) {
          throw q.exception.get
        }
        true
      }, "") {
      override def toString: String = s"AssertStreamExecThreadIsWaitingForTime($targetTime)"
    }

    case class AssertClockTime(time: Long)
      extends AssertOnQuery(q => clock.getTimeMillis() === time, "") {
      override def toString: String = s"AssertClockTime($time)"
    }

    var lastProgressBeforeStop: StreamingQueryProgress = null

    testStream(mapped, OutputMode.Complete)(
      StartStream(ProcessingTime(1000), triggerClock = clock),
      AssertStreamExecThreadIsWaitingForTime(1000),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Waiting for next trigger"),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      // Test status and progress while offset is being fetched
      AddData(inputData, 1, 2),
      AdvanceManualClock(1000), // time = 1000 to start new trigger, will block on getOffset
      AssertStreamExecThreadIsWaitingForTime(1050),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message.startsWith("Getting offsets from")),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      // Test status and progress while batch is being fetched
      AdvanceManualClock(50), // time = 1050 to unblock getOffset
      AssertClockTime(1050),
      AssertStreamExecThreadIsWaitingForTime(1150),      // will block on getBatch that needs 1150
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message === "Processing new data"),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      // Test status and progress while batch is being processed
      AdvanceManualClock(100), // time = 1150 to unblock getBatch
      AssertClockTime(1150),
      AssertStreamExecThreadIsWaitingForTime(1500), // will block in Spark job that needs 1500
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message === "Processing new data"),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      // Test status and progress while batch processing has completed
      AssertOnQuery { _ => clock.getTimeMillis() === 1150 },
      AdvanceManualClock(350), // time = 1500 to unblock job
      AssertClockTime(1500),
      CheckAnswer(2),
      AssertStreamExecThreadIsWaitingForTime(2000),
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Waiting for next trigger"),
      AssertOnQuery { query =>
        assert(query.lastProgress != null)
        assert(query.recentProgress.exists(_.numInputRows > 0))
        assert(query.recentProgress.last.eq(query.lastProgress))

        val progress = query.lastProgress
        assert(progress.id === query.id)
        assert(progress.name === query.name)
        assert(progress.batchId === 0)
        assert(progress.timestamp === "1970-01-01T00:00:01.000Z") // 100 ms in UTC
        assert(progress.numInputRows === 2)
        assert(progress.processedRowsPerSecond === 4.0)

        assert(progress.durationMs.get("getOffset") === 50)
        assert(progress.durationMs.get("getBatch") === 100)
        assert(progress.durationMs.get("queryPlanning") === 0)
        assert(progress.durationMs.get("walCommit") === 0)
        assert(progress.durationMs.get("triggerExecution") === 500)

        assert(progress.sources.length === 1)
        assert(progress.sources(0).description contains "MemoryStream")
        assert(progress.sources(0).startOffset === null)
        assert(progress.sources(0).endOffset !== null)
        assert(progress.sources(0).processedRowsPerSecond === 4.0)  // 2 rows processed in 500 ms

        assert(progress.stateOperators.length === 1)
        assert(progress.stateOperators(0).numRowsUpdated === 1)
        assert(progress.stateOperators(0).numRowsTotal === 1)

        assert(progress.sink.description contains "MemorySink")
        true
      },

      // Test whether input rate is updated after two batches
      AssertStreamExecThreadIsWaitingForTime(2000),  // blocked waiting for next trigger time
      AddData(inputData, 1, 2),
      AdvanceManualClock(500), // allow another trigger
      AssertClockTime(2000),
      AssertStreamExecThreadIsWaitingForTime(3000),  // will block waiting for next trigger time
      CheckAnswer(4),
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Waiting for next trigger"),
      AssertOnQuery { query =>
        assert(query.recentProgress.last.eq(query.lastProgress))
        assert(query.lastProgress.batchId === 1)
        assert(query.lastProgress.inputRowsPerSecond === 2.0)
        assert(query.lastProgress.sources(0).inputRowsPerSecond === 2.0)
        true
      },

      // Test status and progress after data is not available for a trigger
      AdvanceManualClock(1000), // allow another trigger
      AssertStreamExecThreadIsWaitingForTime(4000),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Waiting for next trigger"),

      // Test status and progress after query stopped
      AssertOnQuery { query =>
        lastProgressBeforeStop = query.lastProgress
        true
      },
      StopStream,
      AssertOnQuery(_.lastProgress.json === lastProgressBeforeStop.json),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Stopped"),

      // Test status and progress after query terminated with error
      StartStream(ProcessingTime(1000), triggerClock = clock),
      AdvanceManualClock(1000), // ensure initial trigger completes before AddData
      AddData(inputData, 0),
      AdvanceManualClock(1000), // allow another trigger
      ExpectFailure[SparkException](),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message.startsWith("Terminated with exception"))
    )
  }

  test("lastProgress should be null when recentProgress is empty") {
    BlockingSource.latch = new CountDownLatch(1)
    withTempDir { tempDir =>
      val sq = spark.readStream
        .format("org.apache.spark.sql.streaming.util.BlockingSource")
        .load()
        .writeStream
        .format("org.apache.spark.sql.streaming.util.BlockingSource")
        .option("checkpointLocation", tempDir.toString)
        .start()
      // Creating source is blocked so recentProgress is empty and lastProgress should be null
      assert(sq.lastProgress === null)
      // Release the latch and stop the query
      BlockingSource.latch.countDown()
      sq.stop()
    }
  }

  test("codahale metrics") {
    val inputData = MemoryStream[Int]

    /** Whether metrics of a query is registered for reporting */
    def isMetricsRegistered(query: StreamingQuery): Boolean = {
      val sourceName = s"spark.streaming.${query.id}"
      val sources = spark.sparkContext.env.metricsSystem.getSourcesByName(sourceName)
      require(sources.size <= 1)
      sources.nonEmpty
    }
    // Disabled by default
    assert(spark.conf.get("spark.sql.streaming.metricsEnabled").toBoolean === false)

    withSQLConf("spark.sql.streaming.metricsEnabled" -> "false") {
      testStream(inputData.toDF)(
        AssertOnQuery { q => !isMetricsRegistered(q) },
        StopStream,
        AssertOnQuery { q => !isMetricsRegistered(q) }
      )
    }

    // Registered when enabled
    withSQLConf("spark.sql.streaming.metricsEnabled" -> "true") {
      testStream(inputData.toDF)(
        AssertOnQuery { q => isMetricsRegistered(q) },
        StopStream,
        AssertOnQuery { q => !isMetricsRegistered(q) }
      )
    }
  }

  test("input row calculation with mixed batch and streaming sources") {
    val streamingTriggerDF = spark.createDataset(1 to 10).toDF
    val streamingInputDF = createSingleTriggerStreamingDF(streamingTriggerDF).toDF("value")
    val staticInputDF = spark.createDataFrame(Seq(1 -> "1", 2 -> "2")).toDF("value", "anotherValue")

    // Trigger input has 10 rows, static input has 2 rows,
    // therefore after the first trigger, the calculated input rows should be 10
    val progress = getFirstProgress(streamingInputDF.join(staticInputDF, "value"))
    assert(progress.numInputRows === 10)
    assert(progress.sources.size === 1)
    assert(progress.sources(0).numInputRows === 10)
  }

  test("input row calculation with trigger input DF having multiple leaves") {
    val streamingTriggerDF =
      spark.createDataset(1 to 5).toDF.union(spark.createDataset(6 to 10).toDF)
    require(streamingTriggerDF.logicalPlan.collectLeaves().size > 1)
    val streamingInputDF = createSingleTriggerStreamingDF(streamingTriggerDF)

    // After the first trigger, the calculated input rows should be 10
    val progress = getFirstProgress(streamingInputDF)
    assert(progress.numInputRows === 10)
    assert(progress.sources.size === 1)
    assert(progress.sources(0).numInputRows === 10)
  }

  testQuietly("StreamExecution metadata garbage collection") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(6 / _)
    withSQLConf(SQLConf.MIN_BATCHES_TO_RETAIN.key -> "1") {
      // Run 3 batches, and then assert that only 2 metadata files is are at the end
      // since the first should have been purged.
      testStream(mapped)(
        AddData(inputData, 1, 2),
        CheckAnswer(6, 3),
        AddData(inputData, 1, 2),
        CheckAnswer(6, 3, 6, 3),
        AddData(inputData, 4, 6),
        CheckAnswer(6, 3, 6, 3, 1, 1),

        AssertOnQuery("metadata log should contain only two files") { q =>
          val metadataLogDir = new java.io.File(q.offsetLog.metadataPath.toString)
          val logFileNames = metadataLogDir.listFiles().toSeq.map(_.getName())
          val toTest = logFileNames.filter(!_.endsWith(".crc")).sorted // Workaround for SPARK-17475
          assert(toTest.size == 2 && toTest.head == "1")
          true
        }
      )
    }

    val inputData2 = MemoryStream[Int]
    withSQLConf(SQLConf.MIN_BATCHES_TO_RETAIN.key -> "2") {
      // Run 5 batches, and then assert that 3 metadata files is are at the end
      // since the two should have been purged.
      testStream(inputData2.toDS())(
        AddData(inputData2, 1, 2),
        CheckAnswer(1, 2),
        AddData(inputData2, 1, 2),
        CheckAnswer(1, 2, 1, 2),
        AddData(inputData2, 3, 4),
        CheckAnswer(1, 2, 1, 2, 3, 4),
        AddData(inputData2, 5, 6),
        CheckAnswer(1, 2, 1, 2, 3, 4, 5, 6),
        AddData(inputData2, 7, 8),
        CheckAnswer(1, 2, 1, 2, 3, 4, 5, 6, 7, 8),

        AssertOnQuery("metadata log should contain three files") { q =>
          val metadataLogDir = new java.io.File(q.offsetLog.metadataPath.toString)
          val logFileNames = metadataLogDir.listFiles().toSeq.map(_.getName())
          val toTest = logFileNames.filter(!_.endsWith(".crc")).sorted // Workaround for SPARK-17475
          assert(toTest.size == 3 && toTest.head == "2")
          true
        }
      )
    }
  }

  testQuietly("StreamingQuery should be Serializable but cannot be used in executors") {
    def startQuery(ds: Dataset[Int], queryName: String): StreamingQuery = {
      ds.writeStream
        .queryName(queryName)
        .format("memory")
        .start()
    }

    val input = MemoryStream[Int]
    val q1 = startQuery(input.toDS, "stream_serializable_test_1")
    val q2 = startQuery(input.toDS.map { i =>
      // Emulate that `StreamingQuery` get captured with normal usage unintentionally.
      // It should not fail the query.
      q1
      i
    }, "stream_serializable_test_2")
    val q3 = startQuery(input.toDS.map { i =>
      // Emulate that `StreamingQuery` is used in executors. We should fail the query with a clear
      // error message.
      q1.explain()
      i
    }, "stream_serializable_test_3")
    try {
      input.addData(1)

      // q2 should not fail since it doesn't use `q1` in the closure
      q2.processAllAvailable()

      // The user calls `StreamingQuery` in the closure and it should fail
      val e = intercept[StreamingQueryException] {
        q3.processAllAvailable()
      }
      assert(e.getCause.isInstanceOf[SparkException])
      assert(e.getCause.getCause.isInstanceOf[IllegalStateException])
      assert(e.getMessage.contains("StreamingQuery cannot be used in executors"))
    } finally {
      q1.stop()
      q2.stop()
      q3.stop()
    }
  }

  test("StreamExecution should call stop() on sources when a stream is stopped") {
    var calledStop = false
    val source = new Source {
      override def stop(): Unit = {
        calledStop = true
      }
      override def getOffset: Option[Offset] = None
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        spark.emptyDataFrame
      }
      override def schema: StructType = MockSourceProvider.fakeSchema
    }

    MockSourceProvider.withMockSources(source) {
      val df = spark.readStream
        .format("org.apache.spark.sql.streaming.util.MockSourceProvider")
        .load()

      testStream(df)(StopStream)

      assert(calledStop, "Did not call stop on source for stopped stream")
    }
  }

  testQuietly("SPARK-19774: StreamExecution should call stop() on sources when a stream fails") {
    var calledStop = false
    val source1 = new Source {
      override def stop(): Unit = {
        throw new RuntimeException("Oh no!")
      }
      override def getOffset: Option[Offset] = Some(LongOffset(1))
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        spark.range(2).toDF(MockSourceProvider.fakeSchema.fieldNames: _*)
      }
      override def schema: StructType = MockSourceProvider.fakeSchema
    }
    val source2 = new Source {
      override def stop(): Unit = {
        calledStop = true
      }
      override def getOffset: Option[Offset] = None
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        spark.emptyDataFrame
      }
      override def schema: StructType = MockSourceProvider.fakeSchema
    }

    MockSourceProvider.withMockSources(source1, source2) {
      val df1 = spark.readStream
        .format("org.apache.spark.sql.streaming.util.MockSourceProvider")
        .load()
        .as[Int]

      val df2 = spark.readStream
        .format("org.apache.spark.sql.streaming.util.MockSourceProvider")
        .load()
        .as[Int]

      testStream(df1.union(df2).map(i => i / 0))(
        AssertOnQuery { sq =>
          intercept[StreamingQueryException](sq.processAllAvailable())
          sq.exception.isDefined && !sq.isActive
        }
      )

      assert(calledStop, "Did not call stop on source for stopped stream")
    }
  }

  /** Create a streaming DF that only execute one batch in which it returns the given static DF */
  private def createSingleTriggerStreamingDF(triggerDF: DataFrame): DataFrame = {
    require(!triggerDF.isStreaming)
    // A streaming Source that generate only on trigger and returns the given Dataframe as batch
    val source = new Source() {
      override def schema: StructType = triggerDF.schema
      override def getOffset: Option[Offset] = Some(LongOffset(0))
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = triggerDF
      override def stop(): Unit = {}
    }
    StreamingExecutionRelation(source)
  }

  /** Returns the query progress at the end of the first trigger of streaming DF */
  private def getFirstProgress(streamingDF: DataFrame): StreamingQueryProgress = {
    try {
      val q = streamingDF.writeStream.format("memory").queryName("test").start()
      q.processAllAvailable()
      q.recentProgress.head
    } finally {
      spark.streams.active.map(_.stop())
    }
  }

  /**
   * A [[StreamAction]] to test the behavior of `StreamingQuery.awaitTermination()`.
   *
   * @param expectedBehavior  Expected behavior (not blocked, blocked, or exception thrown)
   * @param timeoutMs         Timeout in milliseconds
   *                          When timeoutMs is less than or equal to 0, awaitTermination() is
   *                          tested (i.e. w/o timeout)
   *                          When timeoutMs is greater than 0, awaitTermination(timeoutMs) is
   *                          tested
   * @param expectedReturnValue Expected return value when awaitTermination(timeoutMs) is used
   */
  case class TestAwaitTermination(
      expectedBehavior: ExpectedBehavior,
      timeoutMs: Int = -1,
      expectedReturnValue: Boolean = false
    ) extends AssertOnQuery(
      TestAwaitTermination.assertOnQueryCondition(expectedBehavior, timeoutMs, expectedReturnValue),
      "Error testing awaitTermination behavior"
    ) {
    override def toString(): String = {
      s"TestAwaitTermination($expectedBehavior, timeoutMs = $timeoutMs, " +
        s"expectedReturnValue = $expectedReturnValue)"
    }
  }

  object TestAwaitTermination {

    /**
     * Tests the behavior of `StreamingQuery.awaitTermination`.
     *
     * @param expectedBehavior  Expected behavior (not blocked, blocked, or exception thrown)
     * @param timeoutMs         Timeout in milliseconds
     *                          When timeoutMs is less than or equal to 0, awaitTermination() is
     *                          tested (i.e. w/o timeout)
     *                          When timeoutMs is greater than 0, awaitTermination(timeoutMs) is
     *                          tested
     * @param expectedReturnValue Expected return value when awaitTermination(timeoutMs) is used
     */
    def assertOnQueryCondition(
        expectedBehavior: ExpectedBehavior,
        timeoutMs: Int,
        expectedReturnValue: Boolean
      )(q: StreamExecution): Boolean = {

      def awaitTermFunc(): Unit = {
        if (timeoutMs <= 0) {
          q.awaitTermination()
        } else {
          val returnedValue = q.awaitTermination(timeoutMs)
          assert(returnedValue === expectedReturnValue, "Returned value does not match expected")
        }
      }
      AwaitTerminationTester.test(expectedBehavior, awaitTermFunc)
      true // If the control reached here, then everything worked as expected
    }
  }
}

object StreamingQuerySuite {
  // Singleton reference to clock that does not get serialized in task closures
  var clock: StreamManualClock = null
}

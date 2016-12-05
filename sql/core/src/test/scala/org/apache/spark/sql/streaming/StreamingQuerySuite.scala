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

import org.scalactic.TolerantNumerics
import org.scalatest.concurrent.Eventually._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.util.{ManualClock, Utils}


class StreamingQuerySuite extends StreamTest with BeforeAndAfter with Logging {

  import AwaitTerminationTester._
  import testImplicits._

  // To make === between double tolerate inexact values
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.01)

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("names unique across active queries, ids unique across all started queries") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map { 6 / _}

    def startQuery(queryName: String): StreamingQuery = {
      val metadataRoot = Utils.createTempDir(namePrefix = "streaming.checkpoint").getCanonicalPath
      val writer = mapped.writeStream
      writer
        .queryName(queryName)
        .format("memory")
        .option("checkpointLocation", metadataRoot)
        .start()
    }

    val q1 = startQuery("q1")
    assert(q1.name === "q1")

    // Verify that another query with same name cannot be started
    val e1 = intercept[IllegalArgumentException] {
      startQuery("q1")
    }
    Seq("q1", "already active").foreach { s => assert(e1.getMessage.contains(s)) }

    // Verify q1 was unaffected by the above exception and stop it
    assert(q1.isActive)
    q1.stop()

    // Verify another query can be started with name q1, but will have different id
    val q2 = startQuery("q1")
    assert(q2.name === "q1")
    assert(q2.id !== q1.id)
    q2.stop()
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
      ExpectFailure[SparkException],
      AssertOnQuery(_.isActive === false),
      TestAwaitTermination(ExpectException[SparkException]),
      TestAwaitTermination(ExpectException[SparkException], timeoutMs = 2000),
      TestAwaitTermination(ExpectException[SparkException], timeoutMs = 10),
      AssertOnQuery(
        q => q.exception.get.startOffset.get.offsets ===
          q.committedOffsets.toOffsetSeq(Seq(inputData), "{}").offsets,
        "incorrect start offset on exception")
    )
  }

  testQuietly("status, lastProgress, and recentProgresses") {
    import StreamingQuerySuite._
    clock = new StreamManualClock

    /** Custom MemoryStream that waits for manual clock to reach a time */
    val inputData = new MemoryStream[Int](0, sqlContext) {
      // Wait for manual clock to be 100 first time there is data
      override def getOffset: Option[Offset] = {
        val offset = super.getOffset
        if (offset.nonEmpty) {
          clock.waitTillTime(300)
        }
        offset
      }

      // Wait for manual clock to be 300 first time there is data
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        clock.waitTillTime(600)
        super.getBatch(start, end)
      }
    }

    // This is to make sure thatquery waits for manual clock to be 600 first time there is data
    val mapped = inputData.toDS().as[Long].map { x =>
      clock.waitTillTime(1100)
      10 / x
    }.agg(count("*")).as[Long]

    case class AssertStreamExecThreadToWaitForClock()
      extends AssertOnQuery(q => {
        eventually(Timeout(streamingTimeout)) {
          if (q.exception.isEmpty) {
            assert(clock.asInstanceOf[StreamManualClock].isStreamWaitingAt(clock.getTimeMillis))
          }
        }
        if (q.exception.isDefined) {
          throw q.exception.get
        }
        true
      }, "")

    var lastProgressBeforeStop: StreamingQueryProgress = null

    testStream(mapped, OutputMode.Complete)(
      StartStream(ProcessingTime(100), triggerClock = clock),
      AssertStreamExecThreadToWaitForClock(),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Waiting for next trigger"),
      AssertOnQuery(_.recentProgresses.count(_.numInputRows > 0) === 0),

      // Test status and progress while offset is being fetched
      AddData(inputData, 1, 2),
      AdvanceManualClock(100), // time = 100 to start new trigger, will block on getOffset
      AssertStreamExecThreadToWaitForClock(),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message.startsWith("Getting offsets from")),
      AssertOnQuery(_.recentProgresses.count(_.numInputRows > 0) === 0),

      // Test status and progress while batch is being fetched
      AdvanceManualClock(200), // time = 300 to unblock getOffset, will block on getBatch
      AssertStreamExecThreadToWaitForClock(),
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message === "Processing new data"),
      AssertOnQuery(_.recentProgresses.count(_.numInputRows > 0) === 0),

      // Test status and progress while batch is being processed
      AdvanceManualClock(300), // time = 600 to unblock getBatch, will block in Spark job
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message === "Processing new data"),
      AssertOnQuery(_.recentProgresses.count(_.numInputRows > 0) === 0),

      // Test status and progress while batch processing has completed
      AdvanceManualClock(500), // time = 1100 to unblock job
      AssertOnQuery { _ => clock.getTimeMillis() === 1100 },
      CheckAnswer(2),
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Waiting for next trigger"),
      AssertOnQuery { query =>
        assert(query.lastProgress != null)
        assert(query.recentProgresses.exists(_.numInputRows > 0))
        assert(query.recentProgresses.last.eq(query.lastProgress))

        val progress = query.lastProgress
        assert(progress.id === query.id)
        assert(progress.name === query.name)
        assert(progress.batchId === 0)
        assert(progress.timestamp === 100)
        assert(progress.numInputRows === 2)
        assert(progress.processedRowsPerSecond === 2.0)

        assert(progress.durationMs.get("getOffset") === 200)
        assert(progress.durationMs.get("getBatch") === 300)
        assert(progress.durationMs.get("queryPlanning") === 0)
        assert(progress.durationMs.get("walCommit") === 0)
        assert(progress.durationMs.get("triggerExecution") === 1000)

        assert(progress.sources.length === 1)
        assert(progress.sources(0).description contains "MemoryStream")
        assert(progress.sources(0).startOffset === null)
        assert(progress.sources(0).endOffset !== null)
        assert(progress.sources(0).processedRowsPerSecond === 2.0)

        assert(progress.stateOperators.length === 1)
        assert(progress.stateOperators(0).numRowsUpdated === 1)
        assert(progress.stateOperators(0).numRowsTotal === 1)

        assert(progress.sink.description contains "MemorySink")
        true
      },

      AddData(inputData, 1, 2),
      AdvanceManualClock(100), // allow another trigger
      CheckAnswer(4),
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message === "Waiting for next trigger"),
      AssertOnQuery { query =>
        assert(query.recentProgresses.last.eq(query.lastProgress))
        assert(query.lastProgress.batchId === 1)
        assert(query.lastProgress.sources(0).inputRowsPerSecond === 1.818)
        true
      },

      // Test status and progress after data is not available for a trigger
      AdvanceManualClock(100), // allow another trigger
      AssertStreamExecThreadToWaitForClock(),
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
      StartStream(ProcessingTime(100), triggerClock = clock),
      AddData(inputData, 0),
      AdvanceManualClock(100),
      ExpectFailure[SparkException],
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === false),
      AssertOnQuery(_.status.message.startsWith("Terminated with exception"))
    )
  }

  test("codahale metrics") {
    val inputData = MemoryStream[Int]

    /** Whether metrics of a query is registered for reporting */
    def isMetricsRegistered(query: StreamingQuery): Boolean = {
      val sourceName = s"spark.streaming.${query.name}"
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
        val toTest = logFileNames.filter(! _.endsWith(".crc")).sorted  // Workaround for SPARK-17475
        assert(toTest.size == 2 && toTest.head == "1")
        true
      }
    )
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
      q.recentProgresses.head
    } finally {
      spark.streams.active.map(_.stop())
    }
  }

  /**
   * A [[StreamAction]] to test the behavior of `StreamingQuery.awaitTermination()`.
   *
   * @param expectedBehavior  Expected behavior (not blocked, blocked, or exception thrown)
   * @param timeoutMs         Timeout in milliseconds
   *                          When timeoutMs <= 0, awaitTermination() is tested (i.e. w/o timeout)
   *                          When timeoutMs > 0, awaitTermination(timeoutMs) is tested
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
     *                          When timeoutMs <= 0, awaitTermination() is tested (i.e. w/o timeout)
     *                          When timeoutMs > 0, awaitTermination(timeoutMs) is tested
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
  var clock: ManualClock = null
}

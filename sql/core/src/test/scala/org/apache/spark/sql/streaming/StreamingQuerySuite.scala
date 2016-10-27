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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.util.Utils


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

  testQuietly("lifecycle states and awaitTermination") {
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
        q =>
          q.exception.get.startOffset.get === q.committedOffsets.toCompositeOffset(Seq(inputData)),
        "incorrect start offset on exception")
    )
  }

  testQuietly("query statuses") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(6 / _)
    testStream(mapped)(
      AssertOnQuery(q => q.status.name === q.name),
      AssertOnQuery(q => q.status.id === q.id),
      AssertOnQuery(_.status.timestamp <= System.currentTimeMillis),
      AssertOnQuery(_.status.inputRate === 0.0),
      AssertOnQuery(_.status.processingRate === 0.0),
      AssertOnQuery(_.status.sourceStatuses.length === 1),
      AssertOnQuery(_.status.sourceStatuses(0).description.contains("Memory")),
      AssertOnQuery(_.status.sourceStatuses(0).offsetDesc === "-"),
      AssertOnQuery(_.status.sourceStatuses(0).inputRate === 0.0),
      AssertOnQuery(_.status.sourceStatuses(0).processingRate === 0.0),
      AssertOnQuery(_.status.sinkStatus.description.contains("Memory")),
      AssertOnQuery(_.status.sinkStatus.offsetDesc === CompositeOffset(None :: Nil).toString),
      AssertOnQuery(_.sourceStatuses(0).description.contains("Memory")),
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === "-"),
      AssertOnQuery(_.sourceStatuses(0).inputRate === 0.0),
      AssertOnQuery(_.sourceStatuses(0).processingRate === 0.0),
      AssertOnQuery(_.sinkStatus.description.contains("Memory")),
      AssertOnQuery(_.sinkStatus.offsetDesc === new CompositeOffset(None :: Nil).toString),

      AddData(inputData, 1, 2),
      CheckAnswer(6, 3),
      AssertOnQuery(_.status.timestamp <= System.currentTimeMillis),
      AssertOnQuery(_.status.inputRate >= 0.0),
      AssertOnQuery(_.status.processingRate >= 0.0),
      AssertOnQuery(_.status.sourceStatuses.length === 1),
      AssertOnQuery(_.status.sourceStatuses(0).description.contains("Memory")),
      AssertOnQuery(_.status.sourceStatuses(0).offsetDesc === LongOffset(0).toString),
      AssertOnQuery(_.status.sourceStatuses(0).inputRate >= 0.0),
      AssertOnQuery(_.status.sourceStatuses(0).processingRate >= 0.0),
      AssertOnQuery(_.status.sinkStatus.description.contains("Memory")),
      AssertOnQuery(_.status.sinkStatus.offsetDesc ===
        CompositeOffset.fill(LongOffset(0)).toString),
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === LongOffset(0).toString),
      AssertOnQuery(_.sourceStatuses(0).inputRate >= 0.0),
      AssertOnQuery(_.sourceStatuses(0).processingRate >= 0.0),
      AssertOnQuery(_.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(0)).toString),

      AddData(inputData, 1, 2),
      CheckAnswer(6, 3, 6, 3),
      AssertOnQuery(_.status.sourceStatuses(0).offsetDesc === LongOffset(1).toString),
      AssertOnQuery(_.status.sinkStatus.offsetDesc ===
        CompositeOffset.fill(LongOffset(1)).toString),
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === LongOffset(1).toString),
      AssertOnQuery(_.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(1)).toString),

      StopStream,
      AssertOnQuery(_.status.inputRate === 0.0),
      AssertOnQuery(_.status.processingRate === 0.0),
      AssertOnQuery(_.status.sourceStatuses.length === 1),
      AssertOnQuery(_.status.sourceStatuses(0).offsetDesc === LongOffset(1).toString),
      AssertOnQuery(_.status.sourceStatuses(0).inputRate === 0.0),
      AssertOnQuery(_.status.sourceStatuses(0).processingRate === 0.0),
      AssertOnQuery(_.status.sinkStatus.offsetDesc ===
        CompositeOffset.fill(LongOffset(1)).toString),
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === LongOffset(1).toString),
      AssertOnQuery(_.sourceStatuses(0).inputRate === 0.0),
      AssertOnQuery(_.sourceStatuses(0).processingRate === 0.0),
      AssertOnQuery(_.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(1)).toString),
      AssertOnQuery(_.status.triggerDetails.isEmpty),

      StartStream(),
      AddData(inputData, 0),
      ExpectFailure[SparkException],
      AssertOnQuery(_.status.inputRate === 0.0),
      AssertOnQuery(_.status.processingRate === 0.0),
      AssertOnQuery(_.status.sourceStatuses.length === 1),
      AssertOnQuery(_.status.sourceStatuses(0).offsetDesc === LongOffset(2).toString),
      AssertOnQuery(_.status.sourceStatuses(0).inputRate === 0.0),
      AssertOnQuery(_.status.sourceStatuses(0).processingRate === 0.0),
      AssertOnQuery(_.status.sinkStatus.offsetDesc ===
        CompositeOffset.fill(LongOffset(1)).toString),
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === LongOffset(2).toString),
      AssertOnQuery(_.sourceStatuses(0).inputRate === 0.0),
      AssertOnQuery(_.sourceStatuses(0).processingRate === 0.0),
      AssertOnQuery(_.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(1)).toString)
    )
  }

  test("codahale metrics") {
    val inputData = MemoryStream[Int]

    /** Whether metrics of a query is registered for reporting */
    def isMetricsRegistered(query: StreamingQuery): Boolean = {
      val sourceName = s"StructuredStreaming.${query.name}"
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
    val status = getFirstTriggerStatus(streamingInputDF.join(staticInputDF, "value"))
    assert(status.triggerDetails.get("numRows.input.total") === "10")
    assert(status.sourceStatuses.size === 1)
    assert(status.sourceStatuses(0).triggerDetails.get("numRows.input.source") === "10")
  }

  test("input row calculation with trigger DF having multiple leaves") {
    val streamingTriggerDF =
      spark.createDataset(1 to 5).toDF.union(spark.createDataset(6 to 10).toDF)
    require(streamingTriggerDF.logicalPlan.collectLeaves().size > 1)
    val streamingInputDF = createSingleTriggerStreamingDF(streamingTriggerDF)

    // After the first trigger, the calculated input rows should be 10
    val status = getFirstTriggerStatus(streamingInputDF)
    assert(status.triggerDetails.get("numRows.input.total") === "10")
    assert(status.sourceStatuses.size === 1)
    assert(status.sourceStatuses(0).triggerDetails.get("numRows.input.source") === "10")
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
        val toTest = logFileNames.filter(! _.endsWith(".crc"))  // Workaround for SPARK-17475
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

  /** Returns the query status at the end of the first trigger of streaming DF */
  private def getFirstTriggerStatus(streamingDF: DataFrame): StreamingQueryStatus = {
    // A StreamingQueryListener that gets the query status after the first completed trigger
    val listener = new StreamingQueryListener {
      @volatile var firstStatus: StreamingQueryStatus = null
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = { }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
       if (firstStatus == null) firstStatus = queryProgress.queryStatus
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = { }
    }

    try {
      spark.streams.addListener(listener)
      val q = streamingDF.writeStream.format("memory").queryName("test").start()
      q.processAllAvailable()
      eventually(timeout(streamingTimeout)) {
        assert(listener.firstStatus != null)
      }
      listener.firstStatus
    } finally {
      spark.streams.active.map(_.stop())
      spark.streams.removeListener(listener)
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

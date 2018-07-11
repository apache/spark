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

import java.{util => ju}
import java.util.Optional
import java.util.concurrent.CountDownLatch

import org.apache.commons.lang3.RandomStringUtils
import org.scalactic.TolerantNumerics
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.TestForeachWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.streaming.util.{BlockingSource, MockSourceProvider, StreamManualClock}
import org.apache.spark.sql.types.StructType

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
        q.commitLog.getLatest().map(_._1).getOrElse(-1L) ==
          q.offsetLog.getLatest().map(_._1).getOrElse(-2L)
      },
      AssertOnQuery { q =>
        // blow away commit log and sink result
        q.commitLog.purge(1)
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

      private def dataAdded: Boolean = currentOffset.offset != -1

      // setOffsetRange should take 50 ms the first time it is called after data is added
      override def setOffsetRange(start: Optional[OffsetV2], end: Optional[OffsetV2]): Unit = {
        synchronized {
          if (dataAdded) clock.waitTillTime(1050)
          super.setOffsetRange(start, end)
        }
      }

      // getEndOffset should take 100 ms the first time it is called after data is added
      override def getEndOffset(): OffsetV2 = synchronized {
        if (dataAdded) clock.waitTillTime(1150)
        super.getEndOffset()
      }

      // getBatch should take 100 ms the first time it is called
      override def planUnsafeInputPartitions(): ju.List[InputPartition[UnsafeRow]] = {
        synchronized {
          clock.waitTillTime(1350)
          super.planUnsafeInputPartitions()
        }
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

      // Test status and progress when setOffsetRange is being called
      AddData(inputData, 1, 2),
      AdvanceManualClock(1000), // time = 1000 to start new trigger, will block on setOffsetRange
      AssertStreamExecThreadIsWaitingForTime(1050),
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message.startsWith("Getting offsets from")),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      AdvanceManualClock(50), // time = 1050 to unblock setOffsetRange
      AssertClockTime(1050),
      AssertStreamExecThreadIsWaitingForTime(1150), // will block on getEndOffset that needs 1150
      AssertOnQuery(_.status.isDataAvailable === false),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message.startsWith("Getting offsets from")),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      AdvanceManualClock(100), // time = 1150 to unblock getEndOffset
      AssertClockTime(1150),
      // will block on planInputPartitions that needs 1350
      AssertStreamExecThreadIsWaitingForTime(1350),
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message === "Processing new data"),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      AdvanceManualClock(200), // time = 1350 to unblock planInputPartitions
      AssertClockTime(1350),
      AssertStreamExecThreadIsWaitingForTime(1500), // will block on map task that needs 1500
      AssertOnQuery(_.status.isDataAvailable === true),
      AssertOnQuery(_.status.isTriggerActive === true),
      AssertOnQuery(_.status.message === "Processing new data"),
      AssertOnQuery(_.recentProgress.count(_.numInputRows > 0) === 0),

      // Test status and progress while batch processing has completed
      AdvanceManualClock(150), // time = 1500 to unblock map task
      AssertClockTime(1500),
      CheckAnswer(2),
      AssertStreamExecThreadIsWaitingForTime(2000),  // will block until the next trigger
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

        assert(progress.durationMs.get("setOffsetRange") === 50)
        assert(progress.durationMs.get("getEndOffset") === 100)
        assert(progress.durationMs.get("queryPlanning") === 200)
        assert(progress.durationMs.get("walCommit") === 0)
        assert(progress.durationMs.get("addBatch") === 150)
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
        assert(query.lastProgress.sources(0).startOffset.toInt <
          query.lastProgress.sources(0).endOffset.toInt)
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

  test("SPARK-22975: MetricsReporter defaults when there was no progress reported") {
    withSQLConf("spark.sql.streaming.metricsEnabled" -> "true") {
      BlockingSource.latch = new CountDownLatch(1)
      withTempDir { tempDir =>
        val sq = spark.readStream
          .format("org.apache.spark.sql.streaming.util.BlockingSource")
          .load()
          .writeStream
          .format("org.apache.spark.sql.streaming.util.BlockingSource")
          .option("checkpointLocation", tempDir.toString)
          .start()
          .asInstanceOf[StreamingQueryWrapper]
          .streamingQuery

        val gauges = sq.streamMetrics.metricRegistry.getGauges
        assert(gauges.get("latency").getValue.asInstanceOf[Long] == 0)
        assert(gauges.get("processingRate-total").getValue.asInstanceOf[Double] == 0.0)
        assert(gauges.get("inputRate-total").getValue.asInstanceOf[Double] == 0.0)
        sq.stop()
      }
    }
  }

  test("input row calculation with same V1 source used twice in self-join") {
    val streamingTriggerDF = spark.createDataset(1 to 10).toDF
    val streamingInputDF = createSingleTriggerStreamingDF(streamingTriggerDF).toDF("value")

    val progress = getFirstProgress(streamingInputDF.join(streamingInputDF, "value"))
    assert(progress.numInputRows === 20) // data is read multiple times in self-joins
    assert(progress.sources.size === 1)
    assert(progress.sources(0).numInputRows === 20)
  }

  test("input row calculation with mixed batch and streaming V1 sources") {
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

  test("input row calculation with trigger input DF having multiple leaves in V1 source") {
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

  test("input row calculation with same V2 source used twice in self-union") {
    val streamInput = MemoryStream[Int]

    testStream(streamInput.toDF().union(streamInput.toDF()), useV2Sink = true)(
      AddData(streamInput, 1, 2, 3),
      CheckAnswer(1, 1, 2, 2, 3, 3),
      AssertOnQuery { q =>
        val lastProgress = getLastProgressWithData(q)
        assert(lastProgress.nonEmpty)
        assert(lastProgress.get.numInputRows == 6)
        assert(lastProgress.get.sources.length == 1)
        assert(lastProgress.get.sources(0).numInputRows == 6)
        true
      }
    )
  }

  test("input row calculation with same V2 source used twice in self-join") {
    val streamInput = MemoryStream[Int]
    val df = streamInput.toDF()
    testStream(df.join(df, "value"), useV2Sink = true)(
      AddData(streamInput, 1, 2, 3),
      CheckAnswer(1, 2, 3),
      AssertOnQuery { q =>
        val lastProgress = getLastProgressWithData(q)
        assert(lastProgress.nonEmpty)
        assert(lastProgress.get.numInputRows == 6)
        assert(lastProgress.get.sources.length == 1)
        assert(lastProgress.get.sources(0).numInputRows == 6)
        true
      }
    )
  }

  test("input row calculation with trigger having data for only one of two V2 sources") {
    val streamInput1 = MemoryStream[Int]
    val streamInput2 = MemoryStream[Int]

    testStream(streamInput1.toDF().union(streamInput2.toDF()), useV2Sink = true)(
      AddData(streamInput1, 1, 2, 3),
      CheckLastBatch(1, 2, 3),
      AssertOnQuery { q =>
        val lastProgress = getLastProgressWithData(q)
        assert(lastProgress.nonEmpty)
        assert(lastProgress.get.numInputRows == 3)
        assert(lastProgress.get.sources.length == 2)
        assert(lastProgress.get.sources(0).numInputRows == 3)
        assert(lastProgress.get.sources(1).numInputRows == 0)
        true
      },
      AddData(streamInput2, 4, 5),
      CheckLastBatch(4, 5),
      AssertOnQuery { q =>
        val lastProgress = getLastProgressWithData(q)
        assert(lastProgress.nonEmpty)
        assert(lastProgress.get.numInputRows == 2)
        assert(lastProgress.get.sources.length == 2)
        assert(lastProgress.get.sources(0).numInputRows == 0)
        assert(lastProgress.get.sources(1).numInputRows == 2)
        true
      }
    )
  }

  test("input row calculation with mixed batch and streaming V2 sources") {

    val streamInput = MemoryStream[Int]
    val staticInputDF = spark.createDataFrame(Seq(1 -> "1", 2 -> "2")).toDF("value", "anotherValue")

    testStream(streamInput.toDF().join(staticInputDF, "value"), useV2Sink = true)(
      AddData(streamInput, 1, 2, 3),
      AssertOnQuery { q =>
        q.processAllAvailable()

        // The number of leaves in the trigger's logical plan should be same as the executed plan.
        require(
          q.lastExecution.logical.collectLeaves().length ==
            q.lastExecution.executedPlan.collectLeaves().length)

        val lastProgress = getLastProgressWithData(q)
        assert(lastProgress.nonEmpty)
        assert(lastProgress.get.numInputRows == 3)
        assert(lastProgress.get.sources.length == 1)
        assert(lastProgress.get.sources(0).numInputRows == 3)
        true
      }
    )

    val streamInput2 = MemoryStream[Int]
    val staticInputDF2 = staticInputDF.union(staticInputDF).cache()

    testStream(streamInput2.toDF().join(staticInputDF2, "value"), useV2Sink = true)(
      AddData(streamInput2, 1, 2, 3),
      AssertOnQuery { q =>
        q.processAllAvailable()
        // The number of leaves in the trigger's logical plan should be different from
        // the executed plan. The static input will have two leaves in the logical plan
        // (due to the union), but will be converted to a single leaf in the executed plan
        // (due to the caching, the cached subplan is replaced by a single InMemoryTableScanExec).
        require(
          q.lastExecution.logical.collectLeaves().length !=
            q.lastExecution.executedPlan.collectLeaves().length)

        // Despite the mismatch in total number of leaves in the logical and executed plans,
        // we should be able to attribute streaming input metrics to the streaming sources.
        val lastProgress = getLastProgressWithData(q)
        assert(lastProgress.nonEmpty)
        assert(lastProgress.get.numInputRows == 3)
        assert(lastProgress.get.sources.length == 1)
        assert(lastProgress.get.sources(0).numInputRows == 3)
        true
      }
    )
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
          val metadataLogDir = new java.io.File(q.offsetLog.metadataPath.toUri)
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
          val metadataLogDir = new java.io.File(q.offsetLog.metadataPath.toUri)
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

    val input = MemoryStream[Int] :: MemoryStream[Int] :: MemoryStream[Int] :: Nil
    val q1 = startQuery(input(0).toDS, "stream_serializable_test_1")
    val q2 = startQuery(input(1).toDS.map { i =>
      // Emulate that `StreamingQuery` get captured with normal usage unintentionally.
      // It should not fail the query.
      q1
      i
    }, "stream_serializable_test_2")
    val q3 = startQuery(input(2).toDS.map { i =>
      // Emulate that `StreamingQuery` is used in executors. We should fail the query with a clear
      // error message.
      q1.explain()
      i
    }, "stream_serializable_test_3")
    try {
      input.foreach(_.addData(1))

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

  test("get the query id in source") {
    @volatile var queryId: String = null
    val source = new Source {
      override def stop(): Unit = {}
      override def getOffset: Option[Offset] = {
        queryId = spark.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY)
        None
      }
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = spark.emptyDataFrame
      override def schema: StructType = MockSourceProvider.fakeSchema
    }

    MockSourceProvider.withMockSources(source) {
      val df = spark.readStream
        .format("org.apache.spark.sql.streaming.util.MockSourceProvider")
        .load()
      testStream(df)(
        AssertOnQuery { sq =>
          sq.processAllAvailable()
          assert(sq.id.toString === queryId)
          assert(sq.runId.toString !== queryId)
          true
        }
      )
    }
  }

  test("processAllAvailable should not block forever when a query is stopped") {
    val input = MemoryStream[Int]
    input.addData(1)
    val query = input.toDF().writeStream
      .trigger(Trigger.Once())
      .format("console")
      .start()
    failAfter(streamingTimeout) {
      query.processAllAvailable()
    }
  }

  test("SPARK-22238: don't check for RDD partitions during streaming aggregation preparation") {
    val stream = MemoryStream[(Int, Int)]
    val baseDf = Seq((1, "A"), (2, "b")).toDF("num", "char").where("char = 'A'")
    val otherDf = stream.toDF().toDF("num", "numSq")
      .join(broadcast(baseDf), "num")
      .groupBy('char)
      .agg(sum('numSq))

    testStream(otherDf, OutputMode.Complete())(
      AddData(stream, (1, 1), (2, 4)),
      CheckLastBatch(("A", 1)))
  }

  test("StreamingRelationV2/StreamingExecutionRelation/ContinuousExecutionRelation.toJSON " +
    "should not fail") {
    val df = spark.readStream.format("rate").load()
    assert(df.logicalPlan.toJSON.contains("StreamingRelationV2"))

    testStream(df)(
      AssertOnQuery(_.logicalPlan.toJSON.contains("StreamingExecutionRelation"))
    )

    testStream(df, useV2Sink = true)(
      StartStream(trigger = Trigger.Continuous(100)),
      AssertOnQuery(_.logicalPlan.toJSON.contains("ContinuousExecutionRelation"))
    )
  }

  /** Create a streaming DF that only execute one batch in which it returns the given static DF */
  private def createSingleTriggerStreamingDF(triggerDF: DataFrame): DataFrame = {
    require(!triggerDF.isStreaming)
    // A streaming Source that generate only on trigger and returns the given Dataframe as batch
    val source = new Source() {
      override def schema: StructType = triggerDF.schema
      override def getOffset: Option[Offset] = Some(LongOffset(0))
      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        sqlContext.internalCreateDataFrame(
          triggerDF.queryExecution.toRdd, triggerDF.schema, isStreaming = true)
      }
      override def stop(): Unit = {}
    }
    StreamingExecutionRelation(source, spark)
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

  /** Returns the last query progress from query.recentProgress where numInputRows is positive */
  def getLastProgressWithData(q: StreamingQuery): Option[StreamingQueryProgress] = {
    q.recentProgress.filter(_.numInputRows > 0).lastOption
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
      AwaitTerminationTester.test(expectedBehavior, () => awaitTermFunc())
      true // If the control reached here, then everything worked as expected
    }
  }
}

object StreamingQuerySuite {
  // Singleton reference to clock that does not get serialized in task closures
  var clock: StreamManualClock = null
}

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

import java.io.IOException
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkIllegalStateException, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.sql.execution.datasources.v2.RealTimeStreamScanExec
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.execution.streaming.runtime.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.execution.streaming.state.{FailureInjectionCheckpointFileManager,
  FailureInjectionFileSystem, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.{broadcast, concat, lit, udf}
import org.apache.spark.sql.internal.SQLConf

class StreamRealTimeModeSuite extends StreamRealTimeModeSuiteBase {
  import testImplicits._

  test("test trigger") {
    def testTrigger(trigger: Trigger, actual: Long): Unit = {
      val realTimeTrigger = trigger.asInstanceOf[RealTimeTrigger]
      assert(
        realTimeTrigger.batchDurationMs == actual,
        s"Real time trigger duration should be ${actual} ms" +
        s" but got ${realTimeTrigger.batchDurationMs} ms"
      )
    }

    // test default
    testTrigger(Trigger.RealTime(), 300000)

    List(
      ("1 second", 1000),
      ("1 minute", 60000),
      ("1 hour", 3600000),
      ("1 day", 86400000),
      ("1 week", 604800000)
    ).foreach {
      case (str, ms) =>
        testTrigger(Trigger.RealTime(str), ms)
        testTrigger(RealTimeTrigger(str), ms)
        testTrigger(RealTimeTrigger.create(str), ms)

    }

    List(1000, 60000, 3600000, 86400000, 604800000).foreach { ms =>
      testTrigger(Trigger.RealTime(ms), ms)
      testTrigger(RealTimeTrigger(ms), ms)
      testTrigger(new RealTimeTrigger(ms), ms)
    }

    List(
      (Duration.apply(1000, "ms"), 1000),
      (Duration.apply(60, "s"), 60000),
      (Duration.apply(1, "h"), 3600000),
      (Duration.apply(1, "d"), 86400000)
    ).foreach {
      case (duration, ms) =>
        testTrigger(Trigger.RealTime(duration), ms)
        testTrigger(RealTimeTrigger(duration), ms)
        testTrigger(RealTimeTrigger(duration), ms)
    }

    List(
      (1000, TimeUnit.MILLISECONDS, 1000),
      (60, TimeUnit.SECONDS, 60000),
      (1, TimeUnit.HOURS, 3600000),
      (1, TimeUnit.DAYS, 86400000)
    ).foreach {
      case (interval, unit, ms) =>
        testTrigger(Trigger.RealTime(interval, unit), ms)
        testTrigger(RealTimeTrigger(interval, unit), ms)
        testTrigger(RealTimeTrigger.create(interval, unit), ms)
    }
    // test invalid
    List("-1", "0").foreach(
      str =>
        intercept[IllegalArgumentException] {
          testTrigger(Trigger.RealTime(str), -1)
          testTrigger(RealTimeTrigger.create(str), -1)
        }
    )

    List(-1, 0).foreach(
      duration =>
        intercept[IllegalArgumentException] {
          testTrigger(Trigger.RealTime(duration), -1)
          testTrigger(RealTimeTrigger(duration), -1)
        }
    )
  }

  test("processAllAvailable") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswer(2, 3, 4),
      AddData(inputData, 4, 5, 6),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      AddData(inputData, 7),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8),
      AddData(inputData, 10, 11),
      ProcessAllAvailable(),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 11, 12)
    )
  }

  test("error: batch duration is set less than minimum") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val mapped = inputData.toDS().map(_ + 1)
    val minBatchDuration =
      spark.conf.get(SQLConf.STREAMING_REAL_TIME_MODE_MIN_BATCH_DURATION)
    val ex = intercept[SparkIllegalArgumentException] {
      testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        StartStream(RealTimeTrigger(minBatchDuration - 1))
      )
    }
    checkError(
      ex,
      "INVALID_STREAMING_REAL_TIME_MODE_TRIGGER_INTERVAL",
      parameters = Map(
        "interval" -> (minBatchDuration - 1).toString,
        "minBatchDuration" -> minBatchDuration.toString
      )
    )
  }

  test("error when unsupported source is used") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      ExpectFailure[SparkIllegalArgumentException] { ex =>
        checkError(
          ex.asInstanceOf[SparkIllegalArgumentException],
          "STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED",
          parameters =
            Map("className" -> "org.apache.spark.sql.execution.streaming.runtime.MemoryStream")
        )
      }
    )
  }

  test("error on self union") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int].toDS()
    val mapped = inputData.map(_ + 1)

    val unioned = mapped
      .union(inputData)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)
      .map(_ + 1)

    testStream(unioned, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      ExpectFailure[SparkIllegalStateException] { ex =>
        checkError(
          ex.asInstanceOf[SparkIllegalStateException],
          "STREAMING_REAL_TIME_MODE.IDENTICAL_SOURCES_IN_UNION_NOT_SUPPORTED",
          parameters = Map("sources" ->
            "MemoryStream\\[value#\\d+\\], MemoryStream\\[value#\\d+\\]"),
          matchPVals = true
        )
      }
    )
  }

  test("LowLatencyMemoryStream load balance among all partitions") {
    val numPartitions = 3
    val inputData = LowLatencyMemoryStream[Int](numPartitions)

    val getPartitionId = udf(() => TaskContext.getPartitionId())

    val mapped = inputData.toDS().select($"value", getPartitionId()).as[(Int, Int)]

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      // 6 items round-robin across 3 partitions: item i goes to partition (i-1) % 3
      AddData(inputData, 1, 2, 3),
      AddData(inputData, 4),
      AddData(inputData, 5),
      AddData(inputData, 6),
      CheckAnswerWithTimeout(10000, (1, 0), (2, 1), (3, 2), (4, 0), (5, 1), (6, 2)),
      StopStream
    )
  }

  test("pipelined shuffle: a static-side shuffle is not marked pipelined") {
    // A broadcast stream-static join can carry a shuffle on its STATIC side, in a subtree with no
    // RealTimeStreamScanExec. That shuffle must materialize normally: a static side runs to
    // completion rather than streaming, so pulling it into the pipelined group would demand
    // concurrent slots for a stage that must instead finish, failing admission. Only shuffles on
    // the streaming path are marked, matching what the operator allowlist actually validates.
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760") {
      val staticData = spark.range(0, 3).selectExpr("id as sk", "id * 10 as sv")
        .repartition(3, $"sk")
      val inputData = LowLatencyMemoryStream[(String, Int)](2)
      val streamDf = inputData.toDF().select($"_1".as("key"), $"_2".cast("long").as("sk"))
      val result = streamDf
        .repartition(2, $"sk")
        .join(broadcast(staticData), Seq("sk"), "left")
        .select($"key")
      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 1), ("b", 2)),
        StartStream(),
        CheckAnswerWithTimeout(60000, "a", "b"),
        Execute { q =>
          val exchanges = q.lastExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          // The streaming-side repartition is pipelined; a static-side shuffle, if it survives
          // into this plan, must not be.
          val streamingSide = exchanges.filter(_.exists {
            case _: RealTimeStreamScanExec => true
            case _ => false
          })
          assert(streamingSide.nonEmpty, "expected a shuffle on the streaming path")
          assert(streamingSide.forall(_.pipelined),
            "a streaming-path shuffle must be pipelined")
          assert(exchanges.filterNot(streamingSide.contains).forall(!_.pipelined),
            "a static-side shuffle must not be marked pipelined")
        },
        StopStream
      )
    }
  }

  test("multiple broadcast joins with the same static table run in Real-Time Mode") {
    // Two broadcast joins against the SAME static table -- exchange reuse collapses the second
    // broadcast into a ReusedExchangeExec. This is the supported reuse shape in Real-Time Mode: a
    // reused BROADCAST exchange (allowlisted), NOT a reused shuffle. The RTM marking rule only
    // marks ShuffleExchangeExec, so it correctly leaves the broadcast reuse alone.
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val staticData = Seq((1, "a"), (2, "b"), (3, "c")).toDF("key", "value")
    val df = inputData.toDS().toDF("key").join(broadcast(staticData), Seq("key"), "left")
    val df2 = df.join(broadcast(staticData), Seq("key"), "left")
    testStream(df2, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswerWithTimeout(30000, (1, "a", "a"), (2, "b", "b"), (3, "c", "c")),
      Execute { q =>
        val plan = q.lastExecution.executedPlan
        // Verify the shape this test is about, not just the answer: the second broadcast is reused,
        // and the marking rule left it alone because it only marks shuffle exchanges.
        assert(plan.exists(_.isInstanceOf[ReusedExchangeExec]),
          s"expected the second broadcast to be reused, got:\n$plan")
        assert(plan.collect { case s: ShuffleExchangeExec => s }.isEmpty,
          "a broadcast stream-static join should introduce no shuffle exchange")
      },
      StopStream
    )
  }
}

class StreamRealTimeModeWithManualClockSuite extends StreamRealTimeModeManualClockSuiteBase {
  import testImplicits._

  test("simple map query") {
    val inputData = LowLatencyMemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswerWithTimeout(10000, 2, 3, 4),
      AddData(inputData, 4, 5, 6),
      // make sure we can output data before batch ends
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      AddData(inputData, 7),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7, 8),
      StopStream
    )
  }

  test("simple map query with restarts") {
    val inputData = LowLatencyMemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      AddData(inputData, 1, 2, 3),
      CheckAnswerWithTimeout(10000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream(),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      StopStream
    )
  }

  test("simple map query switching between RTM and MBM") {
    val inputData = LowLatencyMemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(defaultTrigger),
      AddData(inputData, 1, 2, 3),
      CheckAnswerWithTimeout(10000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream(Trigger.ProcessingTime(1000)),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      WaitUntilBatchProcessed(1),
      StopStream,
      AddData(inputData, 7),
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7, 8),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(2),
      StopStream
    )
  }

  test("listener progress") {
    val inputData = LowLatencyMemoryStream.singlePartition[Int]
    val mapped = inputData.toDS().map(_ + 1)

    var expectedStartOffset: String = null
    var expectedEndOffset = "{\"0\":3}"
    var expectedNumInputRows = 3
    val progressCalled = new AtomicInteger(0)
    var exception: Option[Exception] = None

    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        val progress: StreamingQueryProgress = event.progress
        try {
          assert(progress.sources(0).startOffset == expectedStartOffset, "startOffset not expected")
          assert(progress.sources(0).endOffset == expectedEndOffset, "endOffset not expected")
          assert(
            progress.sources(0).numInputRows == expectedNumInputRows,
            "numInputRows not expected")
        } catch {
          case ex: Exception =>
            exception = Some(ex)
        }
        progressCalled.incrementAndGet()
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
    })

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      Execute { q =>
        eventually(Timeout(streamingTimeout)) {
          assert(progressCalled.get() == 1)
        }
        expectedEndOffset = "{\"0\":6}"
        expectedStartOffset = "{\"0\":3}"
        expectedNumInputRows = 3
      },
      AddData(inputData, 4, 5, 6),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(1),
      Execute { q =>
        eventually(Timeout(streamingTimeout)) {
          assert(progressCalled.get() == 2)
        }
        expectedEndOffset = "{\"0\":7}"
        expectedStartOffset = "{\"0\":6}"
        expectedNumInputRows = 1
      },
      AddData(inputData, 7),
      CheckAnswerWithTimeout(10000, 2, 3, 4, 5, 6, 7, 8),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(2),
      StopStream
    )
    eventually(Timeout(streamingTimeout)) {
      assert(progressCalled.get() == 3)
    }
    assert(!exception.isDefined, s"${exception}")
  }

  test("purge offsetLog when it doesn't match with the commit log") {
    // Simulate when the query fails after commiting the offset log but before the commit log
    // by manually deleting the last entry of the commit log.
    val inputData = LowLatencyMemoryStream[Int](1)
    val mapped = inputData.toDS().map(_ + 1)

    var lastOffset = -1L

    testStream(mapped, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3),
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(60000, 2, 3, 4),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      AddData(inputData, 4, 5, 6),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(1),
      AddData(inputData, 7),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7, 8),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(2),
      StopStream,
      Execute { q =>
        // Delete the last committed batch from the commit log to simulate the query fails
        // between writing the offset log and the commit log.
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        val offset = q.offsetLog.getLatest().map(_._1).getOrElse(-1L)
        assert(commit == offset)
        q.commitLog.purgeAfter(commit - 1)
        val commitAfterDelete = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        assert(commitAfterDelete == offset - 1)
        lastOffset = commitAfterDelete
      },
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7, 8, 8),
      StopStream,
      Execute { q =>
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        val offset = q.offsetLog.getLatest().map(_._1).getOrElse(-1L)
        assert(commit == offset && commit == lastOffset)
      },
      AddData(inputData, 8),
      StartStream(defaultTrigger),
      CheckAnswerWithTimeout(60000, 2, 3, 4, 5, 6, 7, 8, 8, 8, 9),
      StopStream
    )
  }

  test("transformWithState writes batch 0 metadata only after the RTM offset WAL") {
    withSQLConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
        classOf[FailureInjectionCheckpointFileManager].getName,
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpointDir =>
        val injectionState = FailureInjectionFileSystem.registerTempPath(checkpointDir.getPath)
        try {
          val inputData = LowLatencyMemoryStream[String](1)
          val result = inputData.toDS()
            .groupByKey(value => value)
            .transformWithState(
              new RunningCountStatefulProcessor,
              TimeMode.ProcessingTime(),
              OutputMode.Update())
          val metadataFile = new java.io.File(
            checkpointDir, "state/0/_metadata/v2/0")
          val stateSchemaDir = new java.io.File(
            checkpointDir, "state/0/_stateSchema/default")

          injectionState.failureCreateAtomicRegex = Seq(".*/offsets/0")
          testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
            AddData(inputData, "a"),
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
            CheckAnswerWithTimeout(60000, ("a", "1")),
            Execute { _ =>
              assert(Option(stateSchemaDir.listFiles()).exists(_.nonEmpty))
              assert(!metadataFile.exists())
            },
            advanceRealTimeClock,
            ExpectFailure[IOException]()
          )
          assert(!metadataFile.exists())

          injectionState.failureCreateAtomicRegex = Seq.empty
          testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
            CheckAnswerWithTimeout(60000, ("a", "1")),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(0),
            StopStream
          )
          assert(metadataFile.exists())
        } finally {
          FailureInjectionFileSystem.removePathFromTempToInjectionState(checkpointDir.getPath)
        }
      }
    }
  }

  // ========================================================================================
  // Pipelined (streaming) shuffle: a stateful/repartition Real-Time Mode query whose shuffle is a
  // PipelinedShuffleDependency, so the producer (source scan) and consumer stages are co-scheduled
  // and stream records through a transient shuffle instead of the consumer waiting for the producer
  // to fully materialize.
  // ========================================================================================

  override def beforeEach(): Unit = {
    super.beforeEach()
    StreamRealTimeModeSuite.failTasks = false
  }

  /** Assert every shuffle exchange in the query's last executed plan is pipelined. */
  private def assertAllExchangesPipelined(q: StreamExecution): Unit = {
    val exchanges = q.lastExecution.executedPlan.collect { case s: ShuffleExchangeExec => s }
    assert(exchanges.nonEmpty, "expected at least one shuffle exchange in the plan")
    assert(exchanges.forall(_.pipelined),
      "expected all Real-Time Mode shuffle exchanges to be pipelined, got: " +
        exchanges.map(e => s"pipelined=${e.pipelined}").mkString(", "))
  }

  test("pipelined shuffle: stateful dedup runs in Real-Time Mode and co-schedules its stages") {
    // Track, from the driver, whether the producer (source scan) and consumer (dedup) stages of the
    // pipelined group were ever RUNNING simultaneously. A sequential producer-then-consumer
    // schedule never exceeds one running stage at a time; >= 2 proves genuine co-scheduling.
    val runningStages = ConcurrentHashMap.newKeySet[Int]()
    val maxConcurrentStages = new AtomicInteger(0)
    val queryStageIds = ConcurrentHashMap.newKeySet[Int]()
    // Count only stages belonging to the query under test. The suite shares one SparkContext, so a
    // stage from any other streaming query would otherwise satisfy the co-scheduling assertion
    // below even if this query's producer and consumer actually ran one after the other. The id is
    // captured from the query once it is running, and every job is matched against it.
    val queryId = new AtomicReference[String](null)
    val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit = {
        // StreamExecution tags every streaming job with its query id.
        val id = queryId.get()
        if (id != null && e.properties.getProperty(StreamExecution.QUERY_ID_KEY) == id) {
          e.stageIds.foreach(queryStageIds.add(_))
        }
      }
      override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
        if (queryStageIds.contains(e.stageInfo.stageId)) {
          runningStages.add(e.stageInfo.stageId)
          maxConcurrentStages.accumulateAndGet(runningStages.size(), Math.max)
        }
      }
      override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
        runningStages.remove(e.stageInfo.stageId)
      }
    }
    spark.sparkContext.addSparkListener(listener)
    try {
      val inputData = LowLatencyMemoryStream[(String, Int)]
      // scan --shuffle(repartition by key)--> streaming dropDuplicates --> sink.
      val result = inputData.toDF().select($"_1".as("key")).dropDuplicates("key").select($"key")
      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 1), ("b", 1), ("c", 1), ("a", 2), ("b", 2), ("c", 2)),
        StartStream(),
        // StartStream only launches the query -- the stream runs on its own thread and can submit
        // the first batch's jobs before the id is recorded here, in which case those jobs are not
        // attributed. Rather than race, the assertion below relies on the SECOND batch, which is
        // driven after this point and so is always observed in full.
        Execute(q => queryId.set(q.id.toString)),
        CheckAnswerWithTimeout(60000, "a", "b", "c"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(0),
        AddData(inputData, ("a", 3), ("b", 3), ("c", 3), ("d", 1)),
        CheckAnswerWithTimeout(60000, "a", "b", "c", "d"),
        Execute { q =>
          assertAllExchangesPipelined(q)
          assert(maxConcurrentStages.get() >= 2,
            s"expected >= 2 stages running concurrently, saw max ${maxConcurrentStages.get()}")
        },
        StopStream
      )
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("pipelined shuffle: multi-key dedup runs in Real-Time Mode over a pipelined shuffle") {
    // dropDuplicates on more than one column: the hash-partitioning is by the composite key.
    // Confirms the pipelined path is not specific to a single dedup column.
    val inputData = LowLatencyMemoryStream[(String, Int)]
    val result = inputData.toDF().select($"_1".as("k1"), $"_2".as("k2"))
      .dropDuplicates("k1", "k2")
      .select(concat($"k1", lit("-"), $"k2").as("out"))
    testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      // (a,1) twice -> once; (a,2) is a distinct composite key -> also emitted.
      AddData(inputData, ("a", 1), ("a", 1), ("a", 2), ("b", 1)),
      StartStream(),
      CheckAnswerWithTimeout(60000, "a-1", "a-2", "b-1"),
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      AddData(inputData, ("a", 1), ("a", 2), ("b", 2)),
      CheckAnswerWithTimeout(60000, "a-1", "a-2", "b-1", "b-2"),
      Execute { q => assertAllExchangesPipelined(q) },
      StopStream
    )
  }

  test("pipelined shuffle: an explicit repartition-by-key runs in Real-Time Mode") {
    // A stateless repartition (no dedup): the ShuffleExchangeExec is still marked pipelined in RTM.
    val inputData = LowLatencyMemoryStream[Int]
    val result = inputData.toDF().repartition(4, $"value").select($"value")
    testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      AddData(inputData, 1, 2, 3, 4, 5),
      StartStream(),
      CheckAnswerWithTimeout(60000, 1, 2, 3, 4, 5),
      Execute { q => assertAllExchangesPipelined(q) },
      advanceRealTimeClock,
      WaitUntilBatchProcessed(0),
      AddData(inputData, 6, 7),
      CheckAnswerWithTimeout(60000, 1, 2, 3, 4, 5, 6, 7),
      StopStream
    )
  }

  test("pipelined shuffle: a chain of two shuffles is a single co-scheduled pipelined group") {
    // A round-robin repartition (RoundRobinPartitioning) feeding a dropDuplicates (HashPartitioning
    // on the dedup key) produces TWO distinct shuffle exchanges -- neither distribution satisfies
    // the other, so they are not collapsed. BOTH must be marked pipelined and the all-pipelined job
    // co-schedules as one group (>= 2 stages running at once). This is the multi-shuffle-per-
    // group scenario: more than one pipelined shuffle in a single Real-Time Mode job.
    val runningStages = ConcurrentHashMap.newKeySet[Int]()
    val maxConcurrentStages = new AtomicInteger(0)
    val queryStageIds = ConcurrentHashMap.newKeySet[Int]()
    // Count only stages belonging to the query under test. The suite shares one SparkContext, so a
    // stage from any other streaming query would otherwise satisfy the co-scheduling assertion
    // below even if this query's producer and consumer actually ran one after the other. The id is
    // captured from the query once it is running, and every job is matched against it.
    val queryId = new AtomicReference[String](null)
    val listener = new SparkListener {
      override def onJobStart(e: SparkListenerJobStart): Unit = {
        // StreamExecution tags every streaming job with its query id.
        val id = queryId.get()
        if (id != null && e.properties.getProperty(StreamExecution.QUERY_ID_KEY) == id) {
          e.stageIds.foreach(queryStageIds.add(_))
        }
      }
      override def onStageSubmitted(e: SparkListenerStageSubmitted): Unit = {
        if (queryStageIds.contains(e.stageInfo.stageId)) {
          runningStages.add(e.stageInfo.stageId)
          maxConcurrentStages.accumulateAndGet(runningStages.size(), Math.max)
        }
      }
      override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
        runningStages.remove(e.stageInfo.stageId)
      }
    }
    spark.sparkContext.addSparkListener(listener)
    // Keep the shuffle partition count small: the whole group (scan + 2 shuffles) must fit in the
    // test cluster's slots at once (gang admission), so a 2-shuffle chain at the default 200
    // partitions would fail with CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT.
    try {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val inputData = LowLatencyMemoryStream[(String, Int)]
        val result = inputData.toDF().select($"_1".as("key"))
          .repartition(4)          // RoundRobinPartitioning -> shuffle #1
          .dropDuplicates("key")   // HashPartitioning(key) -> shuffle #2
          .select($"key")
        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(inputData, ("a", 1), ("b", 1), ("c", 1), ("a", 2)),
          StartStream(),
          // StartStream only launches the query -- the stream runs on its own thread and can submit
          // the first batch's jobs before the id is recorded here, in which case those jobs are not
          // attributed and contribute nothing to maxConcurrentStages. So do not assert on the first
          // batch: record the id, let batch 0 finish, then drive a SECOND batch that is guaranteed
          // to run entirely after this point, and assert on that one.
          Execute(q => queryId.set(q.id.toString)),
          CheckAnswerWithTimeout(60000, "a", "b", "c"),
          advanceRealTimeClock,
          WaitUntilBatchProcessed(0),
          AddData(inputData, ("d", 1), ("e", 1), ("d", 2)),
          CheckAnswerWithTimeout(60000, "a", "b", "c", "d", "e"),
          Execute { q =>
            val exchanges = q.lastExecution.executedPlan.collect {
              case s: ShuffleExchangeExec => s
            }
            assert(exchanges.size >= 2, s"expected >= 2 shuffle exchanges, got ${exchanges.size}")
            assert(exchanges.forall(_.pipelined),
              "every exchange in the chain must be pipelined, got: " +
                exchanges.map(_.pipelined).mkString(", "))
            assert(maxConcurrentStages.get() >= 2,
              s"a 2-shuffle chain must co-schedule >= 2 stages, saw ${maxConcurrentStages.get()}")
          },
          StopStream
        )
      }
    } finally {
      spark.sparkContext.removeSparkListener(listener)
    }
  }

  test("pipelined shuffle: a chain of two round-robin repartitions runs in Real-Time Mode") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val inputData = LowLatencyMemoryStream[(String, Int)](2)
      // Two round-robin repartitions with a map between them; consecutive repartitions collapse to
      // the last one, so the map is what keeps both shuffles in the plan. Both are pipelined, and
      // the second one's producer reads the first one's output -- an UNORDERED input. A round-robin
      // shuffle is order-sensitive once the deterministic local sort is off, which would escalate
      // that producer to INDETERMINATE and get the group rejected; a pipelined shuffle is exempt
      // from the order-sensitive marking precisely so this shape runs.
      val result = inputData.toDS()
        .repartition(3)
        .map(row => row)
        .repartition(2)
        .map(row => row)
        .toDF().select($"_1".as("key"))
      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 1), ("b", 2), ("c", 3)),
        StartStream(),
        CheckAnswerWithTimeout(60000, "a", "b", "c"),
        Execute { q =>
          val exchanges = q.lastExecution.executedPlan.collect {
            case s: ShuffleExchangeExec => s
          }
          assert(exchanges.size >= 2, s"expected >= 2 shuffle exchanges, got ${exchanges.size}")
          assert(exchanges.forall(_.pipelined),
            "every exchange in the chain must be pipelined, got: " +
              exchanges.map(_.pipelined).mkString(", "))
        },
        StopStream
      )
    }
  }

  test("pipelined shuffle: an explicit sortBeforeRepartition=true is rejected") {
    // The deterministic local sort before a round-robin repartition never drains an unbounded
    // stream, so honouring sortBeforeRepartition=true would hang a Real-Time Mode query forever.
    // Rather than silently override it, the pre-flight in StreamingQueryManager rejects the
    // explicit value up front. See StreamingQueryManager.throwIfConfsAreRealTimeModeIncompatible.
    withSQLConf(
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.SORT_BEFORE_REPARTITION.key -> "true") {
      val inputData = LowLatencyMemoryStream[(String, Int)](2)
      val result = inputData.toDF().select($"_1".as("key"))
        .repartition(4)
        .dropDuplicates("key")
        .select($"key")
      val e = intercept[SparkIllegalArgumentException] {
        testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
          AddData(inputData, ("a", 1), ("b", 1), ("c", 1), ("a", 2)),
          StartStream()
        )
      }
      checkError(e, condition = "STREAMING_REAL_TIME_MODE.SQL_CONFIGURATION_NOT_SUPPORTED",
        parameters = e.getMessageParameters.asScala.toMap)
    }
  }

  test("pipelined shuffle: dedup recovers from a task failure via checkpoint restart") {
    withTempDir { checkpointDir =>
      // A UDF that throws on demand, placed after the dedup so the failure lands in the pipelined
      // consumer stage while the query runs. RTM does not retry tasks, so one failure fails it.
      val failUDF = udf { (key: String) =>
        if (StreamRealTimeModeSuite.failTasks) {
          throw new RuntimeException(s"forced task failure on $key")
        }
        key
      }
      val inputData = LowLatencyMemoryStream[(String, Int)]
      val result = inputData.toDF().select($"_1".as("key")).dropDuplicates("key")
        .select(failUDF($"key").as("key"))
      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        AddData(inputData, ("a", 1), ("b", 1), ("a", 2)),
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        CheckAnswerWithTimeout(60000, "a", "b"),
        advanceRealTimeClock,
        WaitUntilBatchProcessed(0),
        Execute { _ => StreamRealTimeModeSuite.failTasks = true },
        AddData(inputData, ("c", 1)),
        advanceRealTimeClock,
        ExpectFailure[SparkException] { ex =>
          val msg = Option(ex.getCause).map(_.getMessage).getOrElse(ex.getMessage)
          assert(msg != null && msg.contains("forced task failure"),
            s"expected a forced task failure, got: $msg")
        }
      )
      // Restart from the same checkpoint: batch-0 dedup state must survive (a, b not re-emitted),
      // only the genuinely-new c, d appear -- recovery to the last committed batch.
      StreamRealTimeModeSuite.failTasks = false
      testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        AddData(inputData, ("a", 3), ("b", 3), ("c", 3), ("d", 1)),
        CheckAnswerWithTimeout(60000, "c", "d"),
        advanceRealTimeClock,
        StopStream
      )
    }
  }

  test("pipelined shuffle: dedup recovers from a commit-log write failure") {
    withSQLConf(
      SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key ->
        classOf[FailureInjectionCheckpointFileManager].getName) {
      withTempDir { checkpointDir =>
        val injectionState = FailureInjectionFileSystem.registerTempPath(checkpointDir.getPath)
        try {
          val inputData = LowLatencyMemoryStream[(String, Int)]
          val result = inputData.toDF().select($"_1".as("key")).dropDuplicates("key").select($"key")
          // Batch 0 dedups a, b and commits. Then fail the close() of batch 1's commit-log write so
          // batch 1 cannot commit and the query fails after processing.
          injectionState.createAtomicDelayCloseRegex = Seq(".*/commits/1")
          testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
            AddData(inputData, ("a", 1), ("b", 1)),
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
            CheckAnswerWithTimeout(60000, "a", "b"),
            advanceRealTimeClock,
            WaitUntilBatchProcessed(0),
            AddData(inputData, ("c", 1)),
            CheckAnswerWithTimeout(60000, "a", "b", "c"),
            advanceRealTimeClock,
            ExpectFailure[IOException]()
          )
          // Clear injection, restart: batch-0 state survives (a, b seen); uncommitted batch 1
          // re-runs so its new key c is still emitted, plus a further new key d.
          injectionState.createAtomicDelayCloseRegex = Seq.empty
          testStream(result, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
            AddData(inputData, ("a", 2), ("b", 2), ("c", 2), ("d", 1)),
            CheckAnswerWithTimeout(60000, "c", "d"),
            advanceRealTimeClock,
            StopStream
          )
        } finally {
          FailureInjectionFileSystem.removePathFromTempToInjectionState(checkpointDir.getPath)
        }
      }
    }
  }
}

/** Driver-side switch a UDF reads on executors to fail a task on demand (fault-tolerance tests). */
object StreamRealTimeModeSuite {
  @volatile var failTasks: Boolean = false
}

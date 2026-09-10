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

import java.sql.Timestamp
import java.time.Duration
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.scalatest.time.SpanSugar._

import org.apache.spark.{
  SparkConf, SparkException, SparkRuntimeException, SparkThrowable, TaskContext, TaskContextImpl}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.datasources.v2.{LowLatencyClock, RealTimeStreamScanExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.TransformWithStateExec
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemorySink, LowLatencyMemoryStream}
import org.apache.spark.sql.execution.streaming.state.{
  EnableStateStoreRowChecksum, RocksDBConf, RocksDBStateStoreProvider}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.{GlobalSingletonManualClock, StreamManualClock}
import org.apache.spark.tags.SlowSQLTest

private class RealTimeEagerCountProcessor
  extends StatefulProcessor[String, (String, Int), (String, Long)] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    // Eager consumption detects the blocking final group produced by GroupedIterator.
    val newCount = Option(countState.get()).getOrElse(0L) + inputRows.size
    countState.update(newCount)
    // Access state lazily as the output is consumed to verify implicit-key lifecycle handling.
    Iterator.single(key).map(currentKey => (currentKey, countState.get()))
  }
}

private class RealTimeRunningCountStatefulProcessor(emitEvery: Long)
  extends StatefulProcessor[String, String, (String, Long)] {

  @transient private var countState: MapState[String, Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getMapState(
      "countState", Encoders.STRING, Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    inputRows.flatMap { row =>
      val count = countState.getValue(row) + 1L
      countState.updateValue(row, count)
      if (count % emitEvery == 0L) Iterator.single((row, count)) else Iterator.empty
    }
  }
}

private class RealTimeTTLCountProcessor(ttl: Duration = Duration.ofSeconds(10))
  extends StatefulProcessor[String, (String, Int), (String, Long)] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState(
      "count", Encoders.scalaLong, TTLConfig(ttl))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    inputRows.map { _ =>
      val newCount = Option(countState.get()).getOrElse(0L) + 1L
      countState.update(newCount)
      (key, newCount)
    }
  }
}

private class RealTimeListTTLProcessor
  extends StatefulProcessor[String, (String, Int), (String, Long)] {

  @transient private var listState: ListState[Int] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    listState = getHandle.getListState(
      "values", Encoders.scalaInt, TTLConfig(Duration.ofSeconds(10)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    inputRows.map { case (_, value) =>
      listState.appendList(Array(value, value + 1, value + 2))
      (key, listState.get().size.toLong)
    }
  }
}

private class RealTimeMapTTLAndTimerProcessor
  extends StatefulProcessor[String, (String, Int), (String, String, Long)] {

  @transient private var countState: MapState[String, Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getMapState(
      "count", Encoders.STRING, Encoders.scalaLong, TTLConfig(Duration.ofMinutes(10)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, String, Long)] = {
    inputRows.map { case (_, timerDelayMs) =>
      val count = Option(countState.getValue("count")).getOrElse(0L) + 1L
      countState.updateValue("count", count)
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + timerDelayMs)
      (key, "data", count)
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String, Long)] = {
    Iterator.single((key, "timer", expiredTimerInfo.getExpiryTimeInMs()))
  }
}

private class RealTimePartitionProcessor
  extends StatefulProcessor[String, (String, Int), (String, Int)] {

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, Int)] = {
    inputRows.map(_ => (key, TaskContext.getPartitionId()))
  }
}

private class RealTimeProcessingTimerProcessor
  extends StatefulProcessor[String, (String, Int), (String, String)] {

  @transient private var timerRegistered: ValueState[Boolean] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    timerRegistered =
      getHandle.getValueState("timerRegistered", Encoders.scalaBoolean, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    inputRows.map { _ =>
      if (!Option(timerRegistered.get()).getOrElse(false)) {
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 10000L)
        timerRegistered.update(true)
      }
      (key, "data")
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    Iterator.single((key, "timer"))
  }
}

private class RealTimeProcessingTimerValueProcessor
  extends StatefulProcessor[String, (String, Int), (String, String, Long)] {

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, String, Long)] = {
    inputRows.map { _ =>
      val currentTimeMs = timerValues.getCurrentProcessingTimeInMs()
      getHandle.registerTimer(currentTimeMs + 10000L)
      (key, "data", currentTimeMs)
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String, Long)] = {
    Iterator.single((key, "timer", timerValues.getCurrentProcessingTimeInMs()))
  }
}

private object TaskCompletionListenerCount {
  private lazy val listenerStackField = {
    val field = classOf[TaskContextImpl].getDeclaredField("onCompleteCallbacks")
    field.setAccessible(true)
    field
  }

  def get(): Int = listenerStackField.get(TaskContext.get())
    .asInstanceOf[java.util.Stack[_]].size()
}

private class RealTimeTimerIteratorListenerProcessor
  extends StatefulProcessor[String, (String, Int), (Int, Boolean)] {

  private var previousListenerCount = -1

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(Int, Boolean)] = {
    inputRows.map { case (_, value) =>
      val listenerCount = TaskCompletionListenerCount.get()
      val listenerCountIncreased =
        previousListenerCount >= 0 && listenerCount > previousListenerCount
      previousListenerCount = listenerCount
      (value, listenerCountIncreased)
    }
  }
}

private class RealTimeTTLIteratorListenerProcessor
  extends StatefulProcessor[String, (String, Int), (Int, Int)] {

  @transient private var valueState: ValueState[Int] = _
  private var previousListenerCount = -1

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    valueState = getHandle.getValueState(
      "value", Encoders.scalaInt, TTLConfig(Duration.ofHours(1)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(Int, Int)] = {
    inputRows.map { case (_, value) =>
      valueState.update(value)
      val listenerCount = TaskCompletionListenerCount.get()
      val addedListenerCount = if (previousListenerCount >= 0) {
        listenerCount - previousListenerCount
      } else {
        0
      }
      previousListenerCount = listenerCount
      (value, addedListenerCount)
    }
  }
}

private class RTMStatefulProcessorWithProcTimeTimerWithMultipleTimers(timerExpireTs: Long)
    extends RTMStatefulProcessorWithProcTimeTimer(timerExpireTs) {
  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {

    val currCount = Option(_countState.get()).getOrElse(0L)
    if (currCount == 0 && (key == "a" || key == "c")) {
      getHandle.registerTimer(
        timerValues.getCurrentProcessingTimeInMs() + timerExpireTs
      )

      getHandle.registerTimer(
        timerValues.getCurrentProcessingTimeInMs() + (timerExpireTs + 1000)
      )
    }

    val count = currCount + 1
    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

private class RTMStatefulProcessorWithProcTimeTimer(timerExpireTs: Long)
    extends RunningCountStatefulProcessor {

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    _countState.clear()
    Iterator((key, "-1"))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {

    val currCount = Option(_countState.get()).getOrElse(0L)
    if (currCount == 0 && (key == "a" || key == "c")) {
      getHandle.registerTimer(
        timerValues.getCurrentProcessingTimeInMs() + timerExpireTs
      )
    }

    val count = currCount + 1
    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

private class RTMStatefulProcessorWithProcTimeTimerInputInt(timerExpireTs: Long)
    extends StatefulProcessor[Int, Int, (Int, Long)] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState(
      "countState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleExpiredTimer(
      key: Int,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(Int, Long)] = {
    countState.clear()
    Iterator.single((key, -1L))
  }

  override def handleInputRows(
      key: Int,
      inputRows: Iterator[Int],
      timerValues: TimerValues): Iterator[(Int, Long)] = {
    val currentCount = Option(countState.get()).getOrElse(0L)
    if (currentCount == 0L) {
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + timerExpireTs)
    }
    val count = currentCount + 1L
    if (count == 3L) {
      countState.clear()
      Iterator.empty
    } else {
      countState.update(count)
      Iterator.single((key, count))
    }
  }
}

private class RTMStatefulProcessorWithProcTimeTimerInputTuple(timerExpireTs: Long)
    extends StatefulProcessor[(Int, String), (Int, String), (Int, String, Long)] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState(
      "countState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleExpiredTimer(
      key: (Int, String),
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(Int, String, Long)] = {
    countState.clear()
    Iterator.single((key._1, key._2, -1L))
  }

  override def handleInputRows(
      key: (Int, String),
      inputRows: Iterator[(Int, String)],
      timerValues: TimerValues): Iterator[(Int, String, Long)] = {
    val currentCount = Option(countState.get()).getOrElse(0L)
    if (currentCount == 0L) {
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + timerExpireTs)
    }
    val count = currentCount + 1L
    if (count == 3L) {
      countState.clear()
      Iterator.empty
    } else {
      countState.update(count)
      Iterator.single((key._1, key._2, count))
    }
  }
}

private class RealTimeEventTimerProcessor
  extends StatefulProcessor[String, (Timestamp, String), (String, String, Long)] {

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(Timestamp, String)],
      timerValues: TimerValues): Iterator[(String, String, Long)] = {
    inputRows.map { case (eventTime, _) =>
      getHandle.registerTimer(eventTime.getTime)
      (key, "data", timerValues.getCurrentWatermarkInMs())
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String, Long)] = {
    Iterator.single((key, "timer", timerValues.getCurrentWatermarkInMs()))
  }
}

private case class RealTimeEventTimeOutputRow(
    key: String,
    outputEventTime: Timestamp,
    count: Long)

private class RealTimeEventTimeOutputProcessor(
    outputEventTimeOverride: Option[Timestamp] = None)
  extends StatefulProcessor[String, (Timestamp, String), RealTimeEventTimeOutputRow] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(Timestamp, String)],
      timerValues: TimerValues): Iterator[RealTimeEventTimeOutputRow] = {
    inputRows.map { case (eventTime, _) =>
      val newCount = Option(countState.get()).getOrElse(0L) + 1L
      countState.update(newCount)
      RealTimeEventTimeOutputRow(
        key, outputEventTimeOverride.getOrElse(eventTime), newCount)
    }
  }
}

private class RealTimeInitialCountProcessor
  extends StatefulProcessorWithInitialState[
    String, (String, Int), (String, Long), Long] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInitialState(
      key: String,
      initialState: Long,
      timerValues: TimerValues): Unit = {
    countState.update(Option(countState.get()).getOrElse(0L) + initialState)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Int)],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    inputRows.map { _ =>
      val newCount = Option(countState.get()).getOrElse(0L) + 1L
      countState.update(newCount)
      (key, newCount)
    }
  }
}

private class RealTimeEventInitialCountProcessor
  extends StatefulProcessorWithInitialState[
    String, (Timestamp, String), (String, Long), Long] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInitialState(
      key: String,
      initialState: Long,
      timerValues: TimerValues): Unit = {
    countState.update(initialState)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(Timestamp, String)],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    inputRows.map { _ =>
      val newCount = Option(countState.get()).getOrElse(0L) + 1L
      countState.update(newCount)
      (key, newCount)
    }
  }
}

private class RealTimeInitialStateProcTimerWithExpiryProcessor
  extends StatefulProcessorWithInitialStateProcTimerClass {

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    super.handleExpiredTimer(key, timerValues, expiredTimerInfo).map { case (expiredKey, _) =>
      (expiredKey, expiredTimerInfo.getExpiryTimeInMs().toString)
    }
  }
}

private object RealTimeInitialStateFailure {
  @volatile var enabled: Boolean = false
}

private object RealTimeInitialStateBootstrapBlock {
  @volatile private var enabled = false
  @volatile private var taskStarted = new CountDownLatch(0)
  @volatile private var releaseTask = new CountDownLatch(0)

  def enable(): Unit = {
    taskStarted = new CountDownLatch(1)
    releaseTask = new CountDownLatch(1)
    enabled = true
  }

  def awaitTaskStart(): Boolean = taskStarted.await(1, TimeUnit.MINUTES)

  def awaitReleaseIfEnabled(): Unit = {
    if (enabled) {
      taskStarted.countDown()
      releaseTask.await()
    }
  }

  def disable(): Unit = {
    enabled = false
    releaseTask.countDown()
  }
}

@SlowSQLTest
class RealTimeTransformWithStateSuite extends StreamRealTimeModeE2ESuiteBase {
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.STATE_STORE_PROVIDER_CLASS.key, classOf[RocksDBStateStoreProvider].getName)

  private def advanceClock(clock: GlobalSingletonManualClock): ExternalAction = {
    advanceClock(clock, defaultTrigger.batchDurationMs)
  }

  private def advanceClock(
      clock: GlobalSingletonManualClock,
      advanceMs: Long): ExternalAction = {
    new ExternalAction {
      override def runAction(): Unit = clock.advance(advanceMs)
    }
  }

  private def createStringMemoryStream(numPartitions: Int = 2)
      : (LowLatencyMemoryStream[String], GlobalSingletonManualClock) = {
    val clock = new GlobalSingletonManualClock()
    LowLatencyClock.setClock(clock)
    (LowLatencyMemoryStream[String](numPartitions), clock)
  }

  private def waitForNextBatchToStart(): Unit = {
    eventually(timeout(60.seconds)) {
      val tasksRunning = spark.sparkContext.statusTracker
        .getExecutorInfos.map(_.numRunningTasks()).sum
      assert(tasksRunning >= 1, s"tasksRunning: $tasksRunning")
    }
  }

  private def initialState(values: Seq[(String, Long)]) = {
    values.toDS()
      // More initial-state partitions than available task slots exercises finite bootstrap
      // scheduling independently from the later pipelined RTM batches.
      .repartition(12, $"_1")
      .map { value =>
        RealTimeInitialStateBootstrapBlock.awaitReleaseIfEnabled()
        if (RealTimeInitialStateFailure.enabled) {
          throw new RuntimeException("injected initial-state bootstrap failure")
        }
        value
      }
      .groupByKey(_._1)
      .mapValues(_._2)
  }

  private def transformWithInitialState(
      input: LowLatencyMemoryStream[(String, Int)],
      values: Seq[(String, Long)]) = {
    input.toDS()
      .groupByKey(_._1)
      .transformWithState(
        new RealTimeInitialCountProcessor,
        TimeMode.None(),
        OutputMode.Update(),
        initialState(values))
  }

  test("processes repeated keys within a long-running batch without a sort") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val (input, _) = createMemoryStream(numPartitions = 2)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeEagerCountProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1), ("a", 2), ("b", 1), ("a", 3)),
        // The RTM batch remains open for five minutes. Seeing the final input here proves that
        // TransformWithState processes rows individually rather than waiting for batch end.
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 1L), ("a", 2L), ("b", 1L), ("a", 3L)),
        Execute { q =>
          val operators = q.lastExecution.executedPlan.collect {
            case t: TransformWithStateExec => t
          }
          assert(operators.size == 1, q.lastExecution.executedPlan)
          assert(operators.head.isRealTimeMode)
          assert(operators.head.requiredChildOrdering.forall(_.isEmpty))
          assert(!operators.head.child.exists(_.isInstanceOf[SortExec]))
        },
        StopStream
      )
    }
  }

  test("map state can emit an aggregate every other input") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val (input, clock) = createStringMemoryStream(numPartitions = 2)
      val result = input.toDS()
        .groupByKey(identity)
        .transformWithState(
          new RealTimeRunningCountStatefulProcessor(2L),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, "a", "b"),
        Execute { _ => waitForNextBatchToStart() },
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        CheckAnswerWithTimeout(60.seconds.toMillis),
        Execute { _ => waitForNextBatchToStart() },
        AddData(input, "c", "a", "a", "c"),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 2L), ("c", 2L)),
        StopStream
      )
    }
  }

  test("expires TTL state while the RTM batch remains open") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_TRANSFORM_WITH_STATE_REAL_TIME_MODE_TTL_EVICTION_INTERVAL_MS.key -> "1") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeTTLCountProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L)),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(10001L)
        },
        // Processing another key runs the periodic TTL cleanup without closing the RTM batch.
        AddData(input, ("b", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L), ("b", 1L)),
        AddData(input, ("a", 2)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 1L), ("b", 1L), ("a", 1L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { q =>
          val batch0 = q.recentProgress.find(_.batchId == 0).getOrElse {
            fail(s"batch 0 progress was not retained: ${q.recentProgress.toSeq}")
          }
          assert(batch0.stateOperators.length == 1)
          assert(batch0.stateOperators.head.customMetrics
            .get("numValuesRemovedDueToTTLExpiry") > 0L)
        },
        StopStream
      )
    }
  }

  test("cleans up expired TTL state at the end of an RTM batch") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_TRANSFORM_WITH_STATE_REAL_TIME_MODE_TTL_EVICTION_INTERVAL_MS.key ->
          "86400000") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeTTLCountProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L)),
        advanceClock(clock, defaultTrigger.batchDurationMs + 10001L),
        WaitUntilBatchProcessed(0),
        Execute { q =>
          val batch0 = q.recentProgress.find(_.batchId == 0).getOrElse {
            fail(s"batch 0 progress was not retained: ${q.recentProgress.toSeq}")
          }
          assert(batch0.stateOperators.length == 1)
          assert(batch0.stateOperators.head.numRowsTotal == 0L)
          assert(batch0.stateOperators.head.customMetrics
            .get("numValuesRemovedDueToTTLExpiry") == 1L)
        },
        StopStream
      )
    }
  }

  test("expires value, map, and list state while the RTM batch remains open") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_TRANSFORM_WITH_STATE_REAL_TIME_MODE_TTL_EVICTION_INTERVAL_MS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream[String](1)
      val result = input.toDS()
        .groupByKey(identity)
        .transformWithState(
          new MultiStatefulVariableTTLProcessor(TTLConfig(Duration.ofSeconds(10))),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, "a", "b"),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L), ("b", 1L)),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(10001L)
        },
        AddData(input, "c"),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 1L), ("b", 1L), ("c", 1L)),
        AddData(input, "a"),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 1L), ("b", 1L), ("c", 1L), ("a", 1L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { q =>
          val batch0 = q.recentProgress.find(_.batchId == 0).getOrElse {
            fail(s"batch 0 progress was not retained: ${q.recentProgress.toSeq}")
          }
          assert(batch0.stateOperators.length == 1)
          assert(batch0.stateOperators.head.customMetrics
            .get("numValuesRemovedDueToTTLExpiry") >= 6L)
        },
        StopStream
      )
    }
  }

  test("hides expired TTL state before the periodic cleanup scan") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_TRANSFORM_WITH_STATE_REAL_TIME_MODE_TTL_EVICTION_INTERVAL_MS.key ->
          "86400000") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeTTLCountProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L)),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(10001L)
        },
        AddData(input, ("a", 2)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L), ("a", 1L)),
        StopStream
      )
    }
  }

  test("cleans up every entry in an expired ListState") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_TRANSFORM_WITH_STATE_REAL_TIME_MODE_TTL_EVICTION_INTERVAL_MS.key -> "1") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeListTTLProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1), ("b", 10)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 3L), ("b", 3L)),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(10001L)
        },
        AddData(input, ("c", 20)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 3L), ("b", 3L), ("c", 3L)),
        AddData(input, ("a", 30)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 3L), ("b", 3L), ("c", 3L), ("a", 3L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { q =>
          val batch0 = q.recentProgress.find(_.batchId == 0).getOrElse {
            fail(s"batch 0 progress was not retained: ${q.recentProgress.toSeq}")
          }
          assert(batch0.stateOperators.length == 1)
          assert(batch0.stateOperators.head.customMetrics
            .get("numValuesRemovedDueToTTLExpiry") >= 6L)
        },
        StopStream
      )
    }
  }

  test("applies a shorter TTL after an RTM checkpoint restart") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_TRANSFORM_WITH_STATE_REAL_TIME_MODE_TTL_EVICTION_INTERVAL_MS.key -> "1") {
      withTempDir { checkpointDir =>
        val (input, clock) = createMemoryStream(numPartitions = 1)
        val checkpoint = checkpointDir.getCanonicalPath
        val original = input.toDS()
          .groupByKey(_._1)
          .transformWithState(
            new RealTimeTTLCountProcessor(Duration.ofSeconds(400)),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(original, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint),
          AddData(input, ("a", 1)),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L)),
          advanceClock(clock),
          WaitUntilBatchProcessed(0),
          StopStream
        )

        val reduced = input.toDS()
          .groupByKey(_._1)
          .transformWithState(
            new RealTimeTTLCountProcessor(Duration.ofSeconds(10)),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(reduced, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint),
          AddData(input, ("a", 2)),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 2L)),
          new ExternalAction {
            override def runAction(): Unit = clock.advance(10001L)
          },
          AddData(input, ("b", 1)),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 2L), ("b", 1L)),
          AddData(input, ("a", 3)),
          CheckAnswerWithTimeout(
            60.seconds.toMillis, ("a", 2L), ("b", 1L), ("a", 1L)),
          StopStream
        )
      }
    }
  }

  test("runs transformWithState across multiple state store partitions") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "3") {
      val (input, _) = createMemoryStream(numPartitions = 3)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimePartitionProcessor,
          TimeMode.None(),
          OutputMode.Update())
      val sink = new ContinuousMemorySink()
      val rows = (0 until 64).map(i => (s"key-$i", i))

      testStream(result, OutputMode.Update(), sink = sink)(
        StartStream(),
        AddData(input, rows: _*),
        Execute { _ =>
          eventually(timeout(60.seconds)) {
            assert(sink.allData.size == rows.size)
            assert(sink.allData.map(_.getInt(1)).distinct.size > 1)
          }
        },
        StopStream
      )
    }
  }

  test("runs transformWithState after a union") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val leftInput = LowLatencyMemoryStream[(String, Int)](1)
      val rightInput = LowLatencyMemoryStream[(String, Int)](1)

      val result = leftInput.toDS()
        .union(rightInput.toDS())
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeEagerCountProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(leftInput, ("a", 1)),
        AddData(rightInput, ("a", 2), ("b", 1)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 1L), ("a", 2L), ("b", 1L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { _ => waitForNextBatchToStart() },
        AddData(leftInput, ("a", 3)),
        AddData(rightInput, ("b", 2)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("a", 1L),
          ("a", 2L),
          ("b", 1L),
          ("a", 3L),
          ("b", 2L)),
        Execute { q =>
          val operators = q.lastExecution.executedPlan.collect {
            case transform: TransformWithStateExec => transform
          }
          assert(operators.size == 1, q.lastExecution.executedPlan)
          assert(operators.head.isRealTimeMode)
        },
        StopStream
      )
    }
  }

  test("runs transformWithState after real-time deduplication") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val (input, _) = createMemoryStream(numPartitions = 2)
      val result = input.toDS()
        .dropDuplicates()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeEagerCountProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1), ("b", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L), ("b", 1L)),
        AddData(input, ("a", 1), ("a", 1), ("c", 1)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", 1L), ("b", 1L), ("c", 1L)),
        StopStream
      )
    }
  }

  test("fires processing-time timers from a later row while the RTM batch remains open") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeProcessingTimerProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "data")),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(10001L)
        },
        AddData(input, ("b", 1)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", "data"), ("b", "data"), ("a", "timer")),
        StopStream
      )
    }
  }

  test("reuses one timer iterator task listener across RTM input rows") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val (input, _) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeTimerIteratorListenerProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1), ("a", 2), ("a", 3)),
        // The first timer scan adds the reusable iterator's completion listener. Later scans
        // refresh that iterator and must not add another listener.
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          (1, false),
          (2, true),
          (3, false)),
        StopStream
      )
    }
  }

  test("reuses one TTL iterator task listener across RTM cleanup scans") {
    val changelogKey =
      s"${RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX}.changelogCheckpointing.enabled"
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STREAMING_TRANSFORM_WITH_STATE_REAL_TIME_MODE_TTL_EVICTION_INTERVAL_MS.key -> "0",
        changelogKey -> "true") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeTTLIteratorListenerProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, (1, 0)),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(1L)
        },
        AddData(input, ("a", 2)),
        // The first timer and TTL scans each add one reusable iterator listener.
        CheckAnswerWithTimeout(60.seconds.toMillis, (1, 0), (2, 2)),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(1L)
        },
        AddData(input, ("a", 3)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, (1, 0), (2, 2), (3, 0)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        StopStream
      )
    }
  }

  test("expired timer receives the current RTM processing time") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      GlobalSingletonManualClock.reset()
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeProcessingTimerValueProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "data", 0L)),
        new ExternalAction {
          override def runAction(): Unit = clock.advance(10001L)
        },
        AddData(input, ("b", 1)),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("a", "data", 0L),
          ("b", "data", 10001L),
          ("a", "timer", 10001L)),
        StopStream
      )
    }
  }

  test("fires remaining processing-time timers at the end of an RTM batch") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeProcessingTimerProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, ("a", 1)),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "data")),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", "data"), ("a", "timer")),
        StopStream
      )
    }
  }

  test("processing time timers expire after multiple batches") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val (input, clock) = createStringMemoryStream()

      val processor = new RTMStatefulProcessorWithProcTimeTimer(
        defaultTrigger.batchDurationMs * 3)

      val result = input
        .toDS()
        .groupByKey(x => x)
        .transformWithState(processor, TimeMode.ProcessingTime(), OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, "a"),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "1")),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        // In real time mode, batches execute for a fixed amount of time. Wait for the next
        // batch's tasks before advancing a manual clock again to avoid skipping its end time.
        Execute { _ => waitForNextBatchToStart() },
        advanceClock(clock),
        WaitUntilBatchProcessed(1),
        Execute { _ => waitForNextBatchToStart() },
        advanceClock(clock),
        WaitUntilBatchProcessed(2),
        Execute { _ => waitForNextBatchToStart() },
        advanceClock(clock, 1L),
        AddData(input, "a"),
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("a", "1"), ("a", "1"), ("a", "-1")),
        StopStream
      )
    }
  }

  test("processing time timers single key multiple registered timers") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val (input, clock) = createStringMemoryStream()

      val processor = new RTMStatefulProcessorWithProcTimeTimerWithMultipleTimers(30000)

      val result = input
        .toDS()
        .groupByKey(x => x)
        .transformWithState(processor, TimeMode.ProcessingTime(), OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, "a"),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "1")),
        advanceClock(clock, 31001L),
        AddData(input, "a"),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("a", "1"), ("a", "2"), ("a", "-1"), ("a", "-1")),
        StopStream
      )
    }
  }

  test("processing time timers with timers from multiple keys") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val (input, clock) = createStringMemoryStream()

      val processor = new RTMStatefulProcessorWithProcTimeTimer(30000)

      val result = input
        .toDS()
        .groupByKey(x => x)
        .transformWithState(processor, TimeMode.ProcessingTime(), OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, "a"),
        AddData(input, "b"),
        AddData(input, "c"),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "1"), ("b", "1"), ("c", "1")),
        advanceClock(clock, 30001L),
        AddData(input, "a"),
        AddData(input, "b"),
        AddData(input, "c"),
        CheckAnswerRowsContainsWithTimeout(
          60.seconds.toMillis,
          ("a", "1"),
          ("b", "1"),
          ("c", "1"),
          ("a", "-1"),
          ("b", "2"),
          ("c", "-1")
        ),
        StopStream
      )
    }
  }

  test("processing time timers with an integer key") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream[Int](2)
      val result = input.toDS()
        .groupByKey(identity)
        .transformWithState(
          new RTMStatefulProcessorWithProcTimeTimerInputInt(30000L),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, 1),
        CheckAnswerWithTimeout(60.seconds.toMillis, (1, 1L)),
        advanceClock(clock, 30001L),
        AddData(input, 1),
        CheckAnswerRowsContainsWithTimeout(60.seconds.toMillis, (1, 1L), (1, -1L)),
        StopStream
      )
    }
  }

  test("processing time timers with a product key") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream[(Int, String)](2)
      val result = input.toDS()
        .groupByKey(identity)
        .transformWithState(
          new RTMStatefulProcessorWithProcTimeTimerInputTuple(30000L),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, (1, "a")),
        CheckAnswerWithTimeout(60.seconds.toMillis, (1, "a", 1L)),
        advanceClock(clock, 30001L),
        AddData(input, (1, "a")),
        CheckAnswerRowsContainsWithTimeout(60.seconds.toMillis, (1, "a", -1L)),
        StopStream
      )
    }
  }

  test("processing time timers survive an RTM checkpoint restart") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpointDir =>
        val (input, clock) = createStringMemoryStream()
        val processor = new RTMStatefulProcessorWithProcTimeTimer(
          defaultTrigger.batchDurationMs + 1000L)
        val result = input
          .toDS()
          .groupByKey(x => x)
          .transformWithState(processor, TimeMode.ProcessingTime(), OutputMode.Update())
        val checkpoint = checkpointDir.getCanonicalPath

        testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint),
          AddData(input, "a"),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "1")),
          advanceClock(clock),
          WaitUntilBatchProcessed(0),
          StopStream
        )

        testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint),
          Execute { _ => waitForNextBatchToStart() },
          advanceClock(clock, 1001L),
          AddData(input, "b"),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "-1"), ("b", "1")),
          StopStream
        )
      }
    }
  }

  test("uses fixed between-batch watermarks for incremental event-time timers") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream[(Timestamp, String)](1)
      val result = input.toDF()
        .select(col("_1").as("eventTime"), col("_2").as("key"))
        .withWatermark("eventTime", "10 milliseconds")
        .as[(Timestamp, String)]
        .groupByKey(_._2)
        .transformWithState(
          new RealTimeEventTimerProcessor,
          TimeMode.EventTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input,
          (new Timestamp(100L), "a"),
          (new Timestamp(200L), "a"),
          (new Timestamp(150L), "a"),
          (new Timestamp(300L), "a")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { _ => waitForNextBatchToStart() },
        // Batch 1 uses batch 0's fixed eviction watermark (300 - 10 = 290). The first
        // input row scans timers already below it without waiting for the batch to end.
        AddData(input,
          (new Timestamp(280L), "a"),
          (new Timestamp(400L), "a")),
        // The watermark does not move inside batch 1. A newly registered timer at 280 fires
        // immediately against the same fixed 290 watermark.
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "data", 290L),
          ("a", "timer", 290L),
          ("a", "data", 290L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(1),
        Execute { _ => waitForNextBatchToStart() },
        // Batch 2 uses eviction watermark 390 and late-events watermark 290. The late 280 row
        // still triggers the per-row timer scan even though it is not passed to the processor.
        AddData(input, (new Timestamp(280L), "a")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "data", 290L),
          ("a", "data", 290L),
          ("a", "timer", 390L)),
        // The watermark stays fixed while the accepted row is processed in the same batch.
        AddData(input, (new Timestamp(500L), "a")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "data", 0L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "timer", 290L),
          ("a", "data", 290L),
          ("a", "data", 290L),
          ("a", "timer", 390L),
          ("a", "data", 390L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(2),
        Execute { q =>
          val batch2 = q.recentProgress.find(_.batchId == 2).getOrElse {
            fail(s"batch 2 progress was not retained: ${q.recentProgress.toSeq}")
          }
          assert(batch2.stateOperators.length == 1)
          assert(batch2.stateOperators.head.numRowsDroppedByWatermark == 1L)
        },
        StopStream
      )
    }
  }

  test("fires a newly expired event-time timer from its RTM input row") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream[(Timestamp, String)](1)
      val result = input.toDF()
        .select(col("_1").as("eventTime"), col("_2").as("key"))
        .withWatermark("eventTime", "10 milliseconds")
        .as[(Timestamp, String)]
        .groupByKey(_._2)
        .transformWithState(
          new RealTimeEventTimerProcessor,
          TimeMode.EventTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, (new Timestamp(300L), "a")),
        CheckAnswerWithTimeout(10.seconds.toMillis, ("a", "data", 0L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { _ => waitForNextBatchToStart() },
        // Batch 1's fixed eviction watermark is 290, so the timer registered for this row is
        // scanned immediately after the row is processed.
        AddData(input, (new Timestamp(280L), "a")),
        CheckAnswerWithTimeout(
          10.seconds.toMillis,
          ("a", "data", 0L),
          ("a", "data", 290L),
          ("a", "timer", 290L)),
        StopStream
      )
    }
  }

  test("fires event-time timers in the final scan of an empty RTM batch") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream[(Timestamp, String)](1)
      val result = input.toDF()
        .select(col("_1").as("eventTime"), col("_2").as("key"))
        .withWatermark("eventTime", "10 milliseconds")
        .as[(Timestamp, String)]
        .groupByKey(_._2)
        .transformWithState(
          new RealTimeEventTimerProcessor,
          TimeMode.EventTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input,
          (new Timestamp(100L), "expired"),
          (new Timestamp(300L), "watermark")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("expired", "data", 0L),
          ("watermark", "data", 0L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { _ => waitForNextBatchToStart() },
        // Batch 1 has no input rows. Its final scan uses batch 0's fixed eviction watermark.
        advanceClock(clock),
        WaitUntilBatchProcessed(1),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          ("expired", "data", 0L),
          ("watermark", "data", 0L),
          ("expired", "timer", 290L)),
        Execute { q =>
          val batch1 = q.recentProgress.find(_.batchId == 1).getOrElse {
            fail(s"batch 1 progress was not retained: ${q.recentProgress.toSeq}")
          }
          assert(batch1.numInputRows == 0L)
        },
        StopStream
      )
    }
  }

  test("recovers event-time timers and fires them from the next row after restart") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTempDir { checkpointDir =>
        val clock = new GlobalSingletonManualClock()
        LowLatencyClock.setClock(clock)
        val input = LowLatencyMemoryStream[(Timestamp, String)](1)
        val result = input.toDF()
          .select(col("_1").as("eventTime"), col("_2").as("key"))
          .withWatermark("eventTime", "10 milliseconds")
          .as[(Timestamp, String)]
          .groupByKey(_._2)
          .transformWithState(
            new RealTimeEventTimerProcessor,
            TimeMode.EventTime(),
            OutputMode.Update())
        val checkpoint = checkpointDir.getCanonicalPath

        testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint),
          AddData(input,
            (new Timestamp(100L), "expired"),
            (new Timestamp(300L), "watermark")),
          CheckAnswerWithTimeout(
            60.seconds.toMillis,
            ("expired", "data", 0L),
            ("watermark", "data", 0L)),
          advanceClock(clock),
          WaitUntilBatchProcessed(0),
          StopStream
        )

        testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(checkpointLocation = checkpoint),
          Execute { _ => waitForNextBatchToStart() },
          // The recovered batch watermark is 300 - 10 = 290. The next input row scans the
          // recovered timers against that fixed watermark.
          AddData(input, (new Timestamp(400L), "trigger")),
          CheckAnswerWithTimeout(
            60.seconds.toMillis,
            ("trigger", "data", 290L),
            ("expired", "timer", 290L)),
          StopStream
        )
      }
    }
  }

  test("supports an output event-time column in RTM") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream.singlePartition[(Timestamp, String)]
      val result = input.toDF()
        .select(col("_1").as("eventTime"), col("_2").as("key"))
        .withWatermark("eventTime", "1 minute")
        .as[(Timestamp, String)]
        .groupByKey(_._2)
        .transformWithState[RealTimeEventTimeOutputRow](
          new RealTimeEventTimeOutputProcessor,
          "outputEventTime",
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input,
          (new Timestamp(1000000L), "a"),
          (new Timestamp(2000000L), "a")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          RealTimeEventTimeOutputRow("a", new Timestamp(1000000L), 1L),
          RealTimeEventTimeOutputRow("a", new Timestamp(2000000L), 2L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { _ => waitForNextBatchToStart() },
        AddData(input, (new Timestamp(3000000L), "a")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          RealTimeEventTimeOutputRow("a", new Timestamp(1000000L), 1L),
          RealTimeEventTimeOutputRow("a", new Timestamp(2000000L), 2L),
          RealTimeEventTimeOutputRow("a", new Timestamp(3000000L), 3L)),
        StopStream
      )
    }
  }

  test("uses the fixed prior-batch watermark for output event-time validation") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream.singlePartition[(Timestamp, String)]
      val result = input.toDF()
        .select(col("_1").as("eventTime"), col("_2").as("key"))
        .withWatermark("eventTime", "10 milliseconds")
        .as[(Timestamp, String)]
        .groupByKey(_._2)
        .transformWithState[RealTimeEventTimeOutputRow](
          new RealTimeEventTimeOutputProcessor(Some(new Timestamp(1L))),
          "outputEventTime",
          OutputMode.Update())

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(),
        AddData(input, (new Timestamp(300L), "a")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          RealTimeEventTimeOutputRow("a", new Timestamp(1L), 1L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(0),
        Execute { _ => waitForNextBatchToStart() },
        AddData(input, (new Timestamp(400L), "a")),
        CheckAnswerWithTimeout(
          60.seconds.toMillis,
          RealTimeEventTimeOutputRow("a", new Timestamp(1L), 1L),
          RealTimeEventTimeOutputRow("a", new Timestamp(1L), 2L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(1),
        Execute { _ => waitForNextBatchToStart() },
        // Batch 2's late-events watermark is fixed at 290. Its accepted input would emit an
        // output timestamp of 1, which must be rejected against that same fixed watermark.
        AddData(input, (new Timestamp(500L), "a")),
        ExpectFailure[SparkRuntimeException] { error =>
          checkError(
            error.asInstanceOf[SparkThrowable],
            "EMITTING_ROWS_OLDER_THAN_WATERMARK_NOT_ALLOWED",
            parameters = Map(
              "currentWatermark" -> "290",
              "emittedRowEventTime" -> "1000"))
        }
      )
    }
  }

  test("processing-time timer registered from initial state uses the batch timestamp") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      GlobalSingletonManualClock.reset()
      val (input, executorClock) = createStringMemoryStream(numPartitions = 1)
      val driverClock = new StreamManualClock(100000L)
      val result = input.toDS()
        .groupByKey(identity)
        .transformWithState(
          new RealTimeInitialStateProcTimerWithExpiryProcessor,
          TimeMode.ProcessingTime(),
          OutputMode.Update(),
          Seq("a").toDS().groupByKey(identity))

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        StartStream(triggerClock = driverClock),
        WaitUntilBatchProcessed(0),
        Execute { _ => waitForNextBatchToStart() },
        // The initial-state timer is based on batch 0's driver timestamp (100000), not the
        // executor clock (0), so advancing the executor by only the timer delay must not fire it.
        advanceClock(executorClock, 5001L),
        Execute { _ => input.addData(Seq("b")) },
        CheckAnswerWithTimeout(60.seconds.toMillis, ("b", "1")),
        advanceClock(executorClock, 100000L),
        Execute { _ => input.addData(Seq("c")) },
        CheckAnswerWithTimeout(
          60.seconds.toMillis, ("b", "1"), ("c", "1"), ("a", "105000")),
        StopStream
      )
    }
  }

  test("initial-state bootstrap batch does not use pipelined shuffle") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      val (input, _) = createMemoryStream(numPartitions = 2)
      val result = transformWithInitialState(input, Seq("a" -> 2L, "b" -> 5L))

      RealTimeInitialStateBootstrapBlock.enable()
      try {
        testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
          StartStream(),
          Execute { q =>
            try {
              assert(RealTimeInitialStateBootstrapBlock.awaitTaskStart())
              val execution = q.lastExecution
              assert(execution.currentBatchId == 0L)
              val plan = execution.executedPlan
              val scans = plan.collect { case scan: RealTimeStreamScanExec => scan }
              assert(scans.nonEmpty)
              assert(scans.forall(_.batchDurationMs == 0L))

              val shuffles = plan.collect { case exchange: ShuffleExchangeExec => exchange }
              assert(shuffles.nonEmpty)
              assert(shuffles.forall(!_.pipelined))
            } finally {
              RealTimeInitialStateBootstrapBlock.disable()
            }
          },
          WaitUntilBatchProcessed(0),
          StopStream
        )
      } finally {
        RealTimeInitialStateBootstrapBlock.disable()
      }
    }
  }

  Seq(
    "non-empty" -> Seq("a" -> 2L, "b" -> 5L),
    "empty" -> Seq.empty[(String, Long)]
  ).foreach { case (description, initialValues) =>
    test(s"hydrates $description initial state in a finite batch and recovers it") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        withTempDir { checkpointDir =>
          val (input, clock) = createMemoryStream(numPartitions = 2)
          val result = transformWithInitialState(input, initialValues)
          val initialCount = initialValues.toMap.getOrElse("a", 0L)

          testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
            // Input available when the query starts must wait until initial state is durable.
            AddData(input, ("a", 1)),
            StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
            WaitUntilBatchProcessed(0),
            Execute { q =>
              val batch0 = q.recentProgress.find(_.batchId == 0).getOrElse {
                fail(s"batch 0 progress was not retained: ${q.recentProgress.toSeq}")
              }
              assert(batch0.numInputRows == 0)
              assert(batch0.stateOperators.length == 1)
              assert(batch0.stateOperators.head.numRowsTotal == initialValues.size)
            },
            CheckAnswerWithTimeout(60.seconds.toMillis, ("a", initialCount + 1L)),
            Execute { q =>
              val plan = q.lastExecution.executedPlan
              val scans = plan.collect { case scan: RealTimeStreamScanExec => scan }
              assert(scans.nonEmpty)
              assert(scans.forall(_.batchDurationMs == defaultTrigger.batchDurationMs))
              val streamingShuffles = plan.collect {
                case exchange: ShuffleExchangeExec if exchange.exists {
                    case _: RealTimeStreamScanExec => true
                    case _ => false
                  } => exchange
              }
              assert(streamingShuffles.nonEmpty)
              assert(streamingShuffles.forall(_.pipelined))
            },
            advanceClock(clock),
            WaitUntilBatchProcessed(1),
            StopStream
          )

          testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
            AddData(input, ("a", 2)),
            CheckAnswerWithTimeout(60.seconds.toMillis, ("a", initialCount + 2L)),
            advanceClock(clock),
            WaitUntilBatchProcessed(2),
            StopStream
          )
        }
      }
    }
  }

  test("hydrates event-time initial state before the first RTM input batch") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val clock = new GlobalSingletonManualClock()
      LowLatencyClock.setClock(clock)
      val input = LowLatencyMemoryStream[(Timestamp, String)](1)
      val result = input.toDF()
        .select(col("_1").as("eventTime"), col("_2").as("key"))
        .withWatermark("eventTime", "10 milliseconds")
        .as[(Timestamp, String)]
        .groupByKey(_._2)
        .transformWithState(
          new RealTimeEventInitialCountProcessor,
          TimeMode.EventTime(),
          OutputMode.Update(),
          initialState(Seq("a" -> 2L)))

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        AddData(input, (new Timestamp(100L), "a")),
        StartStream(),
        WaitUntilBatchProcessed(0),
        Execute { q =>
          val batch0 = q.recentProgress.find(_.batchId == 0).getOrElse {
            fail(s"batch 0 progress was not retained: ${q.recentProgress.toSeq}")
          }
          assert(batch0.numInputRows == 0)
          assert(batch0.stateOperators.length == 1)
          assert(batch0.stateOperators.head.numRowsTotal == 1)
        },
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 3L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(1),
        StopStream
      )
    }
  }

  test("processes non-contiguous duplicate initial-state keys without sorting") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val (input, clock) = createMemoryStream(numPartitions = 1)
      // A single input partition preserves a, b, a through the one-partition hash exchange.
      val duplicateInitialState = Seq("a" -> 1L, "b" -> 5L, "a" -> 2L)
        .toDS()
        .coalesce(1)
        .groupByKey(_._1)
        .mapValues(_._2)
      val result = input.toDS()
        .groupByKey(_._1)
        .transformWithState(
          new RealTimeInitialCountProcessor,
          TimeMode.None(),
          OutputMode.Update(),
          duplicateInitialState)

      testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
        AddData(input, ("a", 1)),
        StartStream(),
        WaitUntilBatchProcessed(0),
        CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 4L)),
        advanceClock(clock),
        WaitUntilBatchProcessed(1),
        StopStream
      )
    }
  }

  test("retries initial-state bootstrap from batch 0 after failure") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
      withTempDir { checkpointDir =>
        val (input, clock) = createMemoryStream(numPartitions = 2)
        val result = transformWithInitialState(input, Seq("a" -> 2L, "b" -> 5L))
        val offsetFile = new java.io.File(checkpointDir, "offsets/0")

        try {
          RealTimeInitialStateFailure.enabled = true
          testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
            ExpectFailure[SparkException] { error =>
              assert(error.getMessage.contains("injected initial-state bootstrap failure"))
            }
          )
          assert(!offsetFile.exists())

          RealTimeInitialStateFailure.enabled = false
          testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
            AddData(input, ("a", 1)),
            StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
            WaitUntilBatchProcessed(0),
            CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 3L)),
            advanceClock(clock),
            WaitUntilBatchProcessed(1),
            StopStream
          )
        } finally {
          RealTimeInitialStateFailure.enabled = false
        }
      }
    }
  }

  Seq(true, false).foreach { changelogCheckpointingEnabled =>
    test(s"recovers transformWithState state with RocksDB changelog checkpointing " +
        s"enabled=$changelogCheckpointingEnabled") {
      val changelogKey =
        s"${RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX}.changelogCheckpointing.enabled"
      withSQLConf(
          SQLConf.SHUFFLE_PARTITIONS.key -> "2",
          changelogKey -> changelogCheckpointingEnabled.toString) {
        withTempDir { checkpointDir =>
          val (input, clock) = createMemoryStream(numPartitions = 2)
          val result = input.toDS()
            .groupByKey(_._1)
            .transformWithState(
              new RealTimeEagerCountProcessor,
              TimeMode.ProcessingTime(),
              OutputMode.Update())

          testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
            AddData(input, ("a", 1), ("a", 2)),
            CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 1L), ("a", 2L)),
            Execute { q =>
              assert(q.sparkSessionForStream.conf.get(changelogKey) ==
                changelogCheckpointingEnabled.toString)
            },
            advanceClock(clock),
            WaitUntilBatchProcessed(0),
            StopStream
          )

          testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
            StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
            AddData(input, ("a", 3), ("b", 1)),
            CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 3L), ("b", 1L)),
            advanceClock(clock),
            WaitUntilBatchProcessed(1),
            StopStream
          )
        }
      }
    }
  }

  test("keeps transformWithState state across MBM to RTM to MBM restarts") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      withTempDir { checkpointDir =>
        val (input, clock) = createMemoryStream(numPartitions = 1)
        val result = input.toDS()
          .groupByKey(_._1)
          .transformWithState(
            new RealTimeEagerCountProcessor,
            TimeMode.None(),
            OutputMode.Update())
        val checkpoint = checkpointDir.getCanonicalPath

        testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
          AddData(input, ("a", 1), ("a", 2)),
          StartStream(
            trigger = Trigger.ProcessingTime(1000),
            checkpointLocation = checkpoint),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", 2L)),
          WaitUntilBatchProcessed(0),
          Execute { q =>
            val operators = q.lastExecution.executedPlan.collect {
              case transform: TransformWithStateExec => transform
            }
            assert(operators.size == 1)
            assert(!operators.head.isRealTimeMode)
          },
          StopStream,
          StartStream(trigger = defaultTrigger, checkpointLocation = checkpoint),
          AddData(input, ("a", 3), ("b", 1)),
          CheckAnswerWithTimeout(
            60.seconds.toMillis, ("a", 2L), ("a", 3L), ("b", 1L)),
          Execute { q =>
            val operators = q.lastExecution.executedPlan.collect {
              case transform: TransformWithStateExec => transform
            }
            assert(operators.size == 1)
            assert(operators.head.isRealTimeMode)
          },
          advanceClock(clock),
          WaitUntilBatchProcessed(1),
          StopStream,
          AddData(input, ("a", 4), ("b", 2)),
          StartStream(
            trigger = Trigger.ProcessingTime(1000),
            checkpointLocation = checkpoint),
          CheckAnswerWithTimeout(
            60.seconds.toMillis,
            ("a", 2L), ("a", 3L), ("b", 1L), ("a", 4L), ("b", 2L)),
          WaitUntilBatchProcessed(2),
          Execute { q =>
            val operators = q.lastExecution.executedPlan.collect {
              case transform: TransformWithStateExec => transform
            }
            assert(operators.size == 1)
            assert(!operators.head.isRealTimeMode)
          },
          StopStream
        )
      }
    }
  }

  test("keeps MapState TTL and processing-time timers across MBM to RTM to MBM restarts") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "1",
        SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key -> "2") {
      withTempDir { checkpointDir =>
        GlobalSingletonManualClock.reset()
        val (input, clock) = createMemoryStream(numPartitions = 1)
        val result = input.toDS()
          .groupByKey(_._1)
          .transformWithState(
            new RealTimeMapTTLAndTimerProcessor,
            TimeMode.ProcessingTime(),
            OutputMode.Update())
        val checkpoint = checkpointDir.getCanonicalPath
        val timerDelayAcrossRtmBoundary = (defaultTrigger.batchDurationMs + 1000L).toInt

        testStream(result, OutputMode.Update(), sink = new ContinuousMemorySink())(
          AddData(input, ("a", 10000)),
          StartStream(
            trigger = Trigger.ProcessingTime(1000),
            triggerClock = clock,
            checkpointLocation = checkpoint),
          CheckAnswerWithTimeout(60.seconds.toMillis, ("a", "data", 1L)),
          WaitUntilBatchProcessed(0),
          StopStream,
          StartStream(trigger = defaultTrigger, checkpointLocation = checkpoint),
          Execute { _ => waitForNextBatchToStart() },
          advanceClock(clock, 10001L),
          AddData(input, ("a", timerDelayAcrossRtmBoundary)),
          CheckAnswerWithTimeout(
            60.seconds.toMillis,
            ("a", "data", 1L),
            ("a", "data", 2L),
            ("a", "timer", 10000L)),
          advanceClock(clock),
          WaitUntilBatchProcessed(1),
          StopStream,
          advanceClock(clock, 1001L),
          AddData(input, ("a", 10000)),
          StartStream(
            trigger = Trigger.ProcessingTime(1000),
            triggerClock = clock,
            checkpointLocation = checkpoint),
          CheckAnswerWithTimeout(
            60.seconds.toMillis,
            ("a", "data", 1L),
            ("a", "data", 2L),
            ("a", "timer", 10000L),
            ("a", "data", 3L),
            ("a", "timer", 311001L)),
          WaitUntilBatchProcessed(2),
          StopStream,
          advanceClock(clock, Duration.ofMinutes(10).toMillis + 1L),
          AddData(input, ("a", 10000)),
          StartStream(
            trigger = Trigger.ProcessingTime(1000),
            triggerClock = clock,
            checkpointLocation = checkpoint),
          CheckAnswerWithTimeout(
            60.seconds.toMillis,
            ("a", "data", 1L),
            ("a", "data", 2L),
            ("a", "timer", 10000L),
            ("a", "data", 3L),
            ("a", "timer", 311001L),
            ("a", "data", 1L),
            ("a", "timer", 321002L)),
          WaitUntilBatchProcessed(3),
          StopStream
        )
      }
    }
  }
}

@SlowSQLTest
class RealTimeTransformWithStateSuiteWithRowChecksum
  extends RealTimeTransformWithStateSuite with EnableStateStoreRowChecksum

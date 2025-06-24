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

import java.io.File
import java.time.Duration
import java.util.UUID

import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{SparkException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.StreamingCheckpointConstants.DIR_NAME_OFFSETS
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types._

object TransformWithStateSuiteUtils {
  val NUM_SHUFFLE_PARTITIONS = 5
}

case class TwoLongs(
    value1: Long,
    value2: Long
)

case class ReorderedLongs(
    value2: Long,
    value1: Long
)

case class RenamedFields(
    value4: Long,
    value2: Long
)

// Initial state with basic fields
case class BasicState(
    id: Int,
    name: String
)

// Evolved state with just primitive types
case class EvolvedState(
    id: Int,
    name: String,
    count: Long,             // Should default to 0
    active: Boolean,         // Should default to false
    score: Double           // Should default to 0.0
)

class ClassifiedTimerErrorProcessor
  extends StatefulProcessor[String, String, (String, BasicState)] {

  @transient var state: ValueState[BasicState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[BasicState](
      "testState",
      Encoders.product[BasicState],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, BasicState)] = {
    getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 1000)
    Seq.empty.iterator
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, BasicState)] = {
    throw StateStoreErrors.multipleColumnFamiliesNotSupported("dummy val")
  }
}

class UnclassifiedTimerErrorProcessor
  extends StatefulProcessor[String, String, (String, BasicState)] {

  @transient var state: ValueState[BasicState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[BasicState](
      "testState",
      Encoders.product[BasicState],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, BasicState)] = {
    getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 1000)
    Seq.empty.iterator
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, BasicState)] = {
    throw new IllegalStateException("dummy unclassified error")
    Seq.empty.iterator
  }
}

class ClassifiedErrorInitialStateProcessor
  extends StatefulProcessorWithInitialState[String, String, (String, BasicState), String] {

  @transient var state: ValueState[BasicState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[BasicState](
      "testState",
      Encoders.product[BasicState],
      TTLConfig.NONE)
  }

  override def handleInitialState(
      key: String,
      initialState: String,
      timerValues: TimerValues): Unit = {
    throw StateStoreErrors.multipleColumnFamiliesNotSupported("dummy val")
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, BasicState)] = {
    Seq.empty.iterator
  }
}

class UnclassifiedErrorInitialStateProcessor
  extends StatefulProcessorWithInitialState[String, String, (String, BasicState), String] {

  @transient var state: ValueState[BasicState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[BasicState](
      "testState",
      Encoders.product[BasicState],
      TTLConfig.NONE)
  }

  override def handleInitialState(
      key: String,
      initialState: String,
      timerValues: TimerValues): Unit = {
    throw new IllegalStateException("dummy unclassified error")
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, BasicState)] = {
    Seq.empty.iterator
  }
}

// Processor with initial schema
class DefaultValueInitialProcessor
  extends StatefulProcessor[String, String, (String, BasicState)] {

  @transient var state: ValueState[BasicState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[BasicState](
      "testState",
      Encoders.product[BasicState],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, BasicState)] = {

    rows.map { value =>
      val stateValue = BasicState(value.hashCode, value)
      state.update(stateValue)
      (key, stateValue)
    }
  }
}

class UnclassifiedErrorProcessor
  extends StatefulProcessor[String, String, (String, BasicState)] {

  @transient var state: ValueState[BasicState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[BasicState](
      "testState",
      Encoders.product[BasicState],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, BasicState)] = {
    throw new IllegalStateException("dummy unclassified error")
    Seq.empty.iterator
  }
}

class ClassifiedErrorProcessor
  extends StatefulProcessor[String, String, (String, BasicState)] {

  @transient var state: ValueState[BasicState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[BasicState](
      "testState",
      Encoders.product[BasicState],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, BasicState)] = {
    throw StateStoreErrors.multipleColumnFamiliesNotSupported("dummy val")
  }
}

// Evolved processor with additional primitive fields
class DefaultValueEvolvedProcessor
  extends StatefulProcessor[String, String, (String, EvolvedState)] {

  @transient var state: ValueState[EvolvedState] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[EvolvedState](
      "testState",
      Encoders.product[EvolvedState],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, EvolvedState)] = {

    rows.map { value =>
      val current = Option(state.get()).getOrElse {
        // If no state exists, create new state
        EvolvedState(
          value.hashCode, value, 100L, true, 99.9
        )
      }
      (key, current)
    }
  }
}

// First schema - String field
case class StateV1(value1: String)
class ProcessorV1 extends StatefulProcessor[String, String, String] {
  @transient var state: ValueState[StateV1] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[StateV1](
      "testState",
      Encoders.product[StateV1],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[String] = {
    rows.map { value =>
      state.update(StateV1(value))
      value
    }
  }
}

// Second schema - Long field
case class StateV2(value2: Long)
class ProcessorV2 extends StatefulProcessor[String, String, String] {
  @transient var state: ValueState[StateV2] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[StateV2](
      "testState",
      Encoders.product[StateV2],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[String] = {
    rows.map { value =>
      state.update(StateV2(value.length))
      value
    }
  }
}

// Third schema - Int field (incompatible with previous Long)
case class StateV3(value1: Int)
class ProcessorV3 extends StatefulProcessor[String, String, String] {
  @transient var state: ValueState[StateV3] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    state = getHandle.getValueState[StateV3](
      "testState",
      Encoders.product[StateV3],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[String] = {
    rows.map { value =>
      state.update(StateV3(value.length))
      value
    }
  }
}

class RunningCountStatefulProcessorInitialOrder
  extends StatefulProcessor[String, String, (String, String)] with Logging {
  @transient protected var _countState: ValueState[TwoLongs] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[TwoLongs]("countState",
      Encoders.product[TwoLongs], TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(TwoLongs(0L, -1L)).value1 + 1

    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(TwoLongs(count, -1L))
      Iterator((key, count.toString))
    }
  }
}

// Evolved processor with renamed field
class RenameEvolvedProcessor extends StatefulProcessor[String, String, (String, String)] {
  @transient protected var _countState: ValueState[RenamedFields] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[RenamedFields](
      "countState",
      Encoders.product[RenamedFields],
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(RenamedFields(0L, -1L)).value4 + 1

    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(RenamedFields(count, -1L))
      Iterator((key, count.toString))
    }
  }
}

class RunningCountStatefulProcessorReorderedFields
  extends StatefulProcessor[String, String, (String, String)] with Logging {
  @transient protected var _countState: ValueState[ReorderedLongs] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[ReorderedLongs]("countState",
      Encoders.product[ReorderedLongs], TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(ReorderedLongs(-1L, 0L)).value1 + 1
    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      // And update value1 here
      _countState.update(ReorderedLongs(-1L, count))
      Iterator((key, count.toString))
    }
  }
}

class RunningCountStatefulProcessor extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  import implicits._
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(0L) + 1

    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

case class NestedLongs(
    value: Long,
    value2: TwoLongs
)

class RunningCountStatefulProcessorTwoLongs
  extends StatefulProcessor[String, String, (String, String)] with Logging {
  @transient protected var _countState: ValueState[TwoLongs] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[TwoLongs]("countState",
      Encoders.product[TwoLongs], TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(TwoLongs(0L, 0L)).value1 + 1

    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(TwoLongs(count, -1))
      Iterator((key, count.toString))
    }
  }
}

class RunningCountStatefulProcessorNestedLongs
  extends StatefulProcessor[String, String, (String, String)] with Logging {
  @transient protected var _countState: ValueState[NestedLongs] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[NestedLongs]("countState",
      Encoders.product[NestedLongs], TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(
      NestedLongs(0L, TwoLongs(0L, 0L))).value + 1

    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(NestedLongs(count, TwoLongs(0L, 0L)))
      Iterator((key, count.toString))
    }
  }
}


class RunningCountStatefulProcessorWithTTL
  extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  import implicits._
  @transient protected var _countState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState",
      TTLConfig(Duration.ofMillis(1000)))
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(0L) + 1

    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

// Class to test that changing between Value and List State fails
// between query runs
class RunningCountListStatefulProcessor
  extends StatefulProcessor[String, String, (String, String)]
    with Logging {
  @transient protected var _countState: ListState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getListState[Long](
      "countState", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    Iterator.empty
  }
}

class RunningCountStatefulProcessorInt
  extends StatefulProcessor[String, String, (String, String)] {
  @transient protected var _countState: ValueState[Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Int]("countState", Encoders.scalaInt,
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = Option(_countState.get()).getOrElse(0) + 1

    if (count == 3) {
      _countState.clear()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }
}

// Class to verify stateful processor usage with adding processing time timers
class RunningCountStatefulProcessorWithProcTimeTimer extends RunningCountStatefulProcessor {

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = Option(_countState.get()).getOrElse(0L)

    if (currCount == 0 && (key == "a" || key == "c")) {
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs()
        + 5000)
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

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    _countState.clear()
    Iterator((key, "-1"))
  }
}

// Class to verify stateful processor usage with updating processing time timers
class RunningCountStatefulProcessorWithProcTimeTimerUpdates
  extends RunningCountStatefulProcessor {
  @transient private var _timerState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode) : Unit = {
    super.init(outputMode, timeMode)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong,
      TTLConfig.NONE)
  }

  protected def processUnexpiredRows(
      key: String,
      currCount: Long,
      count: Long,
      timerValues: TimerValues): Unit = {
    _countState.update(count)
    if (key == "a") {
      var nextTimerTs: Long = 0L
      if (currCount == 0) {
        nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 5000
        getHandle.registerTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      } else if (currCount == 1) {
        getHandle.deleteTimer(_timerState.get())
        nextTimerTs = timerValues.getCurrentProcessingTimeInMs() + 7500
        getHandle.registerTimer(nextTimerTs)
        _timerState.update(nextTimerTs)
      }
    }
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = Option(_countState.get()).getOrElse(0L)

    val count = currCount + inputRows.size
    processUnexpiredRows(key, currCount, count, timerValues)
    Iterator((key, count.toString))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    _timerState.clear()
    Iterator((key, "-1"))
  }
}

class RunningCountStatefulProcessorWithMultipleTimers
  extends RunningCountStatefulProcessor {

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val currCount = Option(_countState.get()).getOrElse(0L)
    val count = currCount + inputRows.size
    _countState.update(count)
    if (getHandle.listTimers().isEmpty) {
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 5000)
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 10000)
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 15000)
      assert(getHandle.listTimers().size == 3)
    }
    Iterator.empty
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, String)] = {
    val currCount = Option(_countState.get()).getOrElse(0L)
    if (getHandle.listTimers().size == 1) {
      _countState.clear()
    }
    Iterator((key, currCount.toString))
  }
}

class MaxEventTimeStatefulProcessor
  extends StatefulProcessor[String, (String, Long), (String, Int)]
  with Logging {
  @transient var _maxEventTimeState: ValueState[Long] = _
  @transient var _timerState: ValueState[Long] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _maxEventTimeState = getHandle.getValueState[Long]("maxEventTimeState",
      Encoders.scalaLong, TTLConfig.NONE)
    _timerState = getHandle.getValueState[Long]("timerState", Encoders.scalaLong,
      TTLConfig.NONE)
  }

  protected def processUnexpiredRows(maxEventTimeSec: Long): Unit = {
    val timeoutDelaySec = 5
    val timeoutTimestampMs = (maxEventTimeSec + timeoutDelaySec) * 1000
    _maxEventTimeState.update(maxEventTimeSec)

    val registeredTimerMs: Long = Option(_timerState.get()).getOrElse(0L)
    if (registeredTimerMs < timeoutTimestampMs) {
      getHandle.deleteTimer(registeredTimerMs)
      getHandle.registerTimer(timeoutTimestampMs)
      _timerState.update(timeoutTimestampMs)
    }
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Long)],
      timerValues: TimerValues): Iterator[(String, Int)] = {
    val valuesSeq = inputRows.toSeq
    val maxEventTimeSec = math.max(valuesSeq.map(_._2).max,
      Option(_maxEventTimeState.get()).getOrElse(0L))
    processUnexpiredRows(maxEventTimeSec)
    Iterator((key, maxEventTimeSec.toInt))
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[(String, Int)] = {
    _maxEventTimeState.clear()
    Iterator((key, -1))
  }
}

class MinEventTimeStatefulProcessor
  extends StatefulProcessor[String, (String, Long), (String, Int)]
  with Logging {
  @transient var _minEventTimeState: ValueState[Long] = _

  override def init(
    outputMode: OutputMode,
    timeMode: TimeMode): Unit = {
    _minEventTimeState = getHandle.getValueState[Long]("minEventTimeState",
      Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
    key: String,
    inputRows: Iterator[(String, Long)],
    timerValues: TimerValues): Iterator[(String, Int)] = {
    val valuesSeq = inputRows.toSeq
    val minEventTimeSec = math.min(valuesSeq.map(_._2).min,
      Option(_minEventTimeState.get()).getOrElse(Long.MaxValue))
    _minEventTimeState.update(minEventTimeSec)
    Iterator((key, minEventTimeSec.toInt))
  }
}

class RunningCountMostRecentStatefulProcessor
  extends StatefulProcessor[String, (String, String), (String, String, String)]
  with Logging {
  @transient private var _countState: ValueState[Long] = _
  @transient private var _mostRecent: ValueState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong,
      TTLConfig.NONE)
    _mostRecent = getHandle.getValueState[String]("mostRecent", Encoders.STRING,
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[(String, String, String)] = {
    val count = Option(_countState.get()).getOrElse(0L) + 1
    val mostRecent = Option(_mostRecent.get()).getOrElse("")

    var output = List[(String, String, String)]()
    inputRows.foreach { row =>
      _mostRecent.update(row._2)
      _countState.update(count)
      output = (key, count.toString, mostRecent) :: output
    }
    output.iterator
  }
}

class MostRecentStatefulProcessorWithDeletion
  extends StatefulProcessor[String, (String, String), (String, String)]
  with Logging {
  @transient private var _mostRecent: ValueState[String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    getHandle.deleteIfExists("countState")
    _mostRecent = getHandle.getValueState[String]("mostRecent", Encoders.STRING,
      TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val mostRecent = Option(_mostRecent.get()).getOrElse("")

    var output = List[(String, String)]()
    inputRows.foreach { row =>
      _mostRecent.update(row._2)
      output = (key, mostRecent) :: output
    }
    output.iterator
  }
}

// Class to verify incorrect usage of stateful processor
class RunningCountStatefulProcessorWithError extends RunningCountStatefulProcessor {
  @transient private var _tempState: ValueState[Long] = _

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    // Trying to create value state here should fail
    _tempState = getHandle.getValueState[Long]("tempState", Encoders.scalaLong,
      TTLConfig.NONE)
    Iterator.empty
  }
}

// class for verify state schema is correctly written for all state var types
class StatefulProcessorWithCompositeTypes(useImplicits: Boolean)
  extends RunningCountStatefulProcessor {
  import implicits._
  @transient private var _listState: ListState[TestClass] = _
  @transient private var _mapState: MapState[POJOTestClass, String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {

    if (useImplicits) {
      _countState = getHandle.getValueState[Long]("countState", TTLConfig.NONE)
      _listState = getHandle.getListState[TestClass](
        "listState", TTLConfig.NONE)
      _mapState = getHandle.getMapState[POJOTestClass, String](
        "mapState", Encoders.bean(classOf[POJOTestClass]), Encoders.STRING,
        TTLConfig.NONE)
    } else {
      _countState = getHandle.getValueState[Long]("countState", Encoders.scalaLong,
        TTLConfig.NONE)
      _listState = getHandle.getListState[TestClass](
        "listState", Encoders.product[TestClass], TTLConfig.NONE)
      _mapState = getHandle.getMapState[POJOTestClass, String](
        "mapState", Encoders.bean(classOf[POJOTestClass]), Encoders.STRING,
        TTLConfig.NONE)
    }
  }
}

// For each record, creates a timer to fire in 10 seconds that sleeps for 1 second.
class SleepingTimerProcessor extends StatefulProcessor[String, String, String] {
  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[String] = {
    inputRows.flatMap { _ =>
      val currentTime = timerValues.getCurrentProcessingTimeInMs()
      getHandle.registerTimer(currentTime + 10000)
      None
    }
  }

  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[String] = {
    Thread.sleep(1000)
    Iterator.single(key)
  }
}

class TestMapStateExistsInInit extends StatefulProcessor[String, String, String] {
  @transient var _mapState: MapState[String, String] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _mapState = getHandle.getMapState[String, String](
      "mapState", Encoders.STRING, Encoders.STRING, TTLConfig.NONE)

    // This should fail as we can't call exists() during init
    val exists = _mapState.exists()
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[String] = {
    Iterator.empty
  }
}

class TestValueStateExistsInInit extends StatefulProcessor[String, String, String] {
  @transient var _valueState: ValueState[String] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _valueState = getHandle.getValueState[String](
      "valueState", Encoders.STRING, TTLConfig.NONE)

    // This should fail as we can't call exists() during init
    val exists = _valueState.exists()
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[String] = {
    Iterator.empty
  }
}

class TestListStateExistsInInit extends StatefulProcessor[String, String, String] {
  @transient var _listState: ListState[String] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    _listState = getHandle.getListState[String](
      "listState", Encoders.STRING, TTLConfig.NONE)

    // This should fail as we can't call exists() during init
    val exists = _listState.exists()
  }

  override def handleInputRows(
      key: String,
      rows: Iterator[String],
      timerValues: TimerValues): Iterator[String] = {
    Iterator.empty
  }
}

/**
 * Class that adds tests for transformWithState stateful streaming operator
 */
abstract class TransformWithStateSuite extends StateStoreMetricsTest
  with AlsoTestWithRocksDBFeatures {

  import testImplicits._

  test("transformWithState - streaming with rocksdb and" +
    " invalid processor should fail") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
      TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessorWithError(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[StatefulProcessorCannotPerformOperationWithInvalidHandleState] { t =>
          assert(t.getMessage.contains("invalid handle state"))
        }
      )
    }
  }

  test("transformWithState - lazy iterators can properly get/set keyed state") {
    val spark = this.spark
    import spark.implicits._

    class ProcessorWithLazyIterators
      extends StatefulProcessor[Long, Long, Long] {
      @transient protected var _myValueState: ValueState[Long] = _
      var hasSetTimer = false

      override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
        _myValueState = getHandle.getValueState[Long](
          "myValueState",
          TTLConfig.NONE
        )
      }

      override def handleInputRows(
          key: Long,
          inputRows: Iterator[Long],
          timerValues: TimerValues): Iterator[Long] = {
        // Eagerly get/set a state variable
        _myValueState.get()
        _myValueState.update(1)

        // Create a timer (but only once) so that we can test timers have their implicit key set
        if (!hasSetTimer) {
          getHandle.registerTimer(0)
          hasSetTimer = true
        }

        // In both of these cases, we return a lazy iterator that gets/sets state variables.
        // This is to test that the stateful processor can handle lazy iterators.
        inputRows.map { r =>
          _myValueState.get()
          _myValueState.update(r)
          r
        }
      }

      override def handleExpiredTimer(
          key: Long,
          timerValues: TimerValues,
          expiredTimerInfo: ExpiredTimerInfo): Iterator[Long] = {
        // The timer uses a Seq(42L) since when the timer fires, inputRows is empty.
        Seq(42L).iterator.map { r =>
          _myValueState.get()
          _myValueState.update(r)
          r
        }
      }
    }

    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString
    ) {
      val inputData = MemoryStream[Int]
      val result = inputData
        .toDS()
        .select(timestamp_seconds($"value").as("timestamp"))
        .withWatermark("timestamp", "10 seconds")
        .as[Long]
        .groupByKey(x => x)
        .transformWithState(
          new ProcessorWithLazyIterators(), TimeMode.EventTime(), OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(),
        // Use 12 so that the watermark advances to 2 seconds and causes the timer to fire
        AddData(inputData, 12),
        // The 12 is from the input data; the 42 is from the timer
        CheckAnswer(12, 42)
      )
    }
  }

  test("transformWithState - streaming with rocksdb should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
      TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        CheckNewAnswer(("a", "1")),
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") == 0)
          assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
        },
        AddData(inputData, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        StopStream,
        StartStream(),
        AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        StopStream,
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).numRowsUpdated === 1)
          assert(q.lastProgress.stateOperators(0).numRowsRemoved === 1)
        },
        StartStream(),
        AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
        CheckNewAnswer(("a", "1"), ("c", "1"))
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")),
        Execute { q =>
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
          assert(q.lastProgress.stateOperators(0).customMetrics.get("numRegisteredTimers") === 1)
        },
        AddData(inputData, "b"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("b", "1")),

        AddData(inputData, "b"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("a", "-1"), ("b", "2")),

        StopStream,
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "b"),
        AddData(inputData, "c"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("c", "1")), // should remove 'b' as count reaches 3

        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("c", "-1"), ("d", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "and updating timers should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(
          new RunningCountStatefulProcessorWithProcTimeTimerUpdates(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(("a", "1")), // at batch 0, ts = 1, timer = "a" -> [6] (= 1 + 5)

        AddData(inputData, "a"),
        AdvanceManualClock(2 * 1000),
        CheckNewAnswer(("a", "2")), // at batch 1, ts = 3, timer = "a" -> [9.5] (2 + 7.5)
        StopStream,

        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "d"),
        AdvanceManualClock(10 * 1000),
        CheckNewAnswer(("a", "-1"), ("d", "1")), // at batch 2, ts = 13, timer for "a" is expired.
        // If the timer of "a" was not replaced (pure addition), it would have triggered the timer
        // two times here and produced ("a", "-1") two times.
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and processing time timer " +
   "and multiple timers should succeed") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val clock = new StreamManualClock

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(
          new RunningCountStatefulProcessorWithMultipleTimers(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000), // at batch 0, add 3 timers for given key = "a"

        AddData(inputData, "a"),
        AdvanceManualClock(6 * 1000),
        CheckNewAnswer(("a", "2")), // at ts = 7, first timer expires and produces ("a", "2")

        AddData(inputData, "a"),
        AdvanceManualClock(5 * 1000),
        CheckNewAnswer(("a", "3")), // at ts = 12, second timer expires and produces ("a", "3")
        StopStream,

        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(5 * 1000),
        CheckNewAnswer(("a", "4")), // at ts = 17, third timer expires and produces ("a", "4")
        StopStream
      )
    }
  }

  test("transformWithState - streaming with rocksdb and event " +
  "time based timer") {
    val inputData = MemoryStream[(String, Int)]
    val result =
      inputData.toDS()
        .select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
        .withWatermark("eventTime", "10 seconds")
        .as[(String, Long)]
        .groupByKey(_._1)
        .transformWithState(
          new MaxEventTimeStatefulProcessor(),
          TimeMode.EventTime(),
          OutputMode.Update())

    testStream(result, OutputMode.Update())(
      StartStream(),

      AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
      // Max event time = 15. Timeout timestamp for "a" = 15 + 5 = 20. Watermark = 15 - 10 = 5.
      CheckNewAnswer(("a", 15)), // Output = max event time of a

      AddData(inputData, ("a", 4)), // Add data older than watermark for "a"
      CheckNewAnswer(), // No output as data should get filtered by watermark

      AddData(inputData, ("a", 10)), // Add data newer than watermark for "a"
      CheckNewAnswer(("a", 15)), // Max event time is still the same
      // Timeout timestamp for "a" is still 20 as max event time for "a" is still 15.
      // Watermark is still 5 as max event time for all data is still 15.

      AddData(inputData, ("b", 31)), // Add data newer than watermark for "b", not "a"
      // Watermark = 31 - 10 = 21, so "a" should be timed out as timeout timestamp for "a" is 20.
      CheckNewAnswer(("a", -1), ("b", 31)), // State for "a" should timeout and emit -1
      Execute { q =>
        // Filter for idle progress events and then verify the custom metrics for stateful operator
        val progData = q.recentProgress.filter(prog => prog.stateOperators.size > 0)
        assert(progData.filter(prog =>
          prog.stateOperators(0).customMetrics.get("numValueStateVars") > 0).size > 0)
        assert(progData.filter(prog =>
          prog.stateOperators(0).customMetrics.get("numRegisteredTimers") > 0).size > 0)
        assert(progData.filter(prog =>
          prog.stateOperators(0).customMetrics.get("numDeletedTimers") > 0).size > 0)
      }
    )
  }

  test("transformWithState - timer duration should be reflected in metrics") {
    val clock = new StreamManualClock
    val inputData = MemoryStream[String]
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(
        new SleepingTimerProcessor, TimeMode.ProcessingTime(), OutputMode.Update())

    testStream(result, OutputMode.Update())(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, "a"),
      AdvanceManualClock(1 * 1000),
      // Side effect: timer scheduled for t = 1 + 10 = 11.
      CheckNewAnswer(),
      Execute { q =>
        val metrics = q.lastProgress.stateOperators(0).customMetrics
        assert(metrics.get("numRegisteredTimers") === 1)
        assert(metrics.get("timerProcessingTimeMs") < 2000)
      },

      AddData(inputData, "b"),
      AdvanceManualClock(1 * 1000),
      // Side effect: timer scheduled for t = 2 + 10 = 12.
      CheckNewAnswer(),
      Execute { q =>
        val metrics = q.lastProgress.stateOperators(0).customMetrics
        assert(metrics.get("numRegisteredTimers") === 1)
        assert(metrics.get("timerProcessingTimeMs") < 2000)
      },

      AddData(inputData, "c"),
      // Time is currently 2 and we need to advance past 12. So, advance by 11 seconds.
      AdvanceManualClock(11 * 1000),
      CheckNewAnswer("a", "b"),
      Execute { q =>
        val metrics = q.lastProgress.stateOperators(0).customMetrics
        assert(metrics.get("numRegisteredTimers") === 1)

        // Both timers should have fired and taken 1 second each to process.
        assert(metrics.get("timerProcessingTimeMs") >= 2000)
      },

      StopStream
    )
  }

  test("Use statefulProcessor without transformWithState -" +
    " handle should be absent") {
    val processor = new RunningCountStatefulProcessor()
    val ex = intercept[Exception] {
      processor.getHandle
    }
    checkError(
      ex.asInstanceOf[SparkRuntimeException],
      condition = "STATE_STORE_HANDLE_NOT_INITIALIZED",
      parameters = Map.empty
    )
  }

  test("transformWithState - batch should succeed") {
    val inputData = Seq("a", "b")
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeMode.None(),
        OutputMode.Append())

    val df = result.toDF()
    checkAnswer(df, Seq(("a", "1"), ("b", "1")).toDF())
  }

  test("transformWithState - test deleteIfExists operator") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { chkptDir =>
        val dirPath = chkptDir.getCanonicalPath
        val inputData = MemoryStream[(String, String)]
        val stream1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        val stream2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(stream1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          StopStream
        )
        testStream(stream2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath),
          AddData(inputData, ("a", "str2"), ("b", "str3")),
          CheckNewAnswer(("a", "str1"),
            ("b", "")), // should not factor in previous count state
          Execute { q =>
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numValueStateVars") > 0)
            assert(q.lastProgress.stateOperators(0).customMetrics.get("numDeletedStateVars") > 0)
          },
          StopStream
        )
      }
    }
  }

  test("transformWithState - two input streams") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData1 = MemoryStream[String]
      val inputData2 = MemoryStream[String]

      val result = inputData1.toDS()
        .union(inputData2.toDS())
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData1, "a"),
        CheckNewAnswer(("a", "1")),
        AddData(inputData2, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        AddData(inputData1, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        AddData(inputData1, "d", "e"),
        AddData(inputData2, "a", "c"), // should recreate state for "a" and return count as 1
        CheckNewAnswer(("a", "1"), ("c", "1"), ("d", "1"), ("e", "1")),
        StopStream
      )
    }
  }

  test("transformWithState - three input streams") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData1 = MemoryStream[String]
      val inputData2 = MemoryStream[String]
      val inputData3 = MemoryStream[String]

      // union 3 input streams
      val result = inputData1.toDS()
        .union(inputData2.toDS())
        .union(inputData3.toDS())
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData1, "a"),
        CheckNewAnswer(("a", "1")),
        AddData(inputData2, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        AddData(inputData3, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        AddData(inputData1, "d", "e"),
        AddData(inputData2, "a", "c"), // should recreate state for "a" and return count as 1
        CheckNewAnswer(("a", "1"), ("c", "1"), ("d", "1"), ("e", "1")),
        AddData(inputData3, "a", "c", "d", "e"),
        CheckNewAnswer(("a", "2"), ("c", "2"), ("d", "2"), ("e", "2")),
        StopStream
      )
    }
  }

  test("transformWithState - two input streams, different key type") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData1 = MemoryStream[String]
      val inputData2 = MemoryStream[Long]

      val result = inputData1.toDS()
        // union inputData2 by casting it to a String
        .union(inputData2.toDS().map(_.toString))
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData1, "1"),
        CheckNewAnswer(("1", "1")),
        AddData(inputData2, 1L, 2L),
        CheckNewAnswer(("1", "2"), ("2", "1")),
        AddData(inputData1, "1", "2"), // should remove state for "1" and not return anything.
        CheckNewAnswer(("2", "2")),
        AddData(inputData1, "4", "5"),
        AddData(inputData2, 1L, 3L), // should recreate state for "1" and return count as 1
        CheckNewAnswer(("1", "1"), ("3", "1"), ("4", "1"), ("5", "1")),
        StopStream
      )
    }
  }

  /** Create a text file with a single data item */
  private def createFile(data: String, srcDir: File): File =
    stringToFile(new File(srcDir, s"${UUID.randomUUID()}.txt"), data)

  private def createFileStream(srcDir: File): Dataset[(String, String)] = {
    spark
      .readStream
      .option("maxFilesPerTrigger", "1")
      .text(srcDir.getCanonicalPath)
      .select("value").as[String]
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeMode.None(),
        OutputMode.Update())
  }

  test("transformWithState - availableNow trigger mode, rate limit is respected") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { srcDir =>

        Seq("a", "b", "c").foreach(createFile(_, srcDir))

        // Set up a query to read text files one at a time
        val df = createFileStream(srcDir)

        testStream(df)(
          StartStream(trigger = Trigger.AvailableNow()),
          ProcessAllAvailable(),
          CheckNewAnswer(("a", "1"), ("b", "1"), ("c", "1")),
          StopStream,
          Execute { _ =>
            createFile("a", srcDir)
          },
          StartStream(trigger = Trigger.AvailableNow()),
          ProcessAllAvailable(),
          CheckNewAnswer(("a", "2"))
        )

        var index = 0
        val foreachBatchDf = df.writeStream
          .foreachBatch((_: Dataset[(String, String)], _: Long) => {
            index += 1
          })
          .trigger(Trigger.AvailableNow())
          .start()

        try {
          foreachBatchDf.awaitTermination()
          assert(index == 4)
        } finally {
          foreachBatchDf.stop()
        }
      }
    }
  }

  test("transformWithState - availableNow trigger mode, multiple restarts") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { srcDir =>
        Seq("a", "b", "c").foreach(createFile(_, srcDir))
        val df = createFileStream(srcDir)

        var index = 0

        def startTriggerAvailableNowQueryAndCheck(expectedIdx: Int): Unit = {
          val q = df.writeStream
            .foreachBatch((_: Dataset[(String, String)], _: Long) => {
              index += 1
            })
            .trigger(Trigger.AvailableNow)
            .start()
          try {
            assert(q.awaitTermination(streamingTimeout.toMillis))
            assert(index == expectedIdx)
          } finally {
            q.stop()
          }
        }
        // start query for the first time
        startTriggerAvailableNowQueryAndCheck(3)

        // add two files and restart
        createFile("a", srcDir)
        createFile("b", srcDir)
        startTriggerAvailableNowQueryAndCheck(8)

        // try restart again
        createFile("d", srcDir)
        startTriggerAvailableNowQueryAndCheck(14)
      }
    }
  }

  Seq("unsaferow", "avro").foreach { encoding =>
    Seq(false, true).foreach { useImplicits =>
      test("transformWithState - verify StateSchemaV3 writes " +
        s"correct SQL schema of key/value with useImplicits=$useImplicits " +
        s"(encoding = $encoding)") {
        withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
          SQLConf.SHUFFLE_PARTITIONS.key ->
            TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
          SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> encoding) {
          withTempDir { checkpointDir =>
            // When Avro is used, we want to set the StructFields to nullable
            val shouldBeNullable = encoding == "avro"
            val metadataPathPostfix = "state/0/_stateSchema/default"
            val stateSchemaPath = new Path(checkpointDir.toString,
              s"$metadataPathPostfix")
            val hadoopConf = spark.sessionState.newHadoopConf()
            val fm = CheckpointFileManager.create(stateSchemaPath, hadoopConf)

            val keySchema = new StructType().add("value", StringType)
            val schema0 = StateStoreColFamilySchema(
              "countState", 0,
              keySchema, 0,
              new StructType().add("value", LongType, nullable = shouldBeNullable),
              Some(NoPrefixKeyStateEncoderSpec(keySchema)),
              None
            )
            val schema1 = StateStoreColFamilySchema(
              "listState", 0,
              keySchema, 0,
              new StructType()
                .add("id", LongType, nullable = shouldBeNullable)
                .add("name", StringType),
              Some(NoPrefixKeyStateEncoderSpec(keySchema)),
              None
            )

            val userKeySchema = new StructType()
              .add("id", IntegerType, false)
              .add("name", StringType)
            val compositeKeySchema = new StructType()
              .add("key", new StructType().add("value", StringType))
              .add("userKey", userKeySchema)
            val schema2 = StateStoreColFamilySchema(
              "mapState", 0,
              compositeKeySchema, 0,
              new StructType().add("value", StringType),
              Some(PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)),
              Option(userKeySchema)
            )

            val schema3 = StateStoreColFamilySchema(
              "$rowCounter_listState", 0,
              keySchema, 0,
              new StructType().add("count", LongType, nullable = true),
              Some(NoPrefixKeyStateEncoderSpec(keySchema)),
              None
            )

            val schema4 = StateStoreColFamilySchema(
              "default", 0,
              keySchema, 0,
              new StructType().add("value", BinaryType),
              Some(NoPrefixKeyStateEncoderSpec(keySchema)),
              None
            )

            val inputData = MemoryStream[String]
            val result = inputData.toDS()
              .groupByKey(x => x)
              .transformWithState(new StatefulProcessorWithCompositeTypes(useImplicits),
                TimeMode.None(),
                OutputMode.Update())

            testStream(result, OutputMode.Update())(
              StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
              AddData(inputData, "a", "b"),
              CheckNewAnswer(("a", "1"), ("b", "1")),
              Execute { q =>
                q.lastProgress.runId
                val schemaFilePath = fm.list(stateSchemaPath).toSeq.head.getPath
                val providerId = StateStoreProviderId(StateStoreId(
                  checkpointDir.getCanonicalPath, 0, 0), q.lastProgress.runId)
                val checker = new StateSchemaCompatibilityChecker(providerId,
                  hadoopConf, List(schemaFilePath))
                val colFamilySeq = checker.readSchemaFile()

                assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
                  q.lastProgress.stateOperators.head.customMetrics.get("numValueStateVars").toInt)
                assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
                  q.lastProgress.stateOperators.head.customMetrics.get("numListStateVars").toInt)
                assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
                  q.lastProgress.stateOperators.head.customMetrics.get("numMapStateVars").toInt)

                assert(colFamilySeq.length == 5)
                assert(colFamilySeq.map(_.toString).toSet == Set(
                  schema0, schema1, schema2, schema3, schema4
                ).map(_.toString))
              },
              StopStream
            )
          }
        }
      }
    }
  }

  test("transformWithState - verify that OperatorStateMetadataV2" +
    " file is being written correctly") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream,
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "2")),
          StopStream
        )

        val df = spark.read.format("state-metadata").load(checkpointDir.toString)

        // check first 6 columns of the row, and then read the last column of the row separately
        checkAnswer(
            df.select(
              "operatorId", "operatorName", "stateStoreName", "numPartitions", "minBatchId",
              "maxBatchId"),
            Seq(Row(0, "transformWithStateExec", "default", 5, 0, 1))
        )
        val operatorPropsJson = df.select("operatorProperties").collect().head.getString(0)
        val operatorProperties = TransformWithStateOperatorProperties.fromJson(operatorPropsJson)
        assert(operatorProperties.timeMode == "NoTime")
        assert(operatorProperties.outputMode == "Update")
        assert(operatorProperties.stateVariables.length == 1)
        assert(operatorProperties.stateVariables.head.stateName == "countState")
      }
    }
  }

  test("test that different outputMode after query restart fails") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Append())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ExpectFailure[StateStoreInvalidConfigAfterRestart] { e =>
            checkError(
              e.asInstanceOf[SparkUnsupportedOperationException],
              condition = "STATE_STORE_INVALID_CONFIG_AFTER_RESTART",
              parameters = Map(
                "configName" -> "outputMode",
                "oldConfig" -> "Update",
                "newConfig" -> "Append")
            )
          }
        )
      }
    }
  }

  test("test that changing between different state variable types fails") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountListStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          ExpectFailure[StateStoreInvalidVariableTypeChange] { t =>
            checkError(
              t.asInstanceOf[SparkUnsupportedOperationException],
              condition = "STATE_STORE_INVALID_VARIABLE_TYPE_CHANGE",
              parameters = Map(
                "stateVarName" -> "countState",
                "newType" -> "ListState",
                "oldType" -> "ValueState")
            )
          }
        )
      }
    }
  }

  test("test that different timeMode after query restart fails") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val clock = new StreamManualClock
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(
            checkpointLocation = checkpointDir.getCanonicalPath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData, "a"),
          AdvanceManualClock(1 * 1000),
          ExpectFailure[StateStoreInvalidConfigAfterRestart] { e =>
            checkError(
              e.asInstanceOf[SparkUnsupportedOperationException],
              condition = "STATE_STORE_INVALID_CONFIG_AFTER_RESTART",
              parameters = Map(
                "configName" -> "timeMode",
                "oldConfig" -> "NoTime",
                "newConfig" -> "ProcessingTime")
            )
          }
        )
      }
    }
  }

  test("test query restart with new state variable succeeds") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val clock = new StreamManualClock

        val inputData1 = MemoryStream[String]
        val result1 = inputData1.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(
            checkpointLocation = checkpointDir.getCanonicalPath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData1, "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "1")),
          StopStream
        )

        val result2 = inputData1.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
            TimeMode.ProcessingTime(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(
            checkpointLocation = checkpointDir.getCanonicalPath,
            trigger = Trigger.ProcessingTime("1 second"),
            triggerClock = clock),
          AddData(inputData1, "a"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", "2")),
          StopStream
        )
      }
    }
  }

  test("test exceeding schema file threshold throws error") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
      SQLConf.STREAMING_MAX_NUM_STATE_SCHEMA_FILES.key -> 1.toString) {
      withTempDir { dirPath =>
        val inputData = MemoryStream[(String, String)]
        // First run with both count and mostRecent states
        val stream1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(stream1, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          StopStream
        )

        // Second run deletes count state but keeps mostRecent
        val stream2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(stream2, OutputMode.Update())(
          StartStream(checkpointLocation = dirPath.getCanonicalPath),
          AddData(inputData, ("a", "str2"), ("b", "str3")),
          ExpectFailure[StateStoreStateSchemaFilesThresholdExceeded] { t =>
            checkError(
              t.asInstanceOf[StateStoreStateSchemaFilesThresholdExceeded],
              condition = "STATE_STORE_STATE_SCHEMA_FILES_THRESHOLD_EXCEEDED",
              parameters = Map(
                "numStateSchemaFiles" -> "2",
                "maxStateSchemaFiles" -> "1",
                "removedColumnFamilies" -> "(countState)",
                "addedColumnFamilies" -> "()"
              )
            )
          }
        )
      }
    }
  }

  test("test query restart succeeds") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[String]
        val result1 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "1")),
          StopStream
        )

        // Here we are writing non-metadata files to the operator metadata directory to ensure that
        // they are ignored during restart.
        val hadoopConf = spark.sessionState.newHadoopConf()
        val fm = CheckpointFileManager.create(new Path(checkpointDir.toString),
          hadoopConf)
        fm.mkdirs(new Path(new Path(checkpointDir.toString, DIR_NAME_OFFSETS),
          "dummy_path_name"))
        fm.mkdirs(
          new Path(OperatorStateMetadataV2.metadataDirPath(
            new Path(new Path(new Path(checkpointDir.toString), "state"), "0")
          ),
            "dummy_path_name")
        )

        val result2 = inputData.toDS()
          .groupByKey(x => x)
          .transformWithState(new RunningCountStatefulProcessorWithProcTimeTimer(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputData, "a"),
          CheckNewAnswer(("a", "2")),
          StopStream
        )
      }
    }
  }

  test("SPARK-49070: transformWithState - valid initial state plan") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withTempDir { srcDir =>
        Seq("a", "b", "c").foreach(createFile(_, srcDir))
        val df = createFileStream(srcDir)

        var index = 0

        val q = df.writeStream
          .foreachBatch((_: Dataset[(String, String)], _: Long) => {
            index += 1
          })
          .trigger(Trigger.AvailableNow)
          .start()

        try {
          assert(q.awaitTermination(streamingTimeout.toMillis))

          val sparkPlan =
            q.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
          val transformWithStateExec = sparkPlan.collect {
            case p: TransformWithStateExec => p
          }.head

          assert(!transformWithStateExec.hasInitialState)

          // EnsureRequirements should not apply on the initial state plan
          val exchange = transformWithStateExec.initialState.collect {
            case s: ShuffleExchangeExec => s
          }

          assert(exchange.isEmpty)
        } finally {
          q.stop()
        }
      }
    }
  }

  private[sql] def getFiles(path: Path): Array[FileStatus] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fileManager = CheckpointFileManager.create(path, hadoopConf)
    fileManager.list(path)
  }

  private[sql] def getStateSchemaPath(stateCheckpointPath: Path): Path = {
    new Path(stateCheckpointPath, "_stateSchema/default/")
  }

  // TODO: [SPARK-50845] Re-enable tests after full-rewrite is enabled.
  ignore("transformWithState - verify that metadata and schema logs are purged") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "1") {
      withTempDir { chkptDir =>
        val stateOpIdPath = new Path(new Path(chkptDir.getCanonicalPath, "state"), "0")
        val stateSchemaPath = getStateSchemaPath(stateOpIdPath)

        val metadataPath = OperatorStateMetadataV2.metadataDirPath(stateOpIdPath)
        // in this test case, we are changing the state spec back and forth
        // to trigger the writing of the schema and metadata files
        val inputData = MemoryStream[(String, String)]
        val result1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str2")),
          CheckNewAnswer(("a", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        // assert that a metadata and schema file has been written for each run
        // as state variables have been deleted
        assert(getFiles(metadataPath).length == 2)
        assert(getFiles(stateSchemaPath).length == 2)

        val result3 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result3, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str3")),
          CheckNewAnswer(("a", "1", "str2")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        // because we don't change the schema for this run, there won't
        // be a new schema file written.
        testStream(result3, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str4")),
          CheckNewAnswer(("a", "2", "str3")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        // by the end of the test, there have been 4 batches,
        // so the metadata and schema logs, and commitLog has been purged
        // for batches 0 and 1 so metadata and schema files exist for batches 0, 1, 2, 3
        // and we only need to keep metadata files for batches 2, 3, and the since schema
        // hasn't changed between batches 2, 3, we only keep the schema file for batch 2
        assert(getFiles(metadataPath).length == 2)
        assert(getFiles(stateSchemaPath).length == 1)
      }
    }
  }

  // TODO: [SPARK-50845] Re-enable tests after StateSchemaV3 threshold change
  ignore("transformWithState - verify that schema file " +
    "is kept after metadata is purged") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "2") {
      withTempDir { chkptDir =>
        val stateOpIdPath = new Path(new Path(chkptDir.getCanonicalPath, "state"), "0")
        val stateSchemaPath = getStateSchemaPath(stateOpIdPath)

        val metadataPath = OperatorStateMetadataV2.metadataDirPath(stateOpIdPath)
        // in this test case, we are changing the state spec back and forth
        // to trigger the writing of the schema and metadata files
        val inputData = MemoryStream[(String, String)]
        val result1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "2", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str2")),
          CheckNewAnswer(("a", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        assert(getFiles(metadataPath).length == 3)
        assert(getFiles(stateSchemaPath).length == 2)

        val result3 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result3, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str3")),
          CheckNewAnswer(("a", "1", "str2")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        // metadata files should be kept for batches 1, 2, 3
        // schema files should be kept for batches 0, 2, 3
        assert(getFiles(metadataPath).length == 3)
        assert(getFiles(stateSchemaPath).length == 3)
        // we want to ensure that we can read batch 1 even though the
        // metadata file for batch 0 was removed
        val batch1Df = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, chkptDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .option(StateSourceOptions.BATCH_ID, 1)
          .load()

        val batch1AnsDf = batch1Df.selectExpr(
          "key.value AS groupingKey",
          "value.value AS valueId")

        checkAnswer(batch1AnsDf, Seq(Row("a", 2L)))

        val batch3Df = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, chkptDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .option(StateSourceOptions.BATCH_ID, 3)
          .load()

        val batch3AnsDf = batch3Df.selectExpr(
          "key.value AS groupingKey",
          "value.value AS valueId")
        checkAnswer(batch3AnsDf, Seq(Row("a", 1L)))
      }
    }
  }

  test("state data source integration - value state supports time travel") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "5") {
      withTempDir { chkptDir =>
        // in this test case, we are changing the state spec back and forth
        // to trigger the writing of the schema and metadata files
        val inputData = MemoryStream[(String, String)]
        val result1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "2", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "3", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "4", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "5", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "6", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "7", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str2")),
          CheckNewAnswer(("a", "str1")),
          AddData(inputData, ("a", "str3")),
          CheckNewAnswer(("a", "str2")),
          AddData(inputData, ("a", "str4")),
          CheckNewAnswer(("a", "str3")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )

        // Batches 0-7: countState, mostRecent
        // Batches 8-9: countState

        // By this time, offset and commit logs for batches 0-3 have been purged.
        // However, if we want to read the data for batch 4, we need to read the corresponding
        // metadata and schema file for batch 4, or the latest files that correspond to
        // batch 4 (in this case, the files were written for batch 0).
        // We want to test the behavior where the metadata files are preserved so that we can
        // read from the state data source, even if the commit and offset logs are purged for
        // a particular batch

        val df = spark.read.format("state-metadata").load(chkptDir.toString)

        // check the min and max batch ids that we have data for
        checkAnswer(
          df.select(
            "operatorId", "operatorName", "stateStoreName", "numPartitions", "minBatchId",
            "maxBatchId"),
          Seq(Row(0, "transformWithStateExec", "default", 5, 4, 9))
        )

        val countStateDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, chkptDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "countState")
          .option(StateSourceOptions.BATCH_ID, 4)
          .load()

        val countStateAnsDf = countStateDf.selectExpr(
          "key.value AS groupingKey",
          "value.value AS valueId")
        checkAnswer(countStateAnsDf, Seq(Row("a", 5L)))

        val mostRecentDf = spark.read
          .format("statestore")
          .option(StateSourceOptions.PATH, chkptDir.getAbsolutePath)
          .option(StateSourceOptions.STATE_VAR_NAME, "mostRecent")
          .option(StateSourceOptions.BATCH_ID, 4)
          .load()

        val mostRecentAnsDf = mostRecentDf.selectExpr(
          "key.value AS groupingKey",
          "value.value")
        checkAnswer(mostRecentAnsDf, Seq(Row("a", "str1")))
      }
    }
  }

  // TODO: [SPARK-50845] Re-enable tests after StateSchemaV3 threshold change
  ignore("transformWithState - verify that all metadata and schema logs are not purged") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "3") {
      withTempDir { chkptDir =>
        val inputData = MemoryStream[(String, String)]
        val result1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "2", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "3", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "4", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "5", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "6", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "7", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "8", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "9", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "10", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "11", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "12", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "13", "str1")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "14", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )

        val stateOpIdPath = new Path(new Path(chkptDir.getCanonicalPath, "state"), "0")
        val stateSchemaPath = getStateSchemaPath(stateOpIdPath)

        val metadataPath = OperatorStateMetadataV2.metadataDirPath(stateOpIdPath)

        // Metadata files exist for batches 0, 12, and the thresholdBatchId is 8
        // as this is the earliest batchId for which the commit log is not present,
        // so we need to keep metadata files for batch 0 so we can read the commit
        // log correspondingly
        assert(getFiles(metadataPath).length == 2)
        assert(getFiles(stateSchemaPath).length == 1)
      }
    }
  }

  // TODO: [SPARK-50845] Re-enable tests after StateSchemaV3 threshold change
  ignore("transformWithState - verify that no metadata and schema logs are purged after" +
    " removing column family") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString,
      SQLConf.MIN_BATCHES_TO_RETAIN.key -> "3") {
      withTempDir { chkptDir =>
        val inputData = MemoryStream[(String, String)]
        val result1 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new RunningCountMostRecentStatefulProcessor(),
            TimeMode.None(),
            OutputMode.Update())
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "1", "")),
          AddData(inputData, ("a", "str1")),
          CheckNewAnswer(("a", "2", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        testStream(result1, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "1", "")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "2", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "3", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "4", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "5", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "6", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "7", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "8", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "9", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "10", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "11", "str1")),
          AddData(inputData, ("b", "str1")),
          CheckNewAnswer(("b", "12", "str1")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )
        val result2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeMode.None(),
            OutputMode.Update())

        testStream(result2, OutputMode.Update())(
          StartStream(checkpointLocation = chkptDir.getCanonicalPath),
          AddData(inputData, ("b", "str2")),
          CheckNewAnswer(("b", "str1")),
          AddData(inputData, ("b", "str3")),
          CheckNewAnswer(("b", "str2")),
          Execute { q =>
            eventually(timeout(Span(5, Seconds))) {
              q.asInstanceOf[MicroBatchExecution].arePendingAsyncPurge should be(false)
            }
          },
          StopStream
        )

        val stateOpIdPath = new Path(new Path(chkptDir.getCanonicalPath, "state"), "0")
        val stateSchemaPath = getStateSchemaPath(stateOpIdPath)

        val metadataPath = OperatorStateMetadataV2.metadataDirPath(stateOpIdPath)

        // Metadata files are written for batches 0, 2, and 14.
        // Schema files are written for 0, 14
        // At the beginning of the last query run, the thresholdBatchId is 11.
        // However, we would need both schema files to be preserved, if we want to
        // be able to read from batch 11 onwards.
        assert(getFiles(metadataPath).length == 2)
        assert(getFiles(stateSchemaPath).length == 2)
      }
    }
  }
}

class TransformWithStateValidationSuite extends StateStoreMetricsTest {
  import testImplicits._

  test("transformWithState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeMode.None(),
        OutputMode.Update())

    testStream(result, OutputMode.Update())(
      AddData(inputData, "a"),
      ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] { t =>
        assert(t.getMessage.contains("not supported"))
      }
    )
  }

  test("transformWithState - check that error within handleInputRows is classified") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new UnclassifiedErrorProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[TransformWithStateUserFunctionException] { error =>
          checkError(
            error.asInstanceOf[SparkException],
            condition = "TRANSFORM_WITH_STATE_USER_FUNCTION_ERROR",
            parameters = Map(
              "reason" -> "dummy unclassified error",
              "function" -> "handleInputRows")
          )
        }
      )
    }
  }

  test("transformWithState - check that classified error is thrown from handleInputRows") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new ClassifiedErrorProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] { error =>
          assert(error.getMessage.contains("not supported"))
        }
      )
    }
  }

  test("transformWithState - check that error within handleInitialState is classified") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val inputData = MemoryStream[String]
      val initDf = Seq("init_1").toDS().groupByKey(x => x)
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new UnclassifiedErrorInitialStateProcessor(),
          TimeMode.None(),
          OutputMode.Update(),
          initDf)

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[TransformWithStateUserFunctionException] { error =>
          checkError(
            error.asInstanceOf[SparkException],
            condition = "TRANSFORM_WITH_STATE_USER_FUNCTION_ERROR",
            parameters = Map(
              "reason" -> "dummy unclassified error",
              "function" -> "handleInitialState")
          )
        }
      )
    }
  }

  test("transformWithState - check that classified error is thrown from handleInitialState") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val inputData = MemoryStream[String]
      val initDf = Seq("init_1").toDS().groupByKey(x => x)
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new ClassifiedErrorInitialStateProcessor(),
          TimeMode.None(),
          OutputMode.Update(),
          initDf)

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] { error =>
          assert(error.getMessage.contains("not supported"))
        }
      )
    }
  }

  test("transformWithState - check that error within handleExpiredTimer is classified") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val clock = new StreamManualClock
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new UnclassifiedTimerErrorProcessor(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        AdvanceManualClock(2 * 1000),
        ExpectFailure[TransformWithStateUserFunctionException] { error =>
          checkError(
            error.asInstanceOf[SparkException],
            condition = "TRANSFORM_WITH_STATE_USER_FUNCTION_ERROR",
            parameters = Map(
              "reason" -> "dummy unclassified error",
              "function" -> "handleExpiredTimer")
          )
        }
      )
    }
  }

  test("transformWithState - check that classified error is thrown from handleExpiredTimer") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val clock = new StreamManualClock
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new ClassifiedTimerErrorProcessor(),
          TimeMode.ProcessingTime(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputData, "a"),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        AdvanceManualClock(2 * 1000),
        ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] { error =>
          assert(error.getMessage.contains("not supported"))
        }
      )
    }
  }

  test("transformWithState - ValueState.exists() should fail in init") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new TestValueStateExistsInInit(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[StatefulProcessorCannotPerformOperationWithInvalidHandleState] { error =>
          checkError(
            error.asInstanceOf[SparkUnsupportedOperationException],
            condition = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_HANDLE_STATE",
            parameters = Map(
              "operationType" -> "valueState.exists",
              "handleState" -> "PRE_INIT")
          )
        }
      )
    }
  }

  test("transformWithState - MapState.exists() should fail in init") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new TestMapStateExistsInInit(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[StatefulProcessorCannotPerformOperationWithInvalidHandleState] { error =>
          checkError(
            error.asInstanceOf[SparkUnsupportedOperationException],
            condition = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_HANDLE_STATE",
            parameters = Map(
              "operationType" -> "mapState.exists",
              "handleState" -> "PRE_INIT")
          )
        }
      )
    }
  }

  test("transformWithState - ListState.exists() should fail in init") {
    withSQLConf(
      SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {

      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new TestListStateExistsInInit(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[StatefulProcessorCannotPerformOperationWithInvalidHandleState] { error =>
          checkError(
            error.asInstanceOf[SparkUnsupportedOperationException],
            condition = "STATEFUL_PROCESSOR_CANNOT_PERFORM_OPERATION_WITH_INVALID_HANDLE_STATE",
            parameters = Map(
              "operationType" -> "listState.exists",
              "handleState" -> "PRE_INIT")
          )
        }
      )
    }
  }

  test("transformWithStateWithInitialState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[InitInputRow]
    val initDf = Seq(("init_1", 40.0), ("init_2", 100.0)).toDS()
      .groupByKey(x => x._1)
      .mapValues(x => x)
    val result = inputData.toDS()
      .groupByKey(x => x.key)
      .transformWithState(new AccumulateStatefulProcessorWithInitState(),
        TimeMode.None(), OutputMode.Append(), initDf
      )
    testStream(result, OutputMode.Update())(
      AddData(inputData, InitInputRow("a", "add", -1.0)),
      ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] {
        (t: Throwable) => {
          assert(t.getMessage.contains("not supported"))
        }
      }
    )
  }

  test("transformWithState - validate timeModes") {
    // validation tests should pass for TimeMode.None
    TransformWithStateVariableUtils.validateTimeMode(TimeMode.None(), None)
    TransformWithStateVariableUtils.validateTimeMode(TimeMode.None(), Some(10L))

    // validation tests should fail for TimeMode.ProcessingTime and TimeMode.EventTime
    // when time values are not provided
    val ex = intercept[SparkException] {
      TransformWithStateVariableUtils.validateTimeMode(TimeMode.ProcessingTime(), None)
    }
    assert(ex.getMessage.contains("Failed to find time values"))
    TransformWithStateVariableUtils.validateTimeMode(TimeMode.ProcessingTime(), Some(10L))

    val ex1 = intercept[SparkException] {
      TransformWithStateVariableUtils.validateTimeMode(TimeMode.EventTime(), None)
    }
    assert(ex1.getMessage.contains("Failed to find time values"))
    TransformWithStateVariableUtils.validateTimeMode(TimeMode.EventTime(), Some(10L))
  }

  Seq(TimeMode.None(), TimeMode.ProcessingTime()).foreach { timeMode =>
    test(s"transformWithState - using watermark but time mode as $timeMode should not perform " +
      s"late record filtering") {
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[RocksDBStateStoreProvider].getName,
        SQLConf.SHUFFLE_PARTITIONS.key ->
          TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString
      ) {
        val inputData = MemoryStream[(String, Int)]
        val result =
          inputData.toDS()
            .select($"_1".as("key"), timestamp_seconds($"_2").as("eventTime"))
            .withWatermark("eventTime", "10 seconds")
            .as[(String, Long)]
            .groupByKey(_._1)
            .transformWithState(
              new MinEventTimeStatefulProcessor(),
              timeMode,
              OutputMode.Update())

        testStream(result, OutputMode.Update())(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = new StreamManualClock),

          AddData(inputData, ("a", 11), ("a", 13), ("a", 15)),
          AdvanceManualClock(1 * 1000),
          // Min event time = 15. Watermark = 15 - 10 = 5.
          CheckNewAnswer(("a", 11)), // Output = min event time of a

          AddData(inputData, ("a", 4)), // Add data older than watermark for "a"
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", 4)), // Data should not get filtered and output will be 4

          AddData(inputData, ("a", 1)), // Add data older than watermark for "a"
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", 1)), // Data should not get filtered and output will be 1

          AddData(inputData, ("a", 85)), // Add data newer than watermark for "a"
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("a", 1)), // Min event time should still be 1
          StopStream
        )
      }
    }
  }
}

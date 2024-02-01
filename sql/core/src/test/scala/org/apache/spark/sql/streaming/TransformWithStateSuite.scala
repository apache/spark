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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.{AlsoTestWithChangelogCheckpointingEnabled, RocksDBStateStoreProvider}
import org.apache.spark.sql.internal.SQLConf

object TransformWithStateSuiteUtils {
  val NUM_SHUFFLE_PARTITIONS = 5
}

class RunningCountStatefulProcessor extends StatefulProcessor[String, String, (String, String)]
  with Logging {
  @transient private var _countState: ValueState[Long] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    _countState = _processorHandle.getValueState[Long]("countState")
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    if (count == 3) {
      _countState.remove()
      Iterator.empty
    } else {
      _countState.update(count)
      Iterator((key, count.toString))
    }
  }

  override def close(): Unit = {}
}

class RunningCountMostRecentStatefulProcessor
  extends StatefulProcessor[String, (String, String), (String, String, String)]
  with Logging {
  @transient private var _countState: ValueState[Long] = _
  @transient private var _mostRecent: ValueState[String] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
      handle: StatefulProcessorHandle,
      outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    _countState = _processorHandle.getValueState[Long]("countState")
    _mostRecent = _processorHandle.getValueState[String]("mostRecent")
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[(String, String, String)] = {
    val count = _countState.getOption().getOrElse(0L) + 1
    val mostRecent = _mostRecent.getOption().getOrElse("")

    var output = List[(String, String, String)]()
    inputRows.foreach { row =>
      _mostRecent.update(row._2)
      _countState.update(count)
      output = (key, count.toString, mostRecent) :: output
    }
    output.iterator
  }

  override def close(): Unit = {}
}

class MostRecentStatefulProcessorWithDeletion
  extends StatefulProcessor[String, (String, String), (String, String)]
  with Logging {
  @transient private var _mostRecent: ValueState[String] = _
  @transient var _processorHandle: StatefulProcessorHandle = _

  override def init(
       handle: StatefulProcessorHandle,
       outputMode: OutputMode) : Unit = {
    _processorHandle = handle
    assert(handle.getQueryInfo().getBatchId >= 0)
    _processorHandle.deleteIfExists("countState")
    _mostRecent = _processorHandle.getValueState[String]("mostRecent")
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, String)],
      timerValues: TimerValues): Iterator[(String, String)] = {
    val mostRecent = _mostRecent.getOption().getOrElse("")

    var output = List[(String, String)]()
    inputRows.foreach { row =>
      _mostRecent.update(row._2)
      output = (key, mostRecent) :: output
    }
    output.iterator
  }

  override def close(): Unit = {}
}

class RunningCountStatefulProcessorWithError extends RunningCountStatefulProcessor {
  @transient private var _tempState: ValueState[Long] = _

  override def handleInputRows(
      key: String,
      inputRows: Iterator[String],
      timerValues: TimerValues): Iterator[(String, String)] = {
    // Trying to create value state here should fail
    _tempState = _processorHandle.getValueState[Long]("tempState")
    Iterator.empty
  }
}

/**
 * Class that adds tests for transformWithState stateful streaming operator
 */
class TransformWithStateSuite extends StateStoreMetricsTest
  with AlsoTestWithChangelogCheckpointingEnabled {

  import testImplicits._

  test("transformWithState - streaming with rocksdb and invalid processor should fail") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
      TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      val inputData = MemoryStream[String]
      val result = inputData.toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessorWithError(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        ExpectFailure[SparkException] {
          (t: Throwable) => { assert(t.getCause
            .getMessage.contains("Cannot create state variable")) }
        }
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
          TimeoutMode.NoTimeouts(),
          OutputMode.Update())

      testStream(result, OutputMode.Update())(
        AddData(inputData, "a"),
        CheckNewAnswer(("a", "1")),
        AddData(inputData, "a", "b"),
        CheckNewAnswer(("a", "2"), ("b", "1")),
        StopStream,
        StartStream(),
        AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
        CheckNewAnswer(("b", "2")),
        StopStream,
        StartStream(),
        AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
        CheckNewAnswer(("a", "1"), ("c", "1"))
      )
    }
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
            TimeoutMode.NoTimeouts(),
            OutputMode.Update())

        val stream2 = inputData.toDS()
          .groupByKey(x => x._1)
          .transformWithState(new MostRecentStatefulProcessorWithDeletion(),
            TimeoutMode.NoTimeouts(),
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
          StopStream
        )
      }
    }
  }
}

class TransformWithStateValidationSuite extends StateStoreMetricsTest {
  import testImplicits._

  test("transformWithState - batch should fail") {
    val ex = intercept[Exception] {
      val df = Seq("a", "a", "b").toDS()
        .groupByKey(x => x)
        .transformWithState(new RunningCountStatefulProcessor,
          TimeoutMode.NoTimeouts(),
          OutputMode.Append())
        .write
        .format("noop")
        .mode(SaveMode.Append)
        .save()
    }
    assert(ex.isInstanceOf[AnalysisException])
    assert(ex.getMessage.contains("not supported"))
  }

  test("transformWithState - streaming with hdfsStateStoreProvider should fail") {
    val inputData = MemoryStream[String]
    val result = inputData.toDS()
      .groupByKey(x => x)
      .transformWithState(new RunningCountStatefulProcessor(),
        TimeoutMode.NoTimeouts(),
        OutputMode.Update())

    testStream(result, OutputMode.Update())(
      AddData(inputData, "a"),
      ExpectFailure[SparkException] {
        (t: Throwable) => { assert(t.getCause.getMessage.contains("not supported")) }
      }
    )
  }
}

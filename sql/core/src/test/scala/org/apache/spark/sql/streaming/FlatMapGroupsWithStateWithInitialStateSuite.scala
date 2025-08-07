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
import org.apache.spark.sql.{AnalysisException, Dataset, KeyValueGroupedDataset}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.FlatMapGroupsWithStateExecHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.FlatMapGroupsWithStateSuite.{assertCanGetProcessingTime, assertCannotGetWatermark}
import org.apache.spark.sql.streaming.GroupStateTimeout.{EventTimeTimeout, NoTimeout, ProcessingTimeTimeout}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class FlatMapGroupsWithStateWithInitialStateSuite extends StateStoreMetricsTest {
  import testImplicits._

  /**
   * FlatMapGroupsWithState function that returns the key, value as passed to it
   * along with the updated state. The state is incremented for every value.
   */
  val flatMapGroupsWithStateFunc =
    (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      val valList = values.toSeq
      if (valList.isEmpty) {
        // When the function is called on just the initial state make sure the other fields
        // are set correctly
        assert(state.exists)
      }
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }
      assert(!state.hasTimedOut)
      if (key.contains("EventTime")) {
        state.setTimeoutTimestamp(0, "1 hour")
      }
      if (key.contains("ProcessingTime")) {
        state.setTimeoutDuration("1  hour")
      }
      val count = state.getOption.map(_.count).getOrElse(0L) + valList.size
      // We need to check if not explicitly calling update will still save the init state or not
      if (!key.contains("NoUpdate")) {
        // this is not reached when valList is empty and the state count is 2
        state.update(new RunningCount(count))
      }
      Iterator((key, valList, count.toString))
    }

  Seq("1", "2", "6").foreach { shufflePartitions =>
    testWithAllStateVersions(s"flatMapGroupsWithState - initial " +
      s"state - all cases - shuffle partitions ${shufflePartitions}") {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> shufflePartitions) {
        // We will test them on different shuffle partition configuration to make sure the
        // grouping by key will still work. On higher number of shuffle partitions its possible
        // that all keys end up on different partitions.
        val initialState: Dataset[(String, RunningCount)] = Seq(
          ("keyInStateAndData-1", new RunningCount(1)),
          ("keyInStateAndData-2", new RunningCount(2)),
          ("keyNoUpdate", new RunningCount(2)), // state.update will not be called
          ("keyOnlyInState-1", new RunningCount(1))
        ).toDS()

        val it = initialState.groupByKey(x => x._1).mapValues(_._2)
        val inputData = MemoryStream[String]
        val result =
          inputData.toDS()
            .groupByKey(x => x)
            .flatMapGroupsWithState(
              Update, GroupStateTimeout.NoTimeout, it)(flatMapGroupsWithStateFunc)

        testStream(result, Update)(
          AddData(inputData, "keyOnlyInData", "keyInStateAndData-2"),
          CheckNewAnswer(
            ("keyOnlyInState-1", Seq[String](), "1"),
            ("keyNoUpdate", Seq[String](), "2"), // update will not be called
            ("keyInStateAndData-2", Seq[String]("keyInStateAndData-2"), "3"), // inc by 1
            ("keyInStateAndData-1", Seq[String](), "1"),
            ("keyOnlyInData", Seq[String]("keyOnlyInData"), "1") // inc by 1
          ),
          assertNumStateRows(total = 5, updated = 4),
          // Stop and Start stream to make sure initial state doesn't get applied again.
          StopStream,
          StartStream(),
          AddData(inputData, "keyInStateAndData-1"),
          CheckNewAnswer(
            // state incremented by 1
            ("keyInStateAndData-1", Seq[String]("keyInStateAndData-1"), "2")
          ),
          assertNumStateRows(total = 5, updated = 1),
          StopStream
        )
      }
    }
  }

  testWithAllStateVersions("flatMapGroupsWithState - initial state - case class key") {
    val stateFunc = (key: User, values: Iterator[User], state: GroupState[Long]) => {
      val valList = values.toSeq
      if (valList.isEmpty) {
        // When the function is called on just the initial state make sure the other fields
        // are set correctly
        assert(state.exists)
      }
      assertCanGetProcessingTime { state.getCurrentProcessingTimeMs() >= 0 }
      assertCannotGetWatermark { state.getCurrentWatermarkMs() }
      assert(!state.hasTimedOut)
      val count = state.getOption.getOrElse(0L) + valList.size
      // We need to check if not explicitly calling update will still save the state or not
      if (!key.name.contains("NoUpdate")) {
        // this is not reached when valList is empty and the state count is 2
        state.update(count)
      }
      Iterator((key, valList.map(_.name), count.toString))
    }

    val ds = Seq(
      (User("keyInStateAndData", "1"), (1L)),
      (User("keyOnlyInState", "1"), (1L)),
      (User("keyNoUpdate", "2"), (2L)) // state.update will not be called on this in the function
    ).toDS().groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[User]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, NoTimeout(), ds)(stateFunc)

    testStream(result, Update)(
      AddData(inputData, User("keyInStateAndData", "1"), User("keyOnlyInData", "1")),
      CheckNewAnswer(
        (("keyInStateAndData", "1"), Seq[String]("keyInStateAndData"), "2"),
        (("keyOnlyInState", "1"), Seq[String](), "1"),
        (("keyNoUpdate", "2"), Seq[String](), "2"),
        (("keyOnlyInData", "1"), Seq[String]("keyOnlyInData"), "1")
      ),
      assertNumStateRows(total = 4, updated = 3), // (keyOnlyInState, 2) does not call update()
      // Stop and Start stream to make sure initial state doesn't get applied again.
      StopStream,
      StartStream(),
      AddData(inputData, User("keyOnlyInData", "1")),
      CheckNewAnswer(
        (("keyOnlyInData", "1"), Seq[String]("keyOnlyInData"), "2")
      ),
      assertNumStateRows(total = 4, updated = 1),
      StopStream
    )
  }

  testQuietly("flatMapGroupsWithState - initial state - duplicate keys") {
    val initialState = Seq(
      ("a", new RunningCount(2)),
      ("a", new RunningCount(1))
    ).toDS().groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(Update, NoTimeout(), initialState)(flatMapGroupsWithStateFunc)
    testStream(result, Update)(
      AddData(inputData, "a"),
      ExpectFailure[SparkException] { e =>
        assert(e.getCause.getMessage.contains("The initial state provided contained " +
          "multiple rows(state) with the same key"))
      }
    )
  }

  Seq(NoTimeout(), EventTimeTimeout(), ProcessingTimeTimeout()).foreach { timeout =>
    test(s"flatMapGroupsWithState - initial state - batch mode - timeout ${timeout}") {
      // We will test them on different shuffle partition configuration to make sure the
      // grouping by key will still work. On higher number of shuffle partitions its possible
      // that all keys end up on different partitions.
      val initialState = Seq(
        (s"keyInStateAndData-1-$timeout", new RunningCount(1)),
        ("keyInStateAndData-2", new RunningCount(2)),
        ("keyNoUpdate", new RunningCount(2)), // state.update will not be called
        ("keyOnlyInState-1", new RunningCount(1))
      ).toDS().groupByKey(x => x._1).mapValues(_._2)

      val inputData = Seq(
        ("keyOnlyInData"), ("keyInStateAndData-2")
      )
      val result = inputData.toDS().groupByKey(x => x)
        .flatMapGroupsWithState(
          Update, timeout, initialState)(flatMapGroupsWithStateFunc)

      val expected = Seq(
        ("keyOnlyInState-1", Seq[String](), "1"),
        ("keyNoUpdate", Seq[String](), "2"), // update will not be called
        ("keyInStateAndData-2", Seq[String]("keyInStateAndData-2"), "3"), // inc by 1
        (s"keyInStateAndData-1-$timeout", Seq[String](), "1"),
        ("keyOnlyInData", Seq[String]("keyOnlyInData"), "1") // inc by 1
      ).toDF()
      checkAnswer(result.toDF(), expected)
    }
  }

  testQuietly("flatMapGroupsWithState - initial state - batch mode - duplicate state") {
    val initialState = Seq(
      ("a", new RunningCount(1)),
      ("a", new RunningCount(2))
    ).toDS().groupByKey(x => x._1).mapValues(_._2)

    val e = intercept[SparkException] {
      Seq("a", "b").toDS().groupByKey(x => x)
        .flatMapGroupsWithState(Update, NoTimeout(), initialState)(flatMapGroupsWithStateFunc)
        .show()
    }
    assert(e.getMessage.contains(
      "The initial state provided contained multiple rows(state) with the same key." +
        " Make sure to de-duplicate the initial state before passing it."))
  }

  testQuietly("flatMapGroupsWithState - initial state - streaming initial state") {
    val initialStateData = MemoryStream[(String, RunningCount)]
    initialStateData.addData(("a", new RunningCount(1)))

    val inputData = MemoryStream[String]

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(
          Update, NoTimeout(), initialStateData.toDS().groupByKey(_._1).mapValues(_._2)
        )(flatMapGroupsWithStateFunc)

    val e = intercept[AnalysisException] {
      result.writeStream
        .format("console")
        .start()
    }

    val expectedError = "Non-streaming DataFrame/Dataset is not supported" +
      " as the initial state in [flatMap|map]GroupsWithState" +
      " operation on a streaming DataFrame/Dataset"
    assert(e.message.contains(expectedError))
  }

  test("flatMapGroupsWithState - initial state - initial state has flatMapGroupsWithState") {
    val initialStateDS = Seq(("keyInStateAndData", new RunningCount(1))).toDS()
    val initialState: KeyValueGroupedDataset[String, RunningCount] =
      initialStateDS.groupByKey(_._1).mapValues(_._2)
        .mapGroupsWithState(
          GroupStateTimeout.NoTimeout())(
          (key: String, values: Iterator[RunningCount], state: GroupState[Boolean]) => {
            (key, values.next())
          }
        ).groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[String]

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(
          Update, NoTimeout(), initialState
        )(flatMapGroupsWithStateFunc)

    testStream(result, Update)(
      AddData(inputData, "keyInStateAndData"),
      CheckNewAnswer(
        ("keyInStateAndData", Seq[String]("keyInStateAndData"), "2")
      ),
      StopStream
    )
  }

  testWithAllStateVersions("mapGroupsWithState - initial state - null key") {
    val mapGroupsWithStateFunc =
      (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
        val valList = values.toList
        val count = state.getOption.map(_.count).getOrElse(0L) + valList.size
        state.update(new RunningCount(count))
        (key, state.get.count.toString)
      }
    val initialState = Seq(
      ("key", new RunningCount(5)),
      (null, new RunningCount(2))
    ).toDS().groupByKey(_._1).mapValues(_._2)

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(NoTimeout(), initialState)(mapGroupsWithStateFunc)
    testStream(result, Update)(
      AddData(inputData, "key", null),
      CheckNewAnswer(
        ("key", "6"), // state is incremented by 1
        (null, "3") // incremented by 1
      ),
      assertNumStateRows(total = 2, updated = 2),
      StopStream
    )
  }

  testWithAllStateVersions("flatMapGroupsWithState - initial state - processing time timeout") {
    // function will return -1 on timeout and returns count of the state otherwise
    val stateFunc =
      (key: String, values: Iterator[(String, Long)], state: GroupState[RunningCount]) => {
        if (state.hasTimedOut) {
          state.remove()
          Iterator((key, "-1"))
        } else {
          val count = state.getOption.map(_.count).getOrElse(0L) + values.size
          state.update(RunningCount(count))
          state.setTimeoutDuration("10 seconds")
          Iterator((key, count.toString))
        }
      }

    val clock = new StreamManualClock
    val inputData = MemoryStream[(String, Long)]
    val initialState = Seq(
      ("c", new RunningCount(2))
    ).toDS().groupByKey(_._1).mapValues(_._2)
    val result =
      inputData.toDF().toDF("key", "time")
        .selectExpr("key", "timestamp_seconds(time) as timestamp")
        .withWatermark("timestamp", "10 second")
        .as[(String, Long)]
        .groupByKey(x => x._1)
        .flatMapGroupsWithState(Update, ProcessingTimeTimeout(), initialState)(stateFunc)

    testStream(result, Update)(
      StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
      AddData(inputData, ("a", 1L)),
      AdvanceManualClock(1 * 1000), // a and c are processed here for the first time.
      CheckNewAnswer(("a", "1"), ("c", "2")),
      AdvanceManualClock(10 * 1000),
      AddData(inputData, ("b", 1L)), // this will trigger c and a to get timed out
      AdvanceManualClock(1 * 1000),
      CheckNewAnswer(("a", "-1"), ("b", "1"), ("c", "-1"))
    )
  }

  // if the keys part of initial state df are different than the keys in the input data, then
  // they will not be emitted as part of the result with skipEmittingInitialStateKeys set to true
  testWithAllStateVersions("flatMapGroupsWithState - initial state - " +
    s"skipEmittingInitialStateKeys=true") {
    withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_SKIP_EMITTING_INITIAL_STATE_KEYS.key -> "true") {
      val initialState = Seq(
        ("apple", 1L),
        ("orange", 2L),
        ("mango", 5L)).toDS().groupByKey(_._1).mapValues(_._2)

      val fruitCountFunc = (key: String, values: Iterator[String], state: GroupState[Long]) => {
        val count = state.getOption.map( x => x).getOrElse(0L) + values.size
        state.update(count)
        Iterator.single((key, count))
      }

      val inputData = MemoryStream[String]
      val result =
        inputData.toDS()
          .groupByKey(x => x)
          .flatMapGroupsWithState(Update, NoTimeout(), initialState)(fruitCountFunc)
      testStream(result, Update)(
        AddData(inputData, "apple"),
        AddData(inputData, "banana"),
        CheckNewAnswer(("apple", 2), ("banana", 1)),
        AddData(inputData, "orange"),
        CheckNewAnswer(("orange", 3)),
        StopStream
      )
    }
  }

  // if the keys part of initial state df are different than the keys in the input data, then
  // they will be emitted as part of the result with skipEmittedInitialStateKeys set to false
  testWithAllStateVersions("flatMapGroupsWithState - initial state - " +
    s"skipEmittingInitialStateKeys=false") {
    withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_SKIP_EMITTING_INITIAL_STATE_KEYS.key -> "false") {
      val initialState = Seq(
        ("apple", 1L),
        ("orange", 2L),
        ("mango", 5L)).toDS().groupByKey(_._1).mapValues(_._2)

      val fruitCountFunc = (key: String, values: Iterator[String], state: GroupState[Long]) => {
        val count = state.getOption.map( x => x).getOrElse(0L) + values.size
        state.update(count)
        Iterator.single((key, count))
      }

      val inputData = MemoryStream[String]
      val result =
        inputData.toDS()
          .groupByKey(x => x)
          .flatMapGroupsWithState(Update, NoTimeout(), initialState)(fruitCountFunc)
      testStream(result, Update)(
        AddData(inputData, "apple"),
        AddData(inputData, "banana"),
        CheckNewAnswer(("apple", 2), ("banana", 1), ("orange", 2), ("mango", 5)),
        AddData(inputData, "orange"),
        CheckNewAnswer(("orange", 3)),
        StopStream
      )
    }
  }

  // if the keys part of the initial state and the first batch are the same, then the result
  // is the same irrespective of the value of skipEmittingInitialStateKeys
  Seq(true, false).foreach { skipEmittingInitialStateKeys =>
    testWithAllStateVersions("flatMapGroupsWithState - initial state and initial batch " +
      s"have same keys and skipEmittingInitialStateKeys=$skipEmittingInitialStateKeys") {
      withSQLConf(
        SQLConf.FLATMAPGROUPSWITHSTATE_SKIP_EMITTING_INITIAL_STATE_KEYS.key ->
        skipEmittingInitialStateKeys.toString
      ) {
        val initialState = Seq(
          ("apple", 1L),
          ("orange", 2L)).toDS().groupByKey(_._1).mapValues(_._2)

        val fruitCountFunc = (key: String, values: Iterator[String], state: GroupState[Long]) => {
          val count = state.getOption.map(x => x).getOrElse(0L) + values.size
          state.update(count)
          Iterator.single((key, count))
        }

        val inputData = MemoryStream[String]
        val result =
          inputData.toDS()
            .groupByKey(x => x)
            .flatMapGroupsWithState(Update, NoTimeout(), initialState)(fruitCountFunc)
        testStream(result, Update)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = new StreamManualClock),
          AddData(inputData, "apple"),
          AddData(inputData, "apple"),
          AddData(inputData, "orange"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("apple", 3), ("orange", 3)),
          AddData(inputData, "orange"),
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(("orange", 4)),
          StopStream
        )
      }
    }
  }

  Seq(true, false).foreach { skipEmittingInitialStateKeys =>
    testWithAllStateVersions("flatMapGroupsWithState - batch query and " +
      s"skipEmittingInitialStateKeys=$skipEmittingInitialStateKeys") {
      withSQLConf(SQLConf.FLATMAPGROUPSWITHSTATE_SKIP_EMITTING_INITIAL_STATE_KEYS.key ->
        skipEmittingInitialStateKeys.toString) {
        val initialState = Seq(
          ("apple", 1L),
          ("orange", 2L)).toDS().groupByKey(_._1).mapValues(_._2)

        val fruitCountFunc = (key: String, values: Iterator[String], state: GroupState[Long]) => {
          val count = state.getOption.map(x => x).getOrElse(0L) + values.size
          state.update(count)
          Iterator.single((key, count))
        }

        val inputData = Seq("orange", "mango")
        val result =
          inputData.toDS()
            .groupByKey(x => x)
            .flatMapGroupsWithState(Update, NoTimeout(), initialState)(fruitCountFunc)
        val df = result.toDF()
        if (skipEmittingInitialStateKeys) {
          checkAnswer(df, Seq(("orange", 3), ("mango", 1)).toDF())
        } else {
          checkAnswer(df, Seq(("apple", 1), ("orange", 3), ("mango", 1)).toDF())
        }
      }
    }
  }

  def testWithAllStateVersions(name: String)(func: => Unit): Unit = {
    for (version <- FlatMapGroupsWithStateExecHelper.supportedVersions) {
      test(s"$name - state format version $version") {
        withSQLConf(
          SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION.key -> version.toString,
          SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
          func
        }
      }
    }
  }
}

case class User(name: String, id: String)

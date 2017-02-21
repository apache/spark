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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.sql.KeyedState
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.streaming.{KeyedStateImpl, MemoryStream}
import org.apache.spark.sql.execution.streaming.state.StateStore

/** Class to check custom state types */
case class RunningCount(count: Long)

class MapGroupsWithStateSuite extends StreamTest with BeforeAndAfterAll {

  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("KeyedState - get, exists, update, remove") {
    var state: KeyedStateImpl[String] = null

    def testState(
        expectedData: Option[String],
        shouldBeUpdated: Boolean = false,
        shouldBeRemoved: Boolean = false): Unit = {
      if (expectedData.isDefined) {
        assert(state.exists)
        assert(state.get === expectedData.get)
      } else {
        assert(!state.exists)
        intercept[NoSuchElementException] {
          state.get
        }
      }
      assert(state.getOption === expectedData)
      assert(state.isUpdated === shouldBeUpdated)
      assert(state.isRemoved === shouldBeRemoved)
    }

    // Updating empty state
    state = new KeyedStateImpl[String](None)
    testState(None)
    state.update("")
    testState(Some(""), shouldBeUpdated = true)

    // Updating exiting state
    state = new KeyedStateImpl[String](Some("2"))
    testState(Some("2"))
    state.update("3")
    testState(Some("3"), shouldBeUpdated = true)

    // Removing state
    state.remove()
    testState(None, shouldBeRemoved = true, shouldBeUpdated = false)
    state.remove()      // should be still callable
    state.update("4")
    testState(Some("4"), shouldBeRemoved = false, shouldBeUpdated = true)

    // Updating by null throw exception
    intercept[IllegalArgumentException] {
      state.update(null)
    }
  }

  test("KeyedState - primitive type") {
    var intState = new KeyedStateImpl[Int](None)
    intercept[NoSuchElementException] {
      intState.get
    }
    assert(intState.getOption === None)

    intState = new KeyedStateImpl[Int](Some(10))
    assert(intState.get == 10)
    intState.update(0)
    assert(intState.get == 0)
    intState.remove()
    intercept[NoSuchElementException] {
      intState.get
    }
  }

  test("flatMapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator.empty
      } else {
        state.update(RunningCount(count))
        Iterator((key, count.toString))
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(stateFunc) // State: Int, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckLastBatch(("b", "2")),
      assertNumStateRows(total = 1, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckLastBatch(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("flatMapGroupsWithState - streaming + func returns iterator that updates state lazily") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    // Additionally, it updates state lazily as the returned iterator get consumed
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      values.flatMap { _ =>
        val count = state.getOption.map(_.count).getOrElse(0L) + 1
        if (count == 3) {
          state.remove()
          None
        } else {
          state.update(RunningCount(count))
          Some((key, count.toString))
        }
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(stateFunc) // State: Int, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a", "a", "b"),
      CheckLastBatch(("a", "1"), ("a", "2"), ("b", "1")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckLastBatch(("b", "2")),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1 and
      CheckLastBatch(("a", "1"), ("c", "1"))
    )
  }

  test("flatMapGroupsWithState - batch") {
    // Function that returns running count only if its even, otherwise does not return
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      if (state.exists) throw new IllegalArgumentException("state.exists should be false")
      Iterator((key, values.size))
    }
    checkAnswer(
      Seq("a", "a", "b").toDS.groupByKey(x => x).flatMapGroupsWithState(stateFunc).toDF,
      Seq(("a", 2), ("b", 1)).toDF)
  }

  test("mapGroupsWithState - streaming") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        (key, "-1")
      } else {
        state.update(RunningCount(count))
        (key, count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(total = 1, updated = 1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(total = 2, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and return count as -1
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(total = 1, updated = 2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"), // should recreate state for "a" and return count as 1
      CheckLastBatch(("a", "1"), ("c", "1")),
      assertNumStateRows(total = 3, updated = 2)
    )
  }

  test("mapGroupsWithState - streaming + aggregation") {
    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {

      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        (key, "-1")
      } else {
        state.update(RunningCount(count))
        (key, count.toString)
      }
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)
        .groupByKey(_._1)
        .count()

    testStream(result, Complete)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", 1)),
      AddData(inputData, "a", "b"),
      // mapGroups generates ("a", "2"), ("b", "1"); so increases counts of a and b by 1
      CheckLastBatch(("a", 2), ("b", 1)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"),
      // mapGroups should remove state for "a" and generate ("a", "-1"), ("b", "2") ;
      // so increment a and b by 1
      CheckLastBatch(("a", 3), ("b", 2)),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "c"),
      // mapGroups should recreate state for "a" and generate ("a", "1"), ("c", "1") ;
      // so increment a and c by 1
      CheckLastBatch(("a", 4), ("b", 2), ("c", 1))
    )
  }

  test("mapGroupsWithState - batch") {
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      if (state.exists) throw new IllegalArgumentException("state.exists should be false")
      (key, values.size)
    }

    checkAnswer(
      spark.createDataset(Seq("a", "a", "b"))
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc)
        .toDF,
      spark.createDataset(Seq(("a", 2), ("b", 1))).toDF)
  }

  testQuietly("StateStore.abort on task failure handling") {
    val stateFunc = (key: String, values: Iterator[String], state: KeyedState[RunningCount]) => {
      if (MapGroupsWithStateSuite.failInTask) throw new Exception("expected failure")
      val count = state.getOption.map(_.count).getOrElse(0L) + values.size
      state.update(RunningCount(count))
      (key, count)
    }

    val inputData = MemoryStream[String]
    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)

    def setFailInTask(value: Boolean): AssertOnQuery = AssertOnQuery { q =>
      MapGroupsWithStateSuite.failInTask = value
      true
    }

    testStream(result, Append)(
      setFailInTask(false),
      AddData(inputData, "a"),
      CheckLastBatch(("a", 1L)),
      AddData(inputData, "a"),
      CheckLastBatch(("a", 2L)),
      setFailInTask(true),
      AddData(inputData, "a"),
      ExpectFailure[SparkException](),   // task should fail but should not increment count
      setFailInTask(false),
      StartStream(),
      CheckLastBatch(("a", 3L))     // task should not fail, and should show correct count
    )
  }

  private def assertNumStateRows(total: Long, updated: Long): AssertOnQuery = AssertOnQuery { q =>
    val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
    assert(progressWithData.stateOperators(0).numRowsTotal === total, "incorrect total rows")
    assert(progressWithData.stateOperators(0).numRowsUpdated === updated, "incorrect updates rows")
    true
  }
}

object MapGroupsWithStateSuite {
  var failInTask = true
}

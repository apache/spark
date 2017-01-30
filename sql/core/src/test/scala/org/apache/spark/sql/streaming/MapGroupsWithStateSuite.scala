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

import org.apache.spark.sql.State
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.StateImpl
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore

/** Class to check custom state types */
case class RunningCount(count: Long)

class MapGroupsWithStateSuite extends StreamTest with BeforeAndAfterAll {

  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("state - get, exists, update, remove, ") {
    var state: StateImpl[String] = null

    def testState(
        expectedData: Option[String],
        shouldBeUpdated: Boolean = false,
        shouldBeRemoved: Boolean = false
      ): Unit = {
      if (expectedData.isDefined) {
        assert(state.exists)
        assert(state.get() === expectedData.get)
        assert(state.getOption() === expectedData)
      } else {
        assert(!state.exists)
        intercept[NoSuchElementException] {
          state.get()
        }
        assert(state.getOption() === None)
      }

      assert(state.isUpdated === shouldBeUpdated)
      assert(state.isRemoved === shouldBeRemoved)
    }

    // Updating empty state
    state = StateImpl[String](None)
    testState(None)
    state.update("")
    testState(Some(""), shouldBeUpdated = true)

    // Updating exiting state, even if with null
    state = StateImpl[String](Some("2"))
    testState(Some("2"))
    state.update("3")
    testState(Some("3"), shouldBeUpdated = true)
    state.update(null)
    testState(Some(null), shouldBeUpdated = true)

    // Removing state
    state.remove()
    testState(None, shouldBeRemoved = true, shouldBeUpdated = false)
    state.remove()      // should be still callable
    state.update("4")
    testState(Some("4"), shouldBeRemoved = false, shouldBeUpdated = true)
  }



  // ************* Batch query tests for [flat]mapGroupsWithState *************

  test("batch - mapGroupsWithState") {
    val stateFunc = (key: String, values: Iterator[String], state: State[RunningCount]) => {
      assert(!state.exists)
      assert(state.getOption.isEmpty)
      (key, values.size)
    }

    checkAnswer(
      spark.createDataset(Seq("a", "a", "b"))
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc)
        .toDF,
      spark.createDataset(Seq(("a", 2), ("b", 1))).toDF)
  }

  test("batch - flatMapGroupsWithState") {
    // Function that returns running count only if its even, otherwise does not return
    val stateFunc = (key: String, values: Iterator[String], state: State[RunningCount]) => {
      assert(!state.exists)
      assert(state.getOption.isEmpty)
      if (values.size == 2) {
        Iterator((key, values.size))
      } else Iterator.empty
    }
    checkAnswer(
      Seq("a", "a", "b").toDS.groupByKey(x => x).flatMapGroupsWithState(stateFunc).toDF,
      Seq(("a", 2), ("b", 1)).toDF)
  }

  // ************* Streaming query tests for [flat]mapGroupsWithState *************

  test("streaming - mapGroupsWithState") {
    val inputData = MemoryStream[String]

    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: State[RunningCount]) => {

      var count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        (key, "-1")
      } else {
        state.update(RunningCount(count))
        (key, count.toString)
      }
    }

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState(stateFunc) // Types = State: MyState, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and return count as -1
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(1),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b", "c"), // should recreate state for "a" and return count as 1
      CheckLastBatch(("a", "1"), ("b", "-1"), ("c", "1")),
      assertNumStateRows(2)
    )
  }

  test("streaming - flatMapGroupsWithState") {
    val inputData = MemoryStream[String]

    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count if state is defined, otherwise does not return anything
    val stateFunc = (key: String, values: Iterator[String], state: State[RunningCount]) => {

      var count = state.getOption.map(_.count).getOrElse(0L) + values.size
      if (count == 3) {
        state.remove()
        Iterator.empty
      } else {
        state.update(RunningCount(count))
        Iterator((key, count.toString))
      }
    }

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState(stateFunc) // State: Int, Out: (Str, Str)

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and not return anything for a
      CheckLastBatch( ("b", "2")),
      assertNumStateRows(1),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b", "c"), // should recreate state for "a" and return count as 1 and
      CheckLastBatch(("a", "1"), ("c", "1")),  // ... not return anything for b
      assertNumStateRows(2)
    )
  }

  private def assertNumStateRows(numTotalRows: Long): AssertOnQuery = AssertOnQuery { q =>
    val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
    assert(progressWithData.stateOperators(0).numRowsTotal === numTotalRows)
    true
  }
}

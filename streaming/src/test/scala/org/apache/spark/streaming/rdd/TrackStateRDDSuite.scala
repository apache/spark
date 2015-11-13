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

package org.apache.spark.streaming.rdd

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.util.OpenHashMapBasedStateMap
import org.apache.spark.streaming.{Time, State}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, SparkFunSuite}

class TrackStateRDDSuite extends SparkFunSuite with BeforeAndAfterAll {

  private var sc: SparkContext = null

  override def beforeAll(): Unit = {
    sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("TrackStateRDDSuite"))
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  test("creation from pair RDD") {
    val data = Seq((1, "1"), (2, "2"), (3, "3"))
    val partitioner = new HashPartitioner(10)
    val rdd = TrackStateRDD.createFromPairRDD[Int, Int, String, Int](
      sc.parallelize(data), partitioner, Time(123))
    assertRDD[Int, Int, String, Int](rdd, data.map { x => (x._1, x._2, 123)}.toSet, Set.empty)
    assert(rdd.partitions.size === partitioner.numPartitions)

    assert(rdd.partitioner === Some(partitioner))
  }

  test("updating state and generating emitted data in TrackStateRecord") {

    val initialTime = 1000L
    val updatedTime = 2000L
    val thresholdTime = 1500L
    @volatile var functionCalled = false

    /**
     * Assert that applying given data on a prior record generates correct updated record, with
     * correct state map and emitted data
     */
    def assertRecordUpdate(
        initStates: Iterable[Int],
        data: Iterable[String],
        expectedStates: Iterable[(Int, Long)],
        timeoutThreshold: Option[Long] = None,
        removeTimedoutData: Boolean = false,
        expectedOutput: Iterable[Int] = None,
        expectedTimingOutStates: Iterable[Int] = None,
        expectedRemovedStates: Iterable[Int] = None
      ): Unit = {
      val initialStateMap = new OpenHashMapBasedStateMap[String, Int]()
      initStates.foreach { s => initialStateMap.put("key", s, initialTime) }
      functionCalled = false
      val record = TrackStateRDDRecord[String, Int, Int](initialStateMap, Seq.empty)
      val dataIterator = data.map { v => ("key", v) }.iterator
      val removedStates = new ArrayBuffer[Int]
      val timingOutStates = new ArrayBuffer[Int]
      /**
       * Tracking function that updates/removes state based on instructions in the data, and
       * return state (when instructed or when state is timing out).
       */
      def testFunc(t: Time, key: String, data: Option[String], state: State[Int]): Option[Int] = {
        functionCalled = true

        assert(t.milliseconds === updatedTime, "tracking func called with wrong time")

        data match {
          case Some("noop") =>
            None
          case Some("get-state") =>
            Some(state.getOption().getOrElse(-1))
          case Some("update-state") =>
            if (state.exists) state.update(state.get + 1) else state.update(0)
            None
          case Some("remove-state") =>
            removedStates += state.get()
            state.remove()
            None
          case None =>
            assert(state.isTimingOut() === true, "State is not timing out when data = None")
            timingOutStates += state.get()
            None
          case _ =>
            fail("Unexpected test data")
        }
      }

      val updatedRecord = TrackStateRDDRecord.updateRecordWithData[String, String, Int, Int](
        Some(record), dataIterator, testFunc,
        Time(updatedTime), timeoutThreshold, removeTimedoutData)

      val updatedStateData = updatedRecord.stateMap.getAll().map { x => (x._2, x._3) }
      assert(updatedStateData.toSet === expectedStates.toSet,
        "states do not match after updating the TrackStateRecord")

      assert(updatedRecord.emittedRecords.toSet === expectedOutput.toSet,
        "emitted data do not match after updating the TrackStateRecord")

      assert(timingOutStates.toSet === expectedTimingOutStates.toSet, "timing out states do not " +
        "match those that were expected to do so while updating the TrackStateRecord")

      assert(removedStates.toSet === expectedRemovedStates.toSet, "removed states do not " +
        "match those that were expected to do so while updating the TrackStateRecord")

    }

    // No data, no state should be changed, function should not be called,
    assertRecordUpdate(initStates = Nil, data = None, expectedStates = Nil)
    assert(functionCalled === false)
    assertRecordUpdate(initStates = Seq(0), data = None, expectedStates = Seq((0, initialTime)))
    assert(functionCalled === false)

    // Data present, function should be called irrespective of whether state exists
    assertRecordUpdate(initStates = Seq(0), data = Seq("noop"),
      expectedStates = Seq((0, initialTime)))
    assert(functionCalled === true)
    assertRecordUpdate(initStates = None, data = Some("noop"), expectedStates = None)
    assert(functionCalled === true)

    // Function called with right state data
    assertRecordUpdate(initStates = None, data = Seq("get-state"),
      expectedStates = None, expectedOutput = Seq(-1))
    assertRecordUpdate(initStates = Seq(123), data = Seq("get-state"),
      expectedStates = Seq((123, initialTime)), expectedOutput = Seq(123))

    // Update state and timestamp, when timeout not present
    assertRecordUpdate(initStates = Nil, data = Seq("update-state"),
      expectedStates = Seq((0, updatedTime)))
    assertRecordUpdate(initStates = Seq(0), data = Seq("update-state"),
      expectedStates = Seq((1, updatedTime)))

    // Remove state
    assertRecordUpdate(initStates = Seq(345), data = Seq("remove-state"),
      expectedStates = Nil, expectedRemovedStates = Seq(345))

    // State strictly older than timeout threshold should be timed out
    assertRecordUpdate(initStates = Seq(123), data = Nil,
      timeoutThreshold = Some(initialTime), removeTimedoutData = true,
      expectedStates = Seq((123, initialTime)), expectedTimingOutStates = Nil)

    assertRecordUpdate(initStates = Seq(123), data = Nil,
      timeoutThreshold = Some(initialTime + 1), removeTimedoutData = true,
      expectedStates = Nil, expectedTimingOutStates = Seq(123))

    // State should not be timed out after it has received data
    assertRecordUpdate(initStates = Seq(123), data = Seq("noop"),
      timeoutThreshold = Some(initialTime + 1), removeTimedoutData = true,
      expectedStates = Seq((123, updatedTime)), expectedTimingOutStates = Nil)
    assertRecordUpdate(initStates = Seq(123), data = Seq("remove-state"),
      timeoutThreshold = Some(initialTime + 1), removeTimedoutData = true,
      expectedStates = Nil, expectedTimingOutStates = Nil, expectedRemovedStates = Seq(123))

  }

  test("states generated by TrackStateRDD") {
    val initStates = Seq(("k1", 0), ("k2", 0))
    val initTime = 123
    val initStateWthTime = initStates.map { x => (x._1, x._2, initTime) }.toSet
    val partitioner = new HashPartitioner(2)
    val initStateRDD = TrackStateRDD.createFromPairRDD[String, Int, Int, Int](
      sc.parallelize(initStates), partitioner, Time(initTime)).persist()
    assertRDD(initStateRDD, initStateWthTime, Set.empty)

    val updateTime = 345

    /**
     * Test that the test state RDD, when operated with new data,
     * creates a new state RDD with expected states
     */
    def testStateUpdates(
        testStateRDD: TrackStateRDD[String, Int, Int, Int],
        testData: Seq[(String, Int)],
        expectedStates: Set[(String, Int, Int)]): TrackStateRDD[String, Int, Int, Int] = {

      // Persist the test TrackStateRDD so that its not recomputed while doing the next operation.
      // This is to make sure that we only track which state keys are being touched in the next op.
      testStateRDD.persist().count()

      // To track which keys are being touched
      TrackStateRDDSuite.touchedStateKeys.clear()

      val trackingFunc = (time: Time, key: String, data: Option[Int], state: State[Int]) => {

        // Track the key that has been touched
        TrackStateRDDSuite.touchedStateKeys += key

        // If the data is 0, do not do anything with the state
        // else if the data is 1, increment the state if it exists, or set new state to 0
        // else if the data is 2, remove the state if it exists
        data match {
          case Some(1) =>
            if (state.exists()) { state.update(state.get + 1) }
            else state.update(0)
          case Some(2) =>
            state.remove()
          case _ =>
        }
        None.asInstanceOf[Option[Int]]  // Do not return anything, not being tested
      }
      val newDataRDD = sc.makeRDD(testData).partitionBy(testStateRDD.partitioner.get)

      // Assert that the new state RDD has expected state data
      val newStateRDD = assertOperation(
        testStateRDD, newDataRDD, trackingFunc, updateTime, expectedStates, Set.empty)

      // Assert that the function was called only for the keys present in the data
      assert(TrackStateRDDSuite.touchedStateKeys.size === testData.size,
        "More number of keys are being touched than that is expected")
      assert(TrackStateRDDSuite.touchedStateKeys.toSet === testData.toMap.keys,
        "Keys not in the data are being touched unexpectedly")

      // Assert that the test RDD's data has not changed
      assertRDD(initStateRDD, initStateWthTime, Set.empty)
      newStateRDD
    }

    // Test no-op, no state should change
    testStateUpdates(initStateRDD, Seq(), initStateWthTime)   // should not scan any state
    testStateUpdates(
      initStateRDD, Seq(("k1", 0)), initStateWthTime)         // should not update existing state
    testStateUpdates(
      initStateRDD, Seq(("k3", 0)), initStateWthTime)         // should not create new state

    // Test creation of new state
    val rdd1 = testStateUpdates(initStateRDD, Seq(("k3", 1)), // should create k3's state as 0
      Set(("k1", 0, initTime), ("k2", 0, initTime), ("k3", 0, updateTime)))

    val rdd2 = testStateUpdates(rdd1, Seq(("k4", 1)),         // should create k4's state as 0
      Set(("k1", 0, initTime), ("k2", 0, initTime), ("k3", 0, updateTime), ("k4", 0, updateTime)))

    // Test updating of state
    val rdd3 = testStateUpdates(
      initStateRDD, Seq(("k1", 1)),                   // should increment k1's state 0 -> 1
      Set(("k1", 1, updateTime), ("k2", 0, initTime)))

    val rdd4 = testStateUpdates(rdd3,
      Seq(("x", 0), ("k2", 1), ("k2", 1), ("k3", 1)),  // should update k2, 0 -> 2 and create k3, 0
      Set(("k1", 1, updateTime), ("k2", 2, updateTime), ("k3", 0, updateTime)))

    val rdd5 = testStateUpdates(
      rdd4, Seq(("k3", 1)),                           // should update k3's state 0 -> 2
      Set(("k1", 1, updateTime), ("k2", 2, updateTime), ("k3", 1, updateTime)))

    // Test removing of state
    val rdd6 = testStateUpdates(                      // should remove k1's state
      initStateRDD, Seq(("k1", 2)), Set(("k2", 0, initTime)))

    val rdd7 = testStateUpdates(                      // should remove k2's state
      rdd6, Seq(("k2", 2), ("k0", 2), ("k3", 1)), Set(("k3", 0, updateTime)))

    val rdd8 = testStateUpdates(                      // should remove k3's state
      rdd7, Seq(("k3", 2)), Set())
  }

  /** Assert whether the `trackStateByKey` operation generates expected results */
  private def assertOperation[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
      testStateRDD: TrackStateRDD[K, V, S, T],
      newDataRDD: RDD[(K, V)],
      trackStateFunc: (Time, K, Option[V], State[S]) => Option[T],
      currentTime: Long,
      expectedStates: Set[(K, S, Int)],
      expectedEmittedRecords: Set[T],
      doFullScan: Boolean = false
    ): TrackStateRDD[K, V, S, T] = {

    val partitionedNewDataRDD = if (newDataRDD.partitioner != testStateRDD.partitioner) {
      newDataRDD.partitionBy(testStateRDD.partitioner.get)
    } else {
      newDataRDD
    }

    val newStateRDD = new TrackStateRDD[K, V, S, T](
      testStateRDD, newDataRDD, trackStateFunc, Time(currentTime), None)
    if (doFullScan) newStateRDD.setFullScan()

    // Persist to make sure that it gets computed only once and we can track precisely how many
    // state keys the computing touched
    newStateRDD.persist().count()
    assertRDD(newStateRDD, expectedStates, expectedEmittedRecords)
    newStateRDD
  }

  /** Assert whether the [[TrackStateRDD]] has the expected state ad emitted records */
  private def assertRDD[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
      trackStateRDD: TrackStateRDD[K, V, S, T],
      expectedStates: Set[(K, S, Int)],
      expectedEmittedRecords: Set[T]): Unit = {
    val states = trackStateRDD.flatMap { _.stateMap.getAll() }.collect().toSet
    val emittedRecords = trackStateRDD.flatMap { _.emittedRecords }.collect().toSet
    assert(states === expectedStates,
      "states after track state operation were not as expected")
    assert(emittedRecords === expectedEmittedRecords,
      "emitted records after track state operation were not as expected")
  }
}

object TrackStateRDDSuite {
  private val touchedStateKeys = new ArrayBuffer[String]()
}

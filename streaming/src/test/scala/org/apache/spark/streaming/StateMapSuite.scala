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

package org.apache.spark.streaming

import scala.collection.{immutable, mutable, Map}
import scala.reflect.ClassTag
import scala.util.Random

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer._
import org.apache.spark.streaming.rdd.MapWithStateRDDRecord
import org.apache.spark.streaming.util.{EmptyStateMap, OpenHashMapBasedStateMap, StateMap}

class StateMapSuite extends SparkFunSuite {

  private val conf = new SparkConf()

  test("EmptyStateMap") {
    val map = new EmptyStateMap[Int, Int]
    intercept[scala.NotImplementedError] {
      map.put(1, 1, 1)
    }
    assert(map.get(1) === None)
    assert(map.getByTime(10000).isEmpty)
    assert(map.getAll().isEmpty)
    map.remove(1)   // no exception
    assert(map.copy().eq(map))
  }

  test("OpenHashMapBasedStateMap - put, get, getByTime, getAll, remove") {
    val map = new OpenHashMapBasedStateMap[Int, Int]()

    map.put(1, 100, 10)
    assert(map.get(1) === Some(100))
    assert(map.get(2) === None)
    assert(map.getByTime(11).toSet === Set((1, 100, 10)))
    assert(map.getByTime(10).toSet === Set.empty)
    assert(map.getByTime(9).toSet === Set.empty)
    assert(map.getAll().toSet === Set((1, 100, 10)))

    map.put(2, 200, 20)
    assert(map.getByTime(21).toSet === Set((1, 100, 10), (2, 200, 20)))
    assert(map.getByTime(11).toSet === Set((1, 100, 10)))
    assert(map.getByTime(10).toSet === Set.empty)
    assert(map.getByTime(9).toSet === Set.empty)
    assert(map.getAll().toSet === Set((1, 100, 10), (2, 200, 20)))

    map.remove(1)
    assert(map.get(1) === None)
    assert(map.getAll().toSet === Set((2, 200, 20)))
  }

  test("OpenHashMapBasedStateMap - put, get, getByTime, getAll, remove with copy") {
    val parentMap = new OpenHashMapBasedStateMap[Int, Int]()
    parentMap.put(1, 100, 1)
    parentMap.put(2, 200, 2)
    parentMap.remove(1)

    // Create child map and make changes
    val map = parentMap.copy()
    assert(map.get(1) === None)
    assert(map.get(2) === Some(200))
    assert(map.getByTime(10).toSet === Set((2, 200, 2)))
    assert(map.getByTime(2).toSet === Set.empty)
    assert(map.getAll().toSet === Set((2, 200, 2)))

    // Add new items
    map.put(3, 300, 3)
    assert(map.get(3) === Some(300))
    map.put(4, 400, 4)
    assert(map.get(4) === Some(400))
    assert(map.getByTime(10).toSet === Set((2, 200, 2), (3, 300, 3), (4, 400, 4)))
    assert(map.getByTime(4).toSet === Set((2, 200, 2), (3, 300, 3)))
    assert(map.getAll().toSet === Set((2, 200, 2), (3, 300, 3), (4, 400, 4)))
    assert(parentMap.getAll().toSet === Set((2, 200, 2)))

    // Remove items
    map.remove(4)
    assert(map.get(4) === None)       // item added in this map, then removed in this map
    map.remove(2)
    assert(map.get(2) === None)       // item removed in parent map, then added in this map
    assert(map.getAll().toSet === Set((3, 300, 3)))
    assert(parentMap.getAll().toSet === Set((2, 200, 2)))

    // Update items
    map.put(1, 1000, 100)
    assert(map.get(1) === Some(1000)) // item removed in parent map, then added in this map
    map.put(2, 2000, 200)
    assert(map.get(2) === Some(2000)) // item added in parent map, then removed + added in this map
    map.put(3, 3000, 300)
    assert(map.get(3) === Some(3000)) // item added + updated in this map
    map.put(4, 4000, 400)
    assert(map.get(4) === Some(4000)) // item removed + updated in this map

    assert(map.getAll().toSet ===
      Set((1, 1000, 100), (2, 2000, 200), (3, 3000, 300), (4, 4000, 400)))
    assert(parentMap.getAll().toSet === Set((2, 200, 2)))

    map.remove(2)         // remove item present in parent map, so that its not visible in child map

    // Create child map and see availability of items
    val childMap = map.copy()
    assert(childMap.getAll().toSet === map.getAll().toSet)
    assert(childMap.get(1) === Some(1000))  // item removed in grandparent, but added in parent map
    assert(childMap.get(2) === None)        // item added in grandparent, but removed in parent map
    assert(childMap.get(3) === Some(3000))  // item added and updated in parent map

    childMap.put(2, 20000, 200)
    assert(childMap.get(2) === Some(20000)) // item map
  }

  test("OpenHashMapBasedStateMap - serializing and deserializing") {
    val map1 = new OpenHashMapBasedStateMap[Int, Int]()
    testSerialization(map1, "error deserializing and serialized empty map")

    map1.put(1, 100, 1)
    map1.put(2, 200, 2)
    testSerialization(map1, "error deserializing and serialized map with data + no delta")

    val map2 = map1.copy().asInstanceOf[OpenHashMapBasedStateMap[Int, Int]]
    // Do not test compaction
    assert(map2.shouldCompact === false)
    testSerialization(map2, "error deserializing and serialized map with 1 delta + no new data")

    map2.put(3, 300, 3)
    map2.put(4, 400, 4)
    testSerialization(map2, "error deserializing and serialized map with 1 delta + new data")

    val map3 = map2.copy().asInstanceOf[OpenHashMapBasedStateMap[Int, Int]]
    assert(map3.shouldCompact === false)
    testSerialization(map3, "error deserializing and serialized map with 2 delta + no new data")
    map3.put(3, 600, 3)
    map3.remove(2)
    testSerialization(map3, "error deserializing and serialized map with 2 delta + new data")
  }

  test("OpenHashMapBasedStateMap - serializing and deserializing with compaction") {
    val targetDeltaLength = 10
    val deltaChainThreshold = 5

    var map = new OpenHashMapBasedStateMap[Int, Int](
      deltaChainThreshold = deltaChainThreshold)

    // Make large delta chain with length more than deltaChainThreshold
    for(i <- 1 to targetDeltaLength) {
      map.put(Random.nextInt(), Random.nextInt(), 1)
      map = map.copy().asInstanceOf[OpenHashMapBasedStateMap[Int, Int]]
    }
    assert(map.deltaChainLength > deltaChainThreshold)
    assert(map.shouldCompact === true)

    val deser_map = testSerialization(map, "Deserialized + compacted map not same as original map")
    assert(deser_map.deltaChainLength < deltaChainThreshold)
    assert(deser_map.shouldCompact === false)
  }

  test("OpenHashMapBasedStateMap - all possible sequences of operations with copies ") {
    /*
     * This tests the map using all permutations of sequences operations, across multiple map
     * copies as well as between copies. It is to ensure complete coverage, though it is
     * kind of hard to debug this. It is set up as follows.
     *
     * - For any key, there can be 2 types of update ops on a state map - put or remove
     *
     * - These operations are done on a test map in "sets". After each set, the map is "copied"
     *   to create a new map, and the next set of operations are done on the new one. This tests
     *   whether the map data persist correctly across copies.
     *
     * - Within each set, there are a number of operations to test whether the map correctly
     *   updates and removes data without affecting the parent state map.
     *
     * - Overall this creates (numSets * numOpsPerSet) operations, each of which that can 2 types
     *   of operations. This leads to a total of [2 ^ (numSets * numOpsPerSet)] different sequence
     *   of operations, which we will test with different keys.
     *
     * Example: With numSets = 2, and numOpsPerSet = 2 give numTotalOps = 4. This means that
     * 2 ^ 4 = 16 possible permutations needs to be tested using 16 keys.
     * _______________________________________________
     * |         |      Set1       |     Set2        |
     * |         |-----------------|-----------------|
     * |         |   Op1    Op2   |c|   Op3    Op4   |
     * |---------|----------------|o|----------------|
     * | key 0   |   put    put   |p|   put    put   |
     * | key 1   |   put    put   |y|   put    rem   |
     * | key 2   |   put    put   | |   rem    put   |
     * | key 3   |   put    put   |t|   rem    rem   |
     * | key 4   |   put    rem   |h|   put    put   |
     * | key 5   |   put    rem   |e|   put    rem   |
     * | key 6   |   put    rem   | |   rem    put   |
     * | key 7   |   put    rem   |s|   rem    rem   |
     * | key 8   |   rem    put   |t|   put    put   |
     * | key 9   |   rem    put   |a|   put    rem   |
     * | key 10  |   rem    put   |t|   rem    put   |
     * | key 11  |   rem    put   |e|   rem    rem   |
     * | key 12  |   rem    rem   | |   put    put   |
     * | key 13  |   rem    rem   |m|   put    rem   |
     * | key 14  |   rem    rem   |a|   rem    put   |
     * | key 15  |   rem    rem   |p|   rem    rem   |
     * |_________|________________|_|________________|
     */

    val numTypeMapOps = 2   // 0 = put a new value, 1 = remove value
    val numSets = 3
    val numOpsPerSet = 3    // to test seq of ops like update -> remove -> update in same set
    val numTotalOps = numOpsPerSet * numSets
    val numKeys = math.pow(numTypeMapOps, numTotalOps).toInt  // to get all combinations of ops

    val refMap = new mutable.HashMap[Int, (Int, Long)]()
    var prevSetRefMap: immutable.Map[Int, (Int, Long)] = null

    var stateMap: StateMap[Int, Int] = new OpenHashMapBasedStateMap[Int, Int]()
    var prevSetStateMap: StateMap[Int, Int] = null

    var time = 1L

    for (setId <- 0 until numSets) {
      for (opInSetId <- 0 until numOpsPerSet) {
        val opId = setId * numOpsPerSet + opInSetId
        for (keyId <- 0 until numKeys) {
          time += 1
          // Find the operation type that needs to be done
          // This is similar to finding the nth bit value of a binary number
          // E.g.  nth bit from the right of any binary number B is [ B / (2 ^ (n - 1)) ] % 2
          val opCode =
            (keyId / math.pow(numTypeMapOps, numTotalOps - opId - 1).toInt) % numTypeMapOps
          opCode match {
            case 0 =>
              val value = Random.nextInt()
              stateMap.put(keyId, value, time)
              refMap.put(keyId, (value, time))
            case 1 =>
              stateMap.remove(keyId)
              refMap.remove(keyId)
          }
        }

        // Test whether the current state map after all key updates is correct
        assertMap(stateMap, refMap, time, "State map does not match reference map")

        // Test whether the previous map before copy has not changed
        if (prevSetStateMap != null && prevSetRefMap != null) {
          assertMap(prevSetStateMap, prevSetRefMap, time,
            "Parent state map somehow got modified, does not match corresponding reference map")
        }
      }

      // Copy the map and remember the previous maps for future tests
      prevSetStateMap = stateMap
      prevSetRefMap = refMap.toMap
      stateMap = stateMap.copy()

      // Assert that the copied map has the same data
      assertMap(stateMap, prevSetRefMap, time,
        "State map does not match reference map after copying")
    }
    assertMap(stateMap, refMap.toMap, time, "Final state map does not match reference map")
  }

  private def testSerialization[T: ClassTag](
      map: OpenHashMapBasedStateMap[T, T], msg: String): OpenHashMapBasedStateMap[T, T] = {
    testSerialization(new JavaSerializer(conf), map, msg)
    testSerialization(new KryoSerializer(conf), map, msg)
  }

  private def testSerialization[T: ClassTag](
      serializer: Serializer,
      map: OpenHashMapBasedStateMap[T, T],
      msg: String): OpenHashMapBasedStateMap[T, T] = {
    val deserMap = serializeAndDeserialize(serializer, map)
    assertMap(deserMap, map, 1, msg)
    deserMap
  }

  // Assert whether all the data and operations on a state map matches that of a reference state map
  private def assertMap[T](
      mapToTest: StateMap[T, T],
      refMapToTestWith: StateMap[T, T],
      time: Long,
      msg: String): Unit = {
    withClue(msg) {
      // Assert all the data is same as the reference map
      assert(mapToTest.getAll().toSet === refMapToTestWith.getAll().toSet)

      // Assert that get on every key returns the right value
      for (keyId <- refMapToTestWith.getAll().map { _._1 }) {
        assert(mapToTest.get(keyId) === refMapToTestWith.get(keyId))
      }

      // Assert that every time threshold returns the correct data
      for (t <- 0L to (time + 1)) {
        assert(mapToTest.getByTime(t).toSet ===  refMapToTestWith.getByTime(t).toSet)
      }
    }
  }

  // Assert whether all the data and operations on a state map matches that of a reference map
  private def assertMap(
      mapToTest: StateMap[Int, Int],
      refMapToTestWith: Map[Int, (Int, Long)],
      time: Long,
      msg: String): Unit = {
    withClue(msg) {
      // Assert all the data is same as the reference map
      assert(mapToTest.getAll().toSet ===
        refMapToTestWith.iterator.map { x => (x._1, x._2._1, x._2._2) }.toSet)

      // Assert that get on every key returns the right value
      for (keyId <- refMapToTestWith.keys) {
        assert(mapToTest.get(keyId) === refMapToTestWith.get(keyId).map { _._1 })
      }

      // Assert that every time threshold returns the correct data
      for (t <- 0L to (time + 1)) {
        val expectedRecords =
          refMapToTestWith.iterator.filter { _._2._2 < t }.map { x => (x._1, x._2._1, x._2._2) }
        assert(mapToTest.getByTime(t).toSet ===  expectedRecords.toSet)
      }
    }
  }

  test("OpenHashMapBasedStateMap - serializing and deserializing with KryoSerializable states") {
    val map = new OpenHashMapBasedStateMap[KryoState, KryoState]()
    map.put(new KryoState("a"), new KryoState("b"), 1)
    testSerialization(
      new KryoSerializer(conf), map, "error deserializing and serialized KryoSerializable states")
  }

  test("EmptyStateMap - serializing and deserializing") {
    val map = StateMap.empty[KryoState, KryoState]
    // Since EmptyStateMap doesn't contains any date, KryoState won't break JavaSerializer.
    assert(serializeAndDeserialize(new JavaSerializer(conf), map).
      isInstanceOf[EmptyStateMap[KryoState, KryoState]])
    assert(serializeAndDeserialize(new KryoSerializer(conf), map).
      isInstanceOf[EmptyStateMap[KryoState, KryoState]])
  }

  test("MapWithStateRDDRecord - serializing and deserializing with KryoSerializable states") {
    val map = new OpenHashMapBasedStateMap[KryoState, KryoState]()
    map.put(new KryoState("a"), new KryoState("b"), 1)

    val record =
      MapWithStateRDDRecord[KryoState, KryoState, KryoState](map, Seq(new KryoState("c")))
    val deserRecord = serializeAndDeserialize(new KryoSerializer(conf), record)
    assert(!(record eq deserRecord))
    assert(record.stateMap.getAll().toSeq === deserRecord.stateMap.getAll().toSeq)
    assert(record.mappedData === deserRecord.mappedData)
  }

  private def serializeAndDeserialize[T: ClassTag](serializer: Serializer, t: T): T = {
    val serializerInstance = serializer.newInstance()
    serializerInstance.deserialize[T](
      serializerInstance.serialize(t), Thread.currentThread().getContextClassLoader)
  }
}

/** A class that only supports Kryo serialization. */
private[streaming] final class KryoState(var state: String) extends KryoSerializable {

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, state)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    state = kryo.readClassAndObject(input).asInstanceOf[String]
  }

  override def equals(other: Any): Boolean = other match {
    case that: KryoState => state == that.state
    case _ => false
  }

  override def hashCode(): Int = {
    if (state == null) 0 else state.hashCode()
  }
}

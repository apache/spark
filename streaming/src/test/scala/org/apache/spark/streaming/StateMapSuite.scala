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

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.streaming.util.{OpenHashMapBasedStateMap, StateMap}
import org.apache.spark.util.Utils

class StateMapSuite extends SparkFunSuite {

  test("OpenHashMapBasedStateMap - put, get, getall, remove") {
    val map = new OpenHashMapBasedStateMap[Int, Int]()

    map.put(1, 100, 10)
    assert(map.get(1) === Some(100))
    assert(map.get(2) === None)
    map.put(2, 200, 20)
    assert(map.getAll().toSet === Set((1, 100, 10), (2, 200, 20)))

    map.remove(1)
    assert(map.get(1) === None)
    assert(map.getAll().toSet === Set((2, 200, 20)))
  }

  test("OpenHashMapBasedStateMap - put, get, getall, remove after copy") {
    val parentMap = new OpenHashMapBasedStateMap[Int, Int]()
    parentMap.put(1, 100, 1)
    parentMap.put(2, 200, 2)
    parentMap.remove(1)

    // Create child map and make changes
    val map = parentMap.copy()
    assert(map.getAll().toSet === Set((2, 200, 2)))
    assert(map.get(1) === None)
    assert(map.get(2) === Some(200))

    // Add new items
    map.put(3, 300, 3)
    assert(map.get(3) === Some(300))
    map.put(4, 400, 4)
    assert(map.get(4) === Some(400))
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

  test("OpenHashMapBasedStateMap - all operation combo testing with copies ") {
    val numTypeMapOps = 2  // 0 = put a new value, 1 = remove value
    val numMapCopies = 4   // to test all combos of operations across 4 copies
    val numOpsPerCopy = numTypeMapOps
    val numTotalOps = numOpsPerCopy * numMapCopies
    val numKeys = math.pow(numTypeMapOps, numTotalOps).toInt  // to get all combinations of ops

    var stateMap: StateMap[Int, Int] = new OpenHashMapBasedStateMap[Int, Int]()
    val refMap = new mutable.HashMap[Int, Int]()

    def assertMap(): Unit = {
      assert(stateMap.getAll().map { x => (x._1, x._2) }.toSet === refMap.iterator.toSet)
      for (keyId <- 0 until numKeys) {
        assert(stateMap.get(keyId) === refMap.get(keyId))
      }
    }

    /**
     * Example: Operations combinations with 2 map copies
     *
     * -----------------------------------------------
     * |         |      Copy1      |     Copy2       |
     * |         |-----------------|-----------------|
     * |         |   Op1    Op2    |    Op3    Op4   |
     * | --------|-----------------|-----------------|
     * | key 0   |   put    put   | |   put    put   |
     * | key 1   |   put    put   | |   put    rem   |
     * | key 2   |   put    put   |c|   rem    put   |
     * | key 3   |   put    put   |o|   rem    rem   |
     * | key 4   |   put    rem   |p|   put    put   |
     * | key 5   |   put    rem   |y|   put    rem   |
     * | key 6   |   put    rem   | |   rem    put   |
     * | key 7   |   put    rem   |t|   rem    rem   |
     * | key 8   |   rem    put   |h|   put    put   |
     * | key 9   |   rem    put   |e|   put    rem   |
     * | key 10  |   rem    put   | |   rem    put   |
     * | key 11  |   rem    put   |m|   rem    rem   |
     * | key 12  |   rem    rem   |a|   put    put   |
     * | key 13  |   rem    rem   |p|   put    rem   |
     * | key 14  |   rem    rem   | |   rem    put   |
     * | key 15  |   rem    rem   | |   rem    rem   |
     * -----------------------------------------------
     */


    for(opId <- 0 until numTotalOps) {
      for (keyId <- 0 until numKeys) {
        // Find the operation type that needs to be done
        // This is similar to finding the nth bit value of a binary number
        // E.g.  nth bit from the right of any binary number B is [ B / (2 ^ (n - 1)) ] % 2
        val opCode = (keyId / math.pow(numTypeMapOps, numTotalOps - opId - 1).toInt) % numTypeMapOps
        opCode match {
          case 0 =>
            val value = Random.nextInt()
            stateMap.put(keyId, value, value * 2)
            refMap.put(keyId, value)
          case 1 =>
            stateMap.remove(keyId)
            refMap.remove(keyId)
        }
      }
      if (opId % numOpsPerCopy == 0) {
        assertMap()
        stateMap = stateMap.copy()
      }
    }
    assertMap()
  }

  test("OpenHashMapBasedStateMap - serializing and deserializing") {
    val map1 = new OpenHashMapBasedStateMap[Int, Int]()
    map1.put(1, 100, 1)
    map1.put(2, 200, 2)

    val map2 = map1.copy()
    map2.put(3, 300, 3)
    map2.put(4, 400, 4)

    val map3 = map2.copy()
    map3.put(3, 600, 3)
    map3.remove(2)

    // Do not test compaction
    assert(map3.asInstanceOf[OpenHashMapBasedStateMap[_, _]].shouldCompact === false)

    val map3_ = Utils.deserialize[StateMap[Int, Int]](
      Utils.serialize(map3), Thread.currentThread().getContextClassLoader)
    assert(map3_.getAll().toSet === map3.getAll().toSet)
    assert(map3.getAll().forall { case (key, state, _) => map3_.get(key) === Some(state)})
  }

  test("OpenHashMapBasedStateMap - serializing and deserializing with compaction") {
    val targetDeltaLength = 10
    val deltaChainThreshold = 5

    var map = new OpenHashMapBasedStateMap[Int, Int](
      deltaChainThreshold = deltaChainThreshold)

    // Make large delta chain with length more than deltaChainThreshold
    for(i <- 1 to targetDeltaLength) {
      map.put(Random.nextInt(), Random.nextInt(), Random.nextLong())
      map = map.copy().asInstanceOf[OpenHashMapBasedStateMap[Int, Int]]
    }
    assert(map.deltaChainLength > deltaChainThreshold)
    assert(map.shouldCompact === true)

    val deser_map = Utils.deserialize[OpenHashMapBasedStateMap[Int, Int]](
      Utils.serialize(map), Thread.currentThread().getContextClassLoader)
    assert(deser_map.deltaChainLength < deltaChainThreshold)
    assert(deser_map.shouldCompact === false)
    assert(deser_map.getAll().toSet === map.getAll().toSet)
    assert(map.getAll().forall { case (key, state, _) => deser_map.get(key) === Some(state)})
  }
}
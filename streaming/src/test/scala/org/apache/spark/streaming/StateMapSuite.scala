package org.apache.spark.streaming

import scala.reflect._
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.streaming.dstream.{StateMap, OpenHashMapBasedStateMap}
import org.apache.spark.util.Utils

class StateMapSuite extends SparkFunSuite {

  test("OpenHashMapBasedStateMap - basic operations") {
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

  test("OpenHashMapBasedStateMap - basic operations after copy") {
    val parentMap = new OpenHashMapBasedStateMap[Int, Int]()
    parentMap.put(1, 100, 1)
    parentMap.put(2, 200, 2)
    parentMap.remove(1)

    val map = parentMap.copy()
    assert(map.getAll().toSet === Set((2, 200, 2)))

    // Add new items
    map.put(3, 300, 3)
    map.put(4, 400, 4)
    assert(map.getAll().toSet === Set((2, 200, 2), (3, 300, 3), (4, 400, 4)))
    assert(parentMap.getAll().toSet === Set((2, 200, 2)))

    // Remove items
    map.remove(4)   // remove item added to this map
    map.remove(2)   // remove item remove in parent map
    assert(map.getAll().toSet === Set((3, 300, 3)))
    assert(parentMap.getAll().toSet === Set((2, 200, 2)))

    // Update items
    map.put(1, 1000, 100) // update item removed in parent map
    map.put(2, 2000, 200) // update item added in parent map and removed in this map
    map.put(3, 3000, 300) // update item added in this map
    map.put(4, 4000, 400) // update item removed in this map

    assert(map.getAll().toSet ===
      Set((1, 1000, 100), (2, 2000, 200), (3, 3000, 300), (4, 4000, 400)))
    assert(parentMap.getAll().toSet === Set((2, 200, 2)))
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

    val map3_ = Utils.deserialize[StateMap[Int, Int]](Utils.serialize(map3), Thread.currentThread().getContextClassLoader)
    assert(map3_.getAll().toSet === map3.getAll().toSet)
    assert(map3.getAll().forall { case (key, state, _) => map3_.get(key) === Some(state)})
  }

  test("OpenHashMapBasedStateMap - serializing and deserializing with compaction") {
    val targetDeltaLength = 10
    val deltaChainThreshold = 5

    var map = new OpenHashMapBasedStateMap[Int, Int](
      deltaChainThreshold = deltaChainThreshold)

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
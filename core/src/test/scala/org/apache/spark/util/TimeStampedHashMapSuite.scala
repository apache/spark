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

package org.apache.spark.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkFunSuite

class TimeStampedHashMapSuite extends SparkFunSuite {

  // Test the testMap function - a Scala HashMap should obviously pass
  testMap(new mutable.HashMap[String, String]())

  // Test TimeStampedHashMap basic functionality
  testMap(new TimeStampedHashMap[String, String]())
  testMapThreadSafety(new TimeStampedHashMap[String, String]())

  test("TimeStampedHashMap - clearing by timestamp") {
    // clearing by insertion time
    val map = new TimeStampedHashMap[String, String](updateTimeStampOnGet = false)
    map("k1") = "v1"
    assert(map("k1") === "v1")
    Thread.sleep(10)
    val threshTime = System.currentTimeMillis
    assert(map.getTimestamp("k1").isDefined)
    assert(map.getTimestamp("k1").get < threshTime)
    map.clearOldValues(threshTime)
    assert(map.get("k1") === None)

    // clearing by modification time
    val map1 = new TimeStampedHashMap[String, String](updateTimeStampOnGet = true)
    map1("k1") = "v1"
    map1("k2") = "v2"
    assert(map1("k1") === "v1")
    Thread.sleep(10)
    val threshTime1 = System.currentTimeMillis
    Thread.sleep(10)
    assert(map1("k2") === "v2")     // access k2 to update its access time to > threshTime
    assert(map1.getTimestamp("k1").isDefined)
    assert(map1.getTimestamp("k1").get < threshTime1)
    assert(map1.getTimestamp("k2").isDefined)
    assert(map1.getTimestamp("k2").get >= threshTime1)
    map1.clearOldValues(threshTime1) // should only clear k1
    assert(map1.get("k1") === None)
    assert(map1.get("k2").isDefined)
  }

  /** Test basic operations of a Scala mutable Map. */
  def testMap(hashMapConstructor: => mutable.Map[String, String]) {
    def newMap() = hashMapConstructor
    val testMap1 = newMap()
    val testMap2 = newMap()
    val name = testMap1.getClass.getSimpleName

    test(name + " - basic test") {
      // put, get, and apply
      testMap1 += (("k1", "v1"))
      assert(testMap1.get("k1").isDefined)
      assert(testMap1.get("k1").get === "v1")
      testMap1("k2") = "v2"
      assert(testMap1.get("k2").isDefined)
      assert(testMap1.get("k2").get === "v2")
      assert(testMap1("k2") === "v2")
      testMap1.update("k3", "v3")
      assert(testMap1.get("k3").isDefined)
      assert(testMap1.get("k3").get === "v3")

      // remove
      testMap1.remove("k1")
      assert(testMap1.get("k1").isEmpty)
      testMap1.remove("k2")
      intercept[NoSuchElementException] {
        testMap1("k2") // Map.apply(<non-existent-key>) causes exception
      }
      testMap1 -= "k3"
      assert(testMap1.get("k3").isEmpty)

      // multi put
      val keys = (1 to 100).map(_.toString)
      val pairs = keys.map(x => (x, x * 2))
      assert((testMap2 ++ pairs).iterator.toSet === pairs.toSet)
      testMap2 ++= pairs

      // iterator
      assert(testMap2.iterator.toSet === pairs.toSet)

      // filter
      val filtered = testMap2.filter { case (_, v) => v.toInt % 2 == 0 }
      val evenPairs = pairs.filter { case (_, v) => v.toInt % 2 == 0 }
      assert(filtered.iterator.toSet === evenPairs.toSet)

      // foreach
      val buffer = new ArrayBuffer[(String, String)]
      testMap2.foreach(x => buffer += x)
      assert(testMap2.toSet === buffer.toSet)

      // multi remove
      testMap2("k1") = "v1"
      testMap2 --= keys
      assert(testMap2.size === 1)
      assert(testMap2.iterator.toSeq.head === ("k1", "v1"))

      // +
      val testMap3 = testMap2 + (("k0", "v0"))
      assert(testMap3.size === 2)
      assert(testMap3.get("k1").isDefined)
      assert(testMap3.get("k1").get === "v1")
      assert(testMap3.get("k0").isDefined)
      assert(testMap3.get("k0").get === "v0")

      // -
      val testMap4 = testMap3 - "k0"
      assert(testMap4.size === 1)
      assert(testMap4.get("k1").isDefined)
      assert(testMap4.get("k1").get === "v1")
    }
  }

  /** Test thread safety of a Scala mutable map. */
  def testMapThreadSafety(hashMapConstructor: => mutable.Map[String, String]) {
    def newMap() = hashMapConstructor
    val name = newMap().getClass.getSimpleName
    val testMap = newMap()
    @volatile var error = false

    def getRandomKey(m: mutable.Map[String, String]): Option[String] = {
      val keys = testMap.keysIterator.toSeq
      if (keys.nonEmpty) {
        Some(keys(Random.nextInt(keys.size)))
      } else {
        None
      }
    }

    val threads = (1 to 25).map(i => new Thread() {
      override def run() {
        try {
          for (j <- 1 to 1000) {
            Random.nextInt(3) match {
              case 0 =>
                testMap(Random.nextString(10)) = Random.nextDouble().toString // put
              case 1 =>
                getRandomKey(testMap).map(testMap.get) // get
              case 2 =>
                getRandomKey(testMap).map(testMap.remove) // remove
            }
          }
        } catch {
          case t: Throwable =>
            error = true
            throw t
        }
      }
    })

    test(name + " - threading safety test")  {
      threads.map(_.start)
      threads.map(_.join)
      assert(!error)
    }
  }
}

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

import java.lang.ref.WeakReference
import java.util

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.Random

import org.scalatest.FunSuite

class WrappedJavaHashMapSuite extends FunSuite {

  // Test the testMap function - a Scala HashMap should obviously pass
  testMap(new HashMap[String, String]())

  // Test a simple WrappedJavaHashMap
  testMap(new TestMap[String, String]())

  // Test TimeStampedHashMap
  testMap(new TimeStampedHashMap[String, String])

  testMapThreadSafety(new TimeStampedHashMap[String, String])

  test("TimeStampedHashMap - clearing by timestamp") {
    // clearing by insertion time
    val map = new TimeStampedHashMap[String, String](false)
    map("k1") = "v1"
    assert(map("k1") === "v1")
    Thread.sleep(10)
    val threshTime = System.currentTimeMillis()
    assert(map.internalMap.get("k1").timestamp < threshTime)
    map.clearOldValues(threshTime)
    assert(map.get("k1") === None)

    // clearing by modification time
    val map1 = new TimeStampedHashMap[String, String](true)
    map1("k1") = "v1"
    map1("k2") = "v2"
    assert(map1("k1") === "v1")
    Thread.sleep(10)
    val threshTime1 = System.currentTimeMillis()
    Thread.sleep(10)
    assert(map1("k2") === "v2")     // access k2 to update its access time to > threshTime
    assert(map1.internalMap.get("k1").timestamp < threshTime1)
    assert(map1.internalMap.get("k2").timestamp >= threshTime1)
    map1.clearOldValues(threshTime1) //should only clear k1
    assert(map1.get("k1") === None)
    assert(map1.get("k2").isDefined)
  }

  // Test TimeStampedHashMap
  testMap(new TimeStampedWeakValueHashMap[String, String])

  testMapThreadSafety(new TimeStampedWeakValueHashMap[String, String])

  test("TimeStampedWeakValueHashMap - clearing by timestamp") {
    // clearing by insertion time
    val map = new TimeStampedWeakValueHashMap[String, String]()
    map("k1") = "v1"
    assert(map("k1") === "v1")
    Thread.sleep(10)
    val threshTime = System.currentTimeMillis()
    assert(map.internalJavaMap.get("k1").timestamp < threshTime)
    map.clearOldValues(threshTime)
    assert(map.get("k1") === None)
  }


  test("TimeStampedWeakValueHashMap - get not returning null when weak reference is cleared") {
    var strongRef = new Object
    val weakRef = new WeakReference(strongRef)
    val map = new TimeStampedWeakValueHashMap[String, Object]

    map("k1") = strongRef
    assert(map("k1") === strongRef)

    strongRef = null
    val startTime = System.currentTimeMillis
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    System.runFinalization()  // Make a best effort to call finalizer on all cleaned objects.
    while(System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      System.runFinalization()
      Thread.sleep(100)
    }
    assert(map.internalJavaMap.get("k1").weakValue.get == null)
    assert(map.get("k1") === None)

    // TODO (TD): Test clearing of null-value pairs
  }

  def testMap(hashMapConstructor: => Map[String, String]) {
    def newMap() = hashMapConstructor

    val name = newMap().getClass.getSimpleName

    test(name + " - basic test") {
      val testMap1 = newMap()

      // put and get
      testMap1 += (("k1", "v1"))
      assert(testMap1.get("k1").get === "v1")
      testMap1("k2") = "v2"
      assert(testMap1.get("k2").get === "v2")
      assert(testMap1("k2") === "v2")

      // remove
      testMap1.remove("k1")
      assert(testMap1.get("k1").isEmpty)
      testMap1.remove("k2")
      intercept[Exception] {
        testMap1("k2") // Map.apply(<non-existent-key>) causes exception
      }

      // multi put
      val keys = (1 to 100).map(_.toString)
      val pairs = keys.map(x => (x, x * 2))
      val testMap2 = newMap()
      assert((testMap2 ++ pairs).iterator.toSet === pairs.toSet)
      testMap2 ++= pairs

      // iterator
      assert(testMap2.iterator.toSet === pairs.toSet)
      testMap2("k1") = "v1"

      // foreach
      val buffer = new ArrayBuffer[(String, String)]
      testMap2.foreach(x => buffer += x)
      assert(testMap2.toSet === buffer.toSet)

      // multi remove
      testMap2 --= keys
      assert(testMap2.size === 1)
      assert(testMap2.iterator.toSeq.head === ("k1", "v1"))
    }
  }

  def testMapThreadSafety(hashMapConstructor: => Map[String, String]) {
    def newMap() = hashMapConstructor

    val name = newMap().getClass.getSimpleName
    val testMap = newMap()
    @volatile var error = false

    def getRandomKey(m: Map[String, String]): Option[String] = {
      val keys = testMap.keysIterator.toSeq
      if (keys.nonEmpty) {
        Some(keys(Random.nextInt(keys.size)))
      } else {
        None
      }
    }

    val threads = (1 to 100).map(i => new Thread() {
      override def run() {
        try {
          for (j <- 1 to 1000) {
            Random.nextInt(3) match {
              case 0 =>
                testMap(Random.nextString(10)) = Random.nextDouble.toString // put
              case 1 =>
                getRandomKey(testMap).map(testMap.get) // get
              case 2 =>
                getRandomKey(testMap).map(testMap.remove) // remove
            }
          }
        } catch {
          case t : Throwable =>
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

class TestMap[A, B] extends WrappedJavaHashMap[A, B, A, B] {
  private[util] val internalJavaMap: util.Map[A, B] = new util.HashMap[A, B]()

  private[util] def newInstance[K1, V1](): WrappedJavaHashMap[K1, V1, _, _] = {
    new TestMap[K1, V1]
  }
}

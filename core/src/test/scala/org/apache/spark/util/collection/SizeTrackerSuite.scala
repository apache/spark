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

package org.apache.spark.util.collection

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.SizeEstimator

class SizeTrackerSuite extends SparkFunSuite {
  val NORMAL_ERROR = 0.20
  val HIGH_ERROR = 0.30

  import SizeTrackerSuite._

  test("vector fixed size insertions") {
    testVector[Long](10000, i => i.toLong)
    testVector[(Long, Long)](10000, i => (i.toLong, i.toLong))
    testVector[LargeDummyClass](10000, i => new LargeDummyClass)
  }

  test("vector variable size insertions") {
    val rand = new Random(123456789)
    def randString(minLen: Int, maxLen: Int): String = {
      "a" * (rand.nextInt(maxLen - minLen) + minLen)
    }
    testVector[String](10000, i => randString(0, 10))
    testVector[String](10000, i => randString(0, 100))
    testVector[String](10000, i => randString(90, 100))
  }

  test("map fixed size insertions") {
    testMap[Int, Long](10000, i => (i, i.toLong))
    testMap[Int, (Long, Long)](10000, i => (i, (i.toLong, i.toLong)))
    testMap[Int, LargeDummyClass](10000, i => (i, new LargeDummyClass))
  }

  test("map variable size insertions") {
    val rand = new Random(123456789)
    def randString(minLen: Int, maxLen: Int): String = {
      "a" * (rand.nextInt(maxLen - minLen) + minLen)
    }
    testMap[Int, String](10000, i => (i, randString(0, 10)))
    testMap[Int, String](10000, i => (i, randString(0, 100)))
    testMap[Int, String](10000, i => (i, randString(90, 100)))
  }

  test("map updates") {
    val rand = new Random(123456789)
    def randString(minLen: Int, maxLen: Int): String = {
      "a" * (rand.nextInt(maxLen - minLen) + minLen)
    }
    testMap[String, Int](10000, i => (randString(0, 10000), i))
  }

  def testVector[T: ClassTag](numElements: Int, makeElement: Int => T) {
    val vector = new SizeTrackingVector[T]
    for (i <- 0 until numElements) {
      val item = makeElement(i)
      vector += item
      expectWithinError(vector, vector.estimateSize(), if (i < 32) HIGH_ERROR else NORMAL_ERROR)
    }
  }

  def testMap[K, V](numElements: Int, makeElement: (Int) => (K, V)) {
    val map = new SizeTrackingAppendOnlyMap[K, V]
    for (i <- 0 until numElements) {
      val (k, v) = makeElement(i)
      map(k) = v
      expectWithinError(map, map.estimateSize(), if (i < 32) HIGH_ERROR else NORMAL_ERROR)
    }
  }

  def expectWithinError(obj: AnyRef, estimatedSize: Long, error: Double) {
    val betterEstimatedSize = SizeEstimator.estimate(obj)
    assert(betterEstimatedSize * (1 - error) < estimatedSize,
      s"Estimated size $estimatedSize was less than expected size $betterEstimatedSize")
    assert(betterEstimatedSize * (1 + 2 * error) > estimatedSize,
      s"Estimated size $estimatedSize was greater than expected size $betterEstimatedSize")
  }
}

private object SizeTrackerSuite {

  /**
   * Run speed tests for size tracking collections.
   */
  def main(args: Array[String]): Unit = {
    if (args.size < 1) {
      // scalastyle:off println
      println("Usage: SizeTrackerSuite [num elements]")
      // scalastyle:on println
      System.exit(1)
    }
    val numElements = args(0).toInt
    vectorSpeedTest(numElements)
    mapSpeedTest(numElements)
  }

  /**
   * Speed test for SizeTrackingVector.
   *
   * Results for 100000 elements (possibly non-deterministic):
   *   PrimitiveVector  15 ms
   *   SizeTracker      51 ms
   *   SizeEstimator    2000 ms
   */
  def vectorSpeedTest(numElements: Int): Unit = {
    val baseTimes = for (i <- 0 until 10) yield time {
      val vector = new PrimitiveVector[LargeDummyClass]
      for (i <- 0 until numElements) {
        vector += new LargeDummyClass
      }
    }
    val sampledTimes = for (i <- 0 until 10) yield time {
      val vector = new SizeTrackingVector[LargeDummyClass]
      for (i <- 0 until numElements) {
        vector += new LargeDummyClass
        vector.estimateSize()
      }
    }
    val unsampledTimes = for (i <- 0 until 3) yield time {
      val vector = new PrimitiveVector[LargeDummyClass]
      for (i <- 0 until numElements) {
        vector += new LargeDummyClass
        SizeEstimator.estimate(vector)
      }
    }
    printSpeedTestResult("SizeTrackingVector", baseTimes, sampledTimes, unsampledTimes)
  }

  /**
   * Speed test for SizeTrackingAppendOnlyMap.
   *
   * Results for 100000 elements (possibly non-deterministic):
   *   AppendOnlyMap  30 ms
   *   SizeTracker    41 ms
   *   SizeEstimator  1666 ms
   */
  def mapSpeedTest(numElements: Int): Unit = {
    val baseTimes = for (i <- 0 until 10) yield time {
      val map = new AppendOnlyMap[Int, LargeDummyClass]
      for (i <- 0 until numElements) {
        map(i) = new LargeDummyClass
      }
    }
    val sampledTimes = for (i <- 0 until 10) yield time {
      val map = new SizeTrackingAppendOnlyMap[Int, LargeDummyClass]
      for (i <- 0 until numElements) {
        map(i) = new LargeDummyClass
        map.estimateSize()
      }
    }
    val unsampledTimes = for (i <- 0 until 3) yield time {
      val map = new AppendOnlyMap[Int, LargeDummyClass]
      for (i <- 0 until numElements) {
        map(i) = new LargeDummyClass
        SizeEstimator.estimate(map)
      }
    }
    printSpeedTestResult("SizeTrackingAppendOnlyMap", baseTimes, sampledTimes, unsampledTimes)
  }

  def printSpeedTestResult(
      testName: String,
      baseTimes: Seq[Long],
      sampledTimes: Seq[Long],
      unsampledTimes: Seq[Long]): Unit = {
    // scalastyle:off println
    println(s"Average times for $testName (ms):")
    println("  Base - " + averageTime(baseTimes))
    println("  SizeTracker (sampled) - " + averageTime(sampledTimes))
    println("  SizeEstimator (unsampled) - " + averageTime(unsampledTimes))
    println()
    // scalastyle:on println
  }

  def time(f: => Unit): Long = {
    val start = System.currentTimeMillis()
    f
    System.currentTimeMillis() - start
  }

  def averageTime(v: Seq[Long]): Long = {
    v.sum / v.size
  }

  private class LargeDummyClass {
    val arr = new Array[Int](100)
  }
}

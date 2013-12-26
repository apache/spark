package org.apache.spark.util

import scala.util.Random

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.util.SamplingSizeTrackerSuite.LargeDummyClass
import org.apache.spark.util.collection.{AppendOnlyMap, SizeTrackingAppendOnlyMap}

class SamplingSizeTrackerSuite extends FunSuite with BeforeAndAfterAll {
  val NORMAL_ERROR = 0.20
  val HIGH_ERROR = 0.30

  test("fixed size insertions") {
    testWith[Int, Long](10000, i => (i, i.toLong))
    testWith[Int, (Long, Long)](10000, i => (i, (i.toLong, i.toLong)))
    testWith[Int, LargeDummyClass](10000, i => (i, new LargeDummyClass()))
  }

  test("variable size insertions") {
    val rand = new Random(123456789)
    def randString(minLen: Int, maxLen: Int): String = {
      "a" * (rand.nextInt(maxLen - minLen) + minLen)
    }
    testWith[Int, String](10000, i => (i, randString(0, 10)))
    testWith[Int, String](10000, i => (i, randString(0, 100)))
    testWith[Int, String](10000, i => (i, randString(90, 100)))
  }

  test("updates") {
    val rand = new Random(123456789)
    def randString(minLen: Int, maxLen: Int): String = {
      "a" * (rand.nextInt(maxLen - minLen) + minLen)
    }
    testWith[String, Int](10000, i => (randString(0, 10000), i))
  }

  def testWith[K, V](numElements: Int, makeElement: (Int) => (K, V)) {
    val map = new SizeTrackingAppendOnlyMap[K, V]()
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

object SamplingSizeTrackerSuite {
  // Speed test, for reproducibility of results.
  // These could be highly non-deterministic in general, however.
  // Results:
  // AppendOnlyMap:   30 ms
  // SizeTracker:     45 ms
  // SizeEstimator: 1500 ms
  def main(args: Array[String]) {
    val numElements = 100000

    val baseTimes = for (i <- 0 until 3) yield time {
      val map = new AppendOnlyMap[Int, LargeDummyClass]()
      for (i <- 0 until numElements) {
        map(i) = new LargeDummyClass()
      }
    }

    val sampledTimes = for (i <- 0 until 3) yield time {
      val map = new SizeTrackingAppendOnlyMap[Int, LargeDummyClass]()
      for (i <- 0 until numElements) {
        map(i) = new LargeDummyClass()
        map.estimateSize()
      }
    }

    val unsampledTimes = for (i <- 0 until 3) yield time {
      val map = new AppendOnlyMap[Int, LargeDummyClass]()
      for (i <- 0 until numElements) {
        map(i) = new LargeDummyClass()
        SizeEstimator.estimate(map)
      }
    }

    println("Base: " + baseTimes)
    println("SizeTracker (sampled): " + sampledTimes)
    println("SizeEstimator (unsampled): " + unsampledTimes)
  }

  def time(f: => Unit): Long = {
    val start = System.currentTimeMillis()
    f
    System.currentTimeMillis() - start
  }

  private class LargeDummyClass {
    val arr = new Array[Int](100)
  }
}

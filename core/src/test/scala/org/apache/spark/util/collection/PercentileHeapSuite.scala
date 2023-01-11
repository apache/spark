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

import java.util.NoSuchElementException

import org.apache.spark.SparkFunSuite

class PercentileHeapSuite extends SparkFunSuite {

  test("If no numbers in PercentileHeap, NoSuchElementException is thrown.") {
    val medianHeap = new PercentileHeap()
    intercept[NoSuchElementException] {
      medianHeap.percentile
    }
  }

  test("Median should be correct when size of PercentileHeap is even") {
    val array = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val medianHeap = new PercentileHeap()
    array.foreach(medianHeap.insert(_))
    assert(medianHeap.size() === 10)
    assert(medianHeap.smallerSize() === 5)
    assert(medianHeap.percentile === 4.5)
  }

  test("Median should be correct when size of PercentileHeap is odd") {
    val array = Array(0, 1, 2, 3, 4, 5, 6, 7, 8)
    val medianHeap = new PercentileHeap()
    array.foreach(medianHeap.insert(_))
    assert(medianHeap.size() === 9)
    assert(medianHeap.smallerSize() === 4)
    assert(medianHeap.percentile === 4)
  }

  test("Median should be correct though there are duplicated numbers inside.") {
    val array = Array(0, 0, 1, 1, 2, 3, 4)
    val medianHeap = new PercentileHeap()
    array.foreach(medianHeap.insert(_))
    assert(medianHeap.size === 7)
    assert(medianHeap.smallerSize() === 3)
    assert(medianHeap.percentile === 1)
  }

  test("Median should be correct when input data is skewed.") {
    val medianHeap = new PercentileHeap()
    (0 until 10).foreach(_ => medianHeap.insert(5))
    assert(medianHeap.percentile === 5)
    (0 until 100).foreach(_ => medianHeap.insert(10))
    assert(medianHeap.percentile === 10)
    (0 until 1000).foreach(_ => medianHeap.insert(0))
    assert(medianHeap.percentile === 0)
  }

  test("Percentile should be correct when size of PercentileHeap is even") {
    val array = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val percentileMap = new PercentileHeap(0.7)
    array.foreach(percentileMap.insert(_))
    assert(percentileMap.size() === 10)
    assert(percentileMap.smallerSize() == 7)
    assert(percentileMap.percentile === 6.5)
  }

  test("Percentile should be correct when size of PercentileHeap is odd") {
    val array = Array(0, 1, 2, 3, 4, 5, 6, 7, 8)
    val percentileMap = new PercentileHeap(0.7)
    array.foreach(percentileMap.insert(_))
    assert(percentileMap.size() === 9)
    assert(percentileMap.smallerSize() == 6)
    assert(percentileMap.percentile === 6)
  }
}

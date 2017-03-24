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

class MedianHeapSuite extends SparkFunSuite {

  test("If no numbers in MedianHeap, NoSuchElementException is thrown.") {
    val medianHeap = new MedianHeap()
    intercept[NoSuchElementException] {
      medianHeap.median
    }
  }

  test("Median should be correct when size of MedianHeap is even") {
    val array = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val medianHeap = new MedianHeap()
    array.foreach(medianHeap.insert(_))
    assert(medianHeap.size() === 10)
    assert(medianHeap.median === 4.5)
  }

  test("Median should be correct when size of MedianHeap is odd") {
    val array = Array(0, 1, 2, 3, 4, 5, 6, 7, 8)
    val medianHeap = new MedianHeap()
    array.foreach(medianHeap.insert(_))
    assert(medianHeap.size() === 9)
    assert(medianHeap.median === 4)
  }

  test("Median should be correct though there are duplicated numbers inside.") {
    val array = Array(0, 0, 1, 1, 2, 3, 4)
    val medianHeap = new MedianHeap()
    array.foreach(medianHeap.insert(_))
    assert(medianHeap.size === 7)
    assert(medianHeap.median === 1)
  }

  test("Median should be correct when input data is skewed.") {
    val medianHeap = new MedianHeap()
    (0 until 10).foreach(_ => medianHeap.insert(5))
    assert(medianHeap.median === 5)
    (0 until 100).foreach(_ => medianHeap.insert(10))
    assert(medianHeap.median === 10)
    (0 until 1000).foreach(_ => medianHeap.insert(0))
    assert(medianHeap.median === 0)
  }
}

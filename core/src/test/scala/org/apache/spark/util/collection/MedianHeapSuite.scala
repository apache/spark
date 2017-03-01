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

import java.util
import java.util.NoSuchElementException

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkFunSuite

class MedianHeapSuite extends SparkFunSuite {

  test("If no numbers in MedianHeap, NoSuchElementException is thrown.") {
    val medianHeap = new MedianHeap()
    var valid = false
    try {
      medianHeap.findMedian()
    } catch {
      case e: NoSuchElementException =>
        valid = true
    }

    assert(valid)
  }

  test("Median should be correct when size of MedianHeap is even or odd") {
    val random = new Random()
    val medianHeap1 = new MedianHeap()
    val array1 = new Array[Int](100)
    (0 until 100).foreach {
      case i =>
        val randomNumber = random.nextInt(1000)
        medianHeap1.insert(randomNumber)
        array1(i) += randomNumber
    }
    util.Arrays.sort(array1)
    assert(medianHeap1.findMedian() === ((array1(49) + array1(50)) / 2.0))

    val medianHeap2 = new MedianHeap()
    val array2 = new Array[Int](101)
    (0 until 101).foreach {
      case i =>
        val randomNumber = random.nextInt(1000)
        medianHeap2.insert(randomNumber)
        array2(i) += randomNumber
    }
    util.Arrays.sort(array2)
    assert(medianHeap2.findMedian() === array2(50))
  }

  test("Size of Median should be correct though there are duplicated numbers inside.") {
    val random = new Random()
    val medianHeap = new MedianHeap()
    val array = new Array[Int](1000)
    (0 until 1000).foreach {
      case i =>
        val randomNumber = random.nextInt(100)
        medianHeap.insert(randomNumber)
        array(i) += randomNumber
    }
    util.Arrays.sort(array)
    assert(medianHeap.size === 1000)
    assert(medianHeap.findMedian() === ((array(499) + array(500)) / 2.0))
  }

  test("Remove a number from MedianHeap.") {
    val random = new Random()
    val medianHeap = new MedianHeap()
    val array = new Array[Int](99)
    var lastNumber = 0
    (0 until 100).foreach {
      case i =>
        val randomNumber = random.nextInt(100)
        medianHeap.insert(randomNumber)
        if (i != 99) {
          array(i) += randomNumber
        } else {
          lastNumber = randomNumber
        }
    }
    util.Arrays.sort(array)
    assert(medianHeap.remove(lastNumber) === true)
    assert(medianHeap.findMedian() === array(49))

  }
}

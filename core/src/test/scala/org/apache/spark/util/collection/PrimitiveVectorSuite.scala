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

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.SizeEstimator

class PrimitiveVectorSuite extends SparkFunSuite {

  test("primitive value") {
    val vector = new PrimitiveVector[Int]

    for (i <- 0 until 1000) {
      vector += i
      assert(vector(i) === i)
    }

    assert(vector.size === 1000)
    assert(vector.size == vector.length)
    intercept[IllegalArgumentException] {
      vector(1000)
    }

    for (i <- 0 until 1000) {
      assert(vector(i) == i)
    }
  }

  test("non-primitive value") {
    val vector = new PrimitiveVector[String]

    for (i <- 0 until 1000) {
      vector += i.toString
      assert(vector(i) === i.toString)
    }

    assert(vector.size === 1000)
    assert(vector.size == vector.length)
    intercept[IllegalArgumentException] {
      vector(1000)
    }

    for (i <- 0 until 1000) {
      assert(vector(i) == i.toString)
    }
  }

  test("ideal growth") {
    val vector = new PrimitiveVector[Long](initialSize = 1)
    vector += 1
    for (i <- 1 until 1024) {
      vector += i
      assert(vector.size === i + 1)
      assert(vector.capacity === Integer.highestOneBit(i) * 2)
    }
    assert(vector.capacity === 1024)
    vector += 1024
    assert(vector.capacity === 2048)
  }

  test("ideal size") {
    val vector = new PrimitiveVector[Long](8192)
    for (i <- 0 until 8192) {
      vector += i
    }
    assert(vector.size === 8192)
    assert(vector.capacity === 8192)
    val actualSize = SizeEstimator.estimate(vector)
    val expectedSize = 8192 * 8
    // Make sure we are not allocating a significant amount of memory beyond our expected.
    // Due to specialization wonkiness, we need to ensure we don't have 2 copies of the array.
    assert(actualSize < expectedSize * 1.1)
  }

  test("resizing") {
    val vector = new PrimitiveVector[Long]
    for (i <- 0 until 4097) {
      vector += i
    }
    assert(vector.size === 4097)
    assert(vector.capacity === 8192)
    vector.trim()
    assert(vector.size === 4097)
    assert(vector.capacity === 4097)
    vector.resize(5000)
    assert(vector.size === 4097)
    assert(vector.capacity === 5000)
    vector.resize(4000)
    assert(vector.size === 4000)
    assert(vector.capacity === 4000)
    vector.resize(5000)
    assert(vector.size === 4000)
    assert(vector.capacity === 5000)
    for (i <- 0 until 4000) {
      assert(vector(i) == i)
    }
    intercept[IllegalArgumentException] {
      vector(4000)
    }
  }
}

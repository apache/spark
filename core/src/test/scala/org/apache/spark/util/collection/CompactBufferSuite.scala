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

class CompactBufferSuite extends SparkFunSuite {
  test("empty buffer") {
    val b = new CompactBuffer[Int]
    assert(b.size === 0)
    assert(b.iterator.toList === Nil)
    assert(b.size === 0)
    assert(b.iterator.toList === Nil)
    intercept[IndexOutOfBoundsException] { b(0) }
    intercept[IndexOutOfBoundsException] { b(1) }
    intercept[IndexOutOfBoundsException] { b(2) }
    intercept[IndexOutOfBoundsException] { b(-1) }
  }

  test("basic inserts") {
    val b = new CompactBuffer[Int]
    assert(b.size === 0)
    assert(b.iterator.toList === Nil)
    for (i <- 0 until 1000) {
      b += i
      assert(b.size === i + 1)
      assert(b(i) === i)
    }
    assert(b.iterator.toList === (0 until 1000).toList)
    assert(b.iterator.toList === (0 until 1000).toList)
    assert(b.size === 1000)
  }

  test("adding sequences") {
    val b = new CompactBuffer[Int]
    assert(b.size === 0)
    assert(b.iterator.toList === Nil)

    // Add some simple lists and iterators
    b ++= List(0)
    assert(b.size === 1)
    assert(b.iterator.toList === List(0))
    b ++= Iterator(1)
    assert(b.size === 2)
    assert(b.iterator.toList === List(0, 1))
    b ++= List(2)
    assert(b.size === 3)
    assert(b.iterator.toList === List(0, 1, 2))
    b ++= Iterator(3, 4, 5, 6, 7, 8, 9)
    assert(b.size === 10)
    assert(b.iterator.toList === (0 until 10).toList)

    // Add CompactBuffers
    val b2 = new CompactBuffer[Int]
    b2 ++= 0 until 10
    b ++= b2
    assert(b.iterator.toList === (1 to 2).flatMap(i => 0 until 10).toList)
    b ++= b2
    assert(b.iterator.toList === (1 to 3).flatMap(i => 0 until 10).toList)
    b ++= b2
    assert(b.iterator.toList === (1 to 4).flatMap(i => 0 until 10).toList)

    // Add some small CompactBuffers as well
    val b3 = new CompactBuffer[Int]
    b ++= b3
    assert(b.iterator.toList === (1 to 4).flatMap(i => 0 until 10).toList)
    b3 += 0
    b ++= b3
    assert(b.iterator.toList === (1 to 4).flatMap(i => 0 until 10).toList ++ List(0))
    b3 += 1
    b ++= b3
    assert(b.iterator.toList === (1 to 4).flatMap(i => 0 until 10).toList ++ List(0, 0, 1))
    b3 += 2
    b ++= b3
    assert(b.iterator.toList === (1 to 4).flatMap(i => 0 until 10).toList ++ List(0, 0, 1, 0, 1, 2))
  }

  test("adding the same buffer to itself") {
    val b = new CompactBuffer[Int]
    assert(b.size === 0)
    assert(b.iterator.toList === Nil)
    b += 1
    assert(b.toList === List(1))
    for (j <- 1 until 8) {
      b ++= b
      assert(b.size === (1 << j))
      assert(b.iterator.toList === (1 to (1 << j)).map(i => 1).toList)
    }
  }
}

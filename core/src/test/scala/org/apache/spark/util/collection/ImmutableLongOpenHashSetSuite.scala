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

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.spark.util.SizeEstimator

class ImmutableLongOpenHashSetSuite extends FunSuite with Matchers {

  test("size for primitive long") {
    val loadFactor = 0.7
    var set = ImmutableLongOpenHashSet.empty(64, loadFactor)
    for (i <- 0 until 1024) {
      set = set.add(i)
    }
    assert(set.size === 1024)
    assert(set.capacity > 1024)
    val actualSize = SizeEstimator.estimate(set)
    // 64 bits per long + 1 bit per long for the bitset
    val expectedSize = set.capacity * (64 + 1) / 8
    // Make sure we are not allocating a significant amount of memory beyond our expected.
    actualSize should be <= (expectedSize * 1.5).toLong
  }

  test("primitive long") {
    var set = ImmutableLongOpenHashSet.empty
    assert(set.size === 0)
    assert(!set.contains(10L))
    assert(!set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(10L)
    assert(set.size === 1)
    assert(set.contains(10L))
    assert(!set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(50L)
    assert(set.size === 2)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(!set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(999L)
    assert(set.size === 3)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(set.contains(999L))
    assert(!set.contains(10000L))

    set = set.add(50L)
    assert(set.size === 3)
    assert(set.contains(10L))
    assert(set.contains(50L))
    assert(set.contains(999L))
    assert(!set.contains(10000L))
  }

  test("primitive set growth") {
    var set = ImmutableLongOpenHashSet.empty
    for (i <- 1 to 1000) {
      set = set.add(i.toLong)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
    for (i <- 1 to 100) {
      set = set.add(i.toLong)
    }
    assert(set.size === 1000)
    assert(set.capacity > 1000)
  }
}

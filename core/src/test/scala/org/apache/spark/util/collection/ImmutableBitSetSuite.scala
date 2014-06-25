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

class ImmutableBitSetSuite extends FunSuite {

  test("get, set, iterator") {
    val setBits = List(0, 9, 1, 10, 90, 96)
    var bitset = new ImmutableBitSet(100)

    for (i <- 0 until 100) {
      assert(!bitset.get(i))
    }

    setBits.foreach(i => bitset = bitset.set(i))

    for (i <- 0 until 100) {
      if (setBits.contains(i)) {
        assert(bitset.get(i))
      } else {
        assert(!bitset.get(i))
      }
    }
    assert(bitset.cardinality() === setBits.size)
    assert(bitset.iterator.toSet === setBits.toSet)
  }

  test("100% full bit set") {
    var bitset = new ImmutableBitSet(10000)
    for (i <- 0 until 10000) {
      assert(!bitset.get(i))
      bitset = bitset.set(i)
    }
    for (i <- 0 until 10000) {
      assert(bitset.get(i))
    }
    assert(bitset.cardinality() === 10000)
  }
}

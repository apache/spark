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

class ImmutableBitSetSuite extends SparkFunSuite {

  test("basic get") {
    val bitset = new ImmutableBitSet(100, 0, 9, 1, 10, 90, 96)
    val setBits = Seq(0, 9, 1, 10, 90, 96)
    for (i <- 0 until 100) {
      if (setBits.contains(i)) {
        assert(bitset.get(i))
      } else {
        assert(!bitset.get(i))
      }
    }
    assert(bitset.cardinality() === setBits.size)
  }

  test("nextSetBit") {
    val bitset = new ImmutableBitSet(100, 0, 9, 1, 10, 90, 96)

    assert(bitset.nextSetBit(0) === 0)
    assert(bitset.nextSetBit(1) === 1)
    assert(bitset.nextSetBit(2) === 9)
    assert(bitset.nextSetBit(9) === 9)
    assert(bitset.nextSetBit(10) === 10)
    assert(bitset.nextSetBit(11) === 90)
    assert(bitset.nextSetBit(80) === 90)
    assert(bitset.nextSetBit(91) === 96)
    assert(bitset.nextSetBit(96) === 96)
    assert(bitset.nextSetBit(97) === -1)
  }

  test( "xor len(bitsetX) < len(bitsetY)" ) {
    val bitsetX = new ImmutableBitSet(60, 0, 2, 3, 37, 41)
    val bitsetY = new ImmutableBitSet(100, 0, 1, 3, 37, 38, 41, 85)

    val bitsetXor = bitsetX ^ bitsetY

    assert(bitsetXor.nextSetBit(0) === 1)
    assert(bitsetXor.nextSetBit(1) === 1)
    assert(bitsetXor.nextSetBit(2) === 2)
    assert(bitsetXor.nextSetBit(3) === 38)
    assert(bitsetXor.nextSetBit(38) === 38)
    assert(bitsetXor.nextSetBit(39) === 85)
    assert(bitsetXor.nextSetBit(42) === 85)
    assert(bitsetXor.nextSetBit(85) === 85)
    assert(bitsetXor.nextSetBit(86) === -1)

  }

  test( "xor len(bitsetX) > len(bitsetY)" ) {
    val bitsetX = new ImmutableBitSet(100, 0, 1, 3, 37, 38, 41, 85)
    val bitsetY = new ImmutableBitSet(60, 0, 2, 3, 37, 41)

    val bitsetXor = bitsetX ^ bitsetY

    assert(bitsetXor.nextSetBit(0) === 1)
    assert(bitsetXor.nextSetBit(1) === 1)
    assert(bitsetXor.nextSetBit(2) === 2)
    assert(bitsetXor.nextSetBit(3) === 38)
    assert(bitsetXor.nextSetBit(38) === 38)
    assert(bitsetXor.nextSetBit(39) === 85)
    assert(bitsetXor.nextSetBit(42) === 85)
    assert(bitsetXor.nextSetBit(85) === 85)
    assert(bitsetXor.nextSetBit(86) === -1)

  }

  test( "andNot len(bitsetX) < len(bitsetY)" ) {
    val bitsetX = new ImmutableBitSet(60, 0, 2, 3, 37, 41, 48)
    val bitsetY = new ImmutableBitSet(100, 0, 1, 3, 37, 38, 41, 85)

    val bitsetDiff = bitsetX.andNot( bitsetY )

    assert(bitsetDiff.nextSetBit(0) === 2)
    assert(bitsetDiff.nextSetBit(1) === 2)
    assert(bitsetDiff.nextSetBit(2) === 2)
    assert(bitsetDiff.nextSetBit(3) === 48)
    assert(bitsetDiff.nextSetBit(48) === 48)
    assert(bitsetDiff.nextSetBit(49) === -1)
    assert(bitsetDiff.nextSetBit(65) === -1)
  }

  test( "andNot len(bitsetX) > len(bitsetY)" ) {
    val bitsetX = new ImmutableBitSet(100, 0, 1, 3, 37, 38, 41, 85)
    val bitsetY = new ImmutableBitSet(60, 0, 2, 3, 37, 41, 48)

    val bitsetDiff = bitsetX.andNot( bitsetY )

    assert(bitsetDiff.nextSetBit(0) === 1)
    assert(bitsetDiff.nextSetBit(1) === 1)
    assert(bitsetDiff.nextSetBit(2) === 38)
    assert(bitsetDiff.nextSetBit(3) === 38)
    assert(bitsetDiff.nextSetBit(38) === 38)
    assert(bitsetDiff.nextSetBit(39) === 85)
    assert(bitsetDiff.nextSetBit(85) === 85)
    assert(bitsetDiff.nextSetBit(86) === -1)
  }

  test( "immutability" ) {
    val bitset = new ImmutableBitSet(100)
    intercept[UnsupportedOperationException] {
      bitset.set(1)
    }
    intercept[UnsupportedOperationException] {
      bitset.setUntil(10)
    }
    intercept[UnsupportedOperationException] {
      bitset.unset(1)
    }
    intercept[UnsupportedOperationException] {
      bitset.clear()
    }
    intercept[UnsupportedOperationException] {
      bitset.clearUntil(10)
    }
    intercept[UnsupportedOperationException] {
      bitset.union(new ImmutableBitSet(100))
    }
  }
}

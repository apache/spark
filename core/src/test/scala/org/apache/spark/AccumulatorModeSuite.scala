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

package org.apache.spark

import java.{lang => jl}

import org.apache.spark.util.{AccumulatorV2, _}

class AccumulatorModeSuite extends SparkFunSuite with SharedSparkContext {

  override def afterEach(): Unit = {
    try {
      AccumulatorContext.clear()
    } finally {
      super.afterEach()
    }
  }

  Seq(
    // accumulator constructor and its supported modes
    (() => new AllAccumulator(), Set(AccumulatorMode.All)),
    (() => new AllAndFirstAccumulator(), Set(AccumulatorMode.All, AccumulatorMode.First)),
    (() => new AllAndLargerAccumulator(), Set(AccumulatorMode.All, AccumulatorMode.Larger)),
    (() => new AllAndLastAccumulator(), Set(AccumulatorMode.All, AccumulatorMode.Last))
  ).foreach { case (createAccumulator, supportedModes) =>
    AccumulatorMode.values.diff(supportedModes).foreach { mode =>
      val supported = supportedModes.map(_.name).mkString(",")

      test(s"register throws exception for ${mode.name} mode when $supported supported") {
        val acc = createAccumulator()
        val message = intercept[UnsupportedOperationException](
          acc.register(sc, mode = mode)
        ).getMessage
        assert(message ===
          s"Unsupported accumulator mode for accumulator type ${acc.getClass}: $mode",
          s"Supported modes: $supportedModes, tested unsupported mode: $mode")
      }

    }
  }

  test("first accumulator mode merges reliable accumulator") {
    val acc = ReliableLongAccumulator()
    assert(acc.count === 0)
    assert(acc.value === 0)
    assert(acc.sum === 0)
    val other = acc.copyAndReset().asInstanceOf[ReliableLongAccumulator]
    other.add(1L)

    AccumulatorMode.First.merge(acc, other, 0)
    assert(acc.count === 1)
    assert(acc.value === 1)
    assert(acc.sum === 1)

    other.add(1L) // other has value 2 now
    AccumulatorMode.First.merge(acc, other, 0)
    assert(acc.count === 1)
    assert(acc.value === 1)
    assert(acc.sum === 1)
  }

  test("last accumulator mode merges reliable accumulator") {
    val acc = ReliableLongAccumulator()
    assert(acc.count === 0)
    assert(acc.value === 0)
    assert(acc.sum === 0)
    val other = acc.copyAndReset().asInstanceOf[ReliableLongAccumulator]
    other.add(1L)

    AccumulatorMode.Last.merge(acc, other, 0)
    assert(acc.count === 1)
    assert(acc.value === 1)
    assert(acc.sum === 1)

    other.add(1L) // other has value 2 now
    AccumulatorMode.Last.merge(acc, other, 0)
    assert(acc.count === 2)
    assert(acc.value === 2)
    assert(acc.sum === 2)
  }

  test("merging with unsupported accumulator mode throws exception") {
    val acc = new AllAccumulator()
    val other = acc.copyAndReset()
    AccumulatorMode.First.merge(acc, other, 0)
    AccumulatorMode.Larger.merge(acc, other, 0)
    AccumulatorMode.Last.merge(acc, other, 0)
  }
}

class AllAccumulator() extends ReliableAccumulator[Long, Long, AllAccumulator] {
  override def isZero: Boolean = true
  override def copy(): AllAccumulator = new AllAccumulator()
  override def copyAndReset(): AllAccumulator = new AllAccumulator()

  override def reset(): Unit = { }
  override def add(v: Long): Unit = throw new UnsupportedOperationException()
  override def mergeFragment(other: AllAccumulator, fragmentId: Int): Unit =
    throw new UnsupportedOperationException()
  override def value: Long = 0

  override def merge(other: AccumulatorV2[Long, Long]): Unit = { }
  override def unMerge(fragmentId: Int): Unit = { }
  override def mergedFragment(other: AllAccumulator, fragmentId: Int): Unit = { }
}

class AllAndFirstAccumulator() extends AllAccumulator
  with FirstMode[Long, Long, AllAccumulator] {
  override def isFirst(fragmentId: Int): Boolean = false
}

class AllAndLargerAccumulator() extends AllAccumulator
  with LargerMode[Long, Long, AllAccumulator] {
  override def unMerge(fragmentId: Int): Unit = { }
  override def isLarger(other: AllAccumulator, fragmentId: Int): Boolean = false
}

class AllAndLastAccumulator() extends AllAccumulator
  with LastMode[Long, Long, AllAccumulator] {
  override def unMerge(fragmentId: Int): Unit = { }
}

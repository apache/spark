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

import scala.util.Random

import org.apache.spark.SparkFunSuite

class CompactLongArraySuite extends SparkFunSuite {
  test("compaction preserves zero, constant, signed and full-range values") {
    val random = new Random(123L)
    val inputs = Seq(
      Array.emptyLongArray,
      Array.fill(513)(0L),
      Array.fill(513)(-1L),
      Array.tabulate(513)(i => i.toLong - 256L),
      Array.tabulate(513)(i => Long.MaxValue - i),
      Array.tabulate(513)(i => Long.MinValue + i),
      Array.tabulate(513)(i => if (i % 2 == 0) Long.MinValue else Long.MaxValue),
      Array.fill(513)(random.nextLong()))
    inputs.foreach { input =>
      val values = new CompactLongArray(input.length)
      input.indices.foreach(i => values(i) = input(i))
      values.compact()
      assert(values.toArray.toSeq == input.toSeq)
      assert(values.iterator.toSeq == input.toSeq)
      values.compact()
      assert(values.toArray.toSeq == input.toSeq)
    }
  }

  test("packed values span words at every supported width") {
    (1 to 63).foreach { width =>
      val input = Array.tabulate(257) { i =>
        if (i % 3 == 0) (1L << (width - 1)) else i.toLong & ((1L << (width - 1)) - 1)
      }
      val values = new CompactLongArray(input.length)
      input.indices.foreach(i => values(i) = input(i))
      values.compact()
      assert(values.toArray.toSeq == input.toSeq)
    }
  }

  test("late updates expand compacted pages without changing other values") {
    val values = new CompactLongArray(1025)
    val expected = Array.fill(1025)(0L)
    (0 until 512).foreach { i =>
      values(i) = 7L
      expected(i) = 7L
    }
    values.compact()
    Seq(0, 255, 256, 511, 512, 1024).foreach { i =>
      values(i) = Long.MinValue + i
      expected(i) = Long.MinValue + i
    }
    values.compact()
    assert(values.toArray.toSeq == expected.toSeq)
    intercept[IndexOutOfBoundsException](values(-1))
    intercept[IndexOutOfBoundsException](values(1025) = 1L)
  }
}

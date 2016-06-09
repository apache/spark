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

package org.apache.spark.util

import org.apache.spark._

class AccumulatorV2Suite extends SparkFunSuite {

  test("LongAccumulator add/avg/sum/count/isZero") {
    val acc = new LongAccumulator
    assert(acc.isZero)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.avg.isNaN)

    acc.add(0)
    assert(!acc.isZero)
    assert(acc.count == 1)
    assert(acc.sum == 0)
    assert(acc.avg == 0.0)

    acc.add(1)
    assert(acc.count == 2)
    assert(acc.sum == 1)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(new java.lang.Long(2))
    assert(acc.count == 3)
    assert(acc.sum == 3)
    assert(acc.avg == 1.0)

    // Test merging
    val acc2 = new LongAccumulator
    acc2.add(2)
    acc.merge(acc2)
    assert(acc.count == 4)
    assert(acc.sum == 5)
    assert(acc.avg == 1.25)
  }

  test("DoubleAccumulator add/avg/sum/count/isZero") {
    val acc = new DoubleAccumulator
    assert(acc.isZero)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.avg.isNaN)

    acc.add(0.0)
    assert(!acc.isZero)
    assert(acc.count == 1)
    assert(acc.sum == 0.0)
    assert(acc.avg == 0.0)

    acc.add(1.0)
    assert(acc.count == 2)
    assert(acc.sum == 1.0)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(new java.lang.Double(2.0))
    assert(acc.count == 3)
    assert(acc.sum == 3.0)
    assert(acc.avg == 1.0)

    // Test merging
    val acc2 = new DoubleAccumulator
    acc2.add(2.0)
    acc.merge(acc2)
    assert(acc.count == 4)
    assert(acc.sum == 5.0)
    assert(acc.avg == 1.25)
  }

  test("ListAccumulator") {
    val acc = new ListAccumulator[Double]
    assert(acc.value.isEmpty)
    assert(acc.isZero)

    acc.add(0.0)
    assert(acc.value.contains(0.0))
    assert(!acc.isZero)

    acc.add(new java.lang.Double(1.0))

    val acc2 = acc.copyAndReset()
    assert(acc2.value.isEmpty)
    assert(acc2.isZero)

    assert(acc.value.contains(1.0))
    assert(!acc.isZero)
    assert(acc.value.size() === 2)

    acc2.add(2.0)
    assert(acc2.value.contains(2.0))
    assert(!acc2.isZero)
    assert(acc2.value.size() === 1)

    // Test merging
    acc.merge(acc2)
    assert(acc.value.contains(2.0))
    assert(!acc.isZero)
    assert(acc.value.size() === 3)

    val acc3 = acc.copy()
    assert(acc3.value.contains(2.0))
    assert(!acc3.isZero)
    assert(acc3.value.size() === 3)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value.isEmpty)
  }

  test("LegacyAccumulatorWrapper") {
    val acc = new LegacyAccumulatorWrapper("default", AccumulatorParam.StringAccumulatorParam)
    assert(acc.value === "default")
    assert(!acc.isZero)

    acc.add("foo")
    assert(acc.value === "foo")
    assert(!acc.isZero)

    acc.add(new java.lang.String("bar"))

    val acc2 = acc.copyAndReset()
    assert(acc2.value === "")
    assert(acc2.isZero)

    assert(acc.value === "bar")
    assert(!acc.isZero)

    acc2.add("baz")
    assert(acc2.value === "baz")
    assert(!acc2.isZero)

    // Test merging
    acc.merge(acc2)
    assert(acc.value === "baz")
    assert(!acc.isZero)

    val acc3 = acc.copy()
    assert(acc3.value === "baz")
    assert(!acc3.isZero)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value === "")
  }
}

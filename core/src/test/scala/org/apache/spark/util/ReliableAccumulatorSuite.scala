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

import scala.collection.JavaConverters._

import org.apache.spark._

class ReliableAccumulatorSuite extends SparkFunSuite {

  // cf. "LongAccumulator add/avg/sum/count/isZero" in AccumulatorV2Suite
  test("ReliableLongAccumulator add/avg/sum/count/isZero") {
    val acc = new ReliableLongAccumulator
    assert(acc.isZero)
    assert(acc.value == 0)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.avg.isNaN)

    acc.add(0)
    assert(!acc.isZero)
    assert(acc.value == 0)
    assert(acc.count == 1)
    assert(acc.sum == 0)
    assert(acc.avg == 0.0)

    acc.add(1)
    assert(acc.value == 1)
    assert(acc.count == 2)
    assert(acc.sum == 1)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(java.lang.Long.valueOf(2))
    assert(acc.value == 3)
    assert(acc.count == 3)
    assert(acc.sum == 3)
    assert(acc.avg == 1.0)

    // Test merging, the reliable part
    val acc2 = new ReliableLongAccumulator
    acc2.add(2)
    acc.merge(acc2, 0)
    assert(acc.value == 5)
    assert(acc.count == 4)
    assert(acc.sum == 5)
    assert(acc.avg == 1.25)

    acc.merge(acc2, 0)
    assert(acc.value == 5)
    assert(acc.count == 4)
    assert(acc.sum == 5)
    assert(acc.avg == 1.25)

    acc.merge(acc2, 1)
    assert(acc.value == 7)
    assert(acc.count == 5)
    assert(acc.sum == 7)
    assert(acc.avg == 1.4)

    // Test copy and copyAndReset
    val acc3 = acc.copy()
    assert(acc3.isInstanceOf[ReliableLongAccumulator])
    assert(!acc3.isZero)
    assert(acc3.value == 7.0)
    assert(acc3.count == 5)
    assert(acc3.sum == 7.0)
    assert(acc3.avg == 1.4)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value == 0.0)
    assert(acc3.count == 0)
    assert(acc3.sum == 0.0)
    assert(acc3.avg.isNaN)

    // simply to test this compiles, hence acc3 resovles to a ReliableLongAccumulator
    acc3.merge(acc, 0)

    val acc4 = acc.copyAndReset()
    assert(acc4.isInstanceOf[ReliableLongAccumulator])
    assert(acc4.isZero)
    assert(acc4.value == 0.0)
    assert(acc4.count == 0)
    assert(acc4.sum == 0.0)
    assert(acc4.avg.isNaN)

    // simply to test this compiles, hence acc3 and acc4 resolve to a ReliableLongAccumulator
    acc3.merge(acc, 0)
    acc4.merge(acc, 0)
  }

  // cf. "DoubleAccumulator add/avg/sum/count/isZero" in AccumulatorV2Suite
  test("ReliableDoubleAccumulator add/avg/sum/count/isZero") {
    val acc = new ReliableDoubleAccumulator
    assert(acc.isZero)
    assert(acc.value == 0.0)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.avg.isNaN)

    acc.add(0.0)
    assert(!acc.isZero)
    assert(acc.value == 0.0)
    assert(acc.count == 1)
    assert(acc.sum == 0.0)
    assert(acc.avg == 0.0)

    acc.add(1.0)
    assert(acc.value == 1.0)
    assert(acc.count == 2)
    assert(acc.sum == 1.0)
    assert(acc.avg == 0.5)

    // Also test add using non-specialized add function
    acc.add(java.lang.Double.valueOf(2.0))
    assert(acc.value == 3.0)
    assert(acc.count == 3)
    assert(acc.sum == 3.0)
    assert(acc.avg == 1.0)

    // Test merging, the reliable part
    val acc2 = new ReliableDoubleAccumulator
    acc2.add(2.0)
    acc.merge(acc2, 0)
    assert(acc.value == 5.0)
    assert(acc.count == 4)
    assert(acc.sum == 5.0)
    assert(acc.avg == 1.25)

    acc.merge(acc2, 0)
    assert(acc.value == 5.0)
    assert(acc.count == 4)
    assert(acc.sum == 5.0)
    assert(acc.avg == 1.25)

    acc.merge(acc2, 1)
    assert(acc.value == 7.0)
    assert(acc.count == 5)
    assert(acc.sum == 7.0)
    assert(acc.avg == 1.4)

    // Test copy and copyAndReset
    val acc3 = acc.copy()
    assert(acc3.isInstanceOf[ReliableDoubleAccumulator])
    assert(!acc3.isZero)
    assert(acc3.value == 7.0)
    assert(acc3.count == 5)
    assert(acc3.sum == 7.0)
    assert(acc3.avg == 1.4)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value == 0.0)
    assert(acc3.count == 0)
    assert(acc3.sum == 0.0)
    assert(acc3.avg.isNaN)

    val acc4 = acc.copyAndReset()
    assert(acc4.isInstanceOf[ReliableDoubleAccumulator])
    assert(acc4.isZero)
    assert(acc4.value == 0.0)
    assert(acc4.count == 0)
    assert(acc4.sum == 0.0)
    assert(acc4.avg.isNaN)

    // simply to test this compiles, hence acc3 and acc4 resolve to a ReliableDoubleAccumulator
    acc3.merge(acc, 0)
    acc4.merge(acc, 0)
  }

  // cf. "ListAccumulator" in AccumulatorV2Suite
  test("ReliableListAccumulator") {
    val acc = new ReliableCollectionAccumulator[Double]
    assert(acc.value.isEmpty)
    assert(acc.isZero)

    acc.add(0.0)
    assert(acc.value.asScala == Seq(0.0))
    assert(!acc.isZero)

    acc.add(java.lang.Double.valueOf(1.0))
    assert(acc.value.asScala == Seq(0.0, 1.0))
    assert(!acc.isZero)

    val acc2 = acc.copyAndReset()
    assert(acc2.isInstanceOf[ReliableCollectionAccumulator[Double]])
    assert(acc2.value.isEmpty)
    assert(acc2.isZero)

    assert(acc.value.asScala == Seq(0.0, 1.0))
    assert(!acc.isZero)

    acc2.add(2.0)
    assert(acc2.value.asScala == Seq(2.0))
    assert(!acc2.isZero)

    // Test merging, the reliable part
    acc.merge(acc2, 0)
    assert(acc.value.asScala == Seq(0.0, 1.0, 2.0))
    assert(!acc.isZero)

    acc.merge(acc2, 0)
    assert(acc.value.asScala == Seq(0.0, 1.0, 2.0))
    assert(!acc.isZero)

    acc.merge(acc2, 1)
    assert(acc.value.asScala == Seq(0.0, 1.0, 2.0, 2.0))
    assert(!acc.isZero)

    val acc3 = acc.copy()
    assert(acc3.isInstanceOf[ReliableCollectionAccumulator[Double]])
    assert(acc3.value.asScala == Seq(0.0, 1.0, 2.0, 2.0))
    assert(!acc3.isZero)

    acc3.reset()
    assert(acc3.isZero)
    assert(acc3.value.isEmpty)

    // simply to test this compiles, hence acc2 and acc3 resolve to a
    // ReliableCollectionAccumulator[Double]
    acc2.merge(acc, 0)
    acc3.merge(acc, 0)
  }

}

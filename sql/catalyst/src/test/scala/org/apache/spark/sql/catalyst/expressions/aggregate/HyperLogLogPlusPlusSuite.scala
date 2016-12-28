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

package org.apache.spark.sql.catalyst.expressions.aggregate

import java.util.Random

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, SpecificInternalRow}
import org.apache.spark.sql.types.{DataType, IntegerType}

class HyperLogLogPlusPlusSuite extends SparkFunSuite {

  /** Create a HLL++ instance and an input and output buffer. */
  def createEstimator(rsd: Double, dt: DataType = IntegerType):
      (HyperLogLogPlusPlus, InternalRow, InternalRow) = {
    val input = new SpecificInternalRow(Seq(dt))
    val hll = new HyperLogLogPlusPlus(new BoundReference(0, dt, true), rsd)
    val buffer = createBuffer(hll)
    (hll, input, buffer)
  }

  def createBuffer(hll: HyperLogLogPlusPlus): InternalRow = {
    val buffer = new SpecificInternalRow(hll.aggBufferAttributes.map(_.dataType))
    hll.initialize(buffer)
    buffer
  }

  /** Evaluate the estimate. It should be within 3*SD's of the given true rsd. */
  def evaluateEstimate(hll: HyperLogLogPlusPlus, buffer: InternalRow, cardinality: Int): Unit = {
    val estimate = hll.eval(buffer).asInstanceOf[Long].toDouble
    val error = math.abs((estimate / cardinality.toDouble) - 1.0d)
    assert(error < hll.trueRsd * 3.0d, "Error should be within 3 std. errors.")
  }

  test("test invalid parameter relativeSD") {
    // `relativeSD` should be at most 39%.
    intercept[IllegalArgumentException] {
      new HyperLogLogPlusPlus(new BoundReference(0, IntegerType, true), relativeSD = 0.4)
    }
  }

  test("add nulls") {
    val (hll, input, buffer) = createEstimator(0.05)
    input.setNullAt(0)
    hll.update(buffer, input)
    hll.update(buffer, input)
    val estimate = hll.eval(buffer).asInstanceOf[Long]
    assert(estimate == 0L, "Nothing meaningful added; estimate should be 0.")
  }

  def testCardinalityEstimates(
      rsds: Seq[Double],
      ns: Seq[Int],
      f: Int => Int,
      c: Int => Int): Unit = {
    rsds.flatMap(rsd => ns.map(n => (rsd, n))).foreach {
      case (rsd, n) =>
        val (hll, input, buffer) = createEstimator(rsd)
        var i = 0
        while (i < n) {
          input.setInt(0, f(i))
          hll.update(buffer, input)
          i += 1
        }
        val estimate = hll.eval(buffer).asInstanceOf[Long].toDouble
        val cardinality = c(n)
        val error = math.abs((estimate / cardinality.toDouble) - 1.0d)
        assert(error < hll.trueRsd * 3.0d, "Error should be within 3 std. errors.")
    }
  }

  test("deterministic cardinality estimation") {
    val repeats = 10
    testCardinalityEstimates(
      Seq(0.1, 0.05, 0.025, 0.01, 0.001),
      Seq(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000).map(_ * repeats),
      i => i / repeats,
      i => i / repeats)
  }

  test("random cardinality estimation") {
    val srng = new Random(323981238L)
    val seen = mutable.HashSet.empty[Int]
    val update = (i: Int) => {
      val value = srng.nextInt()
      seen += value
      value
    }
    val eval = (n: Int) => {
      val cardinality = seen.size
      seen.clear()
      cardinality
    }
    testCardinalityEstimates(
      Seq(0.05, 0.01),
      Seq(100, 10000, 500000),
      update,
      eval)
  }

  // Test merging
  test("merging HLL instances") {
    val (hll, input, buffer1a) = createEstimator(0.05)
    val buffer1b = createBuffer(hll)
    val buffer2 = createBuffer(hll)

    // Create the
    // Add the lower half
    var i = 0
    while (i < 500000) {
      input.setInt(0, i)
      hll.update(buffer1a, input)
      i += 1
    }

    // Add the upper half
    i = 500000
    while (i < 1000000) {
      input.setInt(0, i)
      hll.update(buffer1b, input)
      i += 1
    }

    // Merge the lower and upper halves.
    hll.merge(buffer1a, buffer1b)

    // Create the other buffer in reverse
    i = 999999
    while (i >= 0) {
      input.setInt(0, i)
      hll.update(buffer2, input)
      i -= 1
    }

    // Check if the buffers are equal.
    assert(buffer2 == buffer1a, "Buffers should be equal")
  }
}

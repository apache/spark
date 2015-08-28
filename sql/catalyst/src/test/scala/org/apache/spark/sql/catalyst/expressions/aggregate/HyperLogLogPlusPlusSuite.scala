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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{MutableRow, BoundReference, SpecificMutableRow}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable
import org.scalatest.Assertions._

/**
 * Created by hvanhovell on 8/27/15.
 */
class HyperLogLogPlusPlusSuite extends SparkFunSuite {

  def aggregator(rsd: Double): (HyperLogLogPlusPlus, MutableRow) = {
    val hll = new HyperLogLogPlusPlus(new BoundReference(0, IntegerType, false), rsd)
    val buffer = new SpecificMutableRow(hll.bufferAttributes.map(_.dataType))
    hll.initialize(buffer)
    (hll, buffer)
  }

  test("deterministic cardinality estimation") {
    val repeats = 10
    for {
      rsd <- Seq(0.1, 0.05, 0.025, 0.01)
      n <- Seq(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000)} {
      val input = new SpecificMutableRow(Seq(IntegerType))
      val (hll, buffer) = aggregator(rsd)
      var j = 0
      while (j < repeats) {
        var i = 0
        while (i < n) {
          input.setInt(0, i)
          hll.update(buffer, input)
          i += 1
        }
        j += 1
      }
      val estimate = hll.eval(buffer).asInstanceOf[Long].toDouble
      val error = math.abs(estimate/n - 1.0d)
      assert(error < hll.trueRsd * 3.0d, "Error should be within 3 std. errors.")
    }
  }

  // TODO Make more DRY...
  test("random cardinality estimation") {
    val srng = new Random(323981238L)
    for {
      rsd <- Seq(0.05, 0.01)
      n <- Seq(100, 10000, 500000)} {
      val rng = new Random(srng.nextLong())
      val input = new SpecificMutableRow(Seq(IntegerType))
      val (hll, buffer) = aggregator(rsd)
      val seen = mutable.HashSet.empty[Int]
      (0 to n * 10).foreach { _ =>
        val value = rng.nextInt(n)
        input.setInt(0, value)
        hll.update(buffer, input)
        seen += value
      }
      val cardinality = seen.size.toDouble
      val estimate = hll.eval(buffer).asInstanceOf[Long].toDouble
      val error = math.abs(estimate/cardinality - 1.0d)
      assert(error < hll.trueRsd * 3.0d, "Error should be within 3 std. errors.")
    }
  }

  // Test merging
  test("merging HLL instances") {
    val (hll1a, buffer1a) = aggregator(0.05)
    val (hll1b, buffer1b) = aggregator(0.05)
    val (hll2, buffer2) = aggregator(0.05)
    val input = new SpecificMutableRow(Seq(IntegerType))

    // Create the
    // Add the lower half
    var i = 0
    while (i < 500000) {
      input.setInt(0, i)
      hll1a.update(buffer1a, input)
      i += 1
    }

    // Add the upper half
    i = 500000
    while (i < 1000000) {
      input.setInt(0, i)
      hll1b.update(buffer1b, input)
      i += 1
    }

    // Merge the lower and upper halfs.
    hll1a.merge(buffer1a, buffer1b)

    // Create the other buffer in reverse
    i = 999999
    while (i >= 0) {
      input.setInt(0, i)
      hll2.update(buffer2, input)
      i -= 1
    }

    // Check if the buffers are equal.
    assert(buffer2 == buffer1a, "Buffers should be equal")
  }
}

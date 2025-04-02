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

class PercentileHeapSuite extends SparkFunSuite {

  test("When PercentileHeap is empty, NoSuchElementException is thrown.") {
    val medianHeap = new PercentileHeap(0.5)
    intercept[NoSuchElementException] {
      medianHeap.percentile()
    }
  }

  private def percentile(nums: Seq[Int], percentage: Double): Double = {
    val p = (nums.length * percentage).toInt
    nums.sorted.toIndexedSeq(p)
  }

  private def testPercentileFor(nums: Seq[Int], percentage: Double) = {
    val h = new PercentileHeap(percentage)
    Random.shuffle(nums).foreach(h.insert(_))
    assert(h.size() == nums.length)
    assert(h.percentile() == percentile(nums, percentage))
  }

  private val tests = Seq(
    0 until 1,
    0 until 2,
    0 until 11,
    0 until 42,
    0 until 100
  )

  for (t <- tests) {
    for (p <- Seq(1, 50, 99)) {
      test(s"$p% of ${t.mkString(",")}") {
        testPercentileFor(t, p / 100d)
      }
    }
  }

  ignore("benchmark") {
    val input: Seq[Int] = 0 until 1000
    val numRuns = 1000

    def kernel(): Long = {
      val shuffled = Random.shuffle(input).toArray
      val start = System.nanoTime()
      val h = new PercentileHeap(0.95)
      shuffled.foreach { x =>
        h.insert(x)
        for (_ <- 0 until h.size()) h.percentile()
      }
      System.nanoTime() - start
    }
    for (_ <- 0 until numRuns) kernel()  // warmup

    var elapsed: Long = 0
    for (_ <- 0 until numRuns) elapsed += kernel()
    val perOp = elapsed / (numRuns * input.length)
    // scalastyle:off println
    println(s"$perOp ns per op on heaps of size ${input.length}")
    // scalastyle:on println
  }
}

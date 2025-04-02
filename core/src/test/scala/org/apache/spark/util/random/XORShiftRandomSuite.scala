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

package org.apache.spark.util.random

import org.apache.commons.math3.stat.inference.ChiSquareTest
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils.times

class XORShiftRandomSuite extends SparkFunSuite with Matchers {

  /*
   * This test is based on a chi-squared test for randomness.
   */
  test ("XORShift generates valid random numbers") {

    val xorRand = new XORShiftRandom(1L)

    val numBins = 10 // create 10 bins
    val numRows = 5 // create 5 rows
    val bins = Array.ofDim[Long](numRows, numBins)

    // populate bins based on modulus of the random number for each row
    for (r <- 0 until numRows) {
      times(100000000) {
        bins(r)(math.abs(xorRand.nextInt) % numBins) += 1
      }
    }

    /*
     * Perform the chi square test on the 5 rows of randomly generated numbers evenly divided into
     * 10 bins. chiSquareTest returns true iff the null hypothesis (that the classifications
     * represented by the counts in the columns of the input 2-way table are independent of the
     * rows) can be rejected with 100 * (1 - alpha) percent confidence, where alpha is prespecified
     * as 0.05
     */
    val chiTest = new ChiSquareTest
    assert(chiTest.chiSquareTest(bins, 0.05) === false)
  }

  test ("XORShift with zero seed") {
    val random = new XORShiftRandom(0L)
    assert(random.nextInt() != 0)
  }

  test ("hashSeed has random bits throughout") {
    val totalBitCount = (0 until 10).map { seed =>
      val hashed = XORShiftRandom.hashSeed(seed)
      val bitCount = java.lang.Long.bitCount(hashed)
      // make sure we have roughly equal numbers of 0s and 1s.  Mostly just check that we
      // don't have all 0s or 1s in the high bits
      bitCount should be > 20
      bitCount should be < 44
      bitCount
    }.sum
    // and over all the seeds, very close to equal numbers of 0s & 1s
    totalBitCount should be > (32 * 10 - 30)
    totalBitCount should be < (32 * 10 + 30)
  }
}

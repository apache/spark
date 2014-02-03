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

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.apache.spark.util.Utils.times

class XORShiftRandomSuite extends FunSuite with ShouldMatchers {

  def fixture = new {
    val seed = 1L
    val xorRand = new XORShiftRandom(seed)
    val hundMil = 1e8.toInt
  }
   
  /*
   * This test is based on a chi-squared test for randomness. The values are hard-coded 
   * so as not to create Spark's dependency on apache.commons.math3 just to call one
   * method for calculating the exact p-value for a given number of random numbers
   * and bins. In case one would want to move to a full-fledged test based on 
   * apache.commons.math3, the relevant class is here:
   * org.apache.commons.math3.stat.inference.ChiSquareTest
   */
  test ("XORShift generates valid random numbers") {

    val f = fixture

    val numBins = 10
    // create 10 bins
    val bins = Array.fill(numBins)(0)

    // populate bins based on modulus of the random number
    times(f.hundMil) {bins(math.abs(f.xorRand.nextInt) % 10) += 1}

    /* since the seed is deterministic, until the algorithm is changed, we know the result will be 
     * exactly this: Array(10004908, 9993136, 9994600, 10000744, 10000091, 10002474, 10002272, 
     * 10000790, 10002286, 9998699), so the test will never fail at the prespecified (5%) 
     * significance level. However, should the RNG implementation change, the test should still 
     * pass at the same significance level. The chi-squared test done in R gave the following 
     * results:
     *   > chisq.test(c(10004908, 9993136, 9994600, 10000744, 10000091, 10002474, 10002272,
     *     10000790, 10002286, 9998699))
     *     Chi-squared test for given probabilities
     *     data:  c(10004908, 9993136, 9994600, 10000744, 10000091, 10002474, 10002272, 10000790, 
     *            10002286, 9998699)
     *     X-squared = 11.975, df = 9, p-value = 0.2147
     * Note that the p-value was ~0.22. The test will fail if alpha < 0.05, which for 100 million 
     * random numbers
     * and 10 bins will happen at X-squared of ~16.9196. So, the test will fail if X-squared
     * is greater than or equal to that number.
     */
    val binSize = f.hundMil/numBins
    val xSquared = bins.map(x => math.pow((binSize - x), 2)/binSize).sum
    xSquared should be <  (16.9196)

  }

}

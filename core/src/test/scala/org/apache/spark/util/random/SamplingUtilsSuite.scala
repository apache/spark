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

import org.apache.commons.math3.distribution.{BinomialDistribution, PoissonDistribution}
import org.scalatest.FunSuite

class SamplingUtilsSuite extends FunSuite {

  test("computeFraction") {
    // test that the computed fraction guarantees enough data points
    // in the sample with a failure rate <= 0.0001
    val n = 100000

    for (s <- 1 to 15) {
      val frac = SamplingUtils.computeFractionForSampleSize(s, n, true)
      val poisson = new PoissonDistribution(frac * n)
      assert(poisson.inverseCumulativeProbability(0.0001) >= s, "Computed fraction is too low")
    }
    for (s <- List(20, 100, 1000)) {
      val frac = SamplingUtils.computeFractionForSampleSize(s, n, true)
      val poisson = new PoissonDistribution(frac * n)
      assert(poisson.inverseCumulativeProbability(0.0001) >= s, "Computed fraction is too low")
    }
    for (s <- List(1, 10, 100, 1000)) {
      val frac = SamplingUtils.computeFractionForSampleSize(s, n, false)
      val binomial = new BinomialDistribution(n, frac)
      assert(binomial.inverseCumulativeProbability(0.0001)*n >= s, "Computed fraction is too low")
    }
  }
}

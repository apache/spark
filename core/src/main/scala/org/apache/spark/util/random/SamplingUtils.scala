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

import org.apache.commons.math3.distribution.PoissonDistribution

private[spark] object SamplingUtils {

  /**
   * Returns a sampling rate that guarantees a sample of size >= sampleSizeLowerBound 99.99% of
   * the time.
   *
   * How the sampling rate is determined:
   * Let p = num / total, where num is the sample size and total is the total number of
   * datapoints in the RDD. We're trying to compute q > p such that
   *   - when sampling with replacement, we're drawing each datapoint with prob_i ~ Pois(q),
   *     where we want to guarantee Pr[s < num] < 0.0001 for s = sum(prob_i for i from 0 to total),
   *     i.e. the failure rate of not having a sufficiently large sample < 0.0001.
   *     Setting q = p + 5 * sqrt(p/total) is sufficient to guarantee 0.9999 success rate for
   *     num > 12, but we need a slightly larger q (9 empirically determined).
   *   - when sampling without replacement, we're drawing each datapoint with prob_i
   *     ~ Binomial(total, fraction) and our choice of q guarantees 1-delta, or 0.9999 success
   *     rate, where success rate is defined the same as in sampling with replacement.
   *
   * The smallest sampling rate supported is 1e-10 (in order to avoid running into the limit of the
   * RNG's resolution).
   *
   * @param sampleSizeLowerBound sample size
   * @param total size of RDD
   * @param withReplacement whether sampling with replacement
   * @return a sampling rate that guarantees sufficient sample size with 99.99% success rate
   */
  def computeFractionForSampleSize(sampleSizeLowerBound: Int, total: Long,
      withReplacement: Boolean): Double = {
    val fraction = sampleSizeLowerBound.toDouble / total
    if (withReplacement) {
      val numStDev = if (sampleSizeLowerBound < 12) 9 else 5
      math.max(1e-10, fraction + numStDev * math.sqrt(fraction / total))
    } else {
      val delta = 1e-4
      val gamma = - math.log(delta) / total
      math.min(1,
        math.max(1e-10, fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction)))
    }
  }
}

/**
 * Utility functions that help us determine bounds on adjusted sampling rate to guarantee exact
 * sample sizes with high confidence when sampling with replacement.
 *
 * The algorithm for guaranteeing sample size instantly accepts items whose associated value drawn
 * from Pois(s) is less than the lower bound and puts items whose value is between the lower and
 * upper bound in a waitlist. The final sample is consisted of all items accepted on the fly and a
 * portion of the waitlist needed to make the exact sample size.
 */
private[spark] object PoissonBounds {

  val delta = 1e-4 / 3.0
  val epsilon = 1e-15

  /**
   * Compute the threshold for accepting items on the fly. The threshold value is a fairly small
   * number, which means if the item has an associated value < threshold, it is highly likely to
   * be in the final sample. Hence we accept items with values less than the returned value of this
   * function instantly.
   *
   * @param s sample size
   * @return threshold for accepting items on the fly
   */
  def getLowerBound(s: Double): Double = {
    var lb = math.max(0.0, s - math.sqrt(s / delta)) // Chebyshev's inequality
    var ub = s
    while (lb < ub - 1.0) {
      val m = (lb + ub) / 2.0
      val poisson = new PoissonDistribution(m, epsilon)
      val y = poisson.inverseCumulativeProbability(1 - delta)
      if (y > s) ub = m else lb = m
    }
    lb
  }

  def getMinCount(lmbd: Double): Double = {
    if (lmbd == 0) {
      0
    } else {
      val poisson = new PoissonDistribution(lmbd, epsilon)
      poisson.inverseCumulativeProbability(delta)
    }
  }

  /**
   * Compute the threshold for waitlisting items. An item is waitlisted if its associated value is
   * greater than the lower bound determined above but below the upper bound computed here.
   * The value is computed such that we only need to keep log(s) items in the waitlist and still be
   * able to guarantee sample size with high confidence.
   *
   * @param s sample size
   * @return threshold for waitlisting the item
   */
  def getUpperBound(s: Double): Double = {
    var lb = s
    var ub = s + math.sqrt(s / delta) // Chebyshev's inequality
    while (lb < ub - 1.0) {
      val m = (lb + ub) / 2.0
      val poisson = new PoissonDistribution(m, epsilon)
      val y = poisson.inverseCumulativeProbability(delta)
      if (y >= s) ub = m else lb = m
    }
    ub
  }
}

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

package org.apache.spark.partial

import org.apache.commons.math3.distribution.{PascalDistribution, PoissonDistribution}

/**
 * An ApproximateEvaluator for counts.
 */
private[spark] class CountEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[Long, BoundedDouble] {

  private var outputsMerged = 0
  private var sum: Long = 0

  override def merge(outputId: Int, taskResult: Long): Unit = {
    outputsMerged += 1
    sum += taskResult
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(sum, 1.0, sum, sum)
    } else if (outputsMerged == 0 || sum == 0) {
      new BoundedDouble(0, 0.0, 0.0, Double.PositiveInfinity)
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      CountEvaluator.bound(confidence, sum, p)
    }
  }
}

private[partial] object CountEvaluator {

  def bound(confidence: Double, sum: Long, p: Double): BoundedDouble = {
    // Let the total count be N. A fraction p has been counted already, with sum 'sum',
    // as if each element from the total data set had been seen with probability p.
    val dist =
      if (sum <= 10000) {
        // The remaining count, k=N-sum, may be modeled as negative binomial (aka Pascal),
        // where there have been 'sum' successes of probability p already. (There are several
        // conventions, but this is the one followed by Commons Math3.)
        new PascalDistribution(sum.toInt, p)
      } else {
        // For large 'sum' (certainly, > Int.MaxValue!), use a Poisson approximation, which has
        // a different interpretation. "sum" elements have been observed having scanned a fraction
        // p of the data. This suggests data is counted at a rate of sum / p across the whole data
        // set. The total expected count from the rest is distributed as
        // (1-p) Poisson(sum / p) = Poisson(sum*(1-p)/p)
        new PoissonDistribution(sum * (1 - p) / p)
      }
    // Not quite symmetric; calculate interval straight from discrete distribution
    val low = dist.inverseCumulativeProbability((1 - confidence) / 2)
    val high = dist.inverseCumulativeProbability((1 + confidence) / 2)
    // Add 'sum' to each because distribution is just of remaining count, not observed
    new BoundedDouble(sum + dist.getNumericalMean, confidence, sum + low, sum + high)
  }


}

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

import org.apache.commons.math3.distribution.{NormalDistribution, TDistribution}

import org.apache.spark.util.StatCounter

/**
 * An ApproximateEvaluator for sums. It estimates the mean and the count and multiplies them
 * together, then uses the formula for the variance of two independent random variables to get
 * a variance for the result and compute a confidence interval.
 */
private[spark] class SumEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[StatCounter, BoundedDouble] {

  // modified in merge
  var outputsMerged = 0
  val counter = new StatCounter

  override def merge(outputId: Int, taskResult: StatCounter) {
    outputsMerged += 1
    counter.merge(taskResult)
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(counter.sum, 1.0, counter.sum, counter.sum)
    } else if (outputsMerged == 0 || counter.count == 0) {
      new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      val meanEstimate = counter.mean
      val countEstimate = (counter.count + 1 - p) / p
      val sumEstimate = meanEstimate * countEstimate

      val meanVar = counter.sampleVariance / counter.count

      // branch at this point because counter.count == 1 implies counter.sampleVariance == Nan
      // and we don't want to ever return a bound of NaN
      if (meanVar.isNaN || counter.count == 1) {
        new BoundedDouble(sumEstimate, confidence, Double.NegativeInfinity, Double.PositiveInfinity)
      } else {
        val countVar = (counter.count + 1) * (1 - p) / (p * p)
        val sumVar = (meanEstimate * meanEstimate * countVar) +
          (countEstimate * countEstimate * meanVar) +
          (meanVar * countVar)
        val sumStdev = math.sqrt(sumVar)
        val confFactor = if (counter.count > 100) {
          new NormalDistribution().inverseCumulativeProbability(1 - (1 - confidence) / 2)
        } else {
          // note that if this goes to 0, TDistribution will throw an exception.
          // Hence special casing 1 above.
          val degreesOfFreedom = (counter.count - 1).toInt
          new TDistribution(degreesOfFreedom).inverseCumulativeProbability(1 - (1 - confidence) / 2)
        }

        val low = sumEstimate - confFactor * sumStdev
        val high = sumEstimate + confFactor * sumStdev
        new BoundedDouble(sumEstimate, confidence, low, high)
      }
    }
  }
}

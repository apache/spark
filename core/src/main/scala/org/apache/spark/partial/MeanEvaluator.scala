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
 * An ApproximateEvaluator for means.
 */
private[spark] class MeanEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[StatCounter, BoundedDouble] {

  private var outputsMerged = 0
  private val counter = new StatCounter()

  override def merge(outputId: Int, taskResult: StatCounter): Unit = {
    outputsMerged += 1
    counter.merge(taskResult)
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(counter.mean, 1.0, counter.mean, counter.mean)
    } else if (outputsMerged == 0 || counter.count == 0) {
      new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
    } else if (counter.count == 1) {
      new BoundedDouble(counter.mean, confidence, Double.NegativeInfinity, Double.PositiveInfinity)
    } else {
      val mean = counter.mean
      val stdev = math.sqrt(counter.sampleVariance / counter.count)
      val confFactor = if (counter.count > 100) {
          // For large n, the normal distribution is a good approximation to t-distribution
          new NormalDistribution().inverseCumulativeProbability((1 + confidence) / 2)
        } else {
          // t-distribution describes distribution of actual population mean
          // note that if this goes to 0, TDistribution will throw an exception.
          // Hence special casing 1 above.
          val degreesOfFreedom = (counter.count - 1).toInt
          new TDistribution(degreesOfFreedom).inverseCumulativeProbability((1 + confidence) / 2)
        }
      // Symmetric, so confidence interval is symmetric about mean of distribution
      val low = mean - confFactor * stdev
      val high = mean + confFactor * stdev
      new BoundedDouble(mean, confidence, low, high)
    }
  }
}

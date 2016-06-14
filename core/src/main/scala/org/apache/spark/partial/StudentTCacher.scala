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

/**
 * A utility class for caching Student's T distribution values for a given confidence level
 * and various sample sizes. This is used by the MeanEvaluator to efficiently calculate
 * confidence intervals for many keys.
 */
private[spark] class StudentTCacher(confidence: Double) {

  val NORMAL_APPROX_SAMPLE_SIZE = 100  // For samples bigger than this, use Gaussian approximation

  val normalApprox = new NormalDistribution().inverseCumulativeProbability(1 - (1 - confidence) / 2)
  val cache = Array.fill[Double](NORMAL_APPROX_SAMPLE_SIZE)(-1.0)

  def get(sampleSize: Long): Double = {
    if (sampleSize >= NORMAL_APPROX_SAMPLE_SIZE) {
      normalApprox
    } else {
      val size = sampleSize.toInt
      if (cache(size) < 0) {
        val tDist = new TDistribution(size - 1)
        cache(size) = tDist.inverseCumulativeProbability(1 - (1 - confidence) / 2)
      }
      cache(size)
    }
  }
}

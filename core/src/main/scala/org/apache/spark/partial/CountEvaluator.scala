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

import cern.jet.stat.Probability

/**
 * An ApproximateEvaluator for counts.
 *
 * TODO: There's currently a lot of shared code between this and GroupedCountEvaluator. It might
 * be best to make this a special case of GroupedCountEvaluator with one group.
 */
private[spark] class CountEvaluator(totalOutputs: Int, confidence: Double)
  extends ApproximateEvaluator[Long, BoundedDouble] {

  var outputsMerged = 0
  var sum: Long = 0

  override def merge(outputId: Int, taskResult: Long) {
    outputsMerged += 1
    sum += taskResult
  }

  override def currentResult(): BoundedDouble = {
    if (outputsMerged == totalOutputs) {
      new BoundedDouble(sum, 1.0, sum, sum)
    } else if (outputsMerged == 0) {
      new BoundedDouble(0, 0.0, Double.NegativeInfinity, Double.PositiveInfinity)
    } else {
      val p = outputsMerged.toDouble / totalOutputs
      val mean = (sum + 1 - p) / p
      val variance = (sum + 1) * (1 - p) / (p * p)
      val stdev = math.sqrt(variance)
      val confFactor = Probability.normalInverse(1 - (1 - confidence) / 2)
      val low = mean - confFactor * stdev
      val high = mean + confFactor * stdev
      new BoundedDouble(mean, confidence, low, high)
    }
  }
}

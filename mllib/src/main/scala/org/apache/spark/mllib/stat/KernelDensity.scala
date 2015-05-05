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

package org.apache.spark.mllib.stat

import org.apache.spark.rdd.RDD

private[stat] object KernelDensity {
  /**
   * Given a set of samples from a distribution, estimates its density at the set of given points.
   * Uses a Gaussian kernel with the given standard deviation.
   */
  def estimate(samples: RDD[Double], standardDeviation: Double,
      evaluationPoints: Array[Double]): Array[Double] = {
    if (standardDeviation <= 0.0) {
      throw new IllegalArgumentException("Standard deviation must be positive")
    }

    // This gets used in each Gaussian PDF computation, so compute it up front
    val logStandardDeviationPlusHalfLog2Pi =
      Math.log(standardDeviation) + 0.5 * Math.log(2 * Math.PI)

    val (points, count) = samples.aggregate((new Array[Double](evaluationPoints.length), 0))(
      (x, y) => {
        var i = 0
        while (i < evaluationPoints.length) {
          x._1(i) += normPdf(y, standardDeviation, logStandardDeviationPlusHalfLog2Pi,
            evaluationPoints(i))
          i += 1
        }
        (x._1, i)
      },
      (x, y) => {
        var i = 0
        while (i < evaluationPoints.length) {
          x._1(i) += y._1(i)
          i += 1
        }
        (x._1, x._2 + y._2)
      })

    var i = 0
    while (i < points.length) {
      points(i) /= count
      i += 1
    }
    points
  }

  private def normPdf(mean: Double, standardDeviation: Double,
      logStandardDeviationPlusHalfLog2Pi: Double, x: Double): Double = {
    val x0 = x - mean
    val x1 = x0 / standardDeviation
    val logDensity = -0.5 * x1 * x1 - logStandardDeviationPlusHalfLog2Pi
    Math.exp(logDensity)
  }
}

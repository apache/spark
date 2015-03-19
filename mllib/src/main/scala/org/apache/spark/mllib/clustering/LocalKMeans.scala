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

package org.apache.spark.mllib.clustering

import scala.util.Random

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}

/**
 * An utility object to run K-means locally. This is private to the ML package because it's used
 * in the initialization of KMeans but not meant to be publicly exposed.
 */
private[mllib] object LocalKMeans extends Logging {

  /**
   * Run K-means++ on the weighted point set `points`. This first does the K-means++
   * initialization procedure and then rounds of Lloyd's algorithm.
   */
  def kMeansPlusPlus(
      seed: Int,
      points: Array[VectorWithNorm],
      weights: Array[Double],
      k: Int,
      maxIterations: Int
  ): Array[VectorWithNorm] = {
    val rand = new Random(seed)
    val dimensions = points(0).vector.size
    val centers = new Array[VectorWithNorm](k)

    // Initialize centers by sampling using the k-means++ procedure.
    centers(0) = pickWeighted(rand, points, weights).toDense
    for (i <- 1 until k) {
      // Pick the next center with a probability proportional to cost under current centers
      val curCenters = centers.view.take(i)
      val sum = points.view.zip(weights).map { case (p, w) =>
        w * KMeans.pointCost(curCenters, p)
      }.sum
      val r = rand.nextDouble() * sum
      var cumulativeScore = 0.0
      var j = 0
      while (j < points.length && cumulativeScore < r) {
        cumulativeScore += weights(j) * KMeans.pointCost(curCenters, points(j))
        j += 1
      }
      if (j == 0) {
        logWarning("kMeansPlusPlus initialization ran out of distinct points for centers." +
          s" Using duplicate point for center k = $i.")
        centers(i) = points(0).toDense
      } else {
        centers(i) = points(j - 1).toDense
      }
    }

    // Run up to maxIterations iterations of Lloyd's algorithm
    val oldClosest = Array.fill(points.length)(-1)
    var iteration = 0
    var moved = true
    while (moved && iteration < maxIterations) {
      moved = false
      val counts = Array.fill(k)(0.0)
      val sums = Array.fill(k)(Vectors.zeros(dimensions))
      var i = 0
      while (i < points.length) {
        val p = points(i)
        val index = KMeans.findClosest(centers, p)._1
        axpy(weights(i), p.vector, sums(index))
        counts(index) += weights(i)
        if (index != oldClosest(i)) {
          moved = true
          oldClosest(i) = index
        }
        i += 1
      }
      // Update centers
      var j = 0
      while (j < k) {
        if (counts(j) == 0.0) {
          // Assign center to a random point
          centers(j) = points(rand.nextInt(points.length)).toDense
        } else {
          scal(1.0 / counts(j), sums(j))
          centers(j) = new VectorWithNorm(sums(j))
        }
        j += 1
      }
      iteration += 1
    }

    if (iteration == maxIterations) {
      logInfo(s"Local KMeans++ reached the max number of iterations: $maxIterations.")
    } else {
      logInfo(s"Local KMeans++ converged in $iteration iterations.")
    }

    centers
  }

  private def pickWeighted[T](rand: Random, data: Array[T], weights: Array[Double]): T = {
    val r = rand.nextDouble() * weights.sum
    var i = 0
    var curWeight = 0.0
    while (i < data.length && curWeight < r) {
      curWeight += weights(i)
      i += 1
    }
    data(i - 1)
  }
}

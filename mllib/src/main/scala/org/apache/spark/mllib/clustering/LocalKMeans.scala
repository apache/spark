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

import breeze.linalg.{Vector => BV, DenseVector => BDV}

/**
 * An utility object to run K-means locally. This is private to the ML package because it's used
 * in the initialization of KMeans but not meant to be publicly exposed.
 */
private[mllib] object LocalKMeans {

  def kMeansPlusPlus(
      seed: Int,
      points: Array[Array[Double]],
      weights: Array[Double],
      k: Int,
      maxIterations: Int
  ): Array[Array[Double]] = {
    val breezePoints = points.map(v => new BDV[Double](v).asInstanceOf[BV[Double]])
    val breezeCenters = kMeansPlusPlus(seed, breezePoints, weights, k, maxIterations)
    breezeCenters.map(_.toArray)
  }

  /**
   * Run K-means++ on the weighted point set `points`. This first does the K-means++
   * initialization procedure and then rounds of Lloyd's algorithm.
   */
  def kMeansPlusPlus(
      seed: Int,
      points: Array[BV[Double]],
      weights: Array[Double],
      k: Int,
      maxIterations: Int
  )(implicit d: DummyImplicit): Array[BV[Double]] = {
    val rand = new Random(seed)
    val dimensions = points(0).length
    val centers = new Array[BV[Double]](k)

    // Initialize centers by sampling using the k-means++ procedure.
    centers(0) = (pickWeighted(rand, points, weights)).toDenseVector
    for (i <- 1 until k) {
      // Pick the next center with a probability proportional to cost under current centers
      val curCenters = centers.slice(0, i)
      val sum = points.zip(weights).map { case (p, w) =>
        w * KMeans.pointCost(curCenters, p)
      }.sum
      val r = rand.nextDouble() * sum
      var cumulativeScore = 0.0
      var j = 0
      while (j < points.length && cumulativeScore < r) {
        cumulativeScore += weights(j) * KMeans.pointCost(curCenters, points(j))
        j += 1
      }
      centers(i) = points(j-1).toDenseVector
    }

    // Run up to maxIterations iterations of Lloyd's algorithm
    val oldClosest = Array.fill(points.length)(-1)
    var iteration = 0
    var moved = true
    while (moved && iteration < maxIterations) {
      moved = false
      val sums = Array.fill(k)(
        new BDV[Double](new Array[Double](dimensions)).asInstanceOf[BV[Double]]
      )
      val counts = Array.fill(k)(0.0)
      for ((p, i) <- points.zipWithIndex) {
        val index = KMeans.findClosest(centers, p)._1
        breeze.linalg.axpy(weights(i), p, sums(index))
        counts(index) += weights(i)
        if (index != oldClosest(i)) {
          moved = true
          oldClosest(i) = index
        }
      }
      // Update centers
      for (i <- 0 until k) {
        if (counts(i) == 0.0) {
          // Assign center to a random point
          centers(i) = points(rand.nextInt(points.length)).toDenseVector
        } else {
          sums(i) /= counts(i)
          centers(i) = sums(i)
        }
      }
      iteration += 1
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

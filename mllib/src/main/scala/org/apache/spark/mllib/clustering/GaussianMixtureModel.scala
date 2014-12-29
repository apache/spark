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

import breeze.linalg.{DenseVector => BreezeVector}

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.stat.impl.MultivariateGaussian
import org.apache.spark.mllib.util.MLUtils

/**
 * Multivariate Gaussian Mixture Model (GMM) consisting of k Gaussians, where points 
 * are drawn from each Gaussian i=1..k with probability w(i); mu(i) and sigma(i) are 
 * the respective mean and covariance for each Gaussian distribution i=1..k. 
 * 
 * @param weight Weights for each Gaussian distribution in the mixture, where weight(i) is
 *               the weight for Gaussian i, and weight.sum == 1
 * @param mu Means for each Gaussian in the mixture, where mu(i) is the mean for Gaussian i
 * @param sigma Covariance maxtrix for each Gaussian in the mixture, where sigma(i) is the
 *              covariance matrix for Gaussian i
 */
class GaussianMixtureModel(
  val weight: Array[Double], 
  val mu: Array[Vector], 
  val sigma: Array[Matrix]) extends Serializable {
  
  /** Number of gaussians in mixture */
  def k: Int = weight.length

  /** Maps given points to their cluster indices. */
  def predict(points: RDD[Vector]): RDD[Int] = {
    val responsibilityMatrix = predictMembership(points, mu, sigma, weight, k)
    responsibilityMatrix.map(r => r.indexOf(r.max))
  }
  
  /**
   * Given the input vectors, return the membership value of each vector
   * to all mixture components. 
   */
  def predictMembership(
      points: RDD[Vector], 
      mu: Array[Vector], 
      sigma: Array[Matrix],
      weight: Array[Double], 
      k: Int): RDD[Array[Double]] = {
    val sc = points.sparkContext
    val dists = sc.broadcast {
      (0 until k).map { i => 
        new MultivariateGaussian(mu(i).toBreeze.toDenseVector, sigma(i).toBreeze.toDenseMatrix)
      }.toArray
    }
    val weights = sc.broadcast(weight)
    points.map { x => 
      computeSoftAssignments(x.toBreeze.toDenseVector, dists.value, weights.value, k)
    }
  }
  
  /**
   * Compute the partial assignments for each vector
   */
  private def computeSoftAssignments(
      pt: BreezeVector[Double],
      dists: Array[MultivariateGaussian],
      weights: Array[Double],
      k: Int): Array[Double] = {
    val p = weights.zip(dists).map {
      case (weight, dist) => MLUtils.EPSILON + weight * dist.pdf(pt)
    }
    val pSum = p.sum 
    for (i <- 0 until k) {
      p(i) /= pSum
    }
    p
  }  
}

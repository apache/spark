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

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 *
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
@Experimental
class GaussianMixtureModel(
  val weights: Array[Double], 
  val gaussians: Array[MultivariateGaussian]) extends Serializable {
  
  require(weights.length == gaussians.length, "Length of weight and Gaussian arrays must match")
  
  /** Number of gaussians in mixture */
  def k: Int = weights.length

  /** Maps given points to their cluster indices. */
  def predict(points: RDD[Vector]): RDD[Int] = {
    val responsibilityMatrix = predictSoft(points)
    responsibilityMatrix.map(r => r.indexOf(r.max))
  }
  
  /**
   * Given the input vectors, return the membership value of each vector
   * to all mixture components. 
   */
  def predictSoft(points: RDD[Vector]): RDD[Array[Double]] = {
    val sc = points.sparkContext
    val bcDists = sc.broadcast(gaussians)
    val bcWeights = sc.broadcast(weights)
    points.map { x => 
      computeSoftAssignments(x.toBreeze.toDenseVector, bcDists.value, bcWeights.value, k)
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

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

import scala.collection.mutable.IndexedSeq

import breeze.linalg.{DenseVector => BreezeVector, DenseMatrix => BreezeMatrix, diag, Transpose}

import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors, DenseVector, DenseMatrix, BLAS}
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * This class performs expectation maximization for multivariate Gaussian
 * Mixture Models (GMMs).  A GMM represents a composite distribution of
 * independent Gaussian distributions with associated "mixing" weights
 * specifying each's contribution to the composite.
 *
 * Given a set of sample points, this class will maximize the log-likelihood 
 * for a mixture of k Gaussians, iterating until the log-likelihood changes by 
 * less than convergenceTol, or until it has reached the max number of iterations.
 * While this process is generally guaranteed to converge, it is not guaranteed
 * to find a global optimum.  
 * 
 * @param k The number of independent Gaussians in the mixture model
 * @param convergenceTol The maximum change in log-likelihood at which convergence
 * is considered to have occurred.
 * @param maxIterations The maximum number of iterations to perform
 */
class GaussianMixture private (
    private var k: Int, 
    private var convergenceTol: Double, 
    private var maxIterations: Int,
    private var seed: Long) extends Serializable {
  
  /** A default instance, 2 Gaussians, 100 iterations, 0.01 log-likelihood threshold */
  def this() = this(2, 0.01, 100, Utils.random.nextLong())
  
  // number of samples per cluster to use when initializing Gaussians
  private val nSamples = 5
  
  // an initializing GMM can be provided rather than using the 
  // default random starting point
  private var initialModel: Option[GaussianMixtureModel] = None
  
  /** Set the initial GMM starting point, bypassing the random initialization.
   *  You must call setK() prior to calling this method, and the condition
   *  (model.k == this.k) must be met; failure will result in an IllegalArgumentException
   */
  def setInitialModel(model: GaussianMixtureModel): this.type = {
    if (model.k == k) {
      initialModel = Some(model)
    } else {
      throw new IllegalArgumentException("mismatched cluster count (model.k != k)")
    }
    this
  }
  
  /** Return the user supplied initial GMM, if supplied */
  def getInitialModel: Option[GaussianMixtureModel] = initialModel
  
  /** Set the number of Gaussians in the mixture model.  Default: 2 */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }
  
  /** Return the number of Gaussians in the mixture model */
  def getK: Int = k
  
  /** Set the maximum number of iterations to run. Default: 100 */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }
  
  /** Return the maximum number of iterations to run */
  def getMaxIterations: Int = maxIterations
  
  /**
   * Set the largest change in log-likelihood at which convergence is 
   * considered to have occurred.
   */
  def setConvergenceTol(convergenceTol: Double): this.type = {
    this.convergenceTol = convergenceTol
    this
  }
  
  /**
   * Return the largest change in log-likelihood at which convergence is
   * considered to have occurred.
   */
  def getConvergenceTol: Double = convergenceTol

  /** Set the random seed */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /** Return the random seed */
  def getSeed: Long = seed

  /** Perform expectation maximization */
  def run(data: RDD[Vector]): GaussianMixtureModel = {
    val sc = data.sparkContext
    
    // we will operate on the data as breeze data
    val breezeData = data.map(u => u.toBreeze.toDenseVector).cache()
    
    // Get length of the input vectors
    val d = breezeData.first().length
    
    // Determine initial weights and corresponding Gaussians.
    // If the user supplied an initial GMM, we use those values, otherwise
    // we start with uniform weights, a random mean from the data, and
    // diagonal covariance matrices using component variances
    // derived from the samples    
    val (weights, gaussians) = initialModel match {
      case Some(gmm) => (gmm.weights, gmm.gaussians)
      
      case None => {
        val samples = breezeData.takeSample(withReplacement = true, k * nSamples, seed)
        (Array.fill(k)(1.0 / k), Array.tabulate(k) { i => 
          val slice = samples.view(i * nSamples, (i + 1) * nSamples)
          new MultivariateGaussian(vectorMean(slice), initCovariance(slice)) 
        })  
      }
    }
    
    var llh = Double.MinValue // current log-likelihood 
    var llhp = 0.0            // previous log-likelihood
    
    var iter = 0
    while(iter < maxIterations && Math.abs(llh-llhp) > convergenceTol) {
      // create and broadcast curried cluster contribution function
      val compute = sc.broadcast(ExpectationSum.add(weights, gaussians)_)
      
      // aggregate the cluster contribution for all sample points
      val sums = breezeData.aggregate(ExpectationSum.zero(k, d))(compute.value, _ += _)
      
      // Create new distributions based on the partial assignments
      // (often referred to as the "M" step in literature)
      val sumWeights = sums.weights.sum
      var i = 0
      while (i < k) {
        val mu = sums.means(i) / sums.weights(i)
        BLAS.syr(-sums.weights(i), Vectors.fromBreeze(mu).asInstanceOf[DenseVector],
          Matrices.fromBreeze(sums.sigmas(i)).asInstanceOf[DenseMatrix])
        weights(i) = sums.weights(i) / sumWeights
        gaussians(i) = new MultivariateGaussian(mu, sums.sigmas(i) / sums.weights(i))
        i = i + 1
      }
   
      llhp = llh // current becomes previous
      llh = sums.logLikelihood // this is the freshly computed log-likelihood
      iter += 1
    } 
    
    new GaussianMixtureModel(weights, gaussians)
  }
    
  /** Average of dense breeze vectors */
  private def vectorMean(x: IndexedSeq[BreezeVector[Double]]): BreezeVector[Double] = {
    val v = BreezeVector.zeros[Double](x(0).length)
    x.foreach(xi => v += xi)
    v / x.length.toDouble 
  }
  
  /**
   * Construct matrix where diagonal entries are element-wise
   * variance of input vectors (computes biased variance)
   */
  private def initCovariance(x: IndexedSeq[BreezeVector[Double]]): BreezeMatrix[Double] = {
    val mu = vectorMean(x)
    val ss = BreezeVector.zeros[Double](x(0).length)
    x.map(xi => (xi - mu) :^ 2.0).foreach(u => ss += u)
    diag(ss / x.length.toDouble)
  }
}

// companion class to provide zero constructor for ExpectationSum
private object ExpectationSum {
  def zero(k: Int, d: Int): ExpectationSum = {
    new ExpectationSum(0.0, Array.fill(k)(0.0), 
      Array.fill(k)(BreezeVector.zeros(d)), Array.fill(k)(BreezeMatrix.zeros(d,d)))
  }
  
  // compute cluster contributions for each input point
  // (U, T) => U for aggregation
  def add(
      weights: Array[Double], 
      dists: Array[MultivariateGaussian])
      (sums: ExpectationSum, x: BreezeVector[Double]): ExpectationSum = {
    val p = weights.zip(dists).map {
      case (weight, dist) => MLUtils.EPSILON + weight * dist.pdf(x)
    }
    val pSum = p.sum
    sums.logLikelihood += math.log(pSum)
    val xxt = x * new Transpose(x)
    var i = 0
    while (i < sums.k) {
      p(i) /= pSum
      sums.weights(i) += p(i)
      sums.means(i) += x * p(i)
      BLAS.syr(p(i), Vectors.fromBreeze(x).asInstanceOf[DenseVector],
        Matrices.fromBreeze(sums.sigmas(i)).asInstanceOf[DenseMatrix])
      i = i + 1
    }
    sums
  }  
}

// Aggregation class for partial expectation results
private class ExpectationSum(
    var logLikelihood: Double,
    val weights: Array[Double],
    val means: Array[BreezeVector[Double]],
    val sigmas: Array[BreezeMatrix[Double]]) extends Serializable {
  
  val k = weights.length
  
  def +=(x: ExpectationSum): ExpectationSum = {
    var i = 0
    while (i < k) {
      weights(i) += x.weights(i)
      means(i) += x.means(i)
      sigmas(i) += x.sigmas(i)
      i = i + 1
    }
    logLikelihood += x.logLikelihood
    this
  }  
}

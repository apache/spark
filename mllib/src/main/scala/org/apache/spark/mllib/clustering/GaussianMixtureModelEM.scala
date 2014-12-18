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

import breeze.linalg.{DenseVector => BreezeVector, DenseMatrix => BreezeMatrix}
import breeze.linalg.Transpose

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.impl.MultivariateGaussian
import org.apache.spark.{Accumulator, AccumulatorParam, SparkContext}
import org.apache.spark.SparkContext.DoubleAccumulatorParam

import scala.collection.mutable.IndexedSeqView

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
class GaussianMixtureModelEM private (
    private var k: Int, 
    private var convergenceTol: Double, 
    private var maxIterations: Int) extends Serializable {
      
  // Type aliases for convenience
  private type DenseDoubleVector = BreezeVector[Double]
  private type DenseDoubleMatrix = BreezeMatrix[Double]
  private type VectorArrayView = IndexedSeqView[DenseDoubleVector, Array[DenseDoubleVector]]
  
  private type ExpectationSum = (
    Array[Double], // log-likelihood in index 0
    Array[Double], // array of weights
    Array[DenseDoubleVector], // array of means
    Array[DenseDoubleMatrix]) // array of cov matrices
  
  // create a zero'd ExpectationSum instance
  private def zeroExpectationSum(k: Int, d: Int): ExpectationSum = {
    (Array(0.0), 
      new Array[Double](k),
      (0 until k).map(_ => BreezeVector.zeros[Double](d)).toArray,
      (0 until k).map(_ => BreezeMatrix.zeros[Double](d,d)).toArray)
  }
  
  // add two ExpectationSum objects (allowed to use modify m1)
  // (U, U) => U for aggregation
  private def addExpectationSums(m1: ExpectationSum, m2: ExpectationSum): ExpectationSum = {
    m1._1(0) += m2._1(0)
    for (i <- 0 until m1._2.length) {
      m1._2(i) += m2._2(i)
      m1._3(i) += m2._3(i)
      m1._4(i) += m2._4(i)
    }
    m1
  }
  
  // compute cluster contributions for each input point
  // (U, T) => U for aggregation
  private def computeExpectation(
      weights: Array[Double], 
      dists: Array[MultivariateGaussian])
      (model: ExpectationSum, x: DenseDoubleVector): ExpectationSum = {
    val k = model._2.length
    val p = weights.zip(dists).map { case (weight, dist) => eps + weight * dist.pdf(x) }
    val pSum = p.sum
    model._1(0) += math.log(pSum)
    val xxt = x * new Transpose(x)
    for (i <- 0 until k) {
      p(i) /= pSum
      model._2(i) += p(i)
      model._3(i) += x * p(i)
      model._4(i) += xxt * p(i)
    }
    model
  }
  
  // number of samples per cluster to use when initializing Gaussians
  private val nSamples = 5
  
  // an initializing GMM can be provided rather than using the 
  // default random starting point
  private var initialGmm: Option[GaussianMixtureModel] = None
  
  /** A default instance, 2 Gaussians, 100 iterations, 0.01 log-likelihood threshold */
  def this() = this(2, 0.01, 100)
  
  /** Set the initial GMM starting point, bypassing the random initialization.
   *  You must call setK() prior to calling this method, and the condition
   *  (gmm.k == this.k) must be met; failure will result in an IllegalArgumentException
   */
  def setInitialGmm(gmm: GaussianMixtureModel): this.type = {
    if (gmm.k == k) {
      initialGmm = Some(gmm)
    } else {
      throw new IllegalArgumentException("initialing GMM has mismatched cluster count (gmm.k != k)")
    }
    this
  }
  
  /** Return the user supplied initial GMM, if supplied */
  def getInitialiGmm: Option[GaussianMixtureModel] = initialGmm
  
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
  
  /** Return the largest change in log-likelihood at which convergence is
   *  considered to have occurred.
   */
  def getConvergenceTol: Double = convergenceTol
  
  /** Machine precision value used to ensure matrix conditioning */
  private val eps = math.pow(2.0, -52)
  
  /** Perform expectation maximization */
  def run(data: RDD[Vector]): GaussianMixtureModel = {
    val sc = data.sparkContext
    
    // we will operate on the data as breeze data
    val breezeData = data.map(u => u.toBreeze.toDenseVector).cache()
    
    // Get length of the input vectors
    val d = breezeData.first.length 
    
    // Determine initial weights and corresponding Gaussians.
    // If the user supplied an initial GMM, we use those values, otherwise
    // we start with uniform weights, a random mean from the data, and
    // diagonal covariance matrices using component variances
    // derived from the samples    
    val (weights, gaussians) = initialGmm match {
      case Some(gmm) => (gmm.weight, gmm.mu.zip(gmm.sigma).map{ case(mu, sigma) => 
        new MultivariateGaussian(mu.toBreeze.toDenseVector, sigma.toBreeze.toDenseMatrix) 
      }.toArray)
      
      case None => {
        val samples = breezeData.takeSample(true, k * nSamples, scala.util.Random.nextInt)
        ((0 until k).map(_ => 1.0 / k).toArray, (0 until k).map{ i => 
          val slice = samples.view(i * nSamples, (i + 1) * nSamples)
          new MultivariateGaussian(vectorMean(slice), initCovariance(slice)) 
        }.toArray)  
      }
    }
    
    var llh = Double.MinValue // current log-likelihood 
    var llhp = 0.0            // previous log-likelihood
    
    var iter = 0
    do {
      // create and broadcast curried cluster contribution function
      val compute = sc.broadcast(computeExpectation(weights, gaussians)_)
      
      // aggregate the cluster contribution for all sample points
      val (logLikelihood, wSums, muSums, sigmaSums) = 
        breezeData.aggregate(zeroExpectationSum(k, d))(compute.value, addExpectationSums)
      
      // Create new distributions based on the partial assignments
      // (often referred to as the "M" step in literature)
      val sumWeights = wSums.sum
      for (i <- 0 until k) {
        val mu = muSums(i) / wSums(i)
        val sigma = sigmaSums(i) / wSums(i) - mu * new Transpose(mu)
        weights(i) = wSums(i) / sumWeights
        gaussians(i) = new MultivariateGaussian(mu, sigma)
      }
   
      llhp = llh // current becomes previous
      llh = logLikelihood(0) // this is the freshly computed log-likelihood
      iter += 1
    } while(iter < maxIterations && Math.abs(llh-llhp) > convergenceTol)
    
    // Need to convert the breeze matrices to MLlib matrices
    val means   = (0 until k).map(i => Vectors.fromBreeze(gaussians(i).mu)).toArray
    val sigmas  = (0 until k).map(i => Matrices.fromBreeze(gaussians(i).sigma)).toArray
    new GaussianMixtureModel(weights, means, sigmas)
  }
    
  /** Average of dense breeze vectors */
  private def vectorMean(x: VectorArrayView): DenseDoubleVector = {
    val v = BreezeVector.zeros[Double](x(0).length)
    x.foreach(xi => v += xi)
    v / x.length.asInstanceOf[Double] 
  }
  
  /**
   * Construct matrix where diagonal entries are element-wise
   * variance of input vectors (computes biased variance)
   */
  private def initCovariance(x: VectorArrayView): DenseDoubleMatrix = {
    val mu = vectorMean(x)
    val ss = BreezeVector.zeros[Double](x(0).length)
    val cov = BreezeMatrix.eye[Double](ss.length)
    x.map(xi => (xi - mu) :^ 2.0).foreach(u => ss += u)
    (0 until ss.length).foreach(i => cov(i,i) = ss(i) / x.length)
    cov
  }
  
  /**
   * Given the input vectors, return the membership value of each vector
   * to all mixture components. 
   */
  def predictClusters(
      points: RDD[Vector], 
      mu: Array[Vector], 
      sigma: Array[Matrix],
      weight: Array[Double], k: Int): RDD[Array[Double]] = {
    val sc = points.sparkContext
    val dists = sc.broadcast{
      (0 until k).map{ i => 
        new MultivariateGaussian(mu(i).toBreeze.toDenseVector, sigma(i).toBreeze.toDenseMatrix)
      }.toArray
    }
    val weights = sc.broadcast((0 until k).map(i => weight(i)).toArray)
    points.map{ x => 
      computeSoftAssignments(x.toBreeze.toDenseVector, dists.value, weights.value, k)
    }
  }
  
  /**
   * Compute the partial assignments for each vector
   */
  private def computeSoftAssignments(
      pt: DenseDoubleVector,
      dists: Array[MultivariateGaussian],
      weights: Array[Double],
      k: Int): Array[Double] = {
    val p = weights.zip(dists).map { case (weight, dist) => eps + weight * dist.pdf(pt) }
    val pSum = p.sum 
    for (i <- 0 until k){
      p(i) /= pSum
    }
    p
  }
}

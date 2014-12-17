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
  
  // number of samples per cluster to use when initializing Gaussians
  private val nSamples = 5
  
  // an initializing GMM can be provided rather than using the 
  // default random starting point
  private var initialGmm: Option[GaussianMixtureModel] = None
  
  /** A default instance, 2 Gaussians, 100 iterations, 0.01 log-likelihood threshold */
  def this() = this(2, 0.01, 100)
  
  /** Set the initial GMM starting point, bypassing the random initialization */
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
    val ctx = data.sparkContext
    
    // we will operate on the data as breeze data
    val breezeData = data.map( u => u.toBreeze.toDenseVector ).cache()
    
    // Get length of the input vectors
    val d = breezeData.first.length 
    
    // gaussians will be array of (weight, mean, covariance) tuples.
    // If the user supplied an initial GMM, we use those values, otherwise
    // we start with uniform weights, a random mean from the data, and
    // diagonal covariance matrices using component variances
    // derived from the samples 
    var gaussians = initialGmm match {
      case Some(gmm) => (0 until k).map{ i =>
        (gmm.weight(i), gmm.mu(i).toBreeze.toDenseVector, gmm.sigma(i).toBreeze.toDenseMatrix)
      }.toArray
      
      case None => {
        // For each Gaussian, we will initialize the mean as the average
        // of some random samples from the data
        val samples = breezeData.takeSample(true, k * nSamples, scala.util.Random.nextInt)
          
        (0 until k).map{ i => 
          (1.0 / k, 
            vectorMean(samples.slice(i * nSamples, (i + 1) * nSamples)), 
            initCovariance(samples.slice(i * nSamples, (i + 1) * nSamples)))
        }.toArray
      }
    }
    
    /*var gaussians = (0 until k).map{ i => 
      (1.0 / k,
        vectorMean(samples.slice(i * nSamples, (i + 1) * nSamples)),
        initCovariance(samples.slice(i * nSamples, (i + 1) * nSamples)))
    }.toArray
    */
    val accW     = new Array[Accumulator[Double]](k)
    val accMu    = new Array[Accumulator[DenseDoubleVector]](k)
    val accSigma = new Array[Accumulator[DenseDoubleMatrix]](k)
    
    var llh = Double.MinValue // current log-likelihood 
    var llhp = 0.0            // previous log-likelihood
    
    var iter = 0
    do {
      // reset accumulators
      for (i <- 0 until k) {
        accW(i)     = ctx.accumulator(0.0)
        accMu(i)    = ctx.accumulator(
                        BreezeVector.zeros[Double](d))(DenseDoubleVectorAccumulatorParam)
        accSigma(i) = ctx.accumulator(
                        BreezeMatrix.zeros[Double](d,d))(DenseDoubleMatrixAccumulatorParam)
      }
      
      val logLikelihood = ctx.accumulator(0.0)
            
      // broadcast the current weights and distributions to all nodes
      val dists = ctx.broadcast{
        (0 until k).map(i => new MultivariateGaussian(gaussians(i)._2, gaussians(i)._3)).toArray
      }
      val weights = ctx.broadcast((0 until k).map(i => gaussians(i)._1).toArray)
      
      // calculate partial assignments for each sample in the data
      // (often referred to as the "E" step in literature)
      breezeData.foreach{ x =>   
        val p = (0 until k).map(i => eps + weights.value(i) * dists.value(i).pdf(x)).toArray
        
        val pSum = p.sum 
        
        logLikelihood += math.log(pSum)  
          
        // accumulate weighted sums  
        val xxt = x * new Transpose(x)
        for (i <- 0 until k) {
          p(i) /= pSum
          accW(i) += p(i)
          accMu(i) += x * p(i)
          accSigma(i) += xxt * p(i)
        }
      }
      
      // Collect the computed sums
      val W = (0 until k).map(i => accW(i).value).toArray
      val MU = (0 until k).map(i => accMu(i).value).toArray
      val SIGMA = (0 until k).map(i => accSigma(i).value).toArray
      
      // Create new distributions based on the partial assignments
      // (often referred to as the "M" step in literature)
      gaussians = (0 until k).map{ i => 
        val weight = W(i) / W.sum
        val mu = MU(i) / W(i)
        val sigma = SIGMA(i) / W(i) - mu * new Transpose(mu)
        (weight, mu, sigma)
      }.toArray
      
      llhp = llh // current becomes previous
      llh = logLikelihood.value // this is the freshly computed log-likelihood
      iter += 1
    } while(iter < maxIterations && Math.abs(llh-llhp) > convergenceTol)
    
    // Need to convert the breeze matrices to MLlib matrices
    val weights = (0 until k).map(i => gaussians(i)._1).toArray
    val means   = (0 until k).map(i => Vectors.fromBreeze(gaussians(i)._2)).toArray
    val sigmas  = (0 until k).map(i => Matrices.fromBreeze(gaussians(i)._3)).toArray
    new GaussianMixtureModel(weights, means, sigmas)
  }
    
  /** Average of dense breeze vectors */
  private def vectorMean(x: Array[DenseDoubleVector]): DenseDoubleVector = {
    val v = BreezeVector.zeros[Double](x(0).length)
    x.foreach(xi => v += xi)
    v / x.length.asInstanceOf[Double] 
  }
  
  /**
   * Construct matrix where diagonal entries are element-wise
   * variance of input vectors (computes biased variance)
   */
  private def initCovariance(x: Array[DenseDoubleVector]): DenseDoubleMatrix = {
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
  def predictClusters(points: RDD[Vector], mu: Array[Vector], sigma: Array[Matrix],
      weight: Array[Double], k: Int): RDD[Array[Double]] = {
    val ctx = points.sparkContext
    val dists = ctx.broadcast{
      (0 until k).map{ i => 
        new MultivariateGaussian(mu(i).toBreeze.toDenseVector, sigma(i).toBreeze.toDenseMatrix)
      }.toArray
    }
    val weights = ctx.broadcast((0 until k).map(i => weight(i)).toArray)
    points.map{ x => 
      computeSoftAssignments(x.toBreeze.toDenseVector, dists.value, weights.value, k)
    }
  }
  
  /**
   * Compute the partial assignments for each vector
   */
  def computeSoftAssignments(pt: DenseDoubleVector, dists: Array[MultivariateGaussian],
      weights: Array[Double], k: Int): Array[Double] = {
    val p = (0 until k).map(i => eps + weights(i) * dists(i).pdf(pt)).toArray
    val pSum = p.sum 
    for(i<- 0 until k){
      p(i) /= pSum
    }
    p
  }

  /** AccumulatorParam for Dense Breeze Vectors */
  private object DenseDoubleVectorAccumulatorParam extends AccumulatorParam[DenseDoubleVector] {
    def zero(initialVector: DenseDoubleVector): DenseDoubleVector = {
      BreezeVector.zeros[Double](initialVector.length)
    }
    
    def addInPlace(a: DenseDoubleVector, b: DenseDoubleVector): DenseDoubleVector = {
      a += b
    }
  }
  
  /** AccumulatorParam for Dense Breeze Matrices */
  private object DenseDoubleMatrixAccumulatorParam extends AccumulatorParam[DenseDoubleMatrix] {
    def zero(initialMatrix: DenseDoubleMatrix): DenseDoubleMatrix = {
      BreezeMatrix.zeros[Double](initialMatrix.rows, initialMatrix.cols)
    }
    
    def addInPlace(a: DenseDoubleMatrix, b: DenseDoubleMatrix): DenseDoubleMatrix = {
      a += b
    }
  }  
}

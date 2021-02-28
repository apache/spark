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

import breeze.linalg.{diag, DenseMatrix => BreezeMatrix, DenseVector => BDV, Vector => BV}

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{BLAS, DenseMatrix, Matrices, Vector, Vectors}
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
 * @param k Number of independent Gaussians in the mixture model.
 * @param convergenceTol Maximum change in log-likelihood at which convergence
 *                       is considered to have occurred.
 * @param maxIterations Maximum number of iterations allowed.
 *
 * @note This algorithm is limited in its number of features since it requires storing a covariance
 * matrix which has size quadratic in the number of features. Even when the number of features does
 * not exceed this limit, this algorithm may perform poorly on high-dimensional data.
 * This is due to high-dimensional data (a) making it difficult to cluster at all (based
 * on statistical/theoretical arguments) and (b) numerical issues with Gaussian distributions.
 */
@Since("1.3.0")
class GaussianMixture private (
    private var k: Int,
    private var convergenceTol: Double,
    private var maxIterations: Int,
    private var seed: Long) extends Serializable {

  /**
   * Constructs a default instance. The default parameters are {k: 2, convergenceTol: 0.01,
   * maxIterations: 100, seed: random}.
   */
  @Since("1.3.0")
  def this() = this(2, 0.01, 100, Utils.random.nextLong())

  // number of samples per cluster to use when initializing Gaussians
  private val nSamples = 5

  // an initializing GMM can be provided rather than using the
  // default random starting point
  private var initialModel: Option[GaussianMixtureModel] = None

  /**
   * Set the initial GMM starting point, bypassing the random initialization.
   * You must call setK() prior to calling this method, and the condition
   * (model.k == this.k) must be met; failure will result in an IllegalArgumentException
   */
  @Since("1.3.0")
  def setInitialModel(model: GaussianMixtureModel): this.type = {
    require(model.k == k,
      s"Mismatched cluster count (model.k ${model.k} != k ${k})")
    initialModel = Some(model)
    this
  }

  /**
   * Return the user supplied initial GMM, if supplied
   */
  @Since("1.3.0")
  def getInitialModel: Option[GaussianMixtureModel] = initialModel

  /**
   * Set the number of Gaussians in the mixture model.  Default: 2
   */
  @Since("1.3.0")
  def setK(k: Int): this.type = {
    require(k > 0,
      s"Number of Gaussians must be positive but got ${k}")
    this.k = k
    this
  }

  /**
   * Return the number of Gaussians in the mixture model
   */
  @Since("1.3.0")
  def getK: Int = k

  /**
   * Set the maximum number of iterations allowed. Default: 100
   */
  @Since("1.3.0")
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations >= 0,
      s"Maximum of iterations must be nonnegative but got ${maxIterations}")
    this.maxIterations = maxIterations
    this
  }

  /**
   * Return the maximum number of iterations allowed
   */
  @Since("1.3.0")
  def getMaxIterations: Int = maxIterations

  /**
   * Set the largest change in log-likelihood at which convergence is
   * considered to have occurred.
   */
  @Since("1.3.0")
  def setConvergenceTol(convergenceTol: Double): this.type = {
    require(convergenceTol >= 0.0,
      s"Convergence tolerance must be nonnegative but got ${convergenceTol}")
    this.convergenceTol = convergenceTol
    this
  }

  /**
   * Return the largest change in log-likelihood at which convergence is
   * considered to have occurred.
   */
  @Since("1.3.0")
  def getConvergenceTol: Double = convergenceTol

  /**
   * Set the random seed
   */
  @Since("1.3.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Return the random seed
   */
  @Since("1.3.0")
  def getSeed: Long = seed

  /**
   * Perform expectation maximization
   */
  @Since("1.3.0")
  def run(data: RDD[Vector]): GaussianMixtureModel = {
    val sc = data.sparkContext

    // we will operate on the data as breeze data
    val breezeData = data.map(_.asBreeze).cache()

    // Get length of the input vectors
    val d = breezeData.first().length
    require(d < GaussianMixture.MAX_NUM_FEATURES, s"GaussianMixture cannot handle more " +
      s"than ${GaussianMixture.MAX_NUM_FEATURES} features because the size of the covariance" +
      s" matrix is quadratic in the number of features.")

    val shouldDistributeGaussians = GaussianMixture.shouldDistributeGaussians(k, d)

    // Determine initial weights and corresponding Gaussians.
    // If the user supplied an initial GMM, we use those values, otherwise
    // we start with uniform weights, a random mean from the data, and
    // diagonal covariance matrices using component variances
    // derived from the samples
    val (weights, gaussians) = initialModel match {
      case Some(gmm) => (gmm.weights, gmm.gaussians)

      case None =>
        val samples = breezeData.takeSample(withReplacement = true, k * nSamples, seed)
        (Array.fill(k)(1.0 / k), Array.tabulate(k) { i =>
          val slice = samples.view.slice(i * nSamples, (i + 1) * nSamples)
          new MultivariateGaussian(vectorMean(slice.toSeq), initCovariance(slice.toSeq))
        })
    }

    var llh = Double.MinValue // current log-likelihood
    var llhp = 0.0            // previous log-likelihood

    var iter = 0
    while (iter < maxIterations && math.abs(llh-llhp) > convergenceTol) {
      // create and broadcast curried cluster contribution function
      val compute = sc.broadcast(ExpectationSum.add(weights, gaussians)_)

      // aggregate the cluster contribution for all sample points
      val sums = breezeData.treeAggregate(ExpectationSum.zero(k, d))(compute.value, _ += _)

      // Create new distributions based on the partial assignments
      // (often referred to as the "M" step in literature)
      val sumWeights = sums.weights.sum

      if (shouldDistributeGaussians) {
        val numPartitions = math.min(k, 1024)
        val tuples =
          Seq.tabulate(k)(i => (sums.means(i), sums.sigmas(i), sums.weights(i)))
        val (ws, gs) = sc.parallelize(tuples, numPartitions).map { case (mean, sigma, weight) =>
          updateWeightsAndGaussians(mean, sigma, weight, sumWeights)
        }.collect().unzip
        Array.copy(ws, 0, weights, 0, ws.length)
        Array.copy(gs, 0, gaussians, 0, gs.length)
      } else {
        var i = 0
        while (i < k) {
          val (weight, gaussian) =
            updateWeightsAndGaussians(sums.means(i), sums.sigmas(i), sums.weights(i), sumWeights)
          weights(i) = weight
          gaussians(i) = gaussian
          i = i + 1
        }
      }

      llhp = llh // current becomes previous
      llh = sums.logLikelihood // this is the freshly computed log-likelihood
      iter += 1
      compute.destroy()
    }
    breezeData.unpersist()

    new GaussianMixtureModel(weights, gaussians)
  }

  /**
   * Java-friendly version of `run()`
   */
  @Since("1.3.0")
  def run(data: JavaRDD[Vector]): GaussianMixtureModel = run(data.rdd)

  private def updateWeightsAndGaussians(
      mean: BDV[Double],
      sigma: BreezeMatrix[Double],
      weight: Double,
      sumWeights: Double): (Double, MultivariateGaussian) = {
    val mu = (mean /= weight)
    BLAS.syr(-weight, Vectors.fromBreeze(mu),
      Matrices.fromBreeze(sigma).asInstanceOf[DenseMatrix])
    val newWeight = weight / sumWeights
    val newGaussian = new MultivariateGaussian(mu, sigma / weight)
    (newWeight, newGaussian)
  }

  /** Average of dense breeze vectors */
  private def vectorMean(x: Seq[BV[Double]]): BDV[Double] = {
    val v = BDV.zeros[Double](x(0).length)
    x.foreach(xi => v += xi)
    v / x.length.toDouble
  }

  /**
   * Construct matrix where diagonal entries are element-wise
   * variance of input vectors (computes biased variance)
   */
  private def initCovariance(x: Seq[BV[Double]]): BreezeMatrix[Double] = {
    val mu = vectorMean(x)
    val ss = BDV.zeros[Double](x(0).length)
    x.foreach { xi =>
      val d: BV[Double] = xi - mu
      ss += d ^:^ 2.0
    }
    diag(ss / x.length.toDouble)
  }
}

private[clustering] object GaussianMixture {

  /** Limit number of features such that numFeatures^2^ < Int.MaxValue */
  private[clustering] val MAX_NUM_FEATURES = math.sqrt(Int.MaxValue).toInt

  /**
   * Heuristic to distribute the computation of the `MultivariateGaussian`s, approximately when
   * d is greater than 25 except for when k is very small.
   * @param k  Number of topics
   * @param d  Number of features
   */
  def shouldDistributeGaussians(k: Int, d: Int): Boolean = ((k - 1.0) / k) * d > 25
}

// companion class to provide zero constructor for ExpectationSum
private object ExpectationSum {
  def zero(k: Int, d: Int): ExpectationSum = {
    new ExpectationSum(0.0, Array.fill(k)(0.0),
      Array.fill(k)(BDV.zeros(d)), Array.fill(k)(BreezeMatrix.zeros(d, d)))
  }

  // compute cluster contributions for each input point
  // (U, T) => U for aggregation
  def add(
      weights: Array[Double],
      dists: Array[MultivariateGaussian])
      (sums: ExpectationSum, x: BV[Double]): ExpectationSum = {
    val p = weights.zip(dists).map {
      case (weight, dist) => MLUtils.EPSILON + weight * dist.pdf(x)
    }
    val pSum = p.sum
    sums.logLikelihood += math.log(pSum)
    var i = 0
    while (i < sums.k) {
      p(i) /= pSum
      sums.weights(i) += p(i)
      sums.means(i) += x * p(i)
      BLAS.syr(p(i), Vectors.fromBreeze(x),
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
    val means: Array[BDV[Double]],
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

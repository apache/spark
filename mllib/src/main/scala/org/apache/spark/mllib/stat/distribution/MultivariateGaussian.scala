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

package org.apache.spark.mllib.stat.distribution

import breeze.linalg.{diag, eigSym, max, DenseMatrix => DBM, DenseVector => DBV, Vector => BV}

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils

/**
 * :: DeveloperApi ::
 * This class provides basic functionality for a Multivariate Gaussian (Normal) Distribution. In
 * the event that the covariance matrix is singular, the density will be computed in a
 * reduced dimensional subspace under which the distribution is supported.
 * (see [[http://en.wikipedia.org/wiki/Multivariate_normal_distribution#Degenerate_case]])
 *
 * @param mu The mean vector of the distribution
 * @param sigma The covariance matrix of the distribution
 */
@Since("1.3.0")
@DeveloperApi
class MultivariateGaussian @Since("1.3.0") (
    @Since("1.3.0") val mu: Vector,
    @Since("1.3.0") val sigma: Matrix) extends Serializable {

  require(sigma.numCols == sigma.numRows, "Covariance matrix must be square")
  require(mu.size == sigma.numCols, "Mean vector length must match covariance matrix size")

  private val breezeMu = mu.asBreeze.toDenseVector

  /**
   * private[mllib] constructor
   *
   * @param mu The mean vector of the distribution
   * @param sigma The covariance matrix of the distribution
   */
  private[mllib] def this(mu: DBV[Double], sigma: DBM[Double]) = {
    this(Vectors.fromBreeze(mu), Matrices.fromBreeze(sigma))
  }

  /**
   * Compute distribution dependent constants:
   *    rootSigmaInv = D^(-1/2)^ * U.t, where sigma = U * D * U.t
   *    u = log((2*pi)^(-k/2)^ * det(sigma)^(-1/2)^)
   */
  private val (rootSigmaInv: DBM[Double], u: Double) = calculateCovarianceConstants

  /**
   * Returns density of this multivariate Gaussian at given point, x
   */
  @Since("1.3.0")
  def pdf(x: Vector): Double = {
    pdf(x.asBreeze)
  }

  /**
   * Returns the log-density of this multivariate Gaussian at given point, x
   */
  @Since("1.3.0")
  def logpdf(x: Vector): Double = {
    logpdf(x.asBreeze)
  }

  /** Returns density of this multivariate Gaussian at given point, x */
  private[mllib] def pdf(x: BV[Double]): Double = {
    math.exp(logpdf(x))
  }

  /** Returns the log-density of this multivariate Gaussian at given point, x */
  private[mllib] def logpdf(x: BV[Double]): Double = {
    val delta = x - breezeMu
    val v = rootSigmaInv * delta
    u + v.t * v * -0.5
  }

  /**
   * Calculate distribution dependent components used for the density function:
   *    pdf(x) = (2*pi)^(-k/2)^ * det(sigma)^(-1/2)^ * exp((-1/2) * (x-mu).t * inv(sigma) * (x-mu))
   * where k is length of the mean vector.
   *
   * We here compute distribution-fixed parts
   *  log((2*pi)^(-k/2)^ * det(sigma)^(-1/2)^)
   * and
   *  D^(-1/2)^ * U, where sigma = U * D * U.t
   *
   * Both the determinant and the inverse can be computed from the singular value decomposition
   * of sigma.  Noting that covariance matrices are always symmetric and positive semi-definite,
   * we can use the eigendecomposition. We also do not compute the inverse directly; noting
   * that
   *
   *    sigma = U * D * U.t
   *    inv(Sigma) = U * inv(D) * U.t
   *               = (D^{-1/2}^ * U.t).t * (D^{-1/2}^ * U.t)
   *
   * and thus
   *
   *    -0.5 * (x-mu).t * inv(Sigma) * (x-mu) = -0.5 * norm(D^{-1/2}^ * U.t  * (x-mu))^2^
   *
   * To guard against singular covariance matrices, this method computes both the
   * pseudo-determinant and the pseudo-inverse (Moore-Penrose).  Singular values are considered
   * to be non-zero only if they exceed a tolerance based on machine precision, matrix size, and
   * relation to the maximum singular value (same tolerance used by, e.g., Octave).
   */
  private def calculateCovarianceConstants: (DBM[Double], Double) = {
    val eigSym.EigSym(d, u) = eigSym(sigma.asBreeze.toDenseMatrix) // sigma = u * diag(d) * u.t

    // For numerical stability, values are considered to be non-zero only if they exceed tol.
    // This prevents any inverted value from exceeding (eps * n * max(d))^-1
    val tol = MLUtils.EPSILON * max(d) * d.length

    try {
      // log(pseudo-determinant) is sum of the logs of all non-zero singular values
      val logPseudoDetSigma = d.activeValuesIterator.filter(_ > tol).map(math.log).sum

      // calculate the root-pseudo-inverse of the diagonal matrix of singular values
      // by inverting the square root of all non-zero values
      val pinvS = diag(new DBV(d.map(v => if (v > tol) math.sqrt(1.0 / v) else 0.0).toArray))

      (pinvS * u.t, -0.5 * (mu.size * math.log(2.0 * math.Pi) + logPseudoDetSigma))
    } catch {
      case uex: UnsupportedOperationException =>
        throw new IllegalArgumentException("Covariance matrix has no non-zero singular values")
    }
  }
}

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

package edu.uci.eecs.spectralLDA.algorithm

/**
* Tensor Decomposition Algorithms.
* Alternating Least Square algorithm is implemented.
* Created by Furong Huang on 11/2/15.
*/

import breeze.linalg.{*, diag, norm, qr, DenseMatrix, DenseVector}
import breeze.stats.distributions.{Gaussian, Rand, RandBasis}
import edu.uci.eecs.spectralLDA.utils.{AlgebraUtil, TensorOps}


/** Tensor decomposition by Alternating Least Square (ALS)
 *
 * Suppose dimK-by-dimK-by-dimK symmetric tensor T can be decomposed as sum of rank-1 tensors
 *
 * $$ T = \sum_{i=1}^{dimK} \lambda_i a_i\otimes b_i\otimes c_i $$
 *
 * If we pool all \lambda_i in the vector \Lambda, all the column vectors \lambda_i a_i in A,
 * b_i in B, c_i in C, then
 *
 * $$ T^{1} = A \diag(\Lambda)(C \khatri-rao product B)^{\top} $$
 *
 * where T^{1} is a dimK-by-(dimK^2) matrix for the unfolded T.
 *
 * @param dimK               tensor T is of shape dimK-by-dimK-by-dimK
 * @param thirdOrderMoments  dimK-by-(dimK*dimK) matrix for the unfolded 3rd-order moments
 *                           $\sum_{i=1}^k\alpha_i\beta_i^{\otimes 3}$
 * @param maxIterations      max iterations for the ALS algorithm
 * @param tol                tolerance. the dot product threshold is 1-tol
 * @param restarts           number of restarts of the ALS loop
 */
class ALS(dimK: Int,
          thirdOrderMoments: DenseMatrix[Double],
          maxIterations: Int = 500,
          tol: Double = 1e-6,
          restarts: Int = 5)
  extends Serializable {
  assert(dimK > 0, "The number of topics dimK must be positive.")
  assert(thirdOrderMoments.rows == dimK && thirdOrderMoments.cols == dimK * dimK,
    "The thirdOrderMoments must be dimK-by-(dimK * dimK) unfolded matrix")

  assert(maxIterations > 0, "Max iterations must be positive.")
  assert(tol > 0.0, "tol must be positive and probably close to 0.")
  assert(restarts > 0, "Number of restarts for ALS must be positive.")

  /** Run Alternating Least Squares (ALS)
   *
   * Compute the best approximating rank-$k$ tensor $\sum_{i=1}^k\alpha_i\beta_i^{\otimes 3}$
   *
   * @param randBasis   default random seed
   * @return            three dimK-by-dimK matrices with all the $beta_i$ as columns,
   *                    length-dimK vector for all the eigenvalues
   */
  def run(implicit randBasis: RandBasis = Rand)
     : (DenseMatrix[Double], DenseMatrix[Double],
        DenseMatrix[Double], DenseVector[Double]) = {
    val gaussian = Gaussian(mu = 0.0, sigma = 1.0)

    var optimalA = DenseMatrix.zeros[Double](dimK, dimK)
    var optimalB = DenseMatrix.zeros[Double](dimK, dimK)
    var optimalC = DenseMatrix.zeros[Double](dimK, dimK)
    var optimalLambda = DenseVector.zeros[Double](dimK)

    var reconstructedLoss: Double = 0.0
    var optimalReconstructedLoss: Double = Double.PositiveInfinity

    for (s <- 0 until restarts) {
      val qr.QR(a0, _) = qr(DenseMatrix.rand[Double](dimK, dimK, gaussian))
      val qr.QR(b0, _) = qr(DenseMatrix.rand[Double](dimK, dimK, gaussian))
      val qr.QR(c0, _) = qr(DenseMatrix.rand[Double](dimK, dimK, gaussian))

      var A = a0
      var B = b0
      var C = c0

      var A_prev = DenseMatrix.zeros[Double](dimK, dimK)
      var lambda: breeze.linalg.DenseVector[Double] = DenseVector.zeros[Double](dimK)

      var iter: Int = 0
      while ((iter == 0) || (iter < maxIterations &&
        !AlgebraUtil.isConverged(A_prev, A, dotProductThreshold = 1 - tol))) {
        A_prev = A.copy

        val (updatedA, updatedLambda1) = updateALSIteration(thirdOrderMoments, B, C)
        A = updatedA
        lambda = updatedLambda1

        val (updatedB, updatedLambda2) = updateALSIteration(thirdOrderMoments, C, A)
        B = updatedB
        lambda = updatedLambda2

        val (updatedC, updatedLambda3) = updateALSIteration(thirdOrderMoments, A, B)
        C = updatedC
        lambda = updatedLambda3

        iter += 1
      }

      reconstructedLoss = TensorOps.dmatrixNorm(thirdOrderMoments
        - A * diag(lambda) * TensorOps.krprod(C, B).t)

      if (reconstructedLoss < optimalReconstructedLoss) {
        optimalA = A
        optimalB = B
        optimalC = C
        optimalLambda = lambda
        optimalReconstructedLoss = reconstructedLoss
      }
    }

    (optimalA, optimalB, optimalC, optimalLambda)
  }

  private def updateALSIteration(unfoldedM3: DenseMatrix[Double],
                                 B: DenseMatrix[Double],
                                 C: DenseMatrix[Double]
                                ): (DenseMatrix[Double], DenseVector[Double]) = {
    val updatedA = unfoldedM3 * TensorOps.krprod(C, B) * TensorOps.to_invert(C, B)
    val lambda = norm(updatedA(::, *)).t.toDenseVector
    (AlgebraUtil.matrixNormalization(updatedA), lambda)
  }
}

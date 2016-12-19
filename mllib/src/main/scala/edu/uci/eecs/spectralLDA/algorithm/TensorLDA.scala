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

import breeze.linalg.{argsort, diag, max, min, DenseMatrix, DenseVector, SparseVector}
import breeze.numerics._
import breeze.stats.distributions.{Rand, RandBasis}
import edu.uci.eecs.spectralLDA.datamoments.DataCumulant
import edu.uci.eecs.spectralLDA.utils.NonNegativeAdjustment

import org.apache.spark.rdd.RDD


/** Spectral LDA model
 *
 * @param dimK                 number of topics k
 * @param alpha0               sum of alpha for the Dirichlet prior for topic distribution
 * @param maxIterations        max number of iterations for the ALS algorithm for CP decomposition
 * @param tol                  tolerance. the dot product threshold in ALS is 1-tol
 * @param idfLowerBound        lower bound of Inverse Document Frequency (IDF) for the words
 *                             to be taken into account
 * @param m2ConditionNumberUB  upper bound of Condition Number for the shifted M2 matrix,
 *                             if the empirical Condition Number exceeds the uppper bound
 *                             the code quits with error before computing the M3. It allows
 *                             to quickly check if there're any predominant topics
 * @param randomisedSVD        uses randomised SVD on M2, true by default
 */
class TensorLDA(dimK: Int,
                alpha0: Double,
                maxIterations: Int = 500,
                tol: Double = 1e-6,
                idfLowerBound: Double = 1.0,
                m2ConditionNumberUB: Double = Double.PositiveInfinity,
                randomisedSVD: Boolean = true,
                numIterationsKrylovMethod: Int = 1) extends Serializable {
  assert(dimK > 0, "The number of topics dimK must be positive.")
  assert(alpha0 > 0, "The topic concentration alpha0 must be positive.")
  assert(numIterationsKrylovMethod >= 0,
    "No of iterations for the Krylov method must be non-negative.")
  assert(maxIterations > 0, "The number of iterations for ALS must be positive.")
  assert(tol > 0.0, "tol must be positive and probably close to 0.")

  def fit(documents: RDD[(Long, SparseVector[Double])])
         (implicit randBasis: RandBasis = Rand)
          : (DenseMatrix[Double], DenseVector[Double],
             DenseMatrix[Double], DenseVector[Double],
             DenseVector[Double]) = {
    val cumulant: DataCumulant = DataCumulant.getDataCumulant(
      dimK,
      alpha0,
      documents,
      idfLowerBound = idfLowerBound,
      m2ConditionNumberUB = m2ConditionNumberUB,
      randomisedSVD = randomisedSVD,
      numIterationsKrylovMethod = numIterationsKrylovMethod
    )

    val myALS: ALS = new ALS(
      dimK,
      cumulant.thirdOrderMoments,
      maxIterations = maxIterations,
      tol = tol
    )

    val (nu: DenseMatrix[Double], _, _, lambda: DenseVector[Double]) = myALS.run

    // unwhiten the results
    // unwhitening matrix: $(W^T)^{-1}=U\Sigma^{1/2}$
    val unwhiteningMatrix = cumulant.eigenVectorsM2 * diag(sqrt(cumulant.eigenValuesM2))

    val alphaUnordered: DenseVector[Double] = pow(lambda, -2)
    val topicWordMatrixUnordered: DenseMatrix[Double] = unwhiteningMatrix * nu * diag(lambda)

    // re-arrange alpha and topicWordMatrix in descending order of alpha
    val idx = argsort(alphaUnordered).reverse
    val alpha = alphaUnordered(idx).toDenseVector
    val topicWordMatrix = topicWordMatrixUnordered(::, idx).toDenseMatrix

    // Diagnostic information: the ratio of the maximum to the minimum of the
    // top k eigenvalues of shifted M2
    //
    // If it's too large (>10), the algorithm may not be able to output reasonable results.
    // It could be due to some very frequent (low IDF) words or that we specified dimK
    // larger than the rank of the shifted M2.
    val m2ConditionNumber = max(cumulant.eigenValuesM2) / min(cumulant.eigenValuesM2)

    (NonNegativeAdjustment.simplexProj_Matrix(topicWordMatrix), alpha,
      cumulant.eigenVectorsM2, cumulant.eigenValuesM2, cumulant.firstOrderMoments)
  }

}

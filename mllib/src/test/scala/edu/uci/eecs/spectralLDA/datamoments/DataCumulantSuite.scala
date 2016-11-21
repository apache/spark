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

package edu.uci.eecs.spectralLDA.datamoments

import breeze.linalg._
import breeze.numerics.sqrt
import breeze.stats.distributions.{Dirichlet, Multinomial}

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext


class DataCumulantSuite extends SparkFunSuite with MLlibTestSparkContext {
  def simulateLDAData(alpha: DenseVector[Double],
                      allTokenDistributions: DenseMatrix[Double],
                      numDocuments: Int,
                      numTokensPerDocument: Int)
  : Seq[(Long, SparseVector[Double])] = {
    assert(alpha.size == allTokenDistributions.cols)
    val k = alpha.size
    val V = allTokenDistributions.rows

    // Simulate the word histogram of each document
    val dirichlet = Dirichlet(alpha)
    val wordCounts: Seq[(Long, SparseVector[Double])] = for {
      d <- 0 until numDocuments

      topicDistribution: DenseVector[Double] = dirichlet.sample()
      tokenDistribution: DenseVector[Double] = allTokenDistributions * topicDistribution
      tokens = Multinomial(tokenDistribution) sample numTokensPerDocument

      c = SparseVector.zeros[Double](V)
      tokensCount = tokens foreach { t => c(t) += 1.0 }
    } yield (d.toLong, c)

    wordCounts
  }

  test("Whitened M3") {
    val alpha: DenseVector[Double] = DenseVector[Double](5.0, 10.0, 20.0)
    val allTokenDistributions: DenseMatrix[Double] = new DenseMatrix[Double](5, 3,
      Array[Double](0.6, 0.1, 0.1, 0.1, 0.1,
        0.1, 0.1, 0.6, 0.1, 0.1,
        0.1, 0.1, 0.1, 0.1, 0.6))

    val documents = simulateLDAData(
      alpha,
      allTokenDistributions,
      numDocuments = 100,
      numTokensPerDocument = 100
    )
    val documentsRDD = sc.parallelize(documents)

    val cumulant = DataCumulant.getDataCumulant(
      dimK = 3,
      alpha0 = sum(alpha),
      documentsRDD,
      randomisedSVD = false
    )

    val expectedM3: DenseMatrix[Double] = expected_whitened_M3(
      dimK = 3,
      alpha0 = sum(alpha),
      documents
    )

    val diffM3 = cumulant.thirdOrderMoments - expectedM3
    assert(norm(norm(diffM3(::, *)).t.toDenseVector) <= 1e-6)
  }

  private def expected_whitened_M3(dimK: Int,
                                   alpha0: Double,
                                   documents: Seq[(Long, SparseVector[Double])])
                                  (implicit tolerance: Double = 1e-9)
  : DenseMatrix[Double] = {
    val v = documents map { _._2.toDenseVector }
    val numDocs = v.length
    val V = v.head.length

    // Compute unshifted M1, M2, M3
    val M1: DenseVector[Double] = v
      .map { x => x / sum(x) }
      .reduce(_ + _)
      .map { _ / numDocs.toDouble }

    val E_x1_x2: DenseMatrix[Double] = v
      .map { x => (x * x.t - diag(x)) / (sum(x) * (sum(x) - 1)) }
      .reduce(_ + _)
      .map { _ / numDocs.toDouble }

    val seq_x1_x2_x3: Seq[DenseMatrix[Double]] = for {
      c <- v
      l = sum(c)

      rawM3: DenseMatrix[Double] = DenseMatrix.zeros[Double](V, V * V)
      fillM3 = for (i <- 0 until V; j <- 0 until V; k <- 0 until V) {
        val j2 = k * V + j
        rawM3(i, j2) = c(i) * c(j) * c(k)
        if (i == j) {
          rawM3(i, j2) -= c(i) * c(k)
        }
        if (i == k) {
          rawM3(i, j2) -= c(i) * c(j)
        }
        if (j == k) {
          rawM3(i, j2) -= c(i) * c(j)
        }
        if (i == j && j == k) {
          rawM3(i, j2) += 2 * c(i)
        }
      }
      m3 = rawM3 / (l * (l - 1) * (l - 2))
    } yield m3

    val E_x1_x2_x3 = seq_x1_x2_x3
      .reduce(_ + _)
      .map { _ / numDocs.toDouble }

    // Compute shifted M2, M3
    val (shiftedM2, shiftedM3) = shiftMoments(M1, E_x1_x2, E_x1_x2_x3, alpha0)
    val scaledShiftedM2 = shiftedM2 * alpha0 * (alpha0 + 1)
    val scaledShiftedM3 = shiftedM3 * (alpha0 * (alpha0 + 1) * (alpha0 + 2) / 2.0)

    // Eigendecomposition of shiftedM2
    val eigSym.EigSym(sigma, u) = eigSym(scaledShiftedM2)
    val i = argsort(sigma)
    val (truncated_u: DenseMatrix[Double], truncated_sigma: DenseVector[Double]) = (
      u(::, i.slice(V - dimK, V)).copy, sigma(i.slice(V - dimK, V)).copy)

    // Whitening matrix
    val W: DenseMatrix[Double] = truncated_u * diag(
      truncated_sigma map { x => 1 / (sqrt(x) + tolerance) }
    )

    // Whiten M3
    val unfolded_whitenedM3: DenseMatrix[Double] = W.t * scaledShiftedM3 * kron(W.t, W.t).t

    unfolded_whitenedM3
  }


  private def shiftMoments(M1: DenseVector[Double],
                           M2: DenseMatrix[Double],
                           M3: DenseMatrix[Double],
                           alpha0: Double
                          ): (DenseMatrix[Double], DenseMatrix[Double]) = {
    val d: Int = M1.size
    val kronM1: DenseMatrix[Double] = M1 * M1.t

    // [Anand14] Theorem 3.5
    val shiftedM2 = M2 - (alpha0 / (alpha0 + 1)) * kronM1

    // We always unfold the 3rd-order tensors in matrix of size (d,d^2)
    // G = E[x1 tensordot x2 tensordot M1] + E[x1 tensordot M1 tensordot x2]
    // H = G + E[M1 tensordot x1 tensordot x2]
    val G: Seq[DenseVector[Double]] = (0 until d) map { i =>
      val kron = M1 * M2(i, ::)
      (kron + kron.t).toDenseVector
    }
    val G2 = DenseMatrix.zeros[Double](d, d * M2.cols)
    for (i <- 0 until d) {
      G2(i, ::) := G(i).t
    }
    val H = G2 + M1 * M2.toDenseVector.t

    // K = M1 tensordot M1 tensordot M1
    val K = M1 * kronM1.toDenseVector.t

    // [Anand14] Theorem 3.5
    val shiftedM3 = M3 -
      (alpha0 / (alpha0 + 2)) * H +
      (2 * alpha0 * alpha0 / (alpha0 + 2) / (alpha0 + 1)) * K

    (shiftedM2, shiftedM3)
  }
}


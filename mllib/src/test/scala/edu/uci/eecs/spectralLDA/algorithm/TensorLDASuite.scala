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

import breeze.linalg._
import breeze.stats.distributions._
import org.apache.commons.math3.random.MersenneTwister

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext


class TensorLDASuite extends SparkFunSuite with MLlibTestSparkContext {
  test("Simulated LDA with deterministic SVD on M2") {
    val alpha: DenseVector[Double] = DenseVector[Double](20.0, 10.0, 5.0)
    val allTokenDistributions: DenseMatrix[Double] = new DenseMatrix[Double](6, 3,
      Array[Double](0.4, 0.4, 0.05, 0.05, 0.05, 0.05,
        0.05, 0.05, 0.4, 0.4, 0.05, 0.05,
        0.05, 0.05, 0.05, 0.05, 0.4, 0.4))

    implicit val randBasis: RandBasis =
      new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(57175437L)))

    val documents = simulateLDAData(
      alpha,
      allTokenDistributions,
      numDocuments = 5000,
      numTokensPerDocument = 100
    )
    val documentsRDD = sc.parallelize(documents)

    val tensorLDA = new TensorLDA(
      dimK = 3,
      alpha0 = sum(alpha),
      maxIterations = 200,
      randomisedSVD = false
    )

    val (sorted_beta, sorted_alpha, _, _, _) = tensorLDA.fit(documentsRDD)

    // if one vector is all negative, multiply it by -1 to turn it positive
    for (j <- 0 until sorted_beta.cols) {
      if (max(sorted_beta(::, j)) <= 0.0) {
        sorted_beta(::, j) :*= -1.0
      }
    }

    val diff_beta: DenseMatrix[Double] = sorted_beta - allTokenDistributions
    val diff_alpha: DenseVector[Double] = sorted_alpha - alpha

    val norm_diff_beta = norm(norm(diff_beta(::, *)).t.toDenseVector)
    val norm_diff_alpha = norm(diff_alpha)

    info(s"Expecting alpha: $alpha")
    info(s"Obtained alpha: $sorted_alpha")
    info(s"Norm of difference alpha: $norm_diff_alpha")

    info(s"Expecting beta:\n$allTokenDistributions")
    info(s"Obtained beta:\n$sorted_beta")
    info(s"Norm of difference beta: $norm_diff_beta")

    assert(norm_diff_beta <= 0.2)
    assert(norm_diff_alpha <= 4.0)
  }

  test("Simulated LDA with Randomised SVD on M2") {
    implicit val randBasis: RandBasis =
      new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(23476541L)))

    val alpha: DenseVector[Double] = DenseVector[Double](20.0, 10.0, 5.0)
    val allTokenDistributions: DenseMatrix[Double] = DenseMatrix.rand(100, 3, Uniform(0.0, 1.0))
    allTokenDistributions(0 until 10, 0) += 3.0
    allTokenDistributions(10 until 20, 1) += 3.0
    allTokenDistributions(20 until 30, 2) += 3.0


    val s = sum(allTokenDistributions(::, *))
    val normalisedAllTokenDistributions: DenseMatrix[Double] =
      allTokenDistributions * diag(1.0 / s.t.toDenseVector)

    val documents = simulateLDAData(
      alpha,
      allTokenDistributions,
      numDocuments = 5000,
      numTokensPerDocument = 500
    )
    val documentsRDD = sc.parallelize(documents)

    val dimK = 3

    val tensorLDA = new TensorLDA(
      dimK = dimK,
      alpha0 = sum(alpha(0 until dimK)),
      maxIterations = 200,
      randomisedSVD = true
    )

    val (sorted_beta, sorted_alpha, _, _, _) = tensorLDA.fit(documentsRDD)

    val expected_alpha = alpha(0 until dimK)
    val expected_beta = normalisedAllTokenDistributions(::, 0 until dimK)

    val diff_beta: DenseMatrix[Double] = sorted_beta - expected_beta
    val diff_alpha: DenseVector[Double] = sorted_alpha - expected_alpha

    val norm_diff_beta = norm(norm(diff_beta(::, *)).t.toDenseVector)
    val norm_diff_alpha = norm(diff_alpha)

    info(s"Expecting alpha: $expected_alpha")
    info(s"Obtained alpha: $sorted_alpha")
    info(s"Norm of difference alpha: $norm_diff_alpha")

    info(s"Expecting beta:\n$expected_beta")
    info(s"Obtained beta:\n$sorted_beta")
    info(s"Norm of difference beta: $norm_diff_beta")

    assert(norm_diff_beta <= 0.025)
    assert(norm_diff_alpha <= 3.5)
  }

  def simulateLDAData(alpha: DenseVector[Double],
                      allTokenDistributions: DenseMatrix[Double],
                      numDocuments: Int,
                      numTokensPerDocument: Int)
                     (implicit randBasis: RandBasis = Rand)
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
}


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

import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import org.apache.commons.math3.random.MersenneTwister

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * Terminology:
 *  - "word" = "term": an element of the vocabulary
 *  - "token": instance of a term appearing in a document
 *  - "topic": multinomial distribution over words representing some concept
 *
 * References:
 *  - Original LDA paper (journal version):
 *    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
 *
 * @see [[http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation Latent Dirichlet allocation
 *       (Wikipedia)]]
 */
@Since("2.0.2")
class TensorLDA private (private var k: Int,
                         private var alpha0: Double,
                         private var q: Int,
                         private var maxIterations: Int,
                         private var tol: Double,
                         private var seed: Long) extends Logging {
  /**
   * Constructs a LDA instance with default parameters.
   */
  @Since("2.0.2")
  def this() = this(k = 10, alpha0 = 1.0,
    q = 1, maxIterations = 500, tol = 1e-6,
    seed = Utils.random.nextLong())

  /**
   * Number of topics to infer, i.e., the number of soft cluster centers.
   */
  @Since("2.0.2")
  def getK(): Int = k

  /**
   * Set the number of topics to infer, i.e., the number of soft cluster centers.
   * (default = 10)
   */
  @Since("2.0.2")
  def setK(k: Int): this.type = {
    require(k > 0, s"LDA k (number of clusters) must be > 0, but was set to $k")
    this.k = k
    this
  }

  /**
   * Sum of the concentration parameter (commonly named "alpha") for the prior
   * placed on documents' distributions over topics ("theta").
   */
  @Since("2.0.2")
  def getAlpha0(): Double = alpha0

  /**
   * Sum of the concentration parameter (commonly named "alpha") for the prior
   * placed on documents' distributions over topics ("theta").
   */
  @Since("2.0.2")
  def setAlpha0(alpha0: Double): this.type = {
    val alpha0Msg = ("alpha0 (sum of the concentration parameter "
      + "for the Dirichlet prior)")
    require(alpha0 > 0, s"LDA $alpha0Msg must be > 0, but was set to $alpha0")
    this.alpha0 = alpha0
    this
  }

  /**
   * Number of iterations for the Kyrlov method when performing randomised SVD
   */
  @Since("2.0.2")
  def getNumIterationsKrylovMethod(): Int = q

  /**
   * Number of iterations for the Kyrlov method when performing randomised SVD
   */
  @Since("2.0.2")
  def setNumIterationsKrylovMethod(q: Int): this.type = {
    require(q >= 0, s"q (no of iterations for the Krylov method) must be >= 0")
    this.q = q
    this
  }

  /**
   * Max number of iterations for the tensor CP decomposition
   */
  @Since("2.0.2")
  def getMaxIterations(): Int = maxIterations

  /**
   * Set max number of iterations for the tensor CP decomposition
   */
  @Since("2.0.2")
  def setMaxIterations(maxIterations: Int): this.type = {
    require(maxIterations > 0,
      s"Max iterations for the tensor CP decomposition must be > 0")
    this.maxIterations = maxIterations
    this
  }

  /**
   * Convergence tolerance for the tensor CP decomposition
   */
  @Since("2.0.2")
  def getTol(): Double = tol

  /**
   * Set the convergence tolerance for the tensor CP decomposition
   */
  @Since("2.0.2")
  def setTol(tol: Double): this.type = {
    require(tol > 0.0,
      s"Tol for the tensor CP decomposition must be > 0.0")
    this.tol = tol
    this
  }

  /**
   * Random seed
   */
  @Since("2.0.2")
  def getSeed(): Long = seed

  /**
   * Set random seed
   */
  @Since("2.0.2")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Learn an LDA model using the given dataset.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   Document IDs must be unique and >= 0.
   * @return           Fitted topic-word distributions beta,
   *                   Fitted parameter alpha for the Dirichlet prior,
   *                   Eigenvectors of M2 from empirical word counts,
   *                   Eigenvalues of M2 from empirical word counts,
   *                   The average word distribution M1 from empirical word counts
   */
  @Since("2.0.2")
  def run(documents: RDD[(Long, Vector)]): (Matrix, Vector, Matrix, Vector, Vector) = {
    val documentsBreeze = documents.map {
      case (id, c) =>
        val sp = c.toSparse
        (id, new breeze.linalg.SparseVector[Double](sp.indices, sp.values, c.size))
    }

    implicit val randBasis = new RandBasis(new ThreadLocalRandomGenerator(
      new MersenneTwister(seed)))

    val tensorLDA = new edu.uci.eecs.spectralLDA.algorithm.TensorLDA(
      dimK = k,
      alpha0 = alpha0,
      numIterationsKrylovMethod = q,
      maxIterations = maxIterations,
      tol = tol,
      randomisedSVD = true
    )

    val (beta, alpha, eigvecM2, eigvalM2, m1) = tensorLDA.fit(documentsBreeze)

    (Matrices.fromBreeze(beta), Vectors.fromBreeze(alpha),
      Matrices.fromBreeze(eigvecM2), Vectors.fromBreeze(eigvalM2),
      Vectors.fromBreeze(m1))
  }
}


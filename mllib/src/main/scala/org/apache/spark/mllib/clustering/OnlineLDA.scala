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

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, kron, sum}
import breeze.numerics._
import breeze.stats.distributions.Gamma
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD


/**
 * :: Experimental ::
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * Online LDA breaks the massive corps into mini batches and scans the corpus (doc sets) only
 * once. Thus it needs not locally store or collect the documents and can be handily applied to
 * streaming document collections.
 *
 * References:
 *   Hoffman, Blei and Bach, "Online Learning for Latent Dirichlet Allocation." NIPS, 2010.
 */
@Experimental
object OnlineLDA{

  /**
   * Learns an LDA model from the given data set, using online variational Bayes (VB) algorithm.
   * This is just designed as a handy API. For massive corpus, it's recommended to use
   * OnlineLDAOptimizer directly and call submitMiniBatch in your application, which can help
   * downgrade time and space complexity by not loading the entire corpus.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   Document IDs must be unique and >= 0.
   * @param k Number of topics to infer.
   * @param batchNumber Number of batches to split input corpus. For each batch, recommendation
   *                    size is [4, 16384]. -1 for automatic batchNumber.
   * @return  Inferred LDA model
   */
  def run(documents: RDD[(Long, Vector)], k: Int, batchNumber: Int = -1): LDAModel = {
    require(batchNumber > 0 || batchNumber == -1,
      s"batchNumber must be greater or -1, but was set to $batchNumber")
    require(k > 0, s"LDA k (number of clusters) must be > 0, but was set to $k")

    val vocabSize = documents.first._2.size
    val D = documents.count().toInt // total documents count
    val batchSize =
      if (batchNumber == -1) { // auto mode
        if (D / 100 > 16384) 16384
        else if (D / 100 < 4) 4
        else D / 100
      }
      else {
        D / batchNumber
      }

    val onlineLDA = new OnlineLDAOptimizer(k, D, vocabSize)
    val actualBatchNumber = Math.ceil(D.toDouble / batchSize).toInt
    for(i <- 1 to actualBatchNumber){
      val batch = documents.sample(true, batchSize.toDouble / D)
      onlineLDA.submitMiniBatch(batch)
    }
    onlineLDA.getTopicDistribution()
  }
}

/**
 * :: Experimental ::
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * An online training optimizer for LDA. The Optimizer processes a subset (like 1%) of the corpus
 * by each call to submitMiniBatch, and update the term-topic distribution adaptively. User can
 * get the result from getTopicDistribution.
 *
 * References:
 *   Hoffman, Blei and Bach, "Online Learning for Latent Dirichlet Allocation." NIPS, 2010.
 */
@Experimental
class OnlineLDAOptimizer (
    private var k: Int,
    private var D: Int,
    private val vocabSize:Int) extends Serializable {

  // Initialize the variational distribution q(beta|lambda)
  private var lambda = getGammaMatrix(k, vocabSize)       // K * V
  private var Elogbeta = dirichlet_expectation(lambda)    // K * V
  private var expElogbeta = exp(Elogbeta)                 // K * V
  private var i = 0

  /**
   * Submit a a subset (like 1%) of the corpus to the Online LDA model, and it will update
   * the topic distribution adaptively for the terms appearing in the subset (minibatch).
   * The documents RDD can be discarded after submitMiniBatch finished.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   Document IDs must be unique and >= 0.
   * @return  Inferred LDA model
   */
  def submitMiniBatch(documents: RDD[(Long, Vector)]): Unit = {
    var stat = BDM.zeros[Double](k, vocabSize)
    stat = documents.treeAggregate(stat)(gradient, _ += _)
    update(stat, i, documents.count().toInt)
    i += 1
  }

  /**
   * get the topic-term distribution
   */
  def getTopicDistribution(): LDAModel ={
    new LocalLDAModel(Matrices.fromBreeze(lambda).transpose)
  }

  private def update(raw: BDM[Double], iter:Int, batchSize: Int): Unit ={
    // weight of the mini-batch. 1024 helps down weights early iterations
    val weight = math.pow(1024 + iter, -0.5)

    // This step finishes computing the sufficient statistics for the M step
    val stat = raw :* expElogbeta

    // Update lambda based on documents.
    lambda = lambda * (1 - weight) + (stat * D.toDouble / batchSize.toDouble + 1.0 / k) * weight
    Elogbeta = dirichlet_expectation(lambda)
    expElogbeta = exp(Elogbeta)
  }

  // for each document d update that document's gamma and phi
  private def gradient(stat: BDM[Double], doc: (Long, Vector)): BDM[Double] = {
    val termCounts = doc._2
    val (ids, cts) = termCounts match {
      case v: DenseVector => (((0 until v.size).toList), v.values)
      case v: SparseVector => (v.indices.toList, v.values)
      case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
    }

    // Initialize the variational distribution q(theta|gamma) for the mini-batch
    var gammad = new Gamma(100, 1.0 / 100.0).samplesVector(k).t // 1 * K
    var Elogthetad = vector_dirichlet_expectation(gammad.t).t   // 1 * K
    var expElogthetad = exp(Elogthetad.t).t                     // 1 * K
    val expElogbetad = expElogbeta(::, ids).toDenseMatrix       // K * ids

    var phinorm = expElogthetad * expElogbetad + 1e-100         // 1 * ids
    var meanchange = 1D
    val ctsVector = new BDV[Double](cts).t                      // 1 * ids

    // Iterate between gamma and phi until convergence
    while (meanchange > 1e-5) {
      val lastgamma = gammad
      //        1*K                  1 * ids               ids * k
      gammad = (expElogthetad :* ((ctsVector / phinorm) * (expElogbetad.t))) + 1.0/k
      Elogthetad = vector_dirichlet_expectation(gammad.t).t
      expElogthetad = exp(Elogthetad.t).t
      phinorm = expElogthetad * expElogbetad + 1e-100
      meanchange = sum(abs((gammad - lastgamma).t)) / gammad.t.size.toDouble
    }

    val m1 = expElogthetad.t.toDenseMatrix.t
    val m2 = (ctsVector / phinorm).t.toDenseMatrix
    val outerResult = kron(m1, m2) // K * ids
    for (i <- 0 until ids.size) {
      stat(::, ids(i)) := (stat(::, ids(i)) + outerResult(::, i))
    }
    stat
  }

  private def getGammaMatrix(row:Int, col:Int): BDM[Double] ={
    val gammaRandomGenerator = new Gamma(100, 1.0 / 100.0)
    val temp = gammaRandomGenerator.sample(row * col).toArray
    (new BDM[Double](col, row, temp)).t
  }

  private def dirichlet_expectation(alpha : BDM[Double]): BDM[Double] = {
    val rowSum =  sum(alpha(breeze.linalg.*, ::))
    val digAlpha = digamma(alpha)
    val digRowSum = digamma(rowSum)
    val result = digAlpha(::, breeze.linalg.*) - digRowSum
    result
  }

  private def vector_dirichlet_expectation(v : BDV[Double]): (BDV[Double]) ={
    digamma(v) - digamma(sum(v))
  }
}





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

import breeze.linalg.{DenseMatrix => BDM, normalize}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vectors, Vector, Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue

/**
 * :: DeveloperApi ::
 *
 * Latent Dirichlet Allocation (LDA) model.
 *
 * This abstraction permits for different underlying representations,
 * including local and distributed data structures.
 */
@DeveloperApi
abstract class LDAModel private[clustering] {

  import LDA._

  /** Number of topics */
  def k: Int

  /** Vocabulary size (number of terms or terms in the vocabulary) */
  def vocabSize: Int

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   */
  def topicsMatrix: Matrix

  /**
   * Return the topics described by weighted terms.
   *
   * This limits the number of terms per topic.
   * This is approximate; it may not return exactly the top-weighted terms for each topic.
   * To get a more precise set of top terms, increase maxTermsPerTopic.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term index).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(maxTermsPerTopic: Int): Array[Array[(Double, String)]]

  /**
   * Return the topics described by weighted terms.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term index).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(): Array[Array[(Double, String)]] = describeTopics(vocabSize)

  /* TODO
   * Compute the log likelihood of the observed tokens, given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, alpha, eta)
   *
   * Note:
   *  - This excludes the prior.
   *  - Even with the prior, this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   *
   * @param documents  A set of documents, where each is represented as a vector of term counts.
   *                   This must use the same vocabulary (ordering of term counts) as in training.
   *                   Document IDs must be >= 0.
   * @return  Estimated log likelihood of the data under this model
   */
  // def logLikelihood(documents: RDD[Document]): Double

  /* TODO
   * Compute the estimated topic distribution for each document.
   * This is often called “theta” in the literature.
   *
   * @param documents  A set of documents, where each is represented as a vector of term counts.
   *                   This must use the same vocabulary (ordering of term counts) as in training.
   *                   Document IDs must be >= 0.
   * @return  Estimated topic distribution for each document.
   *          The returned RDD may be zipped with the given RDD, where each returned vector
   *          is a multinomial distribution over topics.
   */
  // def topicDistributions(documents: RDD[Document]): RDD[(Long, Vector)]

}

/**
 * :: DeveloperApi ::
 *
 * Local LDA model.
 * This model stores only the inferred topics.
 * It may be used for computing topics for new documents, but it may give less accurate answers
 * than the [[DistributedLDAModel]].
 *
 * NOTE: This is currently marked DeveloperApi since it is under active development and may undergo
 *       API changes.
 *
 * @param topics Inferred topics (vocabSize x k matrix).
 */
@DeveloperApi
class LocalLDAModel private[clustering] (
    private val topics: Matrix) extends LDAModel with Serializable {

  import LDA._

  override def k: Int = topics.numCols

  override def vocabSize: Int = topics.numRows

  override def topicsMatrix: Matrix = topics

  override def describeTopics(maxTermsPerTopic: Int): Array[Array[(Double, String)]] = {
    val brzTopics = topics.toBreeze.toDenseMatrix
    val topicSummary = Range(0, k).map { topicIndex =>
      val topic = normalize(brzTopics(::, topicIndex), 1.0)
      topic.toArray.zipWithIndex.sortBy(-_._1).take(maxTermsPerTopic)
    }.toArray
    topicSummary.map { topic =>
      topic.map { case (weight, term) => (weight, term.toString) }
    }
  }

  // TODO
  // override def logLikelihood(documents: RDD[Document]): Double = ???

  // TODO:
  // override def topicDistributions(documents: RDD[Document]): RDD[(Long, Vector)] = ???

}

/**
 * :: DeveloperApi ::
 *
 * Distributed LDA model.
 * This model stores the inferred topics, the full training dataset, and the topic distributions.
 * When computing topics for new documents, it may give more accurate answers
 * than the [[LocalLDAModel]].
 *
 * NOTE: This is currently marked DeveloperApi since it is under active development and may undergo
 *       API changes.
 */
@DeveloperApi
class DistributedLDAModel private[clustering] (
    private val state: LDA.LearningState) extends LDAModel {

  import LDA._

  def toLocal: LocalLDAModel = new LocalLDAModel(topicsMatrix)

  override def k: Int = state.k

  override def vocabSize: Int = state.vocabSize

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   *
   * WARNING: This matrix is collected from an RDD. Beware memory usage when vocabSize, k are large.
   */
  override lazy val topicsMatrix: Matrix = {
    // Collect row-major topics
    val termTopicCounts: Array[(Int, TopicCounts)] =
      state.graph.vertices.filter(_._1 < 0).map { case (termIndex, cnts) =>
        (index2term(termIndex), cnts)
      }.collect()
    // Convert to Matrix
    val brzTopics = BDM.zeros[Double](vocabSize, k)
    termTopicCounts.foreach { case (term, cnts) =>
      var j = 0
      while (j < k) {
        brzTopics(term, j) = cnts(j)
        j += 1
      }
    }
    Matrices.fromBreeze(brzTopics)
  }

  override def describeTopics(maxTermsPerTopic: Int): Array[Array[(Double, String)]] = {
    val numTopics = k
    // Note: N_k is not needed to find the top terms, but it is needed to normalize weights
    //       to a distribution over terms.
    val N_k: TopicCounts = state.collectTopicTotals()
    val topicSummary = state.graph.vertices.filter(isTermVertex)
      .mapPartitions { termVertices =>
      // For this partition, collect the most common terms for each topic in queues:
      //  queues(topic) = queue of (term weight, term index).
      // Term weights are N_{wk} / N_k.
      val queues = Array.fill(numTopics)(new BoundedPriorityQueue[(Double, Int)](maxTermsPerTopic))
      for ((termId, n_wk) <- termVertices) {
        var topic = 0
        while (topic < numTopics) {
          queues(topic) += (n_wk(topic) / N_k(topic) -> index2term(termId.toInt))
          topic += 1
        }
      }
      Iterator(queues)
    }.reduce { (q1, q2) =>
      q1.zip(q2).foreach { case (a, b) => a ++= b}
      q1
    }.map(_.toArray.sortBy(-_._1))
    topicSummary.map { topic =>
      topic.map { case (weight, term) => (weight, term.toString) }
    }
  }

  // TODO
  // override def logLikelihood(documents: RDD[Document]): Double = ???

  /**
   * Compute the log likelihood of the observed tokens in the training set,
   * given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, alpha, eta)
   *
   * Note:
   *  - This excludes the prior; for that, use [[logPrior]].
   *  - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   */
  def logLikelihood = state.logLikelihood

  /**
   * Compute the log probability of the current parameter estimate, under the prior:
   *  log P(topics, topic distributions for docs | alpha, eta)
   */
  def logPrior = state.logPrior

  /**
   * For each document in the training set, return the distribution over topics for that document
   * (i.e., "theta_doc").
   *
   * @return  RDD of (document ID, topic distribution) pairs
   */
  def topicDistributions: RDD[(Long, Vector)] = {
    state.graph.vertices.filter(LDA.isDocumentVertex).map { case (docID, topicCounts) =>
      (docID.toLong, Vectors.fromBreeze(normalize(topicCounts, 1.0)))
    }
  }

  // TODO:
  // override def topicDistributions(documents: RDD[Document]): RDD[(Long, Vector)] = ???

}

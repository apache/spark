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

import breeze.linalg.{DenseMatrix => BDM, normalize, sum => brzSum}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.graphx.{VertexId, EdgeContext, Graph}
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
  def describeTopics(maxTermsPerTopic: Int): Array[Array[(Double, Int)]]

  /**
   * Return the topics described by weighted terms.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term index).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(): Array[Array[(Double, Int)]] = describeTopics(vocabSize)

  /* TODO (once LDA can be trained with Strings or given a dictionary)
   * Return the topics described by weighted terms.
   *
   * This is similar to [[describeTopics()]] but returns String values for terms.
   * If this model was trained using Strings or was given a dictionary, then this method returns
   * terms as text.  Otherwise, this method returns terms as term indices.
   *
   * This limits the number of terms per topic.
   * This is approximate; it may not return exactly the top-weighted terms for each topic.
   * To get a more precise set of top terms, increase maxTermsPerTopic.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term), where "term" is either the actual term text
   *          (if available) or the term index.
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  //def describeTopicsAsStrings(maxTermsPerTopic: Int): Array[Array[(Double, String)]]

  /* TODO (once LDA can be trained with Strings or given a dictionary)
   * Return the topics described by weighted terms.
   *
   * This is similar to [[describeTopics()]] but returns String values for terms.
   * If this model was trained using Strings or was given a dictionary, then this method returns
   * terms as text.  Otherwise, this method returns terms as term indices.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term), where "term" is either the actual term text
   *          (if available) or the term index.
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  //def describeTopicsAsStrings(): Array[Array[(Double, String)]] =
  //  describeTopicsAsStrings(vocabSize)

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

  override def describeTopics(maxTermsPerTopic: Int): Array[Array[(Double, Int)]] = {
    val brzTopics = topics.toBreeze.toDenseMatrix
    Range(0, k).map { topicIndex =>
      val topic = normalize(brzTopics(::, topicIndex), 1.0)
      topic.toArray.zipWithIndex.sortBy(-_._1).take(maxTermsPerTopic)
    }.toArray
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
class DistributedLDAModel private (
    private val graph: Graph[LDA.TopicCounts, LDA.TokenCount],
    private val globalTopicTotals: LDA.TopicCounts,
    val k: Int,
    val vocabSize: Int,
    private val topicSmoothing: Double,
    private val termSmoothing: Double,
    private[spark] val iterationTimes: Array[Double]) extends LDAModel {

  import LDA._

  private[clustering] def this(state: LDA.LearningState, iterationTimes: Array[Double]) = {
    this(state.graph, state.globalTopicTotals, state.k, state.vocabSize, state.topicSmoothing,
      state.termSmoothing, iterationTimes)
  }

  /**
   * Convert model to a local model.
   * The local model stores the inferred topics but not the topic distributions for training
   * documents.
   */
  def toLocal: LocalLDAModel = new LocalLDAModel(topicsMatrix)

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
      graph.vertices.filter(_._1 < 0).map { case (termIndex, cnts) =>
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

  override def describeTopics(maxTermsPerTopic: Int): Array[Array[(Double, Int)]] = {
    val numTopics = k
    // Note: N_k is not needed to find the top terms, but it is needed to normalize weights
    //       to a distribution over terms.
    val N_k: TopicCounts = globalTopicTotals
    graph.vertices.filter(isTermVertex)
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
  }

  // TODO
  // override def logLikelihood(documents: RDD[Document]): Double = ???

  /**
   * Log likelihood of the observed tokens in the training set,
   * given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, alpha, eta)
   *
   * Note:
   *  - This excludes the prior; for that, use [[logPrior]].
   *  - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   */
  lazy val logLikelihood: Double = {
    val eta = termSmoothing
    val alpha = topicSmoothing
    assert(eta > 1.0)
    assert(alpha > 1.0)
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    // Edges: Compute token log probability from phi_{wk}, theta_{kj}.
    val sendMsg: EdgeContext[TopicCounts, TokenCount, Double] => Unit = (edgeContext) => {
      val N_wj = edgeContext.attr
      val smoothed_N_wk: TopicCounts = edgeContext.dstAttr + (eta - 1.0)
      val smoothed_N_kj: TopicCounts = edgeContext.srcAttr + (alpha - 1.0)
      val phi_wk: TopicCounts = smoothed_N_wk :/ smoothed_N_k
      val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
      val tokenLogLikelihood = N_wj * math.log(phi_wk.dot(theta_kj))
      edgeContext.sendToDst(tokenLogLikelihood)
    }
    graph.aggregateMessages[Double](sendMsg, _ + _)
      .map(_._2).fold(0.0)(_ + _)
  }

  /**
   * Log probability of the current parameter estimate:
   *  log P(topics, topic distributions for docs | alpha, eta)
   */
  lazy val logPrior: Double = {
    val eta = termSmoothing
    val alpha = topicSmoothing
    // Term vertices: Compute phi_{wk}.  Use to compute prior log probability.
    // Doc vertex: Compute theta_{kj}.  Use to compute prior log probability.
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    val seqOp: (Double, (VertexId, TopicCounts)) => Double = {
      case (sumPrior: Double, vertex: (VertexId, TopicCounts)) =>
        if (isTermVertex(vertex)) {
          val N_wk = vertex._2
          val smoothed_N_wk: TopicCounts = N_wk + (eta - 1.0)
          val phi_wk: TopicCounts = smoothed_N_wk :/ smoothed_N_k
          (eta - 1.0) * brzSum(phi_wk.map(math.log))
        } else {
          val N_kj = vertex._2
          val smoothed_N_kj: TopicCounts = N_kj + (alpha - 1.0)
          val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
          (alpha - 1.0) * brzSum(theta_kj.map(math.log))
        }
    }
    graph.vertices.aggregate(0.0)(seqOp, _ + _)
  }

  /**
   * For each document in the training set, return the distribution over topics for that document
   * (i.e., "theta_doc").
   *
   * @return  RDD of (document ID, topic distribution) pairs
   */
  def topicDistributions: RDD[(Long, Vector)] = {
    graph.vertices.filter(LDA.isDocumentVertex).map { case (docID, topicCounts) =>
      (docID.toLong, Vectors.fromBreeze(normalize(topicCounts, 1.0)))
    }
  }

  // TODO:
  // override def topicDistributions(documents: RDD[Document]): RDD[(Long, Vector)] = ???

}

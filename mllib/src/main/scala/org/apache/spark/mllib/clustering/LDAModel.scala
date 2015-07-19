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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum, normalize}
import breeze.numerics.{lgamma, digamma, exp}

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.graphx.{VertexId, EdgeContext, Graph}
import org.apache.spark.mllib.linalg.{Vectors, Vector, Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.BoundedPriorityQueue

/**
 * :: Experimental ::
 *
 * Latent Dirichlet Allocation (LDA) model.
 *
 * This abstraction permits for different underlying representations,
 * including local and distributed data structures.
 */
@Experimental
abstract class LDAModel private[clustering] {

  /** Number of topics */
  def k: Int

  /** Vocabulary size (number of terms or terms in the vocabulary) */
  def vocabSize: Int

  /** Dirichlet parameters for document-topic distribution. */
  protected def alpha: Double

  /** Dirichlet parameters for topic-word distribution. */
  protected def eta: Double

  /** Shape parameter for random initialization of gamma. */
  protected def gammaShape: Double

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
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (term indices, term weights in topic).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(maxTermsPerTopic: Int): Array[(Array[Int], Array[Double])]

  /**
   * Return the topics described by weighted terms.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (term indices, term weights in topic).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  def describeTopics(): Array[(Array[Int], Array[Double])] = describeTopics(vocabSize)

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
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (terms, term weights in topic) where terms are either the actual term text
   *          (if available) or the term indices.
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  // def describeTopicsAsStrings(maxTermsPerTopic: Int): Array[(Array[Double], Array[String])]

  /* TODO (once LDA can be trained with Strings or given a dictionary)
   * Return the topics described by weighted terms.
   *
   * This is similar to [[describeTopics()]] but returns String values for terms.
   * If this model was trained using Strings or was given a dictionary, then this method returns
   * terms as text.  Otherwise, this method returns terms as term indices.
   *
   * WARNING: If vocabSize and k are large, this can return a large object!
   *
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (terms, term weights in topic) where terms are either the actual term text
   *          (if available) or the term indices.
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  // def describeTopicsAsStrings(): Array[(Array[Double], Array[String])] =
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
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   This must use the same vocabulary (ordering of term counts) as in training.
   *                   Document IDs must be unique and >= 0.
   * @return  Estimated log likelihood of the data under this model
   */
  // def logLikelihood(documents: RDD[(Long, Vector)]): Double

  /* TODO
   * Compute the estimated topic distribution for each document.
   * This is often called 'theta' in the literature.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   This must use the same vocabulary (ordering of term counts) as in training.
   *                   Document IDs must be unique and >= 0.
   * @return  Estimated topic distribution for each document.
   *          The returned RDD may be zipped with the given RDD, where each returned vector
   *          is a multinomial distribution over topics.
   */
  // def topicDistributions(documents: RDD[(Long, Vector)]): RDD[(Long, Vector)]

}

/**
 * :: Experimental ::
 *
 * Local LDA model.
 * This model stores only the inferred topics.
 * It may be used for computing topics for new documents, but it may give less accurate answers
 * than the [[DistributedLDAModel]].
 *
 * @param topics Inferred topics (vocabSize x k matrix).
 */
@Experimental
class LocalLDAModel private[clustering] (
    private val topics: Matrix,
    protected val alpha: Double,
    protected val eta: Double,
    protected val gammaShape: Double) extends LDAModel with Serializable {

  override def k: Int = topics.numCols

  override def vocabSize: Int = topics.numRows

  override def topicsMatrix: Matrix = topics

  override def describeTopics(maxTermsPerTopic: Int): Array[(Array[Int], Array[Double])] = {
    val brzTopics = topics.toBreeze.toDenseMatrix
    Range(0, k).map { topicIndex =>
      val topic = normalize(brzTopics(::, topicIndex), 1.0)
      val (termWeights, terms) =
        topic.toArray.zipWithIndex.sortBy(-_._1).take(maxTermsPerTopic).unzip
      (terms.toArray, termWeights.toArray)
    }.toArray
  }

  /**
   * Backwards compatibility constructor, assumes sensible values for other arguments. This will
   * NOT be what you want (prefer setting explicitly in other constructor)
   * @deprecated Provide alpha, eta, and gammaShape in constructor.
   */
  @deprecated("Provide alpha, eta, and gammaShape in constructor", "1.5.0")
  def this(topics: Matrix) {
    this(topics, 1D, 1D, 100D)
  }

  // TODO
  // override def logLikelihood(documents: RDD[(Long, Vector)]): Double = ???

  // TODO:
  // override def topicDistributions(documents: RDD[(Long, Vector)]): RDD[(Long, Vector)] = ???


  /**
   * Calculate and return per-word likelihood bound, using the `batch` of
   * documents as evaluation corpus.
   */
  def logPerplexity(
      batch: RDD[(Long, Vector)],
      totalDocs: Long): Double = {
    val corpusWords = batch
      .flatMap { case (_, termCounts) => termCounts.toArray }
      .reduce(_ + _)
    val subsampleRatio = totalDocs.toDouble / batch.count()
    val batchVariationalBound = bound(
      batch,
      subsampleRatio,
      alpha,
      eta,
      topicsMatrix.toBreeze.toDenseMatrix,
      gammaShape,
      k,
      vocabSize)
    val perWordBound = batchVariationalBound / (subsampleRatio * corpusWords)

    perWordBound
  }


  /**
   *  Estimate the variational bound of documents from `batch`:
   *  E_q[log p(bath)] - E_q[log q(batch)]
   */
  // TODO: precompute gamma during training to use for logPerplexity's call to bound
  private def bound(
      batch: RDD[(Long, Vector)],
      subsampleRatio: Double,
      alpha: Double,
      eta: Double,
      lambda: BDM[Double],
      gammaShape: Double,
      k: Int,
      vocabSize: Long): Double = {
    // Double transpose because dirichletExpectation normalizes by row and we need to normalize
    // by topic (columns of lambda)
    val Elogbeta = LDAUtils.dirichletExpectation(lambda.t).t

    var score = batch.map { case (id: Long, termCounts: Vector) =>
      var docScore = 0.0D
      val (gammad: BDV[Double], _) = OnlineLDAOptimizer.variationalTopicInference(
        termCounts, exp(Elogbeta), alpha, gammaShape, k)
      val Elogthetad: BDV[Double] = LDAUtils.dirichletExpectation(gammad)

      // E[log p(doc | theta, beta)]
      termCounts.foreachActive { case (id, count) =>
        docScore += LDAUtils.logSumExp(Elogthetad + Elogbeta(id, ::).t)
      }
      // E[log p(theta | alpha) - log q(theta | gamma)]; assumes alpha is a vector
      docScore += sum((alpha - gammad) :* Elogthetad)
      docScore += sum(lgamma(gammad) - lgamma(alpha))
      // TODO: change k * alpha to sum(alpha) when alpha is vector
      docScore += lgamma(k * alpha) - lgamma(sum(gammad))

      docScore
    }.sum()

    // compensate likelihood for when `corpus` above is only a sample of the whole corpus
    score *= subsampleRatio

    // E[log p(beta | eta) - log q (beta | lambda)]; assumes eta is a scalar
    score += sum((eta - lambda) :* Elogbeta)
    score += sum(lgamma(lambda) - lgamma(eta))

    val sumEta = eta * vocabSize
    score += sum(lgamma(sumEta) - lgamma(sum(lambda(::, breeze.linalg.*))))

    score
  }

  /**
   * Predicts the topic mixture distribution gamma for a document.
   */
  def topicDistribution(doc: (Long, Vector)): (Long, Vector) = {
    // Double transpose because dirichletExpectation normalizes by row and we need to normalize
    // by topic (columns of lambda)
    val Elogbeta = LDAUtils.dirichletExpectation(topicsMatrix.toBreeze.toDenseMatrix.t).t

    val (gamma, _) = OnlineLDAOptimizer.variationalTopicInference(
      doc._2,
      exp(Elogbeta),
      this.alpha,
      this.gammaShape,
      this.k)
    (doc._1, Vectors.dense((gamma / sum(gamma)).toArray))
  }

}

/**
 * :: Experimental ::
 *
 * Distributed LDA model.
 * This model stores the inferred topics, the full training dataset, and the topic distributions.
 * When computing topics for new documents, it may give more accurate answers
 * than the [[LocalLDAModel]].
 */
@Experimental
class DistributedLDAModel private (
    private val graph: Graph[LDA.TopicCounts, LDA.TokenCount],
    private val globalTopicTotals: LDA.TopicCounts,
    val k: Int,
    val vocabSize: Int,
    protected val alpha: Double,
    protected val eta: Double,
    protected val gammaShape: Double,
    private[spark] val iterationTimes: Array[Double]) extends LDAModel {

  import LDA._

  private[clustering] def this(
      state: EMLDAOptimizer,
      iterationTimes: Array[Double],
      gammaShape: Double) = {
    this(state.graph, state.globalTopicTotals, state.k, state.vocabSize, state.docConcentration,
      state.topicConcentration, gammaShape, iterationTimes)
  }

  /**
   * Convert model to a local model.
   * The local model stores the inferred topics but not the topic distributions for training
   * documents.
   */
  def toLocal: LocalLDAModel = new LocalLDAModel(topicsMatrix, alpha, eta, gammaShape)

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

  override def describeTopics(maxTermsPerTopic: Int): Array[(Array[Int], Array[Double])] = {
    val numTopics = k
    // Note: N_k is not needed to find the top terms, but it is needed to normalize weights
    //       to a distribution over terms.
    val N_k: TopicCounts = globalTopicTotals
    val topicsInQueues: Array[BoundedPriorityQueue[(Double, Int)]] =
      graph.vertices.filter(isTermVertex)
        .mapPartitions { termVertices =>
        // For this partition, collect the most common terms for each topic in queues:
        //  queues(topic) = queue of (term weight, term index).
        // Term weights are N_{wk} / N_k.
        val queues =
          Array.fill(numTopics)(new BoundedPriorityQueue[(Double, Int)](maxTermsPerTopic))
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
      }
    topicsInQueues.map { q =>
      val (termWeights, terms) = q.toArray.sortBy(-_._1).unzip
      (terms.toArray, termWeights.toArray)
    }
  }

  // TODO
  // override def logLikelihood(documents: RDD[(Long, Vector)]): Double = ???

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
    val alpha = this.alpha // To avoid closure capture of this
    val eta = this.eta
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
    // Term vertices: Compute phi_{wk}.  Use to compute prior log probability.
    // Doc vertex: Compute theta_{kj}.  Use to compute prior log probability.
    val alpha = this.alpha // To avoid closure capture of this
    val eta = this.eta
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    val seqOp: (Double, (VertexId, TopicCounts)) => Double = {
      case (sumPrior: Double, vertex: (VertexId, TopicCounts)) =>
        if (isTermVertex(vertex)) {
          val N_wk = vertex._2
          val smoothed_N_wk: TopicCounts = N_wk + (eta - 1.0)
          val phi_wk: TopicCounts = smoothed_N_wk :/ smoothed_N_k
          (eta - 1.0) * sum(phi_wk.map(math.log))
        } else {
          val N_kj = vertex._2
          val smoothed_N_kj: TopicCounts = N_kj + (alpha - 1.0)
          val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
          (alpha - 1.0) * sum(theta_kj.map(math.log))
        }
    }
    graph.vertices.aggregate(0.0)(seqOp, _ + _)
  }

  /**
   * For each document in the training set, return the distribution over topics for that document
   * ("theta_doc").
   *
   * @return  RDD of (document ID, topic distribution) pairs
   */
  def topicDistributions: RDD[(Long, Vector)] = {
    graph.vertices.filter(LDA.isDocumentVertex).map { case (docID, topicCounts) =>
      (docID.toLong, Vectors.fromBreeze(normalize(topicCounts, 1.0)))
    }
  }

  /** Java-friendly version of [[topicDistributions]] */
  def javaTopicDistributions: JavaPairRDD[java.lang.Long, Vector] = {
    JavaPairRDD.fromRDD(topicDistributions.asInstanceOf[RDD[(java.lang.Long, Vector)]])
  }

  // TODO:
  // override def topicDistributions(documents: RDD[(Long, Vector)]): RDD[(Long, Vector)] = ???

}

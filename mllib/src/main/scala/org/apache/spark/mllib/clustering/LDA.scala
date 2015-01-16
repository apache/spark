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

import java.util.Random

import breeze.linalg.{DenseVector => BDV, sum => brzSum, normalize, axpy => brzAxpy}

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
 * :: DeveloperApi ::
 *
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * Terminology:
 *  - "word" = "term": an element of the vocabulary
 *  - "token": instance of a term appearing in a document
 *  - "topic": multinomial distribution over words representing some concept
 *
 * Currently, the underlying implementation uses Expectation-Maximization (EM), implemented
 * according to the Asuncion et al. (2009) paper referenced below.
 *
 * References:
 *  - Original LDA paper (journal version):
 *    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
 *     - This class implements their "smoothed" LDA model.
 *  - Paper which clearly explains several algorithms, including EM:
 *    Asuncion, Welling, Smyth, and Teh.
 *    "On Smoothing and Inference for Topic Models."  UAI, 2009.
 *
 * NOTE: This is currently marked DeveloperApi since it is under active development and may undergo
 *       API changes.
 */
@DeveloperApi
class LDA private (
    private var k: Int,
    private var maxIterations: Int,
    private var topicSmoothing: Double,
    private var termSmoothing: Double,
    private var seed: Long) extends Logging {

  import LDA._

  def this() = this(k = 10, maxIterations = 20, topicSmoothing = -1, termSmoothing = -1,
    seed = Utils.random.nextLong())

  /**
   * Number of topics to infer.  I.e., the number of soft cluster centers.
   * (default = 10)
   */
  def getK: Int = k

  def setK(k: Int): this.type = {
    require(k > 0, s"LDA k (number of clusters) must be > 0, but was set to $k")
    this.k = k
    this
  }

  /**
   * Topic smoothing parameter (commonly named "alpha").
   *
   * This is the parameter to the Dirichlet prior placed on the per-document topic distributions
   * ("theta").  We use a symmetric Dirichlet prior.
   *
   * This value should be > 0.0, where larger values mean more smoothing (more regularization).
   * If set to -1, then topicSmoothing is set automatically.
   *  (default = -1 = automatic)
   *
   * Automatic setting of parameter:
   *  - For EM: default = (50 / k) + 1.
   *     - The 50/k is common in LDA libraries.
   *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   */
  def getTopicSmoothing: Double = topicSmoothing

  def setTopicSmoothing(topicSmoothing: Double): this.type = {
    require(topicSmoothing > 0.0 || topicSmoothing == -1.0,
      s"LDA topicSmoothing must be > 0 (or -1 for auto), but was set to $topicSmoothing")
    if (topicSmoothing > 0.0 && topicSmoothing <= 1.0) {
      logWarning(s"LDA.topicSmoothing was set to $topicSmoothing, but for EM, we recommend > 1.0")
    }
    this.topicSmoothing = topicSmoothing
    this
  }

  /**
   * Term smoothing parameter (commonly named "eta").
   *
   * This is the parameter to the Dirichlet prior placed on the per-topic word distributions
   * (which are called "beta" in the original LDA paper by Blei et al., but are called "phi" in many
   *  later papers such as Asuncion et al., 2009.)
   *
   * This value should be > 0.0.
   * If set to -1, then termSmoothing is set automatically.
   *  (default = -1 = automatic)
   *
   * Automatic setting of parameter:
   *  - For EM: default = 0.1 + 1.
   *     - The 0.1 gives a small amount of smoothing.
   *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   */
  def getTermSmoothing: Double = termSmoothing

  def setTermSmoothing(termSmoothing: Double): this.type = {
    require(termSmoothing > 0.0 || termSmoothing == -1.0,
      s"LDA termSmoothing must be > 0 (or -1 for auto), but was set to $termSmoothing")
    if (termSmoothing > 0.0 && termSmoothing <= 1.0) {
      logWarning(s"LDA.termSmoothing was set to $termSmoothing, but for EM, we recommend > 1.0")
    }
    this.termSmoothing = termSmoothing
    this
  }

  /**
   * Maximum number of iterations for learning.
   * (default = 20)
   */
  def getMaxIterations: Int = maxIterations

  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /** Random seed */
  def getSeed: Long = seed

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Learn an LDA model using the given dataset.
   *
   * @param documents  RDD of documents, where each document is represented as a vector of term
   *                   counts plus an ID.  Document IDs must be >= 0.
   * @return  Inferred LDA model
   */
  def run(documents: RDD[Document]): DistributedLDAModel = {
    val topicSmoothing = if (this.topicSmoothing > 0) {
      this.topicSmoothing
    } else {
      (50.0 / k) + 1.0
    }
    val termSmoothing = if (this.termSmoothing > 0) {
      this.termSmoothing
    } else {
      1.1
    }
    var state = LDA.initialState(documents, k, topicSmoothing, termSmoothing, seed)
    var iter = 0
    while (iter < maxIterations) {
      state = state.next()
      iter += 1
    }
    new DistributedLDAModel(state)
  }
}


object LDA {

  /*
    DEVELOPERS NOTE:

    This implementation uses GraphX, where the graph is bipartite with 2 types of vertices:
     - Document vertices
        - indexed {0, 1, ..., numDocuments-1}
        - Store vectors of length k (# topics).
     - Term vertices
        - indexed {-1, -2, ..., -vocabSize}
        - Store vectors of length k (# topics).
     - Edges correspond to terms appearing in documents.
        - Edges are directed Document -> Term.
        - Edges are partitioned by documents.

    Info on EM implementation.
     - We follow Section 2.2 from Asuncion et al., 2009.  We use some of their notation.
     - In this implementation, there is one edge for every unique term appearing in a document,
       i.e., for every unique (document, term) pair.
     - Notation:
        - N_{wkj} = count of tokens of term w currently assigned to topic k in document j
        - N_{*} where * is missing a subscript w/k/j is the count summed over missing subscript(s)
        - gamma_{wjk} = P(z_i = k | x_i = w, d_i = j),
          the probability of term x_i in document d_i having topic z_i.
     - Data graph
        - Document vertices store N_{kj}
        - Term vertices store N_{wk}
        - Edges store N_{wj}.
        - Global data N_k
     - Algorithm
        - Initial state:
           - Document and term vertices store random counts N_{wk}, N_{kj}.
        - E-step: For each (document,term) pair i, compute P(z_i | x_i, d_i).
           - Aggregate N_k from term vertices.
           - Compute gamma_{wjk} for each possible topic k, from each triplet.
             using inputs N_{wk}, N_{kj}, N_k.
        - M-step: Compute sufficient statistics for hidden parameters phi and theta
          (counts N_{wk}, N_{kj}, N_k).
           - Document update:
              - N_{kj} <- sum_w N_{wj} gamma_{wjk}
              - N_j <- sum_k N_{kj}  (only needed to output predictions)
           - Term update:
              - N_{wk} <- sum_j N_{wj} gamma_{wjk}
              - N_k <- sum_w N_{wk}
   */

  /**
   * :: DeveloperApi ::
   *
   * Document with an ID.
   *
   * @param counts  Vector of term (word) counts in the document.
   *                This is the "bag of words" representation.
   * @param id  Unique ID associated with this document.
   *            Documents should be indexed {0, 1, ..., numDocuments-1}.
   *
   * TODO: Can we remove the id and still be able to zip predicted topics with the Documents?
   *
   * NOTE: This is currently marked DeveloperApi since it is under active development and may
   *       undergo API changes.
   */
  @DeveloperApi
  case class Document(counts: Vector, id: Long)

  /**
   * Vector over topics (length k) of token counts.
   * The meaning of these counts can vary, and it may or may not be normalized to be a distribution.
   */
  private[clustering] type TopicCounts = BDV[Double]

  private[clustering] type TokenCount = Double

  /** Term vertex IDs are {-1, -2, ..., -vocabSize} */
  private[clustering] def term2index(term: Int): Long = -(1 + term.toLong)

  private[clustering] def index2term(termIndex: Long): Int = -(1 + termIndex).toInt

  private[clustering] def isDocumentVertex(v: (VertexId, _)): Boolean = v._1 >= 0

  private[clustering] def isTermVertex(v: (VertexId, _)): Boolean = v._1 < 0

  /**
   * State for EM algorithm: data + parameter graph, plus algorithm parameters.
   *
   * @param graph  EM graph, storing current parameter estimates in vertex descriptors and
   *               data (token counts) in edge descriptors.
   * @param k  Number of topics
   * @param vocabSize  Number of unique terms
   * @param topicSmoothing  "alpha"
   * @param termSmoothing  "eta"
   */
  private[clustering] case class LearningState(
      graph: Graph[TopicCounts, TokenCount],
      k: Int,
      vocabSize: Int,
      topicSmoothing: Double,
      termSmoothing: Double) {
    // TODO: Checkpoint periodically?
    def next(): LearningState = copy(graph = step(graph))

    private def step(graph: Graph[TopicCounts, TokenCount]): Graph[TopicCounts, TokenCount] = {
      val eta = termSmoothing
      val W = vocabSize
      val alpha = topicSmoothing

      val N_k = collectTopicTotals()
      val sendMsg: EdgeContext[TopicCounts, TokenCount, (Boolean, TopicCounts)] => Unit =
        (edgeContext) => {
          // Compute N_{wj} gamma_{wjk}
          val N_wj = edgeContext.attr
          // E-STEP: Compute gamma_{wjk} (smoothed topic distributions), scaled by token count
          // N_{wj}.
          val scaledTopicDistribution: TopicCounts =
            computePTopic(edgeContext.srcAttr, edgeContext.dstAttr, N_k, W, eta, alpha) *= N_wj
          edgeContext.sendToDst((false, scaledTopicDistribution))
          edgeContext.sendToSrc((false, scaledTopicDistribution))
        }
      // This is a hack to detect whether we could modify the values in-place.
      // TODO: Add zero/seqOp/combOp option to aggregateMessages.
      val mergeMsg: ((Boolean, TopicCounts), (Boolean, TopicCounts)) => (Boolean, TopicCounts) =
        (m0, m1) => {
          val sum =
            if (m0._1) {
              m0._2 += m1._2
            } else if (m1._1) {
              m1._2 += m0._2
            } else {
              val k = m0._2.length
              m0._2 + m1._2
            }
          (true, sum)
        }
      // M-STEP: Aggregation computes new N_{kj}, N_{wk} counts.
      val docTopicDistributions: VertexRDD[TopicCounts] =
        graph.aggregateMessages[(Boolean, TopicCounts)](sendMsg, mergeMsg)
          .mapValues(_._2)
      // Update the vertex descriptors with the new counts.
      graph.outerJoinVertices(docTopicDistributions) { (vid, oldDist, newDist) => newDist.get}
    }

    def collectTopicTotals(): TopicCounts = {
      val numTopics = k
      graph.vertices.filter(isTermVertex).values.fold(BDV.zeros[Double](numTopics))(_ += _)
    }

    /**
     * Compute the log likelihood of the observed tokens, given the current parameter estimates:
     * log P(docs | topics, topic distributions for docs, alpha, eta)
     *
     * Note:
     * - This excludes the prior; for that, use [[logPrior]].
     * - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
     * hyperparameters.
     */
    lazy val logLikelihood: Double = {
      val eta = termSmoothing
      val alpha = topicSmoothing
      assert(eta > 1.0)
      assert(alpha > 1.0)
      val N_k = collectTopicTotals()
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
     * Compute the log probability of the current parameter estimate:
     * log P(topics, topic distributions for docs | alpha, eta)
     */
    lazy val logPrior: Double = {
      val eta = termSmoothing
      val alpha = topicSmoothing
      // Term vertices: Compute phi_{wk}.  Use to compute prior log probability.
      // Doc vertex: Compute theta_{kj}.  Use to compute prior log probability.
      val N_k = collectTopicTotals()
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
  }

  /**
   * Compute gamma_{wjk}, a distribution over topics k.
   */
  private def computePTopic(
      docTopicCounts: TopicCounts,
      wordTopicCounts: TopicCounts,
      totalTopicCounts: TopicCounts,
      vocabSize: Int,
      eta: Double,
      alpha: Double): TopicCounts = {
    val K = docTopicCounts.length
    val N_j = docTopicCounts.data
    val N_w = wordTopicCounts.data
    val N = totalTopicCounts.data
    val eta1 = eta - 1.0
    val alpha1 = alpha - 1.0
    val Weta1 = vocabSize * eta1
    var sum = 0.0
    val gamma_wj = new Array[Double](K)
    var k = 0
    while (k < K) {
      val gamma_wjk = (N_w(k) + eta1) * (N_j(k) + alpha1) / (N(k) + Weta1)
      gamma_wj(k) = gamma_wjk
      sum += gamma_wjk
      k += 1
    }
    // normalize
    BDV(gamma_wj) /= sum
  }

    /**
     * Compute bipartite term/doc graph.
     * doc ids are shifted by vocabSize to maintain uniqueness
     */
    private def initialState(
      docs: RDD[Document],
      k: Int,
      topicSmoothing: Double,
      termSmoothing: Double,
      randomSeed: Long): LearningState = {
    // For each document, create an edge (Document -> Term) for each unique term in the document.
    val edges: RDD[Edge[TokenCount]] = docs.flatMap { case doc =>
      // Add edges for terms with non-zero counts.
      doc.counts.toBreeze.activeIterator.filter(_._2 != 0.0).map { case (term, cnt) =>
        Edge(doc.id, term2index(term), cnt)
      }
    }

    val vocabSize = docs.take(1).head.counts.size

    // Create vertices.
    // Initially, we use random soft assignments of tokens to topics (random gamma).
    val edgesWithGamma: RDD[(Edge[TokenCount], TopicCounts)] =
      edges.mapPartitionsWithIndex { case (partIndex, partEdges) =>
        val random = new Random(partIndex + randomSeed)
        partEdges.map { edge =>
          // Create a random gamma_{wjk}
          (edge, normalize(BDV.fill[Double](k)(random.nextDouble()), 1.0))
        }
      }
    def createVertices(sendToWhere: Edge[TokenCount] => VertexId): RDD[(VertexId, TopicCounts)] = {
      val verticesTMP: RDD[(VertexId, (TokenCount, TopicCounts))] =
        edgesWithGamma.map { case (edge, gamma: TopicCounts) =>
          (sendToWhere(edge), (edge.attr, gamma))
        }
      verticesTMP.aggregateByKey(BDV.zeros[Double](k))(
        (sum, t) => {
          brzAxpy(t._1, t._2, sum)
          sum
        },
        (sum0, sum1) => {
          sum0 += sum1
        }
      )
    }
    val docVertices = createVertices(_.srcId)
    val termVertices = createVertices(_.dstId)

    // Partition such that edges are grouped by document
    val graph = Graph(docVertices ++ termVertices, edges)
      .partitionBy(PartitionStrategy.EdgePartition1D)

    LearningState(graph, k, vocabSize, topicSmoothing, termSmoothing)
  }
}

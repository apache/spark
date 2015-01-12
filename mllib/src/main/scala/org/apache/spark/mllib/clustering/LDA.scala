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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum => brzSum, normalize}

import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors, Matrix, Matrices}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{BoundedPriorityQueue, Utils}


/**
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
 *     Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
 *  - Paper which clearly explains several algorithms, including EM:
 *     Asuncion, Welling, Smyth, and Teh.
 *     "On Smoothing and Inference for Topic Models."  UAI, 2009.
 */
class LDA private (
    private var k: Int,
    private var maxIterations: Int,
    private var topicSmoothing: Double,
    private var termSmoothing: Double,
    private var seed: Long) {

  import LDA._

  def this() = this(k = 10, maxIterations = 10, topicSmoothing = -1, termSmoothing = 0.1,
    seed = Utils.random.nextLong())

  /**
   * Number of topics to infer.  I.e., the number of soft cluster centers.
   * (default = 10)
   */
  def getK: Int = k

  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  // TODO: UDPATE alpha, eta to be > 1 automatically for MAP

  /**
   * Topic smoothing parameter (commonly named "alpha").
   *
   * This is the parameter to the Dirichlet prior placed on the per-document topic distributions
   * ("theta").  We use a symmetric Dirichlet prior.
   *
   * This value should be > 0.0, where larger values mean more smoothing (more regularization).
   * If set <= 0, then topicSmoothing is set to equal 50 / k (where k is the number of topics).
   *  (default = 50 / k)
   */
  def getTopicSmoothing: Double = topicSmoothing

  def setTopicSmoothing(alpha: Double): this.type = {
    topicSmoothing = alpha
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
   *  (default = 0.1)
   */
  def getTermSmoothing: Double = termSmoothing

  def setTermSmoothing(eta: Double): this.type = {
    termSmoothing = eta
    this
  }

  /**
   * Maximum number of iterations for learning.
   * (default = 10)
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
   * @param docs  RDD of documents, where each document is represented as a vector of term counts.
   *              Document IDs must be >= 0.
   * @return  Inferred LDA model
   */
  def run(docs: RDD[Document]): DistributedLDAModel = {
    var state =
      LDA.initialState(docs, k, termSmoothing, topicSmoothing, seed)
    var iter = 0
    while (iter < maxIterations) {
      state = state.next()
      iter += 1
    }
    new DistributedLDAModel(state)
  }
}

/**
 * Latent Dirichlet Allocation (LDA) model
 */
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

  /* TODO
   * Computes the estimated log likelihood of data (a set of documents), given the model.
   *
   * Note that this is an estimate since it requires inference (and exact inference is intractable
   * for the LDA model).
   *
   * @param documents  A set of documents, where each is represented as a vector of term counts.
   *                   This must use the same vocabulary (ordering of term counts) as in training.
   *                   Document IDs must be >= 0.
   * @return  Estimated log likelihood of the data under this model
   */
  // TODO
  //def logLikelihood(documents: RDD[Document]): Double

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
 * Local LDA model.
 * This model stores only the inferred topics.
 * It may be used for computing topics for new documents, but it may give less accurate answers
 * than the [[DistributedLDAModel]].
 *
 * @param topics Inferred topics (vocabSize x k matrix).
 */
class LocalLDAModel private[clustering] (
    private val topics: Matrix) extends LDAModel with Serializable {

  import LDA._

  override def k: Int = topics.numCols

  override def vocabSize: Int = topics.numRows

  override def topicsMatrix: Matrix = topics

  // TODO
  //override def logLikelihood(documents: RDD[Document]): Double = ???

  // TODO:
  // override def topicDistributions(documents: RDD[Document]): RDD[(Long, Vector)] = ???

}

/**
 * Distributed LDA model.
 * This model stores the inferred topics, the full training dataset, and the topic distributions.
 * When computing topics for new documents, it may give more accurate answers
 * than the [[LocalLDAModel]].
 */
class DistributedLDAModel private[clustering] (
    private val state: LDA.LearningState) extends LDAModel {

  import LDA._

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

  // TODO
  //override def logLikelihood(documents: RDD[Document]): Double = ???

  /**
   * For each document in the training set, return the distribution over topics for that document
   * (i.e., "theta_doc").
   *
   * @return  RDD of (document ID, topic distribution) pairs
   */
  def topicDistributions: RDD[(Long, Vector)] = {
    state.graph.vertices.filter(_._1 >= 0).map { case (docID, topicCounts) =>
      (docID.toLong, Vectors.fromBreeze(topicCounts))
    }
  }

  // TODO:
  // override def topicDistributions(documents: RDD[Document]): RDD[(Long, Vector)] = ???

  /*
  // TODO: Do this properly
  lazy val logLikelihood = {
    graph.triplets.aggregate(0.0)({ (acc, triple) =>
      val scores = triple.srcAttr :* triple.dstAttr
      val logScores = breeze.numerics.log(scores)
      scores /= brzSum(scores)
      brzSum(scores :*= logScores) * triple.attr
    }, _ + _)
  }
  */

  /**
   *
   * @param maxTermsPerTopic
   * @return  Array over topics, where each element is a set of top terms represented
   *          as (term weight in topic, term index).
   */
  def getTopics(maxTermsPerTopic: Int): Array[Array[(Double, Int)]] = {
    val nt = maxTermsPerTopic
    state.graph.vertices.filter(_._1 < 0) // select term vertices
      .mapPartitions { items =>
      // Create queue of
      val queues = Array.fill(nt)(new BoundedPriorityQueue[(Double, Int)](maxTermsPerTopic))
      for ((termId, factor) <- items) {
        var t = 0
        while (t < nt) {
          queues(t) += (factor(t)  -> termId.toInt)
          t += 1
        }
      }
      Iterator(queues)
    }.reduce { (q1, q2) =>
      q1.zip(q2).foreach { case (a,b) => a ++= b }
      q1
    }.map ( q => q.toArray )
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
     - We follow Section 2.2 from Asuncion et al., 2009.
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
   * Document
   *
   * @param counts  Vector of term (word) counts in the document.
   *                This is the "bag of words" representation.
   * @param id  Unique ID associated with this document.
   *            Documents should be indexed {0, 1, ..., numDocuments-1}.
   *
   * TODO: Can we remove the id and still be able to zip predicted topics with the Documents?
   */
  case class Document(counts: SparseVector, id: VertexId)

  /**
   * Vector over topics (length k) of token counts.
   * The meaning of these counts can vary, and it may or may not be normalized to be a distribution.
   */
  private[clustering] type TopicCounts = BDV[Double]

  private[clustering] type TokenCount = Double

  /** Term vertex IDs are {-1, -2, ..., -vocabSize} */
  private[clustering] def term2index(term: Int): Long = -(1 + term.toLong)

  private[clustering] def index2term(termIndex: Long): Int = -(1 + termIndex).toInt

  private[clustering] def isTermVertex(v: Tuple2[VertexId, _]): Boolean = v._1 < 0

  private[clustering] def isDocVertex(v: Tuple2[VertexId, _]): Boolean = v._1 >= 0

  /**
   *
   * Has all the information needed to run collapsed Gibbs sampling.
   *
   * @param graph
   * @param k
   * @param vocabSize
   * @param topicSmoothing
   * @param termSmoothing
   */
  private[clustering] case class LearningState(
      graph: Graph[TopicCounts, TokenCount],
      k: Int,
      vocabSize: Int,
      topicSmoothing: Double,
      termSmoothing: Double) {

    // TODO: Checkpoint periodically
    def next() = copy(graph = step(graph))

    private def step(graph: Graph[TopicCounts, TokenCount]): Graph[TopicCounts, TokenCount] = {
      val eta = termSmoothing
      val W = vocabSize
      val alpha = topicSmoothing

      // Collect N_k from term vertices.
      val N_k = collectTopicTotals()
      val sendMsg: EdgeContext[TopicCounts, TokenCount, TopicCounts] => Unit = (edgeContext) => {
        // Compute N_{wj} gamma_{wjk}
        val N_wj = edgeContext.attr
        // E-STEP: Compute gamma_{wjk} (smoothed topic distributions), scaled by token count N_{wj}.
        val scaledTopicDistribution: TopicCounts =
          computePTopic(edgeContext, N_k, W, eta, alpha) * N_wj
        edgeContext.sendToDst(scaledTopicDistribution)
        edgeContext.sendToSrc(scaledTopicDistribution)
      }
      // M-STEP: Aggregation computes new N_{kj}, N_{wk} counts.
      val docTopicDistributions: VertexRDD[TopicCounts] =
        graph.aggregateMessages[TopicCounts](sendMsg, _ + _)
      // Update the vertex descriptors with the new counts.
      graph.outerJoinVertices(docTopicDistributions){ (vid, oldDist, newDist) => newDist.get }
    }

    /*
    /**
     * Update document topic distributions, i.e., theta_doc = p(z|doc) for each doc.
     * @return Graph with updated document vertex descriptors
     */
    private def updateDocs(graph: Graph[TopicCounts, TokenCount]): Graph[TopicCounts, TokenCount] = {
      val alpha = topicSmoothing
      // Compute smoothed topic distributions for each document (size: numDocuments x k).
      val docTopicTotals = updateExpectedCounts(_.srcId)
      val newTotals = docTopicTotals.mapValues(total => normalize(total += alpha, 1))
      println(s"E-STEP newTotals.take(1): ${newTotals.take(1)(0)._2}")
      // Update document vertices with new topic distributions.
      graph.outerJoinVertices(newTotals){ (vid, old, newOpt) => newOpt.getOrElse(old) }
    }

    /**
     * Update topics, i.e., beta_z = p(w|z) for each topic z.
     * (Conceptually, these are the topics.  However, they are stored transposed, where each
     *  term vertex stores the distribution value for each topic.)
     * @return Graph with updated term vertex descriptors
     */
    private def updateTerms(graph: Graph[TopicCounts, TokenCount]): Graph[TopicCounts, TokenCount] = {
      // Compute new topics.
      val termTotals = updateExpectedCounts(_.dstId)
      // Collect the aggregate counts over terms (summing all topics).
      val eta: Double = termSmoothing
      val topicTotals = termTotals.map(_._2).fold(BDV.zeros[Double](k))(_ + _)
      topicTotals += (eta * vocabSize)
      println(s"M-STEP topicTotals: $topicTotals")
      // Update term vertices with new topic weights.
      graph.outerJoinVertices(termTotals)( (vid, old, newOpt) =>
        newOpt
          .map { counts => (counts += eta) :/= topicTotals } // smooth individual counts; normalize
          .getOrElse(old)
      )
    }

    private def updateExpectedCounts(sendToWhere: (EdgeTriplet[_, _]) => VertexId): VertexRDD[TopicCounts] = {
      //  Collect N_k from term vertices.
      val N_k = collectTopicTotals()
      val eta = termSmoothing
      val W = vocabSize
      val alpha = topicSmoothing
      graph.mapReduceTriplets[TopicCounts]({
        trip => Iterator(sendToWhere(trip) -> computePTopic(trip, N_k, W, eta, alpha))
      }, _ += _)
    }
    */

    private def collectTopicTotals(): TopicCounts = {
      val numTopics = k
      graph.vertices.filter(isTermVertex).map(_._2).fold(BDV.zeros[Double](numTopics))(_ + _)
    }

  }

  private def computePTopic(edgeContext: EdgeContext[TopicCounts, TokenCount, TopicCounts],
                            N_k: TopicCounts, vocabSize: Int, eta: Double, alpha: Double): TopicCounts = {
    val smoothed_N_wk: TopicCounts = edgeContext.dstAttr + (eta - 1.0)
    val smoothed_N_kj: TopicCounts = edgeContext.srcAttr + (alpha - 1.0)
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    // proportional to p(w|z) * p(z|d) / p(z)
    val unnormalizedGamma = smoothed_N_wk :* smoothed_N_kj :/ smoothed_N_k
    // normalize
    unnormalizedGamma /= brzSum(unnormalizedGamma)
  }

  /*
  private def computePTopic(edge: EdgeTriplet[TopicCounts, TokenCount], N_k: TopicCounts, vocabSize: Int, eta: Double, alpha: Double): TopicCounts = {
    val smoothed_N_wk: TopicCounts = edge.dstAttr + (eta - 1.0)
    val smoothed_N_kj: TopicCounts = edge.srcAttr + (alpha - 1.0)
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    // proportional to p(w|z) * p(z|d) / p(z)
    val unnormalizedGamma = smoothed_N_wk :* smoothed_N_kj :/ smoothed_N_k
    // normalize
    unnormalizedGamma /= brzSum(unnormalizedGamma)
  }
  */

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
    val edges: RDD[Edge[TokenCount]] = docs.mapPartitionsWithIndex { case (partIndex, partDocs) =>
      partDocs.flatMap { doc: Document =>
        // Add edges for terms with non-zero counts.
        doc.counts.toBreeze.activeIterator.filter(_._2 != 0.0).map { case (term, cnt) =>
          Edge(doc.id, term2index(term), cnt)
        }
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
      val verticesTMP: RDD[(VertexId, TopicCounts)] =
        edgesWithGamma.map { case (edge, gamma: TopicCounts) =>
          val N_wj = edge.attr
          (sendToWhere(edge), gamma * N_wj)
        }
      verticesTMP.foldByKey(BDV.zeros[Double](k))(_ + _)
    }
    val docVertices = createVertices(_.srcId)
    val termVertices = createVertices(_.dstId)

    // Partition such that edges are grouped by document
    val graph = Graph(docVertices ++ termVertices, edges)
      .partitionBy(PartitionStrategy.EdgePartition1D)

    LearningState(graph, k, vocabSize, topicSmoothing, termSmoothing)
  }

}

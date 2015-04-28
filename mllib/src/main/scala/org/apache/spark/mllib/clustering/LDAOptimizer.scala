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

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, sum, normalize, kron}
import breeze.numerics.{digamma, exp, abs}
import breeze.stats.distributions.Gamma

import org.apache.spark.annotation.Experimental
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.mllib.impl.PeriodicGraphCheckpointer
import org.apache.spark.mllib.linalg.{Matrices, SparseVector, DenseVector, Vector}
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 *
 * An LDAOptimizer specifies which optimization/learning/inference algorithm to use, and it can
 * hold optimizer-specific parameters for users to set.
 */
@Experimental
trait LDAOptimizer {

  /*
    DEVELOPERS NOTE:

    An LDAOptimizer contains an algorithm for LDA and performs the actual computation, which
    stores internal data structure (Graph or Matrix) and other parameters for the algorithm.
    The interface is isolated to improve the extensibility of LDA.
   */

  /**
   * Initializer for the optimizer. LDA passes the common parameters to the optimizer and
   * the internal structure can be initialized properly.
   */
  private[clustering] def initialize(docs: RDD[(Long, Vector)], lda: LDA): LDAOptimizer

  private[clustering] def next(): LDAOptimizer

  private[clustering] def getLDAModel(iterationTimes: Array[Double]): LDAModel
}

/**
 * :: Experimental ::
 *
 * Optimizer for EM algorithm which stores data + parameter graph, plus algorithm parameters.
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
 */
@Experimental
class EMLDAOptimizer extends LDAOptimizer {

  import LDA._

  /**
   * Following fields will only be initialized through initialize method
   */
  private[clustering] var graph: Graph[TopicCounts, TokenCount] = null
  private[clustering] var k: Int = 0
  private[clustering] var vocabSize: Int = 0
  private[clustering] var docConcentration: Double = 0
  private[clustering] var topicConcentration: Double = 0
  private[clustering] var checkpointInterval: Int = 10
  private var graphCheckpointer: PeriodicGraphCheckpointer[TopicCounts, TokenCount] = null

  /**
   * Compute bipartite term/doc graph.
   */
  private[clustering] override def initialize(docs: RDD[(Long, Vector)], lda: LDA):
  LDAOptimizer = {

    val docConcentration = lda.getDocConcentration
    val topicConcentration = lda.getTopicConcentration
    val k = lda.getK

    /**
     * Note: The restriction > 1.0 may be relaxed in the future (allowing sparse solutions),
     *       but values in (0,1) are not yet supported.
     */
    require(docConcentration > 1.0 || docConcentration == -1.0, s"LDA docConcentration must be" +
      s" > 1.0 (or -1 for auto) for EM Optimizer, but was set to $docConcentration")
    require(topicConcentration > 1.0 || topicConcentration == -1.0, s"LDA topicConcentration " +
      s"must be > 1.0 (or -1 for auto) for EM Optimizer, but was set to $topicConcentration")

    /**
     *  - For EM: default = (50 / k) + 1.
     *     - The 50/k is common in LDA libraries.
     *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
     */
    this.docConcentration = if (docConcentration == -1) (50.0 / k) + 1.0 else docConcentration

    /**
     *  - For EM: default = 0.1 + 1.
     *     - The 0.1 gives a small amount of smoothing.
     *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
     */
    this.topicConcentration = if (topicConcentration == -1) 1.1 else topicConcentration

    val randomSeed = lda.getSeed

    // For each document, create an edge (Document -> Term) for each unique term in the document.
    val edges: RDD[Edge[TokenCount]] = docs.flatMap { case (docID: Long, termCounts: Vector) =>
      // Add edges for terms with non-zero counts.
      termCounts.toBreeze.activeIterator.filter(_._2 != 0.0).map { case (term, cnt) =>
        Edge(docID, term2index(term), cnt)
      }
    }

    // Create vertices.
    // Initially, we use random soft assignments of tokens to topics (random gamma).
    def createVertices(): RDD[(VertexId, TopicCounts)] = {
      val verticesTMP: RDD[(VertexId, TopicCounts)] =
        edges.mapPartitionsWithIndex { case (partIndex, partEdges) =>
          val random = new Random(partIndex + randomSeed)
          partEdges.flatMap { edge =>
            val gamma = normalize(BDV.fill[Double](k)(random.nextDouble()), 1.0)
            val sum = gamma * edge.attr
            Seq((edge.srcId, sum), (edge.dstId, sum))
          }
        }
      verticesTMP.reduceByKey(_ + _)
    }

    val docTermVertices = createVertices()

    // Partition such that edges are grouped by document
    this.graph = Graph(docTermVertices, edges).partitionBy(PartitionStrategy.EdgePartition1D)
    this.k = k
    this.vocabSize = docs.take(1).head._2.size
    this.checkpointInterval = lda.getCheckpointInterval
    this.graphCheckpointer = new
      PeriodicGraphCheckpointer[TopicCounts, TokenCount](graph, checkpointInterval)
    this.globalTopicTotals = computeGlobalTopicTotals()
    this
  }

  private[clustering] override def next(): EMLDAOptimizer = {
    require(graph != null, "graph is null, EMLDAOptimizer not initialized.")

    val eta = topicConcentration
    val W = vocabSize
    val alpha = docConcentration

    val N_k = globalTopicTotals
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
    // TODO: Add zero/seqOp/combOp option to aggregateMessages. (SPARK-5438)
    val mergeMsg: ((Boolean, TopicCounts), (Boolean, TopicCounts)) => (Boolean, TopicCounts) =
      (m0, m1) => {
        val sum =
          if (m0._1) {
            m0._2 += m1._2
          } else if (m1._1) {
            m1._2 += m0._2
          } else {
            m0._2 + m1._2
          }
        (true, sum)
      }
    // M-STEP: Aggregation computes new N_{kj}, N_{wk} counts.
    val docTopicDistributions: VertexRDD[TopicCounts] =
      graph.aggregateMessages[(Boolean, TopicCounts)](sendMsg, mergeMsg)
        .mapValues(_._2)
    // Update the vertex descriptors with the new counts.
    val newGraph = GraphImpl.fromExistingRDDs(docTopicDistributions, graph.edges)
    graph = newGraph
    graphCheckpointer.updateGraph(newGraph)
    globalTopicTotals = computeGlobalTopicTotals()
    this
  }

  /**
   * Aggregate distributions over topics from all term vertices.
   *
   * Note: This executes an action on the graph RDDs.
   */
  private[clustering] var globalTopicTotals: TopicCounts = null

  private def computeGlobalTopicTotals(): TopicCounts = {
    val numTopics = k
    graph.vertices.filter(isTermVertex).values.fold(BDV.zeros[Double](numTopics))(_ += _)
  }

  private[clustering] override def getLDAModel(iterationTimes: Array[Double]): LDAModel = {
    require(graph != null, "graph is null, EMLDAOptimizer not initialized.")
    this.graphCheckpointer.deleteAllCheckpoints()
    new DistributedLDAModel(this, iterationTimes)
  }
}


/**
 * :: Experimental ::
 *
 * An online optimizer for LDA. The Optimizer implements the Online LDA algorithm, which
 * processes a subset of the corpus by each call to next, and update the term-topic
 * distribution adaptively.
 *
 * References:
 *   Hoffman, Blei and Bach, "Online Learning for Latent Dirichlet Allocation." NIPS, 2010.
 */
@Experimental
class OnlineLDAOptimizer extends LDAOptimizer {

  // LDA common parameters
  private var k: Int = 0
  private var D: Int = 0
  private var vocabSize: Int = 0
  private var alpha: Double = 0
  private var eta: Double = 0
  private var randomSeed: Long = 0

  // Online LDA specific parameters
  private var tau_0: Double = -1
  private var kappa: Double = -1
  private var batchSize: Int = -1

  // internal data structure
  private var docs: RDD[(Long, Vector)] = null
  private var lambda: BDM[Double] = null
  private var Elogbeta: BDM[Double]= null
  private var expElogbeta: BDM[Double] = null

  // count of invocation to next, used to help deciding the weight for each iteration
  private var iteration = 0

  /**
   * A (positive) learning parameter that downweights early iterations
   */
  def getTau_0: Double = {
    if (this.tau_0 == -1) {
      1024
    } else {
      this.tau_0
    }
  }

  /**
   * A (positive) learning parameter that downweights early iterations
   * Automatic setting of parameter:
   *  - default = 1024, which follows the recommendation from OnlineLDA paper.
   */
  def setTau_0(tau_0: Double): this.type = {
    require(tau_0 > 0 || tau_0 == -1.0,  s"LDA tau_0 must be positive, but was set to $tau_0")
    this.tau_0 = tau_0
    this
  }

  /**
   * Learning rate: exponential decay rate
   */
  def getKappa: Double = {
    if (this.kappa == -1) {
      0.5
    } else {
      this.kappa
    }
  }

  /**
   * Learning rate: exponential decay rate---should be between
   * (0.5, 1.0] to guarantee asymptotic convergence.
   *  - default = 0.5, which follows the recommendation from OnlineLDA paper.
   */
  def setKappa(kappa: Double): this.type = {
    require(kappa >= 0 || kappa == -1.0,
      s"Online LDA kappa must be nonnegative (or -1 for auto), but was set to $kappa")
    this.kappa = kappa
    this
  }

  /**
   * Mini-batch size, which controls how many documents are used in each iteration
   */
  def getBatchSize: Int = {
    if (this.batchSize == -1) {
      D / 100
    } else {
      this.batchSize
    }
  }

  /**
   * Mini-batch size, which controls how many documents are used in each iteration
   * default = 1% from total documents.
   */
  def setBatchSize(batchSize: Int): this.type = {
    this.batchSize = batchSize
    this
  }

  private[clustering] override def initialize(docs: RDD[(Long, Vector)], lda: LDA): LDAOptimizer = {

    this.k = lda.getK
    this.D = docs.count().toInt
    this.vocabSize = docs.first()._2.size
    this.alpha = if (lda.getDocConcentration == -1) 1.0 / k else lda.getDocConcentration
    this.eta = if (lda.getTopicConcentration == -1) 1.0 / k else lda.getTopicConcentration
    this.randomSeed = randomSeed

    this.docs = docs

    // Initialize the variational distribution q(beta|lambda)
    this.lambda = getGammaMatrix(k, vocabSize)
    this.Elogbeta = dirichlet_expectation(lambda)
    this.expElogbeta = exp(Elogbeta)
    this.iteration = 0
    this
  }

  /**
   * Submit a a subset (like 1%, decide by the batchSize) of the corpus to the Online LDA model,
   * and it will update the topic distribution adaptively for the terms appearing in the subset.
   *
   * @return  Inferred LDA model
   */
  private[clustering] override def next(): OnlineLDAOptimizer = {
    iteration += 1
    val batchSize = getBatchSize
    val batch = docs.sample(true, batchSize.toDouble / D, randomSeed).cache()
    if(batch.isEmpty()) return this

    val k = this.k
    val vocabSize = this.vocabSize
    val expElogbeta = this.expElogbeta
    val alpha = this.alpha

    val stats = batch.mapPartitions(docs =>{
      val stat = BDM.zeros[Double](k, vocabSize)
      docs.foreach(doc =>{
        val termCounts = doc._2
        val (ids, cts) = termCounts match {
          case v: DenseVector => (((0 until v.size).toList), v.values)
          case v: SparseVector => (v.indices.toList, v.values)
          case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
        }

        // Initialize the variational distribution q(theta|gamma) for the mini-batch
        var gammad = new Gamma(100, 1.0 / 100.0).samplesVector(k).t // 1 * K
        var Elogthetad = digamma(gammad) - digamma(sum(gammad))     // 1 * K
        var expElogthetad = exp(Elogthetad)                         // 1 * K
        val expElogbetad = expElogbeta(::, ids).toDenseMatrix       // K * ids

        var phinorm = expElogthetad * expElogbetad + 1e-100         // 1 * ids
        var meanchange = 1D
        val ctsVector = new BDV[Double](cts).t                      // 1 * ids

        // Iterate between gamma and phi until convergence
        while (meanchange > 1e-5) {
          val lastgamma = gammad
          //        1*K                  1 * ids               ids * k
          gammad = (expElogthetad :* ((ctsVector / phinorm) * (expElogbetad.t))) + alpha
          Elogthetad = digamma(gammad) - digamma(sum(gammad))
          expElogthetad = exp(Elogthetad)
          phinorm = expElogthetad * expElogbetad + 1e-100
          meanchange = sum(abs(gammad - lastgamma)) / k
        }

        val m1 = expElogthetad.t.toDenseMatrix.t
        val m2 = (ctsVector / phinorm).t.toDenseMatrix
        val outerResult = kron(m1, m2) // K * ids
        for (i <- 0 until ids.size) {
          stat(::, ids(i)) := (stat(::, ids(i)) + outerResult(::, i))
        }
        stat
      })
      Iterator(stat)
    })

    val batchResult = stats.reduce(_ += _)
    update(batchResult, iteration, batchSize)
    batch.unpersist()
    this
  }

  private[clustering] override def getLDAModel(iterationTimes: Array[Double]): LDAModel = {
    new LocalLDAModel(Matrices.fromBreeze(lambda).transpose)
  }

  private def update(raw: BDM[Double], iter:Int, batchSize: Int): Unit ={

    val tau_0 = this.getTau_0
    val kappa = this.getKappa

    // weight of the mini-batch.
    val weight = math.pow(tau_0 + iter, -kappa)

    // This step finishes computing the sufficient statistics for the M step
    val stat = raw :* expElogbeta

    // Update lambda based on documents.
    lambda = lambda * (1 - weight) + (stat * D.toDouble / batchSize.toDouble + eta) * weight
    Elogbeta = dirichlet_expectation(lambda)
    expElogbeta = exp(Elogbeta)
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
}

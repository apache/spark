package org.apache.spark.mllib.clustering

import java.util.Random

import breeze.linalg.{DenseVector => BDV, DenseMatrix, normalize}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, kron, sum}
import breeze.numerics._
import breeze.stats.distributions.Gamma

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.mllib.impl.PeriodicGraphCheckpointer
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Matrices, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
/**
 * Created by yuhao on 4/22/15.
 */
trait LDAOptimizer

/**
 * Optimizer for EM algorithm which stores data + parameter graph, plus algorithm parameters.
 *
 * @param graph  EM graph, storing current parameter estimates in vertex descriptors and
 *               data (token counts) in edge descriptors.
 * @param k  Number of topics
 * @param vocabSize  Number of unique terms
 * @param docConcentration  "alpha"
 * @param topicConcentration  "beta" or "eta"
 */
class EMOptimizer(
                                       var graph: Graph[BDV[Double], Double],
                                       val k: Int,
                                       val vocabSize: Int,
                                       val docConcentration: Double,
                                       val topicConcentration: Double,
                                       checkpointInterval: Int) extends LDAOptimizer{

  private[clustering] type TopicCounts = BDV[Double]

  private[clustering] type TokenCount = Double

  private[LDA] val graphCheckpointer = new PeriodicGraphCheckpointer[TopicCounts, TokenCount](
    graph, checkpointInterval)

  def next(): EMOptimizer = {
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
  var globalTopicTotals: TopicCounts = computeGlobalTopicTotals()

  private def computeGlobalTopicTotals(): TopicCounts = {
    val numTopics = k
    graph.vertices.filter(isTermVertex).values.fold(BDV.zeros[Double](numTopics))(_ += _)
  }

  /** Term vertex IDs are {-1, -2, ..., -vocabSize} */
  private[clustering] def term2index(term: Int): Long = -(1 + term.toLong)

  private[clustering] def index2term(termIndex: Long): Int = -(1 + termIndex).toInt

  private[clustering] def isDocumentVertex(v: (VertexId, _)): Boolean = v._1 >= 0

  private[clustering] def isTermVertex(v: (VertexId, _)): Boolean = v._1 < 0

  /**
   * Compute gamma_{wjk}, a distribution over topics k.
   */
  private def computePTopic(
                             docTopicCounts: TopicCounts,
                             termTopicCounts: TopicCounts,
                             totalTopicCounts: TopicCounts,
                             vocabSize: Int,
                             eta: Double,
                             alpha: Double): TopicCounts = {
    val K = docTopicCounts.length
    val N_j = docTopicCounts.data
    val N_w = termTopicCounts.data
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
                           private val vocabSize: Int,
                           private val alpha: Double,
                           private val eta: Double,
                           private val tau_0: Double,
                           private val kappa: Double) extends Serializable with LDAOptimizer {

  // Initialize the variational distribution q(beta|lambda)
  var lambda = new BDM[Double](k, vocabSize, Array.fill(k * vocabSize)(0.5))
  private var Elogbeta = dirichlet_expectation(lambda)    // K * V
  private var expElogbeta = exp(Elogbeta)                 // K * V
  private var i = 0

  def update(): Unit ={
    Elogbeta = dirichlet_expectation(lambda)
    expElogbeta = exp(Elogbeta)
  }

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
    if(documents.isEmpty()){
      return
    }

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
    // weight of the mini-batch.
    val weight = math.pow(tau_0 + iter, -kappa)

    // This step finishes computing the sufficient statistics for the M step
    val stat = raw :* expElogbeta

    // Update lambda based on documents.
    lambda = lambda * (1 - weight) + (stat * D.toDouble / batchSize.toDouble + eta) * weight
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
    var gammad = new BDV[Double](Array.fill(k)(0.5)).t
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
      gammad = (expElogthetad :* ((ctsVector / phinorm) * (expElogbetad.t))) + alpha
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


//specific questions:
1. use   "numIterations and miniBatchFraction" or randomsplit
2. How would the stream interface fit in?
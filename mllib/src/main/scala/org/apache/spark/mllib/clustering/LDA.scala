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

import breeze.linalg.{DenseVector => BDV, normalize}

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.mllib.clustering.LDAOptimizer
import org.apache.spark.mllib.impl.PeriodicGraphCheckpointer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
 * :: Experimental ::
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
 * @see [[http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation Latent Dirichlet allocation
 *       (Wikipedia)]]
 */
@Experimental
class LDA private (
    private var k: Int,
    private var maxIterations: Int,
    private var docConcentration: Double,
    private var topicConcentration: Double,
    private var seed: Long,
    private var checkpointInterval: Int) extends Logging {

  def this() = this(k = 10, maxIterations = 20, docConcentration = -1, topicConcentration = -1,
    seed = Utils.random.nextLong(), checkpointInterval = 10)

  var ldaOptimizer = setOptimizer("EM")

  def getOptimizer(): LDAOptimizer = {
    ldaOptimizer
  }

  def setOptimizer(optimizer: LDAOptimizer): this.type = {
    this.ldaOptimizer = optimizer
    this
  }

  def setOptimizer(optimizer: String): this.type = {
    optimizer match{
      case "EM" => this.setOptimizer(new EMOptimizer(default parameter))
      case "Gibbs"=> this.setOptimizer(new GibbsOptimizer(default parameter))
      case "Online"=> this.setOptimizer(new OnlineLDAOptimizer(default parameter))
    }
  }


  /**
   * Number of topics to infer.  I.e., the number of soft cluster centers.
   */
  def getK: Int = k

  /**
   * Number of topics to infer.  I.e., the number of soft cluster centers.
   * (default = 10)
   */
  def setK(k: Int): this.type = {
    require(k > 0, s"LDA k (number of clusters) must be > 0, but was set to $k")
    this.k = k
    this
  }

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   */
  def getDocConcentration: Double = {
    if (this.docConcentration == -1) {
      (50.0 / k) + 1.0
    } else {
      this.docConcentration
    }
  }

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * This value should be > 1.0, where larger values mean more smoothing (more regularization).
   * If set to -1, then docConcentration is set automatically.
   *  (default = -1 = automatic)
   *
   * Automatic setting of parameter:
   *  - For EM: default = (50 / k) + 1.
   *     - The 50/k is common in LDA libraries.
   *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *
   * Note: The restriction > 1.0 may be relaxed in the future (allowing sparse solutions),
   *       but values in (0,1) are not yet supported.
   */
  def setDocConcentration(docConcentration: Double): this.type = {
    require(docConcentration > 1.0 || docConcentration == -1.0,
      s"LDA docConcentration must be > 1.0 (or -1 for auto), but was set to $docConcentration")
    this.docConcentration = docConcentration
    this
  }

  /** Alias for [[getDocConcentration]] */
  def getAlpha: Double = getDocConcentration

  /** Alias for [[setDocConcentration()]] */
  def setAlpha(alpha: Double): this.type = setDocConcentration(alpha)

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * Note: The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   */
  def getTopicConcentration: Double = {
    if (this.topicConcentration == -1) {
      1.1
    } else {
      this.topicConcentration
    }
  }

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * Note: The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   *
   * This value should be > 0.0.
   * If set to -1, then topicConcentration is set automatically.
   *  (default = -1 = automatic)
   *
   * Automatic setting of parameter:
   *  - For EM: default = 0.1 + 1.
   *     - The 0.1 gives a small amount of smoothing.
   *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *
   * Note: The restriction > 1.0 may be relaxed in the future (allowing sparse solutions),
   *       but values in (0,1) are not yet supported.
   */
  def setTopicConcentration(topicConcentration: Double): this.type = {
    require(topicConcentration > 1.0 || topicConcentration == -1.0,
      s"LDA topicConcentration must be > 1.0 (or -1 for auto), but was set to $topicConcentration")
    this.topicConcentration = topicConcentration
    this
  }

  /** Alias for [[getTopicConcentration]] */
  def getBeta: Double = getTopicConcentration

  /** Alias for [[setTopicConcentration()]] */
  def setBeta(beta: Double): this.type = setBeta(beta)

  /**
   * Maximum number of iterations for learning.
   */
  def getMaxIterations: Int = maxIterations

  /**
   * Maximum number of iterations for learning.
   * (default = 20)
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /** Random seed */
  def getSeed: Long = seed

  /** Random seed */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
   * Period (in iterations) between checkpoints.
   */
  def getCheckpointInterval: Int = checkpointInterval

  /**
   * Period (in iterations) between checkpoints (default = 10). Checkpointing helps with recovery
   * (when nodes fail). It also helps with eliminating temporary shuffle files on disk, which can be
   * important when LDA is run for many iterations. If the checkpoint directory is not set in
   * [[org.apache.spark.SparkContext]], this setting is ignored.
   *
   * @see [[org.apache.spark.SparkContext#setCheckpointDir]]
   */
  def setCheckpointInterval(checkpointInterval: Int): this.type = {
    this.checkpointInterval = checkpointInterval
    this
  }

  /**
   * Learn an LDA model using the given dataset.
   *
   * @param documents  RDD of documents, which are term (word) count vectors paired with IDs.
   *                   The term count vectors are "bags of words" with a fixed-size vocabulary
   *                   (where the vocabulary size is the length of the vector).
   *                   Document IDs must be unique and >= 0.
   * @return  Inferred LDA model
   */
  def run(documents: RDD[(Long, Vector)]): DistributedLDAModel = {
    if(ldaOptimizer.isInstanceOf[EMOptimizer]){
      val state = LDA.initialState(documents, k, getDocConcentration, getTopicConcentration, seed,
        checkpointInterval)
      var iter = 0
      val iterationTimes = Array.fill[Double](maxIterations)(0)
      while (iter < maxIterations) {
        val start = System.nanoTime()
        state.next()
        val elapsedSeconds = (System.nanoTime() - start) / 1e9
        iterationTimes(iter) = elapsedSeconds
        iter += 1
      }
      state.graphCheckpointer.deleteAllCheckpoints()
      new DistributedLDAModel(state, iterationTimes)
    }
    else if(ldaOptimizer.isInstanceOf[OnlineLDAOptimizer]){
      val vocabSize = documents.first._2.size
      val D = documents.count().toInt // total documents count
      val onlineLDA = new OnlineLDAOptimizer(k, D, vocabSize, 1.0/k, 1.0/k, tau_0, kappa)

      val arr = Array.fill(math.ceil(1.0 / miniBatchFraction).toInt)(miniBatchFraction)
      val splits = documents.randomSplit(arr)
      for(i <- 0 until numIterations){
        val index = i % splits.size
        onlineLDA.submitMiniBatch(splits(index))
      }
      onlineLDA.getTopicDistribution()
    }



  }

  /** Java-friendly version of [[run()]] */
  def run(documents: JavaPairRDD[java.lang.Long, Vector]): DistributedLDAModel = {
    run(documents.rdd.asInstanceOf[RDD[(Long, Vector)]])
  }
}


private[clustering] object LDA {

  /*
    DEVELOPERS NOTE:

    This implementation uses GraphX, where the graph is bipartite with 2 types of vertices:
     - Document vertices
        - indexed with unique indices >= 0
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

    TODO: Add simplex constraints to allow alpha in (0,1).
          See: Vorontsov and Potapenko. "Tutorial on Probabilistic Topic Modeling : Additive
               Regularization for Stochastic Matrix Factorization." 2014.
   */

  /**
   * Vector over topics (length k) of token counts.
   * The meaning of these counts can vary, and it may or may not be normalized to be a distribution.
   */


  /**
   * Compute bipartite term/doc graph.
   */
  private def initialState(
      docs: RDD[(Long, Vector)],
      k: Int,
      docConcentration: Double,
      topicConcentration: Double,
      randomSeed: Long,
      checkpointInterval: Int): EMOptimizer = {
    // For each document, create an edge (Document -> Term) for each unique term in the document.
    val edges: RDD[Edge[TokenCount]] = docs.flatMap { case (docID: Long, termCounts: Vector) =>
      // Add edges for terms with non-zero counts.
      termCounts.toBreeze.activeIterator.filter(_._2 != 0.0).map { case (term, cnt) =>
        Edge(docID, term2index(term), cnt)
      }
    }

    val vocabSize = docs.take(1).head._2.size

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
    val graph = Graph(docTermVertices, edges)
      .partitionBy(PartitionStrategy.EdgePartition1D)

    new EMOptimizer(graph, k, vocabSize, docConcentration, topicConcentration, checkpointInterval)
  }

}


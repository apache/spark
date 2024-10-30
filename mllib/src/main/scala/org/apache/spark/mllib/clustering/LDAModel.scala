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

import breeze.linalg.{argmax, argtopk, normalize, sum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{exp, lgamma}
import org.apache.hadoop.fs.Path
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.{BoundedPriorityQueue, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * Latent Dirichlet Allocation (LDA) model.
 *
 * This abstraction permits for different underlying representations,
 * including local and distributed data structures.
 */
@Since("1.3.0")
abstract class LDAModel private[clustering] extends Saveable {

  /** Number of topics */
  @Since("1.3.0")
  def k: Int

  /** Vocabulary size (number of terms or terms in the vocabulary) */
  @Since("1.3.0")
  def vocabSize: Int

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a Dirichlet distribution.
   */
  @Since("1.5.0")
  def docConcentration: Vector

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * @note The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   */
  @Since("1.5.0")
  def topicConcentration: Double

  /**
  * Shape parameter for random initialization of variational parameter gamma.
  * Used for variational inference for perplexity and other test-time computations.
  */
  protected def gammaShape: Double

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   */
  @Since("1.3.0")
  def topicsMatrix: Matrix

  /**
   * Return the topics described by weighted terms.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   * @return  Array over topics.  Each topic is represented as a pair of matching arrays:
   *          (term indices, term weights in topic).
   *          Each topic's terms are sorted in order of decreasing weight.
   */
  @Since("1.3.0")
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
  @Since("1.3.0")
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
   *                   Document IDs must be unique and greater than or equal to 0.
   * @return  Estimated topic distribution for each document.
   *          The returned RDD may be zipped with the given RDD, where each returned vector
   *          is a multinomial distribution over topics.
   */
  // def topicDistributions(documents: RDD[(Long, Vector)]): RDD[(Long, Vector)]

}

/**
 * Local LDA model.
 * This model stores only the inferred topics.
 *
 * @param topics Inferred topics (vocabSize x k matrix).
 */
@Since("1.3.0")
class LocalLDAModel private[spark] (
    @Since("1.3.0") val topics: Matrix,
    @Since("1.5.0") override val docConcentration: Vector,
    @Since("1.5.0") override val topicConcentration: Double,
    override protected[spark] val gammaShape: Double = 100)
  extends LDAModel with Serializable {

  private[spark] var seed: Long = Utils.random.nextLong()

  @Since("1.3.0")
  override def k: Int = topics.numCols

  @Since("1.3.0")
  override def vocabSize: Int = topics.numRows

  @Since("1.3.0")
  override def topicsMatrix: Matrix = topics

  @Since("1.3.0")
  override def describeTopics(maxTermsPerTopic: Int): Array[(Array[Int], Array[Double])] = {
    val brzTopics = topics.asBreeze.toDenseMatrix
    Range(0, k).map { topicIndex =>
      val topic = normalize(brzTopics(::, topicIndex), 1.0)
      val (termWeights, terms) =
        topic.toArray.zipWithIndex.sortBy(-_._1).take(maxTermsPerTopic).unzip
      (terms, termWeights)
    }.toArray
  }

  /**
   * Random seed for cluster initialization.
   */
  @Since("2.4.0")
  def getSeed: Long = seed

  /**
   * Set the random seed for cluster initialization.
   */
  @Since("2.4.0")
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  @Since("1.5.0")
  override def save(sc: SparkContext, path: String): Unit = {
    LocalLDAModel.SaveLoadV1_0.save(sc, path, topicsMatrix, docConcentration, topicConcentration,
      gammaShape)
  }

  // TODO: declare in LDAModel and override once implemented in DistributedLDAModel
  /**
   * Calculates a lower bound on the log likelihood of the entire corpus.
   *
   * See Equation (16) in original Online LDA paper.
   *
   * @param documents test corpus to use for calculating log likelihood
   * @return variational lower bound on the log likelihood of the entire corpus
   */
  @Since("1.5.0")
  def logLikelihood(documents: RDD[(Long, Vector)]): Double = logLikelihoodBound(documents,
    docConcentration, topicConcentration, topicsMatrix.asBreeze.toDenseMatrix, gammaShape, k,
    vocabSize)

  /**
   * Java-friendly version of `logLikelihood`
   */
  @Since("1.5.0")
  def logLikelihood(documents: JavaPairRDD[java.lang.Long, Vector]): Double = {
    logLikelihood(documents.rdd.asInstanceOf[RDD[(Long, Vector)]])
  }

  /**
   * Calculate an upper bound on perplexity.  (Lower is better.)
   * See Equation (16) in original Online LDA paper.
   *
   * @param documents test corpus to use for calculating perplexity
   * @return Variational upper bound on log perplexity per token.
   */
  @Since("1.5.0")
  def logPerplexity(documents: RDD[(Long, Vector)]): Double = {
    val corpusTokenCount = documents
      .map { case (_, termCounts) => termCounts.toArray.sum }
      .sum()
    -logLikelihood(documents) / corpusTokenCount
  }

  /**
   * Java-friendly version of `logPerplexity`
   */
  @Since("1.5.0")
  def logPerplexity(documents: JavaPairRDD[java.lang.Long, Vector]): Double = {
    logPerplexity(documents.rdd.asInstanceOf[RDD[(Long, Vector)]])
  }

  /**
   * Estimate the variational likelihood bound of from `documents`:
   *    log p(documents) >= E_q[log p(documents)] - E_q[log q(documents)]
   * This bound is derived by decomposing the LDA model to:
   *    log p(documents) = E_q[log p(documents)] - E_q[log q(documents)] + D(q|p)
   * and noting that the KL-divergence D(q|p) >= 0.
   *
   * See Equation (16) in original Online LDA paper, as well as Appendix A.3 in the JMLR version of
   * the original LDA paper.
   * @param documents a subset of the test corpus
   * @param alpha document-topic Dirichlet prior parameters
   * @param eta topic-word Dirichlet prior parameter
   * @param lambda parameters for variational q(beta | lambda) topic-word distributions
   * @param gammaShape shape parameter for random initialization of variational q(theta | gamma)
   *                   topic mixture distributions
   * @param k number of topics
   * @param vocabSize number of unique terms in the entire test corpus
   */
  private def logLikelihoodBound(
      documents: RDD[(Long, Vector)],
      alpha: Vector,
      eta: Double,
      lambda: BDM[Double],
      gammaShape: Double,
      k: Int,
      vocabSize: Long): Double = {
    val brzAlpha = alpha.asBreeze.toDenseVector
    // transpose because dirichletExpectation normalizes by row and we need to normalize
    // by topic (columns of lambda)
    val Elogbeta = LDAUtils.dirichletExpectation(lambda.t).t
    val ElogbetaBc = documents.sparkContext.broadcast(Elogbeta)
    val gammaSeed = this.seed

    // Sum bound components for each document:
    //  component for prob(tokens) + component for prob(document-topic distribution)
    val corpusPart =
      documents.filter(_._2.numNonzeros > 0).map { case (id: Long, termCounts: Vector) =>
        val localElogbeta = ElogbetaBc.value
        var docBound = 0.0D
        val (gammad: BDV[Double], _, _) = OnlineLDAOptimizer.variationalTopicInference(
          termCounts, exp(localElogbeta), brzAlpha, gammaShape, k, gammaSeed + id)
        val Elogthetad: BDV[Double] = LDAUtils.dirichletExpectation(gammad)

        // E[log p(doc | theta, beta)]
        termCounts.foreachNonZero { case (idx, count) =>
          docBound += count * LDAUtils.logSumExp(Elogthetad + localElogbeta(idx, ::).t)
        }
        // E[log p(theta | alpha) - log q(theta | gamma)]
        docBound += sum((brzAlpha - gammad) *:* Elogthetad)
        docBound += sum(lgamma(gammad) - lgamma(brzAlpha))
        docBound += lgamma(sum(brzAlpha)) - lgamma(sum(gammad))

        docBound
      }.sum()
    ElogbetaBc.destroy()

    // Bound component for prob(topic-term distributions):
    //   E[log p(beta | eta) - log q(beta | lambda)]
    val sumEta = eta * vocabSize
    val topicsPart = sum((eta - lambda) *:* Elogbeta) +
      sum(lgamma(lambda) - lgamma(eta)) +
      sum(lgamma(sumEta) - lgamma(sum(lambda(::, breeze.linalg.*))))

    corpusPart + topicsPart
  }

  /**
   * Predicts the topic mixture distribution for each document (often called "theta" in the
   * literature).  Returns a vector of zeros for an empty document.
   *
   * This uses a variational approximation following Hoffman et al. (2010), where the approximate
   * distribution is called "gamma."  Technically, this method returns this approximation "gamma"
   * for each document.
   * @param documents documents to predict topic mixture distributions for
   * @return An RDD of (document ID, topic mixture distribution for document)
   */
  @Since("1.3.0")
  // TODO: declare in LDAModel and override once implemented in DistributedLDAModel
  def topicDistributions(documents: RDD[(Long, Vector)]): RDD[(Long, Vector)] = {
    // Double transpose because dirichletExpectation normalizes by row and we need to normalize
    // by topic (columns of lambda)
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.asBreeze.toDenseMatrix.t).t)
    val expElogbetaBc = documents.sparkContext.broadcast(expElogbeta)
    val docConcentrationBrz = this.docConcentration.asBreeze
    val gammaShape = this.gammaShape
    val k = this.k
    val gammaSeed = this.seed

    documents.map { case (id: Long, termCounts: Vector) =>
      if (termCounts.numNonzeros == 0) {
        (id, Vectors.zeros(k))
      } else {
        val (gamma, _, _) = OnlineLDAOptimizer.variationalTopicInference(
          termCounts,
          expElogbetaBc.value,
          docConcentrationBrz,
          gammaShape,
          k,
          gammaSeed + id)
        (id, Vectors.dense(normalize(gamma, 1.0).toArray))
      }
    }
  }

  /**
   * Predicts the topic mixture distribution for a document (often called "theta" in the
   * literature).  Returns a vector of zeros for an empty document.
   *
   * Note this means to allow quick query for single document. For batch documents, please refer
   * to `topicDistributions()` to avoid overhead.
   *
   * @param document document to predict topic mixture distributions for
   * @return topic mixture distribution for the document
   */
  @Since("2.0.0")
  def topicDistribution(document: Vector): Vector = {
    val gammaSeed = this.seed
    val expElogbeta = exp(LDAUtils.dirichletExpectation(topicsMatrix.asBreeze.toDenseMatrix.t).t)
    if (document.numNonzeros == 0) {
      Vectors.zeros(this.k)
    } else {
      val (gamma, _, _) = OnlineLDAOptimizer.variationalTopicInference(
        document,
        expElogbeta,
        this.docConcentration.asBreeze,
        gammaShape,
        this.k,
        gammaSeed)
      Vectors.dense(normalize(gamma, 1.0).toArray)
    }
  }

  /**
   * Java-friendly version of `topicDistributions`
   */
  @Since("1.4.1")
  def topicDistributions(
      documents: JavaPairRDD[java.lang.Long, Vector]): JavaPairRDD[java.lang.Long, Vector] = {
    val distributions = topicDistributions(documents.rdd.asInstanceOf[RDD[(Long, Vector)]])
    JavaPairRDD.fromRDD(distributions.asInstanceOf[RDD[(java.lang.Long, Vector)]])
  }

}

/**
 * Local (non-distributed) model fitted by [[LDA]].
 *
 * This model stores the inferred topics only; it does not store info about the training dataset.
 */
@Since("1.5.0")
object LocalLDAModel extends Loader[LocalLDAModel] {

  private object SaveLoadV1_0 {

    val thisFormatVersion = "1.0"

    val thisClassName = "org.apache.spark.mllib.clustering.LocalLDAModel"

    // Store the distribution of terms of each topic and the column index in topicsMatrix
    // as a Row in data.
    case class Data(topic: Vector, index: Int)

    def save(
        sc: SparkContext,
        path: String,
        topicsMatrix: Matrix,
        docConcentration: Vector,
        topicConcentration: Double,
        gammaShape: Double): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val k = topicsMatrix.numCols
      val metadata = compact(render
        (("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("k" -> k) ~ ("vocabSize" -> topicsMatrix.numRows) ~
          ("docConcentration" -> docConcentration.toArray.toImmutableArraySeq) ~
          ("topicConcentration" -> topicConcentration) ~
          ("gammaShape" -> gammaShape)))
      spark.createDataFrame(Seq(Tuple1(metadata))).write.text(Loader.metadataPath(path))

      val topicsDenseMatrix = topicsMatrix.asBreeze.toDenseMatrix
      val topics = Range(0, k).map { topicInd =>
        Data(Vectors.dense((topicsDenseMatrix(::, topicInd).toArray)), topicInd)
      }
      spark.createDataFrame(topics).repartition(1).write.parquet(Loader.dataPath(path))
    }

    def load(
        sc: SparkContext,
        path: String,
        docConcentration: Vector,
        topicConcentration: Double,
        gammaShape: Double): LocalLDAModel = {
      val dataPath = Loader.dataPath(path)
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val dataFrame = spark.read.parquet(dataPath)

      Loader.checkSchema[Data](dataFrame.schema)
      val topics = dataFrame.collect()
      val vocabSize = topics(0).getAs[Vector](0).size
      val k = topics.length

      val brzTopics = BDM.zeros[Double](vocabSize, k)
      topics.foreach { case Row(vec: Vector, ind: Int) =>
        brzTopics(::, ind) := vec.asBreeze
      }
      val topicsMat = Matrices.fromBreeze(brzTopics)

      new LocalLDAModel(topicsMat, docConcentration, topicConcentration, gammaShape)
    }
  }

  @Since("1.5.0")
  override def load(sc: SparkContext, path: String): LocalLDAModel = {
    val (loadedClassName, loadedVersion, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats: Formats = DefaultFormats
    val expectedK = (metadata \ "k").extract[Int]
    val expectedVocabSize = (metadata \ "vocabSize").extract[Int]
    val docConcentration =
      Vectors.dense((metadata \ "docConcentration").extract[Seq[Double]].toArray)
    val topicConcentration = (metadata \ "topicConcentration").extract[Double]
    val gammaShape = (metadata \ "gammaShape").extract[Double]
    val classNameV1_0 = SaveLoadV1_0.thisClassName

    val model = (loadedClassName, loadedVersion) match {
      case (className, "1.0") if className == classNameV1_0 =>
        SaveLoadV1_0.load(sc, path, docConcentration, topicConcentration, gammaShape)
      case _ => throw new Exception(
        s"LocalLDAModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $loadedVersion).  Supported:\n" +
          s"  ($classNameV1_0, 1.0)")
    }

    val topicsMatrix = model.topicsMatrix
    require(expectedK == topicsMatrix.numCols,
      s"LocalLDAModel requires $expectedK topics, got ${topicsMatrix.numCols} topics")
    require(expectedVocabSize == topicsMatrix.numRows,
      s"LocalLDAModel requires $expectedVocabSize terms for each topic, " +
        s"but got ${topicsMatrix.numRows}")
    model
  }
}

/**
 * Distributed LDA model.
 * This model stores the inferred topics, the full training dataset, and the topic distributions.
 */
@Since("1.3.0")
class DistributedLDAModel private[clustering] (
    private[clustering] val graph: Graph[LDA.TopicCounts, LDA.TokenCount],
    private[clustering] val globalTopicTotals: LDA.TopicCounts,
    @Since("1.3.0") val k: Int,
    @Since("1.3.0") val vocabSize: Int,
    @Since("1.5.0") override val docConcentration: Vector,
    @Since("1.5.0") override val topicConcentration: Double,
    private[spark] val iterationTimes: Array[Double],
    override protected[clustering] val gammaShape: Double = DistributedLDAModel.defaultGammaShape,
    private[spark] val checkpointFiles: Array[String] = Array.empty[String])
  extends LDAModel {

  import LDA._

  /**
   * Convert model to a local model.
   * The local model stores the inferred topics but not the topic distributions for training
   * documents.
   */
  @Since("1.3.0")
  def toLocal: LocalLDAModel = new LocalLDAModel(topicsMatrix, docConcentration, topicConcentration,
    gammaShape)

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   *
   * WARNING: This matrix is collected from an RDD. Beware memory usage when vocabSize, k are large.
   */
  @Since("1.3.0")
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

  @Since("1.3.0")
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
      (terms, termWeights)
    }
  }

  /**
   * Return the top documents for each topic
   *
   * @param maxDocumentsPerTopic  Maximum number of documents to collect for each topic.
   * @return  Array over topics.  Each element represent as a pair of matching arrays:
   *          (IDs for the documents, weights of the topic in these documents).
   *          For each topic, documents are sorted in order of decreasing topic weights.
   */
  @Since("1.5.0")
  def topDocumentsPerTopic(maxDocumentsPerTopic: Int): Array[(Array[Long], Array[Double])] = {
    val numTopics = k
    val topicsInQueues: Array[BoundedPriorityQueue[(Double, Long)]] =
      topicDistributions.mapPartitions { docVertices =>
        // For this partition, collect the most common docs for each topic in queues:
        //  queues(topic) = queue of (doc topic, doc ID).
        val queues =
          Array.fill(numTopics)(new BoundedPriorityQueue[(Double, Long)](maxDocumentsPerTopic))
        for ((docId, docTopics) <- docVertices) {
          var topic = 0
          while (topic < numTopics) {
            queues(topic) += (docTopics(topic) -> docId)
            topic += 1
          }
        }
        Iterator(queues)
      }.treeReduce { (q1, q2) =>
        q1.zip(q2).foreach { case (a, b) => a ++= b }
        q1
      }
    topicsInQueues.map { q =>
      val (docTopics, docs) = q.toArray.sortBy(-_._1).unzip
      (docs, docTopics)
    }
  }

  /**
   * Return the top topic for each (doc, term) pair.  I.e., for each document, what is the most
   * likely topic generating each term?
   *
   * @return RDD of (doc ID, assignment of top topic index for each term),
   *         where the assignment is specified via a pair of zippable arrays
   *         (term indices, topic indices).  Note that terms will be omitted if not present in
   *         the document.
   */
  @Since("1.5.0")
  lazy val topicAssignments: RDD[(Long, Array[Int], Array[Int])] = {
    // For reference, compare the below code with the core part of EMLDAOptimizer.next().
    val eta = topicConcentration
    val W = vocabSize
    val alpha = docConcentration(0)
    val N_k = globalTopicTotals
    val sendMsg: EdgeContext[TopicCounts, TokenCount, (Array[Int], Array[Int])] => Unit =
      (edgeContext) => {
        // E-STEP: Compute gamma_{wjk} (smoothed topic distributions).
        val scaledTopicDistribution: TopicCounts =
          computePTopic(edgeContext.srcAttr, edgeContext.dstAttr, N_k, W, eta, alpha)
        // For this (doc j, term w), send top topic k to doc vertex.
        val topTopic: Int = argmax(scaledTopicDistribution)
        val term: Int = index2term(edgeContext.dstId)
        edgeContext.sendToSrc((Array(term), Array(topTopic)))
      }
    val mergeMsg: ((Array[Int], Array[Int]), (Array[Int], Array[Int])) => (Array[Int], Array[Int]) =
      (terms_topics0, terms_topics1) => {
        (terms_topics0._1 ++ terms_topics1._1, terms_topics0._2 ++ terms_topics1._2)
      }
    // M-STEP: Aggregation computes new N_{kj}, N_{wk} counts.
    val perDocAssignments =
      graph.aggregateMessages[(Array[Int], Array[Int])](sendMsg, mergeMsg).filter(isDocumentVertex)
    perDocAssignments.map { case (docID: Long, (terms: Array[Int], topics: Array[Int])) =>
      // TODO: Avoid zip, which is inefficient.
      val (sortedTerms, sortedTopics) = terms.zip(topics).sortBy(_._1).unzip
      (docID, sortedTerms, sortedTopics)
    }
  }

  /** Java-friendly version of [[topicAssignments]] */
  @Since("1.5.0")
  lazy val javaTopicAssignments: JavaRDD[(java.lang.Long, Array[Int], Array[Int])] = {
    topicAssignments.asInstanceOf[RDD[(java.lang.Long, Array[Int], Array[Int])]].toJavaRDD()
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
  @Since("1.3.0")
  lazy val logLikelihood: Double = {
    // TODO: generalize this for asymmetric (non-scalar) alpha
    val alpha = this.docConcentration(0) // To avoid closure capture of enclosing object
    val eta = this.topicConcentration
    assert(eta > 1.0)
    assert(alpha > 1.0)
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    // Edges: Compute token log probability from phi_{wk}, theta_{kj}.
    val sendMsg: EdgeContext[TopicCounts, TokenCount, Double] => Unit = (edgeContext) => {
      val N_wj = edgeContext.attr
      val smoothed_N_wk: TopicCounts = edgeContext.dstAttr + (eta - 1.0)
      val smoothed_N_kj: TopicCounts = edgeContext.srcAttr + (alpha - 1.0)
      val phi_wk: TopicCounts = smoothed_N_wk /:/ smoothed_N_k
      val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
      val tokenLogLikelihood = N_wj * math.log(phi_wk.dot(theta_kj))
      edgeContext.sendToDst(tokenLogLikelihood)
    }
    graph.aggregateMessages[Double](sendMsg, _ + _)
      .map(_._2).fold(0.0)(_ + _)
  }

  /**
   * Log probability of the current parameter estimate:
   * log P(topics, topic distributions for docs | alpha, eta)
   */
  @Since("1.3.0")
  lazy val logPrior: Double = {
    // TODO: generalize this for asymmetric (non-scalar) alpha
    val alpha = this.docConcentration(0) // To avoid closure capture of enclosing object
    val eta = this.topicConcentration
    // Term vertices: Compute phi_{wk}.  Use to compute prior log probability.
    // Doc vertex: Compute theta_{kj}.  Use to compute prior log probability.
    val N_k = globalTopicTotals
    val smoothed_N_k: TopicCounts = N_k + (vocabSize * (eta - 1.0))
    val seqOp: (Double, (VertexId, TopicCounts)) => Double = {
      case (sumPrior: Double, vertex: (VertexId, TopicCounts)) =>
        if (isTermVertex(vertex)) {
          val N_wk = vertex._2
          val smoothed_N_wk: TopicCounts = N_wk + (eta - 1.0)
          val phi_wk: TopicCounts = smoothed_N_wk /:/ smoothed_N_k
          sumPrior + (eta - 1.0) * sum(phi_wk.map(math.log))
        } else {
          val N_kj = vertex._2
          val smoothed_N_kj: TopicCounts = N_kj + (alpha - 1.0)
          val theta_kj: TopicCounts = normalize(smoothed_N_kj, 1.0)
          sumPrior + (alpha - 1.0) * sum(theta_kj.map(math.log))
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
  @Since("1.3.0")
  def topicDistributions: RDD[(Long, Vector)] = {
    graph.vertices.filter(LDA.isDocumentVertex).map { case (docID, topicCounts) =>
      (docID, Vectors.fromBreeze(normalize(topicCounts, 1.0)))
    }
  }

  /**
   * Java-friendly version of [[topicDistributions]]
   */
  @Since("1.4.1")
  def javaTopicDistributions: JavaPairRDD[java.lang.Long, Vector] = {
    JavaPairRDD.fromRDD(topicDistributions.asInstanceOf[RDD[(java.lang.Long, Vector)]])
  }

  /**
   * For each document, return the top k weighted topics for that document and their weights.
   * @return RDD of (doc ID, topic indices, topic weights)
   */
  @Since("1.5.0")
  def topTopicsPerDocument(k: Int): RDD[(Long, Array[Int], Array[Double])] = {
    graph.vertices.filter(LDA.isDocumentVertex).map { case (docID, topicCounts) =>
      val topIndices = argtopk(topicCounts, k)
      val sumCounts = sum(topicCounts)
      val weights = if (sumCounts != 0) {
        topicCounts(topIndices).toArray.map(_ / sumCounts)
      } else {
        topicCounts(topIndices).toArray
      }
      (docID, topIndices.toArray, weights)
    }
  }

  /**
   * Java-friendly version of [[topTopicsPerDocument]]
   */
  @Since("1.5.0")
  def javaTopTopicsPerDocument(k: Int): JavaRDD[(java.lang.Long, Array[Int], Array[Double])] = {
    val topics = topTopicsPerDocument(k)
    topics.asInstanceOf[RDD[(java.lang.Long, Array[Int], Array[Double])]].toJavaRDD()
  }

  // TODO:
  // override def topicDistributions(documents: RDD[(Long, Vector)]): RDD[(Long, Vector)] = ???

  @Since("1.5.0")
  override def save(sc: SparkContext, path: String): Unit = {
    // Note: This intentionally does not save checkpointFiles.
    DistributedLDAModel.SaveLoadV1_0.save(
      sc, path, graph, globalTopicTotals, k, vocabSize, docConcentration, topicConcentration,
      iterationTimes, gammaShape)
  }
}

/**
 * Distributed model fitted by [[LDA]].
 * This type of model is currently only produced by Expectation-Maximization (EM).
 *
 * This model stores the inferred topics, the full training dataset, and the topic distribution
 * for each training document.
 */
@Since("1.5.0")
object DistributedLDAModel extends Loader[DistributedLDAModel] {

  /**
   * The [[DistributedLDAModel]] constructor's default arguments assume gammaShape = 100
   * to ensure equivalence in LDAModel.toLocal conversion.
   */
  private[clustering] val defaultGammaShape: Double = 100

  private object SaveLoadV1_0 {

    val thisFormatVersion = "1.0"

    val thisClassName = "org.apache.spark.mllib.clustering.DistributedLDAModel"

    // Store globalTopicTotals as a Vector.
    case class Data(globalTopicTotals: Vector)

    // Store each term and document vertex with an id and the topicWeights.
    case class VertexData(id: Long, topicWeights: Vector)

    // Store each edge with the source id, destination id and tokenCounts.
    case class EdgeData(srcId: Long, dstId: Long, tokenCounts: Double)

    def save(
        sc: SparkContext,
        path: String,
        graph: Graph[LDA.TopicCounts, LDA.TokenCount],
        globalTopicTotals: LDA.TopicCounts,
        k: Int,
        vocabSize: Int,
        docConcentration: Vector,
        topicConcentration: Double,
        iterationTimes: Array[Double],
        gammaShape: Double): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      val metadata = compact(render
        (("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
          ("k" -> k) ~ ("vocabSize" -> vocabSize) ~
          ("docConcentration" -> docConcentration.toArray.toImmutableArraySeq) ~
          ("topicConcentration" -> topicConcentration) ~
          ("iterationTimes" -> iterationTimes.toImmutableArraySeq) ~
          ("gammaShape" -> gammaShape)))
      spark.createDataFrame(Seq(Tuple1(metadata))).write.text(Loader.metadataPath(path))

      val newPath = new Path(Loader.dataPath(path), "globalTopicTotals").toUri.toString
      spark.createDataFrame(Seq(Data(Vectors.fromBreeze(globalTopicTotals)))).write.parquet(newPath)

      val verticesPath = new Path(Loader.dataPath(path), "topicCounts").toUri.toString
      spark.createDataFrame(graph.vertices.map { case (ind, vertex) =>
        VertexData(ind, Vectors.fromBreeze(vertex))
      }).write.parquet(verticesPath)

      val edgesPath = new Path(Loader.dataPath(path), "tokenCounts").toUri.toString
      spark.createDataFrame(graph.edges.map { case Edge(srcId, dstId, prop) =>
        EdgeData(srcId, dstId, prop)
      }).write.parquet(edgesPath)
    }

    def load(
        sc: SparkContext,
        path: String,
        vocabSize: Int,
        docConcentration: Vector,
        topicConcentration: Double,
        iterationTimes: Array[Double],
        gammaShape: Double): DistributedLDAModel = {
      val dataPath = new Path(Loader.dataPath(path), "globalTopicTotals").toUri.toString
      val vertexDataPath = new Path(Loader.dataPath(path), "topicCounts").toUri.toString
      val edgeDataPath = new Path(Loader.dataPath(path), "tokenCounts").toUri.toString
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val dataFrame = spark.read.parquet(dataPath)
      val vertexDataFrame = spark.read.parquet(vertexDataPath)
      val edgeDataFrame = spark.read.parquet(edgeDataPath)

      Loader.checkSchema[Data](dataFrame.schema)
      Loader.checkSchema[VertexData](vertexDataFrame.schema)
      Loader.checkSchema[EdgeData](edgeDataFrame.schema)
      val globalTopicTotals: LDA.TopicCounts =
        dataFrame.first().getAs[Vector](0).asBreeze.toDenseVector
      val vertices: RDD[(VertexId, LDA.TopicCounts)] = vertexDataFrame.rdd.map {
        case Row(ind: Long, vec: Vector) => (ind, vec.asBreeze.toDenseVector)
      }

      val edges: RDD[Edge[LDA.TokenCount]] = edgeDataFrame.rdd.map {
        case Row(srcId: Long, dstId: Long, prop: Double) => Edge(srcId, dstId, prop)
      }
      val graph: Graph[LDA.TopicCounts, LDA.TokenCount] = Graph(vertices, edges)

      new DistributedLDAModel(graph, globalTopicTotals, globalTopicTotals.length, vocabSize,
        docConcentration, topicConcentration, iterationTimes, gammaShape)
    }

  }

  @Since("1.5.0")
  override def load(sc: SparkContext, path: String): DistributedLDAModel = {
    val (loadedClassName, loadedVersion, metadata) = Loader.loadMetadata(sc, path)
    implicit val formats: Formats = DefaultFormats
    val expectedK = (metadata \ "k").extract[Int]
    val vocabSize = (metadata \ "vocabSize").extract[Int]
    val docConcentration =
      Vectors.dense((metadata \ "docConcentration").extract[Seq[Double]].toArray)
    val topicConcentration = (metadata \ "topicConcentration").extract[Double]
    val iterationTimes = (metadata \ "iterationTimes").extract[Seq[Double]]
    val gammaShape = (metadata \ "gammaShape").extract[Double]
    val classNameV1_0 = SaveLoadV1_0.thisClassName

    val model = (loadedClassName, loadedVersion) match {
      case (className, "1.0") if className == classNameV1_0 =>
        DistributedLDAModel.SaveLoadV1_0.load(sc, path, vocabSize, docConcentration,
          topicConcentration, iterationTimes.toArray, gammaShape)
      case _ => throw new Exception(
        s"DistributedLDAModel.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $loadedVersion).  Supported: ($classNameV1_0, 1.0)")
    }

    require(model.vocabSize == vocabSize,
      s"DistributedLDAModel requires $vocabSize vocabSize, got ${model.vocabSize} vocabSize")
    require(model.docConcentration == docConcentration,
      s"DistributedLDAModel requires $docConcentration docConcentration, " +
        s"got ${model.docConcentration} docConcentration")
    require(model.topicConcentration == topicConcentration,
      s"DistributedLDAModel requires $topicConcentration docConcentration, " +
        s"got ${model.topicConcentration} docConcentration")
    require(expectedK == model.k,
      s"DistributedLDAModel requires $expectedK topics, got ${model.k} topics")
    model
  }

}


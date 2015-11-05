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

package org.apache.spark.ml.clustering

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.shared.{HasCheckpointInterval, HasFeaturesCol, HasSeed, HasMaxIter}
import org.apache.spark.ml.param._
import org.apache.spark.mllib.clustering.{DistributedLDAModel => OldDistributedLDAModel,
    EMLDAOptimizer => OldEMLDAOptimizer, LDA => OldLDA, LDAModel => OldLDAModel,
    LDAOptimizer => OldLDAOptimizer, LocalLDAModel => OldLocalLDAModel,
    OnlineLDAOptimizer => OldOnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Vectors, Matrix, Vector}
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions.monotonicallyIncreasingId
import org.apache.spark.sql.types.StructType


// TODO: Add since tags everywhere, and fix the ones which say 1.6.0

private[clustering] trait LDAParams extends Params with HasFeaturesCol with HasMaxIter
  with HasSeed with HasCheckpointInterval {

  /**
   * Param for the number of topics (clusters). Must be > 1. Default: 10.
   * @group param
   */
  @Since("1.6.0")
  final val k = new IntParam(this, "k", "number of clusters to create", ParamValidators.gt(1))

  setDefault(k -> 10)

  /** @group getParam */
  @Since("1.6.0")
  def getK: Int = $(k)

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a Dirichlet distribution, where larger values mean more smoothing
   * (more regularization).
   *
   * If set to a singleton vector [-1], then docConcentration is set automatically. If set to
   * singleton vector [alpha] where alpha != -1, then alpha is replicated to a vector of
   * length k in fitting. Otherwise, the [[docConcentration]] vector must be length k.
   * (default = [-1] = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Currently only supports symmetric distributions, so all values in the vector should be
   *       the same.
   *     - Values should be > 1.0
   *     - default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows
   *       from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Values should be >= 0
   *     - default = uniformly (1.0 / k), following the implementation from
   *       [[https://github.com/Blei-Lab/onlineldavb]].
   * @group param
   */
  @Since("1.6.0")
  final val docConcentration = new DoubleArrayParam(this, "docConcentration",
    "Concentration parameter (commonly named \"alpha\") for the prior placed on documents'" +
      " distributions over topics (\"theta\").", validDocConcentration)

  setDefault(docConcentration -> Array(-1.0))

  /** Check that the docConcentration is valid, independently of other Params */
  private def validDocConcentration(alpha: Array[Double]): Boolean = {
    if (alpha.length == 1) {
      alpha(0) == -1 || alpha(0) >= 1.0
    } else if (alpha.length > 1) {
      alpha.forall(_ >= 1.0)
    } else {
      false
    }
  }

  /** @group getParam */
  @Since("1.6.0")
  def getDocConcentration: Array[Double] = $(docConcentration)

  /**
   * Alias for [[getDocConcentration]]
   * @group getParam
   */
  def getAlpha: Array[Double] = getDocConcentration

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * Note: The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   *
   * If set to -1, then topicConcentration is set automatically.
   *  (default = -1 = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Value should be > 1.0
   *     - default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows
   *       Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Value should be >= 0
   *     - default = (1.0 / k), following the implementation from
   *       [[https://github.com/Blei-Lab/onlineldavb]].
   * @group param
   */
  @Since("1.6.0")
  final val topicConcentration = new DoubleParam(this, "topicConcentration",
    "Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topic'" +
      " distributions over terms.", (beta: Double) => beta == -1 || beta >= 0.0)

  setDefault(topicConcentration -> -1.0)

  /** @group getParam */
  @Since("1.6.0")
  def getTopicConcentration: Double = $(topicConcentration)

  /**
   * Alias for [[getTopicConcentration]]
   * @group getParam
   */
  def getBeta: Double = getTopicConcentration

  /**
   * Optimizer or inference algorithm used to estimate the LDA model, specified as a
   * [[LDAOptimizer]] type.
   * Currently supported:
   *  - Online Variational Bayes: [[OnlineLDAOptimizer]] (default)
   *  - Expectation-Maximization (EM): [[EMLDAOptimizer]]
   * @group param
   */
  final val optimizer = new Param[LDAOptimizer](this, "optimizer", "Optimizer or inference" +
    " algorithm used to estimate the LDA model")

  setDefault(optimizer -> new OnlineLDAOptimizer)

  /** @group getParam */
  def getOptimizer: LDAOptimizer = $(optimizer)

  // Developers should override these setOptimizer() methods.  These are defined here to
  // ensure identical behavior when setting the optimizer using a String.
  /** @group setParam */
  def setOptimizer(value: LDAOptimizer): this.type = set(optimizer, value)

  /**
   * Set [[optimizer]] by name (case-insensitive):
   *  - "online" = [[OnlineLDAOptimizer]]
   *  - "em" = [[EMLDAOptimizer]]
   * @group setParam
   */
  def setOptimizer(optimizer: String): this.type = optimizer.toLowerCase match {
    case "online" => setOptimizer(new OnlineLDAOptimizer)
    case "em" => setOptimizer(new EMLDAOptimizer)
    case _ => throw new IllegalArgumentException(
      s"LDA was given unknown optimizer \"$optimizer\".  Supported values: em, online")
  }

  /**
   * Output column with estimates of the topic mixture distribution for each document (often called
   * "theta" in the literature).  Returns a vector of zeros for an empty document.
   *
   * This uses a variational approximation following Hoffman et al. (2010), where the approximate
   * distribution is called "gamma."  Technically, this method returns this approximation "gamma"
   * for each document.
   */
  final val topicDistributionCol = new Param[String](this, "topicDistribution", "Output column" +
    " with estimates of the topic mixture distribution for each document (often called \"theta\"" +
    " in the literature).  Returns a vector of zeros for an empty document.")

  setDefault(topicDistributionCol -> "topicDistribution")

  def getTopicDistributionCol: String = $(topicDistributionCol)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = ???
}


/**
 * :: Experimental ::
 * Model fitted by LDA.
 *
 * TODO
 * @param vocabSize  Vocabulary size (number of terms or terms in the vocabulary)
 * @param oldLocalModel  Underlying spark.mllib model.  Only used if this is not a
 *                       [[DistributedLDAModel]].
 */
@Since("1.6.0")
@Experimental
class LDAModel private[ml] (
    @Since("1.6.0") override val uid: String,
    @Since("1.6.0") val vocabSize: Int,
    private val oldLocalModel: Option[OldLocalLDAModel],
    protected val sqlContext: SQLContext) extends Model[LDAModel] with LDAParams {

  protected def getModel: OldLDAModel = oldLocalModel match {
    case Some(m) => m
    case None =>
      // Should never happen.
      throw new RuntimeException("LocalLDAModel.estimatedDocConcentration was called," +
        " but the underlying model is missing.")
  }

  // TODO: ADD PARAM SETTERS

  @Since("1.6.0")
  override def copy(extra: ParamMap): LDAModel = ???

  @Since("1.6.0")
  override def transform(dataset: DataFrame): DataFrame = ???

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  /**
   * Value for [[docConcentration]] estimated from data.
   * If [[estimatedDocConcentration]] was set to false, then this returns the fixed (given) value
   * for the [[docConcentration]] parameter.
   */
  def estimatedDocConcentration: Vector = getModel.docConcentration

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   *
   * WARNING: If this model is actually a [[DistributedLDAModel]] instance, then this method
   *          could involve collecting a large amount of data to the driver (vocabSize x k).
   */
  @Since("1.6.0")
  def topicsMatrix: Matrix = getModel.topicsMatrix

  /** Indicates whether this instance is of type [[DistributedLDAModel]] */
  def isDistributed: Boolean = false

  def logLikelihood(data: DataFrame): Double = ???

  def logPerplexity(data: DataFrame): Double = ???

  /**
   * Return the topics described by weighted terms, returned as lists sorted in order of decreasing
   * term importance.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   *                          Default value of 10.
   * @return  Local DataFrame with one topic per Row, with columns:
   *           - "topic": IntegerType: topic index
   *           - "termIndices": ArrayType(IntegerType): sorted term indices
   *           - "termWeights": ArrayType(DoubleType): corresponding sorted term weights
   */
  @Since("1.6.0")
  def describeTopics(maxTermsPerTopic: Int): DataFrame = {
    val topics = getModel.describeTopics(maxTermsPerTopic).zipWithIndex.map {
      case ((termIndices, termWeights), topic) =>
        (topic, termIndices, termWeights)
    }
    sqlContext.createDataFrame() // TODO: RIGHT HERE NOW
  }

  @Since("1.6.0")
  def describeTopics(): DataFrame = describeTopics(10)

}

/**
 * :: Experimental ::
 *
 * TODO
 */
@Experimental
class DistributedLDAModel private[ml] (
    uid: String,
    vocabSize: Int,
    private val oldDistributedModel: OldDistributedLDAModel)
  extends LDAModel(uid, vocabSize, None) {

  override protected def getModel: OldLDAModel = oldDistributedModel

  @Since("1.6.0")
  override def copy(extra: ParamMap): DistributedLDAModel = ???

  // TODO
  override def isDistributed: Boolean = true

  def trainingLogLikelihood(): Double = ???

  def trainingLogPerplexity(): Double = ???
}


/**
 * :: Experimental ::
 *
 * TODO
 */
@Experimental
class LDA @Since("1.6.0") (
    @Since("1.6.0") override val uid: String) extends Estimator[LDAModel] with LDAParams {

  // TODO: setDefault()

  // TODO: ADD PARAM SETTERS

  @Since("1.6.0")
  override def copy(extra: ParamMap): LDA = defaultCopy(extra)

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("lda"))

  /** @group setParam */
  @Since("1.6.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  @Since("1.6.0")
  override def fit(dataset: DataFrame): LDAModel = {
    val oldLDA = new OldLDA()
      .setK($(k))
      .setDocConcentration(Vectors.dense($(docConcentration)))
      .setTopicConcentration($(topicConcentration))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setCheckpointInterval($(checkpointInterval))
      .setOptimizer($(optimizer).getOldOptimizer)
    val oldData = dataset
      .withColumn("docId", monotonicallyIncreasingId())
      .select("docId", $(featuresCol))
      .map { case Row(docId: Long, features: Vector) =>
        (docId, features)
      }
    val oldModel = oldLDA.run(oldData)
    oldModel match {
      case m: OldLocalLDAModel => // TODO
      case m: OldDistributedLDAModel => // TODO
    }
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


/**
 * :: Experimental ::
 *
 * Abstraction for specifying the [[LDA.optimizer]] Param.
 */
@Experimental
sealed abstract class LDAOptimizer extends Params {
  private[clustering] def getOldOptimizer: OldLDAOptimizer
}


/**
 * :: Experimental ::
 *
 * Expectation-Maximization (EM) [[LDA.optimizer]].
 * This class may be used for specifying optimizer-specific Params.
 */
@Experimental
class EMLDAOptimizer @Since("1.6.0") (
    @Since("1.6.0") override val uid: String) extends LDAOptimizer {

  @Since("1.6.0")
  override def copy(extra: ParamMap): EMLDAOptimizer = defaultCopy(extra)

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("EMLDAOpt"))

  private[clustering] override def getOldOptimizer: OldEMLDAOptimizer = {
    new OldEMLDAOptimizer
  }
}


/**
 * :: Experimental ::
 *
 * Online Variational Bayes [[LDA.optimizer]].
 * This class may be used for specifying optimizer-specific Params.
 */
@Experimental
class OnlineLDAOptimizer @Since("1.6.0") (
    @Since("1.6.0") override val uid: String) extends LDAOptimizer {

  @Since("1.6.0")
  override def copy(extra: ParamMap): OnlineLDAOptimizer = defaultCopy(extra)

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("OnlineLDAOpt"))

  private[clustering] override def getOldOptimizer: OldOnlineLDAOptimizer = {
    new OldOnlineLDAOptimizer()
      .setTau0($(tau0))
      .setKappa($(kappa))
      .setMiniBatchFraction($(subsamplingRate))
      .setOptimizeDocConcentration($(optimizeDocConcentration))
  }

  /**
   * A (positive) learning parameter that downweights early iterations. Larger values make early
   * iterations count less.
   * Default: 1024, following the original Online LDA paper.
   * @group param
   */
  @Since("1.6.0")
  final val tau0 = new DoubleParam(this, "tau0", "A (positive) learning parameter that" +
    " downweights early iterations. Larger values make early iterations count less.",
    ParamValidators.gt(0))

  setDefault(tau0 -> 1024)

  /** @group getParam */
  @Since("1.6.0")
  def getTau0: Double = $(tau0)

  /**
   * Learning rate, set as an exponential decay rate.
   * This should be between (0.5, 1.0] to guarantee asymptotic convergence.
   * Default: 0.51, based on the original Online LDA paper.
   * @group param
   */
  @Since("1.6.0")
  final val kappa = new DoubleParam(this, "kappa", "Learning rate, set as an exponential decay" +
    " rate. This should be between (0.5, 1.0] to guarantee asymptotic convergence.",
    ParamValidators.gt(0))

  setDefault(kappa -> 0.51)

  /** @group getParam */
  @Since("1.6.0")
  def getKappa: Double = $(kappa)

  /**
   * Fraction of the corpus to be sampled and used in each iteration of mini-batch gradient descent,
   * in range (0, 1].
   *
   * Note that this should be adjusted in synch with [[LDA.maxIter]]
   * so the entire corpus is used.  Specifically, set both so that
   * maxIterations * miniBatchFraction >= 1.
   *
   * Note: This is the same as the `miniBatchFraction` parameter in
   *       [[org.apache.spark.mllib.clustering.OnlineLDAOptimizer]].
   *
   * Default: 0.05, i.e., 5% of total documents.
   * @group param
   */
  @Since("1.6.0")
  final val subsamplingRate = new DoubleParam(this, "subsamplingRate", "Fraction of the corpus" +
    " to be sampled and used in each iteration of mini-batch gradient descent, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false, upperInclusive = true))

  setDefault(subsamplingRate -> 0.05)

  /** @group getParam */
  @Since("1.6.0")
  def getSubsamplingRate: Double = $(subsamplingRate)

  /**
   * Indicates whether the docConcentration (Dirichlet parameter for
   * document-topic distribution) will be optimized during training.
   * Default: true
   * TODO: Check defaults of other libraries.
   * @group param
   */
  @Since("1.5.0")
  final val optimizeDocConcentration = new BooleanParam(this, "optimizeDocConcentration",
    "Indicates whether the docConcentration (Dirichlet parameter for document-topic" +
      " distribution) will be optimized during training.")

  setDefault(optimizeDocConcentration -> true)

  /** @group getParam */
  @Since("1.6.0")
  def getOptimizeDocConcentration: Boolean = $(optimizeDocConcentration)
}

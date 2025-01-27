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

import java.util.Locale

import breeze.linalg.normalize
import breeze.numerics.exp
import org.apache.hadoop.fs.Path
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasCheckpointInterval, HasFeaturesCol, HasMaxIter, HasSeed}
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.clustering.{DistributedLDAModel => OldDistributedLDAModel,
  EMLDAOptimizer => OldEMLDAOptimizer, LDA => OldLDA, LDAModel => OldLDAModel,
  LDAOptimizer => OldLDAOptimizer, LDAUtils => OldLDAUtils, LocalLDAModel => OldLocalLDAModel,
  OnlineLDAOptimizer => OldOnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.MatrixImplicits._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.PeriodicCheckpointer
import org.apache.spark.util.VersionUtils

private[clustering] trait LDAParams extends Params with HasFeaturesCol with HasMaxIter
  with HasSeed with HasCheckpointInterval {

  /**
   * Param for the number of topics (clusters) to infer. Must be &gt; 1. Default: 10.
   *
   * @group param
   */
  @Since("1.6.0")
  final val k = new IntParam(this, "k", "The number of topics (clusters) to infer. " +
    "Must be > 1.", ParamValidators.gt(1))

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
   * If not set by the user, then docConcentration is set automatically. If set to
   * singleton vector [alpha], then alpha is replicated to a vector of length k in fitting.
   * Otherwise, the [[docConcentration]] vector must be length k.
   * (default = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Currently only supports symmetric distributions, so all values in the vector should be
   *       the same.
   *     - Values should be greater than 1.0
   *     - default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows
   *       from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Values should be greater than or equal to 0
   *     - default = uniformly (1.0 / k), following the implementation from
   *       <a href="https://github.com/Blei-Lab/onlineldavb">here</a>.
   *
   * @group param
   */
  @Since("1.6.0")
  final val docConcentration = new DoubleArrayParam(this, "docConcentration",
    "Concentration parameter (commonly named \"alpha\") for the prior placed on documents'" +
      " distributions over topics (\"theta\").", (alpha: Array[Double]) => alpha.forall(_ >= 0.0))

  /** @group getParam */
  @Since("1.6.0")
  def getDocConcentration: Array[Double] = $(docConcentration)

  /** Get docConcentration used by spark.mllib LDA */
  protected def getOldDocConcentration: Vector = {
    if (isSet(docConcentration)) {
      Vectors.dense(getDocConcentration)
    } else {
      Vectors.dense(-1.0)
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
   * If not set by the user, then topicConcentration is set automatically.
   *  (default = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Value should be greater than 1.0
   *     - default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows
   *       Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Value should be greater than or equal to 0
   *     - default = (1.0 / k), following the implementation from
   *       <a href="https://github.com/Blei-Lab/onlineldavb">here</a>.
   *
   * @group param
   */
  @Since("1.6.0")
  final val topicConcentration = new DoubleParam(this, "topicConcentration",
    "Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topic'" +
      " distributions over terms.", ParamValidators.gtEq(0))

  /** @group getParam */
  @Since("1.6.0")
  def getTopicConcentration: Double = $(topicConcentration)

  /** Get topicConcentration used by spark.mllib LDA */
  protected def getOldTopicConcentration: Double = {
    if (isSet(topicConcentration)) {
      getTopicConcentration
    } else {
      -1.0
    }
  }

  /** Supported values for Param [[optimizer]]. */
  @Since("1.6.0")
  final val supportedOptimizers: Array[String] = Array("online", "em")

  /**
   * Optimizer or inference algorithm used to estimate the LDA model.
   * Currently supported (case-insensitive):
   *  - "online": Online Variational Bayes (default)
   *  - "em": Expectation-Maximization
   *
   * For details, see the following papers:
   *  - Online LDA:
   *     Hoffman, Blei and Bach.  "Online Learning for Latent Dirichlet Allocation."
   *     Neural Information Processing Systems, 2010.
   *     See <a href="http://www.cs.columbia.edu/~blei/papers/HoffmanBleiBach2010b.pdf">here</a>
   *  - EM:
   *     Asuncion et al.  "On Smoothing and Inference for Topic Models."
   *     Uncertainty in Artificial Intelligence, 2009.
   *     See <a href="http://arxiv.org/pdf/1205.2662.pdf">here</a>
   *
   * @group param
   */
  @Since("1.6.0")
  final val optimizer = new Param[String](this, "optimizer", "Optimizer or inference" +
    " algorithm used to estimate the LDA model. Supported: " + supportedOptimizers.mkString(", "),
    (value: String) => supportedOptimizers.contains(value.toLowerCase(Locale.ROOT)))

  /** @group getParam */
  @Since("1.6.0")
  def getOptimizer: String = $(optimizer)

  /**
   * Output column with estimates of the topic mixture distribution for each document (often called
   * "theta" in the literature).  Returns a vector of zeros for an empty document.
   *
   * This uses a variational approximation following Hoffman et al. (2010), where the approximate
   * distribution is called "gamma."  Technically, this method returns this approximation "gamma"
   * for each document.
   *
   * @group param
   */
  @Since("1.6.0")
  final val topicDistributionCol = new Param[String](this, "topicDistributionCol", "Output column" +
    " with estimates of the topic mixture distribution for each document (often called \"theta\"" +
    " in the literature).  Returns a vector of zeros for an empty document.")

  /** @group getParam */
  @Since("1.6.0")
  def getTopicDistributionCol: String = $(topicDistributionCol)

  /**
   * For Online optimizer only: [[optimizer]] = "online".
   *
   * A (positive) learning parameter that downweights early iterations. Larger values make early
   * iterations count less.
   * This is called "tau0" in the Online LDA paper (Hoffman et al., 2010)
   * Default: 1024, following Hoffman et al.
   *
   * @group expertParam
   */
  @Since("1.6.0")
  final val learningOffset = new DoubleParam(this, "learningOffset", "(For online optimizer)" +
    " A (positive) learning parameter that downweights early iterations. Larger values make early" +
    " iterations count less.",
    ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.6.0")
  def getLearningOffset: Double = $(learningOffset)

  /**
   * For Online optimizer only: [[optimizer]] = "online".
   *
   * Learning rate, set as an exponential decay rate.
   * This should be between (0.5, 1.0] to guarantee asymptotic convergence.
   * This is called "kappa" in the Online LDA paper (Hoffman et al., 2010).
   * Default: 0.51, based on Hoffman et al.
   *
   * @group expertParam
   */
  @Since("1.6.0")
  final val learningDecay = new DoubleParam(this, "learningDecay", "(For online optimizer)" +
    " Learning rate, set as an exponential decay rate. This should be between (0.5, 1.0] to" +
    " guarantee asymptotic convergence.", ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.6.0")
  def getLearningDecay: Double = $(learningDecay)

  /**
   * For Online optimizer only: [[optimizer]] = "online".
   *
   * Fraction of the corpus to be sampled and used in each iteration of mini-batch gradient descent,
   * in range (0, 1].
   *
   * Note that this should be adjusted in synch with `LDA.maxIter`
   * so the entire corpus is used.  Specifically, set both so that
   * maxIterations * miniBatchFraction greater than or equal to 1.
   *
   * Note: This is the same as the `miniBatchFraction` parameter in
   *       [[org.apache.spark.mllib.clustering.OnlineLDAOptimizer]].
   *
   * Default: 0.05, i.e., 5% of total documents.
   *
   * @group param
   */
  @Since("1.6.0")
  final val subsamplingRate = new DoubleParam(this, "subsamplingRate", "(For online optimizer)" +
    " Fraction of the corpus to be sampled and used in each iteration of mini-batch" +
    " gradient descent, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false, upperInclusive = true))

  /** @group getParam */
  @Since("1.6.0")
  def getSubsamplingRate: Double = $(subsamplingRate)

  /**
   * For Online optimizer only (currently): [[optimizer]] = "online".
   *
   * Indicates whether the docConcentration (Dirichlet parameter for
   * document-topic distribution) will be optimized during training.
   * Setting this to true will make the model more expressive and fit the training data better.
   * Default: false
   *
   * @group expertParam
   */
  @Since("1.6.0")
  final val optimizeDocConcentration = new BooleanParam(this, "optimizeDocConcentration",
    "(For online optimizer only, currently) Indicates whether the docConcentration" +
      " (Dirichlet parameter for document-topic distribution) will be optimized during training.")

  /** @group expertGetParam */
  @Since("1.6.0")
  def getOptimizeDocConcentration: Boolean = $(optimizeDocConcentration)

  /**
   * For EM optimizer only: [[optimizer]] = "em".
   *
   * If using checkpointing, this indicates whether to keep the last
   * checkpoint. If false, then the checkpoint will be deleted. Deleting the checkpoint can
   * cause failures if a data partition is lost, so set this bit with care.
   * Note that checkpoints will be cleaned up via reference counting, regardless.
   *
   * See `DistributedLDAModel.getCheckpointFiles` for getting remaining checkpoints and
   * `DistributedLDAModel.deleteCheckpointFiles` for removing remaining checkpoints.
   *
   * Default: true
   *
   * @group expertParam
   */
  @Since("2.0.0")
  final val keepLastCheckpoint = new BooleanParam(this, "keepLastCheckpoint",
    "(For EM optimizer) If using checkpointing, this indicates whether to keep the last" +
      " checkpoint. If false, then the checkpoint will be deleted. Deleting the checkpoint can" +
      " cause failures if a data partition is lost, so set this bit with care.")

  /** @group expertGetParam */
  @Since("2.0.0")
  def getKeepLastCheckpoint: Boolean = $(keepLastCheckpoint)

  setDefault(maxIter -> 20, k -> 10, optimizer -> "online", checkpointInterval -> 10,
    learningOffset -> 1024, learningDecay -> 0.51, subsamplingRate -> 0.05,
    optimizeDocConcentration -> true, keepLastCheckpoint -> true,
    topicDistributionCol -> "topicDistribution")

  /**
   * Validates and transforms the input schema.
   *
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    if (isSet(docConcentration)) {
      if (getDocConcentration.length != 1) {
        require(getDocConcentration.length == getK, s"LDA docConcentration was of length" +
          s" ${getDocConcentration.length}, but k = $getK.  docConcentration must be an array of" +
          s" length either 1 (scalar) or k (num topics).")
      }
      getOptimizer.toLowerCase(Locale.ROOT) match {
        case "online" =>
          require(getDocConcentration.forall(_ >= 0),
            "For Online LDA optimizer, docConcentration values must be >= 0.  Found values: " +
              getDocConcentration.mkString(","))
        case "em" =>
          require(getDocConcentration.forall(_ >= 0),
            "For EM optimizer, docConcentration values must be >= 1.  Found values: " +
              getDocConcentration.mkString(","))
      }
    }
    if (isSet(topicConcentration)) {
      getOptimizer.toLowerCase(Locale.ROOT) match {
        case "online" =>
          require(getTopicConcentration >= 0, s"For Online LDA optimizer, topicConcentration" +
            s" must be >= 0.  Found value: $getTopicConcentration")
        case "em" =>
          require(getTopicConcentration >= 0, s"For EM optimizer, topicConcentration" +
            s" must be >= 1.  Found value: $getTopicConcentration")
      }
    }
    SchemaUtils.validateVectorCompatibleColumn(schema, getFeaturesCol)
    SchemaUtils.appendColumn(schema, $(topicDistributionCol), new VectorUDT)
  }

  private[clustering] def getOldOptimizer: OldLDAOptimizer =
    getOptimizer.toLowerCase(Locale.ROOT) match {
      case "online" =>
        new OldOnlineLDAOptimizer()
          .setTau0($(learningOffset))
          .setKappa($(learningDecay))
          .setMiniBatchFraction($(subsamplingRate))
          .setOptimizeDocConcentration($(optimizeDocConcentration))
      case "em" =>
        new OldEMLDAOptimizer()
          .setKeepLastCheckpoint($(keepLastCheckpoint))
    }
}

private object LDAParams {

  /**
   * Equivalent to [[Metadata.getAndSetParams()]], but handles [[LDA]] and [[LDAModel]]
   * formats saved with Spark 1.6, which differ from the formats in Spark 2.0+.
   *
   * @param model    [[LDA]] or [[LDAModel]] instance.  This instance will be modified with
   *                 [[Param]] values extracted from metadata.
   * @param metadata Loaded model metadata
   */
  def getAndSetParams(model: LDAParams, metadata: Metadata): Unit = {
    VersionUtils.majorMinorVersion(metadata.sparkVersion) match {
      case (1, 6) =>
        implicit val format: Formats = DefaultFormats
        metadata.params match {
          case JObject(pairs) =>
            pairs.foreach { case (paramName, jsonValue) =>
              val origParam =
                if (paramName == "topicDistribution") "topicDistributionCol" else paramName
              val param = model.getParam(origParam)
              val value = param.jsonDecode(compact(render(jsonValue)))
              model.set(param, value)
            }
          case _ =>
            throw new IllegalArgumentException(
              s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
        }
      case _ => // 2.0+
        metadata.getAndSetParams(model)
    }
  }
}


/**
 * Model fitted by [[LDA]].
 *
 * @param vocabSize  Vocabulary size (number of terms or words in the vocabulary)
 * @param sparkSession  Used to construct local DataFrames for returning query results
 */
@Since("1.6.0")
abstract class LDAModel private[ml] (
    @Since("1.6.0") override val uid: String,
    @Since("1.6.0") val vocabSize: Int,
    @Since("1.6.0") @transient private[ml] val sparkSession: SparkSession)
  extends Model[LDAModel] with LDAParams with Logging with MLWritable {

  // NOTE to developers:
  //  This abstraction should contain all important functionality for basic LDA usage.
  //  Specializations of this class can contain expert-only functionality.

  /**
   * Underlying spark.mllib model.
   * If this model was produced by Online LDA, then this is the only model representation.
   * If this model was produced by EM, then this local representation may be built lazily.
   */
  @Since("1.6.0")
  private[clustering] def oldLocalModel: OldLocalLDAModel

  /** Returns underlying spark.mllib model, which may be local or distributed */
  @Since("1.6.0")
  private[clustering] def getModel: OldLDAModel

  private[ml] def getEffectiveDocConcentration: Array[Double] = getModel.docConcentration.toArray

  private[ml] def getEffectiveTopicConcentration: Double = getModel.topicConcentration

  /**
   * The features for LDA should be a `Vector` representing the word counts in a document.
   * The vector should be of length vocabSize, with counts for each term (word).
   *
   * @group setParam
   */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  @Since("2.2.0")
  def setTopicDistributionCol(value: String): this.type = set(topicDistributionCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Transforms the input dataset.
   *
   * WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when [[optimizer]]
   *          is set to "em"), this involves collecting a large [[topicsMatrix]] to the driver.
   *          This implementation may be changed in the future.
   */
  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val func = getTopicDistributionMethod
    val transformer = udf(func)
    dataset.withColumn($(topicDistributionCol),
      transformer(columnToVector(dataset, getFeaturesCol)),
      outputSchema($(topicDistributionCol)).metadata)
  }

  /**
   * Get a method usable as a UDF for `topicDistributions()`
   */
  private def getTopicDistributionMethod: Vector => Vector = {
    val expElogbeta = exp(OldLDAUtils.dirichletExpectation(topicsMatrix.asBreeze.toDenseMatrix.t).t)
    val oldModel = oldLocalModel
    val docConcentrationBrz = oldModel.docConcentration.asBreeze
    val gammaShape = oldModel.gammaShape
    val k = oldModel.k
    val gammaSeed = oldModel.seed

    vector: Vector =>
      if (vector.numNonzeros == 0) {
        Vectors.zeros(k)
      } else {
        val (ids: List[Int], cts: Array[Double]) = vector match {
          case v: DenseVector => (List.range(0, v.size), v.values)
          case v: SparseVector => (v.indices.toList, v.values)
          case other =>
            throw new UnsupportedOperationException(
              s"Only sparse and dense vectors are supported but got ${other.getClass}.")
        }

        val (gamma, _, _) = OldOnlineLDAOptimizer.variationalTopicInference(
          ids,
          cts,
          expElogbeta,
          docConcentrationBrz,
          gammaShape,
          k,
          gammaSeed)
        Vectors.dense(normalize(gamma, 1.0).toArray)
      }
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(topicDistributionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(topicDistributionCol), oldLocalModel.k)
    }
    outputSchema
  }

  /**
   * Value for [[docConcentration]] estimated from data.
   * If Online LDA was used and [[optimizeDocConcentration]] was set to false,
   * then this returns the fixed (given) value for the [[docConcentration]] parameter.
   */
  @Since("2.0.0")
  def estimatedDocConcentration: Vector = getModel.docConcentration

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   *
   * WARNING: If this model is actually a [[DistributedLDAModel]] instance produced by
   *          the Expectation-Maximization ("em") [[optimizer]], then this method could involve
   *          collecting a large amount of data to the driver (on the order of vocabSize x k).
   */
  @Since("2.0.0")
  def topicsMatrix: Matrix = oldLocalModel.topicsMatrix.asML

  /** Indicates whether this instance is of type [[DistributedLDAModel]] */
  @Since("1.6.0")
  def isDistributed: Boolean

  /**
   * Calculates a lower bound on the log likelihood of the entire corpus.
   *
   * See Equation (16) in the Online LDA paper (Hoffman et al., 2010).
   *
   * WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when [[optimizer]]
   *          is set to "em"), this involves collecting a large [[topicsMatrix]] to the driver.
   *          This implementation may be changed in the future.
   *
   * @param dataset  test corpus to use for calculating log likelihood
   * @return variational lower bound on the log likelihood of the entire corpus
   */
  @Since("2.0.0")
  def logLikelihood(dataset: Dataset[_]): Double = {
    val oldDataset = LDA.getOldDataset(dataset, $(featuresCol))
    oldLocalModel.logLikelihood(oldDataset)
  }

  /**
   * Calculate an upper bound on perplexity.  (Lower is better.)
   * See Equation (16) in the Online LDA paper (Hoffman et al., 2010).
   *
   * WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when [[optimizer]]
   *          is set to "em"), this involves collecting a large [[topicsMatrix]] to the driver.
   *          This implementation may be changed in the future.
   *
   * @param dataset test corpus to use for calculating perplexity
   * @return Variational upper bound on log perplexity per token.
   */
  @Since("2.0.0")
  def logPerplexity(dataset: Dataset[_]): Double = {
    val oldDataset = LDA.getOldDataset(dataset, $(featuresCol))
    oldLocalModel.logPerplexity(oldDataset)
  }

  /**
   * Return the topics described by their top-weighted terms.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   *                          Default value of 10.
   * @return  Local DataFrame with one topic per Row, with columns:
   *           - "topic": IntegerType: topic index
   *           - "termIndices": ArrayType(IntegerType): term indices, sorted in order of decreasing
   *                            term importance
   *           - "termWeights": ArrayType(DoubleType): corresponding sorted term weights
   */
  @Since("1.6.0")
  def describeTopics(maxTermsPerTopic: Int): DataFrame = {
    val topics = getModel.describeTopics(maxTermsPerTopic).zipWithIndex.map {
      case ((termIndices, termWeights), topic) =>
        (topic, termIndices.toImmutableArraySeq, termWeights.toImmutableArraySeq)
    }
    sparkSession.createDataFrame(topics.toImmutableArraySeq)
      .toDF("topic", "termIndices", "termWeights")
  }

  @Since("1.6.0")
  def describeTopics(): DataFrame = describeTopics(10)
}


/**
 *
 * Local (non-distributed) model fitted by [[LDA]].
 *
 * This model stores the inferred topics only; it does not store info about the training dataset.
 */
@Since("1.6.0")
class LocalLDAModel private[ml] (
    uid: String,
    vocabSize: Int,
    private[clustering] val oldLocalModel : OldLocalLDAModel,
    sparkSession: SparkSession)
  extends LDAModel(uid, vocabSize, sparkSession) {

  private[ml] def this() = this(Identifiable.randomUID("lda"), -1, null, null)

  oldLocalModel.setSeed(getSeed)

  @Since("1.6.0")
  override def copy(extra: ParamMap): LocalLDAModel = {
    val copied = new LocalLDAModel(uid, vocabSize, oldLocalModel, sparkSession)
    copyValues(copied, extra).setParent(parent).asInstanceOf[LocalLDAModel]
  }

  override private[clustering] def getModel: OldLDAModel = oldLocalModel

  @Since("1.6.0")
  override def isDistributed: Boolean = false

  @Since("1.6.0")
  override def write: MLWriter = new LocalLDAModel.LocalLDAModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"LocalLDAModel: uid=$uid, k=${$(k)}, numFeatures=$vocabSize"
  }
}


@Since("1.6.0")
object LocalLDAModel extends MLReadable[LocalLDAModel] {

  private[LocalLDAModel]
  class LocalLDAModelWriter(instance: LocalLDAModel) extends MLWriter {

    private case class Data(
        vocabSize: Int,
        topicsMatrix: Matrix,
        docConcentration: Vector,
        topicConcentration: Double,
        gammaShape: Double)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val oldModel = instance.oldLocalModel
      val data = Data(instance.vocabSize, oldModel.topicsMatrix, oldModel.docConcentration,
        oldModel.topicConcentration, oldModel.gammaShape)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class LocalLDAModelReader extends MLReader[LocalLDAModel] {

    private val className = classOf[LocalLDAModel].getName

    override def load(path: String): LocalLDAModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val vectorConverted = MLUtils.convertVectorColumnsToML(data, "docConcentration")
      val matrixConverted = MLUtils.convertMatrixColumnsToML(vectorConverted, "topicsMatrix")
      val Row(vocabSize: Int, topicsMatrix: Matrix, docConcentration: Vector,
          topicConcentration: Double, gammaShape: Double) =
        matrixConverted.select("vocabSize", "topicsMatrix", "docConcentration",
          "topicConcentration", "gammaShape").head()
      val oldModel = new OldLocalLDAModel(topicsMatrix, docConcentration, topicConcentration,
        gammaShape)
      val model = new LocalLDAModel(metadata.uid, vocabSize, oldModel, sparkSession)
      LDAParams.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[LocalLDAModel] = new LocalLDAModelReader

  @Since("1.6.0")
  override def load(path: String): LocalLDAModel = super.load(path)
}


/**
 *
 * Distributed model fitted by [[LDA]].
 * This type of model is currently only produced by Expectation-Maximization (EM).
 *
 * This model stores the inferred topics, the full training dataset, and the topic distribution
 * for each training document.
 *
 * @param oldLocalModelOption  Used to implement [[oldLocalModel]] as a lazy val, but keeping
 *                             `copy()` cheap.
 */
@Since("1.6.0")
class DistributedLDAModel private[ml] (
    uid: String,
    vocabSize: Int,
    private val oldDistributedModel: OldDistributedLDAModel,
    sparkSession: SparkSession,
    private var oldLocalModelOption: Option[OldLocalLDAModel])
  extends LDAModel(uid, vocabSize, sparkSession) {

  private[ml] def this() = this(Identifiable.randomUID("lda"), -1, null, null, None)

  override private[clustering] def oldLocalModel: OldLocalLDAModel = {
    if (oldLocalModelOption.isEmpty) {
      oldLocalModelOption = Some(oldDistributedModel.toLocal)
    }
    oldLocalModelOption.get
  }

  override private[clustering] def getModel: OldLDAModel = oldDistributedModel

  /**
   * Convert this distributed model to a local representation.  This discards info about the
   * training dataset.
   *
   * WARNING: This involves collecting a large [[topicsMatrix]] to the driver.
   */
  @Since("1.6.0")
  def toLocal: LocalLDAModel = new LocalLDAModel(uid, vocabSize, oldLocalModel, sparkSession)

  @Since("1.6.0")
  override def copy(extra: ParamMap): DistributedLDAModel = {
    val copied = new DistributedLDAModel(
      uid, vocabSize, oldDistributedModel, sparkSession, oldLocalModelOption)
    copyValues(copied, extra).setParent(parent)
    copied
  }

  @Since("1.6.0")
  override def isDistributed: Boolean = true

  /**
   * Log likelihood of the observed tokens in the training set,
   * given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, Dirichlet hyperparameters)
   *
   * Notes:
   *  - This excludes the prior; for that, use [[logPrior]].
   *  - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   *  - This is computed from the topic distributions computed during training. If you call
   *    `logLikelihood()` on the same training dataset, the topic distributions will be computed
   *    again, possibly giving different results.
   */
  @Since("1.6.0")
  lazy val trainingLogLikelihood: Double = oldDistributedModel.logLikelihood

  /**
   * Log probability of the current parameter estimate:
   * log P(topics, topic distributions for docs | Dirichlet hyperparameters)
   */
  @Since("1.6.0")
  lazy val logPrior: Double = oldDistributedModel.logPrior

  private var _checkpointFiles: Array[String] = oldDistributedModel.checkpointFiles

  /**
   * If using checkpointing and `LDA.keepLastCheckpoint` is set to true, then there may be
   * saved checkpoint files.  This method is provided so that users can manage those files.
   *
   * Note that removing the checkpoints can cause failures if a partition is lost and is needed
   * by certain [[DistributedLDAModel]] methods.  Reference counting will clean up the checkpoints
   * when this model and derivative data go out of scope.
   *
   * @return  Checkpoint files from training
   */
  @Since("2.0.0")
  def getCheckpointFiles: Array[String] = _checkpointFiles

  /**
   * Remove any remaining checkpoint files from training.
   *
   * @see [[getCheckpointFiles]]
   */
  @Since("2.0.0")
  def deleteCheckpointFiles(): Unit = {
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
    _checkpointFiles.foreach(PeriodicCheckpointer.removeCheckpointFile(_, hadoopConf))
    _checkpointFiles = Array.empty[String]
  }

  @Since("1.6.0")
  override def write: MLWriter = new DistributedLDAModel.DistributedWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"DistributedLDAModel: uid=$uid, k=${$(k)}, numFeatures=$vocabSize"
  }
}


@Since("1.6.0")
object DistributedLDAModel extends MLReadable[DistributedLDAModel] {

  private[DistributedLDAModel]
  class DistributedWriter(instance: DistributedLDAModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val modelPath = new Path(path, "oldModel").toString
      instance.oldDistributedModel.save(sc, modelPath)
    }
  }

  private class DistributedLDAModelReader extends MLReader[DistributedLDAModel] {

    private val className = classOf[DistributedLDAModel].getName

    override def load(path: String): DistributedLDAModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val modelPath = new Path(path, "oldModel").toString
      val oldModel = OldDistributedLDAModel.load(sc, modelPath)
      val model = new DistributedLDAModel(metadata.uid, oldModel.vocabSize,
        oldModel, sparkSession, None)
      LDAParams.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[DistributedLDAModel] = new DistributedLDAModelReader

  @Since("1.6.0")
  override def load(path: String): DistributedLDAModel = super.load(path)
}


/**
 *
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * Terminology:
 *  - "term" = "word": an element of the vocabulary
 *  - "token": instance of a term appearing in a document
 *  - "topic": multinomial distribution over terms representing some concept
 *  - "document": one piece of text, corresponding to one row in the input data
 *
 * Original LDA paper (journal version):
 *  Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
 *
 * Input data (featuresCol):
 *  LDA is given a collection of documents as input data, via the featuresCol parameter.
 *  Each document is specified as a `Vector` of length vocabSize, where each entry is the
 *  count for the corresponding term (word) in the document.  Feature transformers such as
 *  [[org.apache.spark.ml.feature.Tokenizer]] and [[org.apache.spark.ml.feature.CountVectorizer]]
 *  can be useful for converting text to word count vectors.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation">
 * Latent Dirichlet allocation (Wikipedia)</a>
 */
@Since("1.6.0")
class LDA @Since("1.6.0") (
    @Since("1.6.0") override val uid: String)
  extends Estimator[LDAModel] with LDAParams with DefaultParamsWritable {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("lda"))

  /**
   * The features for LDA should be a `Vector` representing the word counts in a document.
   * The vector should be of length vocabSize, with counts for each term (word).
   *
   * @group setParam
   */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("1.6.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.6.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  @Since("1.6.0")
  def setDocConcentration(value: Array[Double]): this.type = set(docConcentration, value)

  /** @group setParam */
  @Since("1.6.0")
  def setDocConcentration(value: Double): this.type = set(docConcentration, Array(value))

  /** @group setParam */
  @Since("1.6.0")
  def setTopicConcentration(value: Double): this.type = set(topicConcentration, value)

  /** @group setParam */
  @Since("1.6.0")
  def setOptimizer(value: String): this.type = set(optimizer, value)

  /** @group setParam */
  @Since("1.6.0")
  def setTopicDistributionCol(value: String): this.type = set(topicDistributionCol, value)

  /** @group expertSetParam */
  @Since("1.6.0")
  def setLearningOffset(value: Double): this.type = set(learningOffset, value)

  /** @group expertSetParam */
  @Since("1.6.0")
  def setLearningDecay(value: Double): this.type = set(learningDecay, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group expertSetParam */
  @Since("1.6.0")
  def setOptimizeDocConcentration(value: Boolean): this.type = set(optimizeDocConcentration, value)

  /** @group expertSetParam */
  @Since("2.0.0")
  def setKeepLastCheckpoint(value: Boolean): this.type = set(keepLastCheckpoint, value)

  @Since("1.6.0")
  override def copy(extra: ParamMap): LDA = defaultCopy(extra)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): LDAModel = instrumented { instr =>
    transformSchema(dataset.schema, logging = true)

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, featuresCol, topicDistributionCol, k, maxIter, subsamplingRate,
      checkpointInterval, keepLastCheckpoint, optimizeDocConcentration, topicConcentration,
      learningDecay, optimizer, learningOffset, seed)

    val oldData = LDA.getOldDataset(dataset, $(featuresCol))
      .setName("training instances")

    // The EM solver will transform this oldData to a graph, and use a internal graphCheckpointer
    // to update and cache the graph, so we do not need to cache it.
    // The Online solver directly perform sampling on the oldData and update the model.
    // However, Online solver will not cache the dataset internally.
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE &&
      getOptimizer.toLowerCase(Locale.ROOT) == "online"
    if (handlePersistence) {
      oldData.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val oldLDA = new OldLDA()
      .setK($(k))
      .setDocConcentration(getOldDocConcentration)
      .setTopicConcentration(getOldTopicConcentration)
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setCheckpointInterval($(checkpointInterval))
      .setOptimizer(getOldOptimizer)

    val oldModel = oldLDA.run(oldData)
    val newModel = oldModel match {
      case m: OldLocalLDAModel =>
        new LocalLDAModel(uid, m.vocabSize, m, dataset.sparkSession)
      case m: OldDistributedLDAModel =>
        new DistributedLDAModel(uid, m.vocabSize, m, dataset.sparkSession, None)
    }
    if (handlePersistence) {
      oldData.unpersist()
    }

    instr.logNumFeatures(newModel.vocabSize)
    copyValues(newModel).setParent(this)
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("2.0.0")
object LDA extends MLReadable[LDA] {

  /** Get dataset for spark.mllib LDA */
  private[clustering] def getOldDataset(
       dataset: Dataset[_],
       featuresCol: String): RDD[(Long, OldVector)] = {
    dataset.select(
      monotonically_increasing_id(),
      checkNonNanVectors(columnToVector(dataset, featuresCol))
    ).rdd.map { case Row(docId: Long, f: Vector) => (docId, OldVectors.fromML(f)) }
  }

  private class LDAReader extends MLReader[LDA] {

    private val className = classOf[LDA].getName

    override def load(path: String): LDA = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val model = new LDA(metadata.uid)
      LDAParams.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[LDA] = new LDAReader

  @Since("2.0.0")
  override def load(path: String): LDA = super.load(path)
}

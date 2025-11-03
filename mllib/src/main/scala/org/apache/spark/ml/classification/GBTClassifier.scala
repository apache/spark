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

package org.apache.spark.ml.classification

import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{BLAS, DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.GradientBoostedTrees
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils.extractInstances
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel => OldGBTModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * Gradient-Boosted Trees (GBTs) (http://en.wikipedia.org/wiki/Gradient_boosting)
 * learning algorithm for classification.
 * It supports binary labels, as well as both continuous and categorical features.
 *
 * The implementation is based upon: J.H. Friedman. "Stochastic Gradient Boosting." 1999.
 *
 * Notes on Gradient Boosting vs. TreeBoost:
 *  - This implementation is for Stochastic Gradient Boosting, not for TreeBoost.
 *  - Both algorithms learn tree ensembles by minimizing loss functions.
 *  - TreeBoost (Friedman, 1999) additionally modifies the outputs at tree leaf nodes
 *    based on the loss function, whereas the original gradient boosting method does not.
 *  - We expect to implement TreeBoost in the future:
 *    [https://issues.apache.org/jira/browse/SPARK-4240]
 *
 * @note Multiclass labels are not currently supported.
 */
@Since("1.4.0")
class GBTClassifier @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, GBTClassifier, GBTClassificationModel]
  with GBTClassifierParams with DefaultParamsWritable with Logging {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("gbtc"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  /** @group setParam */
  @Since("1.4.0")
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("3.0.0")
  def setMinWeightFractionPerNode(value: Double): this.type = set(minWeightFractionPerNode, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be at least 1.
   * (default = 10)
   * @group setParam
   */
  @Since("1.4.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /**
   * The impurity setting is ignored for GBT models.
   * Individual trees are built using impurity "Variance."
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setImpurity(value: String): this.type = {
    logWarning("GBTClassifier.setImpurity should NOT be used")
    this
  }

  // Parameters from TreeEnsembleParams:

  /** @group setParam */
  @Since("1.4.0")
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group setParam */
  @Since("1.4.0")
  def setSeed(value: Long): this.type = set(seed, value)

  // Parameters from GBTParams:

  /** @group setParam */
  @Since("1.4.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.4.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /** @group setParam */
  @Since("2.3.0")
  def setFeatureSubsetStrategy(value: String): this.type =
    set(featureSubsetStrategy, value)

  // Parameters from GBTClassifierParams:

  /** @group setParam */
  @Since("1.4.0")
  def setLossType(value: String): this.type = set(lossType, value)

  /** @group setParam */
  @Since("2.4.0")
  def setValidationIndicatorCol(value: String): this.type = {
    set(validationIndicatorCol, value)
  }

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * By default the weightCol is not set, so all instances have weight 1.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  override protected def train(
      dataset: Dataset[_]): GBTClassificationModel = instrumented { instr =>
    val withValidation = isDefined(validationIndicatorCol) && $(validationIndicatorCol).nonEmpty
    val (trainDataset, validationDataset) = if (withValidation) {
      (extractInstances(this, dataset.filter(not(col($(validationIndicatorCol)))), Some(2)),
        extractInstances(this, dataset.filter(col($(validationIndicatorCol))), Some(2)))
    } else {
      (extractInstances(this, dataset, Some(2)), null)
    }

    val numClasses = 2
    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, weightCol, featuresCol, predictionCol, leafCol,
      impurity, lossType, maxDepth, maxBins, maxIter, maxMemoryInMB, minInfoGain,
      minInstancesPerNode, minWeightFractionPerNode, seed, stepSize, subsamplingRate, cacheNodeIds,
      checkpointInterval, featureSubsetStrategy, validationIndicatorCol, validationTol, thresholds)
    instr.logNumClasses(numClasses)

    val categoricalFeatures = MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val boostingStrategy = super.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Classification)
    val (baseLearners, learnerWeights) = if (withValidation) {
      GradientBoostedTrees.runWithValidation(trainDataset, validationDataset, boostingStrategy,
        $(seed), $(featureSubsetStrategy), Some(instr))
    } else {
      GradientBoostedTrees.run(trainDataset, boostingStrategy, $(seed), $(featureSubsetStrategy),
        Some(instr))
    }
    baseLearners.foreach(copyValues(_))

    val numFeatures = baseLearners.head.numFeatures
    instr.logNumFeatures(numFeatures)

    new GBTClassificationModel(uid, baseLearners, learnerWeights, numFeatures)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): GBTClassifier = defaultCopy(extra)
}

@Since("1.4.0")
object GBTClassifier extends DefaultParamsReadable[GBTClassifier] {

  /** Accessor for supported loss settings: logistic */
  @Since("1.4.0")
  final val supportedLossTypes: Array[String] = GBTClassifierParams.supportedLossTypes

  @Since("2.0.0")
  override def load(path: String): GBTClassifier = super.load(path)
}

/**
 * Gradient-Boosted Trees (GBTs) (http://en.wikipedia.org/wiki/Gradient_boosting)
 * model for classification.
 * It supports binary labels, as well as both continuous and categorical features.
 *
 * @param _trees  Decision trees in the ensemble.
 * @param _treeWeights  Weights for the decision trees in the ensemble.
 *
 * @note Multiclass labels are not currently supported.
 */
@Since("1.6.0")
class GBTClassificationModel private[ml](
    @Since("1.6.0") override val uid: String,
    private val _trees: Array[DecisionTreeRegressionModel],
    private val _treeWeights: Array[Double],
    @Since("1.6.0") override val numFeatures: Int,
    @Since("2.2.0") override val numClasses: Int)
  extends ProbabilisticClassificationModel[Vector, GBTClassificationModel]
  with GBTClassifierParams with TreeEnsembleModel[DecisionTreeRegressionModel]
  with MLWritable with Serializable {

  require(_trees.nonEmpty, "GBTClassificationModel requires at least 1 tree.")
  require(_trees.length == _treeWeights.length, "GBTClassificationModel given trees, treeWeights" +
    s" of non-matching lengths (${_trees.length}, ${_treeWeights.length}, respectively).")

  /**
   * Construct a GBTClassificationModel
   *
   * @param _trees  Decision trees in the ensemble.
   * @param _treeWeights  Weights for the decision trees in the ensemble.
   * @param numFeatures  The number of features.
   */
  private[ml] def this(
      uid: String,
      _trees: Array[DecisionTreeRegressionModel],
      _treeWeights: Array[Double],
      numFeatures: Int) =
  this(uid, _trees, _treeWeights, numFeatures, 2)

  /**
   * Construct a GBTClassificationModel
   *
   * @param _trees  Decision trees in the ensemble.
   * @param _treeWeights  Weights for the decision trees in the ensemble.
   */
  @Since("1.6.0")
  def this(uid: String, _trees: Array[DecisionTreeRegressionModel], _treeWeights: Array[Double]) =
    this(uid, _trees, _treeWeights, -1, 2)

  // For ml connect only
  private[ml] def this() = this("",
    Array(new DecisionTreeRegressionModel), Array(Double.NaN), -1, -1)

  override def estimatedSize: Long = getEstimatedSize()

  @Since("1.4.0")
  override def trees: Array[DecisionTreeRegressionModel] = _trees

  /**
   * Number of trees in ensemble
   */
  @Since("2.0.0")
  val getNumTrees: Int = trees.length

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = super.transformSchema(schema)
    if ($(leafCol).nonEmpty) {
      outputSchema = SchemaUtils.updateField(outputSchema, getLeafField($(leafCol)))
    }
    outputSchema
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val outputData = super.transform(dataset)
    if ($(leafCol).nonEmpty) {
      val leafUDF = udf { features: Vector => predictLeaf(features) }
      outputData.withColumn($(leafCol), leafUDF(col($(featuresCol))),
        outputSchema($(leafCol)).metadata)
    } else {
      outputData
    }
  }

  override def predict(features: Vector): Double = {
    // If thresholds defined, use predictRaw to get probabilities, otherwise use optimization
    if (isDefined(thresholds)) {
      super.predict(features)
    } else {
      if (margin(features) > 0.0) 1.0 else 0.0
    }
  }

  @Since("3.0.0")
  override def predictRaw(features: Vector): Vector = {
    val prediction: Double = margin(features)
    Vectors.dense(Array(-prediction, prediction))
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        dv.values(0) = loss.computeProbability(dv.values(0))
        dv.values(1) = 1.0 - dv.values(0)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in GBTClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): GBTClassificationModel = {
    copyValues(new GBTClassificationModel(uid, _trees, _treeWeights, numFeatures, numClasses),
      extra).setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"GBTClassificationModel: uid = $uid, numTrees=$getNumTrees, numClasses=$numClasses, " +
      s"numFeatures=$numFeatures"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
   * (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
   * and follows the implementation from scikit-learn.
   *
   * See `DecisionTreeClassificationModel.featureImportances`
   */
  @Since("2.0.0")
  lazy val featureImportances: Vector =
    TreeEnsembleModel.featureImportances(trees, numFeatures, perTreeNormalization = false)

  /** Raw prediction for the positive class. */
  private def margin(features: Vector): Double = {
    val treePredictions = _trees.map(_.rootNode.predictImpl(features).prediction)
    BLAS.nativeBLAS.ddot(getNumTrees, treePredictions, 1, _treeWeights, 1)
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldGBTModel = {
    new OldGBTModel(OldAlgo.Classification, _trees.map(_.toOld), _treeWeights)
  }

  // hard coded loss, which is not meant to be changed in the model
  private val loss = getOldLossType

  /**
   * Method to compute error or loss for every iteration of gradient boosting.
   *
   * @param dataset Dataset for validation.
   */
  @Since("2.4.0")
  def evaluateEachIteration(dataset: Dataset[_]): Array[Double] = {
    val data = extractInstances(this, dataset, Some(2))
    GradientBoostedTrees.evaluateEachIteration(data, trees, treeWeights, loss,
      OldAlgo.Classification)
  }

  @Since("2.0.0")
  override def write: MLWriter = new GBTClassificationModel.GBTClassificationModelWriter(this)
}

@Since("2.0.0")
object GBTClassificationModel extends MLReadable[GBTClassificationModel] {

  private val numFeaturesKey: String = "numFeatures"
  private val numTreesKey: String = "numTrees"

  @Since("2.0.0")
  override def read: MLReader[GBTClassificationModel] = new GBTClassificationModelReader

  @Since("2.0.0")
  override def load(path: String): GBTClassificationModel = super.load(path)

  private[GBTClassificationModel]
  class GBTClassificationModelWriter(instance: GBTClassificationModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {

      val extraMetadata: JObject = Map(
        numFeaturesKey -> instance.numFeatures,
        numTreesKey -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sparkSession, extraMetadata)
    }
  }

  private class GBTClassificationModelReader extends MLReader[GBTClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GBTClassificationModel].getName
    private val treeClassName = classOf[DecisionTreeRegressionModel].getName

    override def load(path: String): GBTClassificationModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], treeWeights: Array[Double]) =
        EnsembleModelReadWrite.loadImpl(path, sparkSession, className, treeClassName)
      val numFeatures = (metadata.metadata \ numFeaturesKey).extract[Int]
      val numTrees = (metadata.metadata \ numTreesKey).extract[Int]

      val trees = treesData.map {
        case (treeMetadata, root) =>
          val tree = new DecisionTreeRegressionModel(treeMetadata.uid, root, numFeatures)
          treeMetadata.getAndSetParams(tree)
          tree
      }
      require(numTrees == trees.length, s"GBTClassificationModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")
      val model = new GBTClassificationModel(metadata.uid,
        trees, treeWeights, numFeatures)
      // We ignore the impurity while loading models because in previous models it was wrongly
      // set to gini (see SPARK-25959).
      metadata.getAndSetParams(model, Some(List("impurity")))
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldGBTModel,
      parent: GBTClassifier,
      categoricalFeatures: Map[Int, Int],
      numFeatures: Int = -1,
      numClasses: Int = 2): GBTClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification, "Cannot convert GradientBoostedTreesModel" +
      s" with algo=${oldModel.algo} (old API) to GBTClassificationModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeRegressionModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("gbtc")
    new GBTClassificationModel(uid, newTrees, oldModel.treeWeights, numFeatures, numClasses)
  }
}

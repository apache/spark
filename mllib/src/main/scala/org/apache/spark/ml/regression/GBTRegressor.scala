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

package org.apache.spark.ml.regression

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.GradientBoostedTrees
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel => OldGBTModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

/**
 * <a href="http://en.wikipedia.org/wiki/Gradient_boosting">Gradient-Boosted Trees (GBTs)</a>
 * learning algorithm for regression.
 * It supports both continuous and categorical features.
 *
 * The implementation is based upon: J.H. Friedman. "Stochastic Gradient Boosting." 1999.
 *
 * Notes on Gradient Boosting vs. TreeBoost:
 *  - This implementation is for Stochastic Gradient Boosting, not for TreeBoost.
 *  - Both algorithms learn tree ensembles by minimizing loss functions.
 *  - TreeBoost (Friedman, 1999) additionally modifies the outputs at tree leaf nodes
 *    based on the loss function, whereas the original gradient boosting method does not.
 *     - When the loss is SquaredError, these methods give the same result, but they could differ
 *       for other loss functions.
 *  - We expect to implement TreeBoost in the future:
 *    [https://issues.apache.org/jira/browse/SPARK-4240]
 */
@Since("1.4.0")
class GBTRegressor @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Predictor[Vector, GBTRegressor, GBTRegressionModel]
  with GBTRegressorParams with DefaultParamsWritable with Logging {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("gbtr"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeRegressorParams:

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertSetParam */
  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

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
  override def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /**
   * The impurity setting is ignored for GBT models.
   * Individual trees are built using impurity "Variance."
   *
   * @group setParam
   */
  @Since("1.4.0")
  override def setImpurity(value: String): this.type = {
    logWarning("GBTRegressor.setImpurity should NOT be used")
    this
  }

  // Parameters from TreeEnsembleParams:

  /** @group setParam */
  @Since("1.4.0")
  override def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setSeed(value: Long): this.type = set(seed, value)

  // Parameters from GBTParams:

  /** @group setParam */
  @Since("1.4.0")
  override def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.4.0")
  override def setStepSize(value: Double): this.type = set(stepSize, value)

  // Parameters from GBTRegressorParams:

  /** @group setParam */
  @Since("1.4.0")
  def setLossType(value: String): this.type = set(lossType, value)

  /** @group setParam */
  @Since("2.3.0")
  override def setFeatureSubsetStrategy(value: String): this.type =
    set(featureSubsetStrategy, value)

  /** @group setParam */
  @Since("2.4.0")
  def setValidationIndicatorCol(value: String): this.type = {
    set(validationIndicatorCol, value)
  }

  override protected def train(dataset: Dataset[_]): GBTRegressionModel = instrumented { instr =>
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))

    val withValidation = isDefined(validationIndicatorCol) && $(validationIndicatorCol).nonEmpty

    val (trainDataset, validationDataset) = if (withValidation) {
      (
        extractLabeledPoints(dataset.filter(not(col($(validationIndicatorCol))))),
        extractLabeledPoints(dataset.filter(col($(validationIndicatorCol))))
      )
    } else {
      (extractLabeledPoints(dataset), null)
    }
    val numFeatures = trainDataset.first().features.size
    val boostingStrategy = super.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Regression)

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, featuresCol, predictionCol, impurity, lossType,
      maxDepth, maxBins, maxIter, maxMemoryInMB, minInfoGain, minInstancesPerNode,
      seed, stepSize, subsamplingRate, cacheNodeIds, checkpointInterval, featureSubsetStrategy)
    instr.logNumFeatures(numFeatures)

    val (baseLearners, learnerWeights) = if (withValidation) {
      GradientBoostedTrees.runWithValidation(trainDataset, validationDataset, boostingStrategy,
        $(seed), $(featureSubsetStrategy))
    } else {
      GradientBoostedTrees.run(trainDataset, boostingStrategy,
        $(seed), $(featureSubsetStrategy))
    }
    new GBTRegressionModel(uid, baseLearners, learnerWeights, numFeatures)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): GBTRegressor = defaultCopy(extra)
}

@Since("1.4.0")
object GBTRegressor extends DefaultParamsReadable[GBTRegressor] {

  /** Accessor for supported loss settings: squared (L2), absolute (L1) */
  @Since("1.4.0")
  final val supportedLossTypes: Array[String] = GBTRegressorParams.supportedLossTypes

  @Since("2.0.0")
  override def load(path: String): GBTRegressor = super.load(path)
}

/**
 * <a href="http://en.wikipedia.org/wiki/Gradient_boosting">Gradient-Boosted Trees (GBTs)</a>
 * model for regression.
 * It supports both continuous and categorical features.
 * @param _trees  Decision trees in the ensemble.
 * @param _treeWeights  Weights for the decision trees in the ensemble.
 */
@Since("1.4.0")
class GBTRegressionModel private[ml](
    override val uid: String,
    private val _trees: Array[DecisionTreeRegressionModel],
    private val _treeWeights: Array[Double],
    override val numFeatures: Int)
  extends PredictionModel[Vector, GBTRegressionModel]
  with GBTRegressorParams with TreeEnsembleModel[DecisionTreeRegressionModel]
  with MLWritable with Serializable {

  require(_trees.nonEmpty, "GBTRegressionModel requires at least 1 tree.")
  require(_trees.length == _treeWeights.length, "GBTRegressionModel given trees, treeWeights of" +
    s" non-matching lengths (${_trees.length}, ${_treeWeights.length}, respectively).")

  /**
   * Construct a GBTRegressionModel
   * @param _trees  Decision trees in the ensemble.
   * @param _treeWeights  Weights for the decision trees in the ensemble.
   */
  @Since("1.4.0")
  def this(uid: String, _trees: Array[DecisionTreeRegressionModel], _treeWeights: Array[Double]) =
    this(uid, _trees, _treeWeights, -1)

  @Since("1.4.0")
  override def trees: Array[DecisionTreeRegressionModel] = _trees

  /**
   * Number of trees in ensemble
   */
  @Since("2.0.0")
  val getNumTrees: Int = trees.length

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  override protected def transformImpl(dataset: Dataset[_]): DataFrame = {
    val bcastModel = dataset.sparkSession.sparkContext.broadcast(this)
    val predictUDF = udf { (features: Any) =>
      bcastModel.value.predict(features.asInstanceOf[Vector])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override def predict(features: Vector): Double = {
    // TODO: When we add a generic Boosting class, handle transform there?  SPARK-7129
    // Classifies by thresholding sum of weighted tree predictions
    val treePredictions = _trees.map(_.rootNode.predictImpl(features).prediction)
    blas.ddot(getNumTrees, treePredictions, 1, _treeWeights, 1)
  }

  /**
   * Number of trees in ensemble
   *
   * @deprecated  Use [[getNumTrees]] instead. This method will be removed in 3.0.0.
   */
  @deprecated("Use getNumTrees instead. This method will be removed in 3.0.0.", "2.4.5")
  val numTrees: Int = trees.length

  @Since("1.4.0")
  override def copy(extra: ParamMap): GBTRegressionModel = {
    copyValues(new GBTRegressionModel(uid, _trees, _treeWeights, numFeatures),
      extra).setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"GBTRegressionModel (uid=$uid) with $getNumTrees trees"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
   * (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
   * and follows the implementation from scikit-learn.
   *
   * @see `DecisionTreeRegressionModel.featureImportances`
   */
  @Since("2.0.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(trees, numFeatures)

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldGBTModel = {
    new OldGBTModel(OldAlgo.Regression, _trees.map(_.toOld), _treeWeights)
  }

  /**
   * Method to compute error or loss for every iteration of gradient boosting.
   *
   * @param dataset Dataset for validation.
   * @param loss The loss function used to compute error. Supported options: squared, absolute
   */
  @Since("2.4.0")
  def evaluateEachIteration(dataset: Dataset[_], loss: String): Array[Double] = {
    val data = dataset.select(col($(labelCol)), col($(featuresCol))).rdd.map {
      case Row(label: Double, features: Vector) => LabeledPoint(label, features)
    }
    GradientBoostedTrees.evaluateEachIteration(data, trees, treeWeights,
      convertToOldLossType(loss), OldAlgo.Regression)
  }

  @Since("2.0.0")
  override def write: MLWriter = new GBTRegressionModel.GBTRegressionModelWriter(this)
}

@Since("2.0.0")
object GBTRegressionModel extends MLReadable[GBTRegressionModel] {

  @Since("2.0.0")
  override def read: MLReader[GBTRegressionModel] = new GBTRegressionModelReader

  @Since("2.0.0")
  override def load(path: String): GBTRegressionModel = super.load(path)

  private[GBTRegressionModel]
  class GBTRegressionModelWriter(instance: GBTRegressionModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMetadata: JObject = Map(
        "numFeatures" -> instance.numFeatures,
        "numTrees" -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sparkSession, extraMetadata)
    }
  }

  private class GBTRegressionModelReader extends MLReader[GBTRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GBTRegressionModel].getName
    private val treeClassName = classOf[DecisionTreeRegressionModel].getName

    override def load(path: String): GBTRegressionModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], treeWeights: Array[Double]) =
        EnsembleModelReadWrite.loadImpl(path, sparkSession, className, treeClassName)

      val numFeatures = (metadata.metadata \ "numFeatures").extract[Int]
      val numTrees = (metadata.metadata \ "numTrees").extract[Int]

      val trees: Array[DecisionTreeRegressionModel] = treesData.map {
        case (treeMetadata, root) =>
          val tree =
            new DecisionTreeRegressionModel(treeMetadata.uid, root, numFeatures)
          treeMetadata.getAndSetParams(tree)
          tree
      }

      require(numTrees == trees.length, s"GBTRegressionModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")

      val model = new GBTRegressionModel(metadata.uid, trees, treeWeights, numFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldGBTModel,
      parent: GBTRegressor,
      categoricalFeatures: Map[Int, Int],
      numFeatures: Int = -1): GBTRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression, "Cannot convert GradientBoostedTreesModel" +
      s" with algo=${oldModel.algo} (old API) to GBTRegressionModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeRegressionModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("gbtr")
    new GBTRegressionModel(uid, newTrees, oldModel.treeWeights, numFeatures)
  }
}

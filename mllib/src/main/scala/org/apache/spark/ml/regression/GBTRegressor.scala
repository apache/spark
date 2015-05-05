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

import org.apache.spark.Logging
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor}
import org.apache.spark.ml.impl.tree._
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.tree.{DecisionTreeModel, TreeEnsembleModel}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.loss.{AbsoluteError => OldAbsoluteError, Loss => OldLoss, SquaredError => OldSquaredError}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel => OldGBTModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)]]
 * learning algorithm for regression.
 * It supports both continuous and categorical features.
 */
@AlphaComponent
final class GBTRegressor
  extends Predictor[Vector, GBTRegressor, GBTRegressionModel]
  with GBTParams with TreeRegressorParams with Logging {

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeRegressorParams:

  override def setMaxDepth(value: Int): this.type = super.setMaxDepth(value)

  override def setMaxBins(value: Int): this.type = super.setMaxBins(value)

  override def setMinInstancesPerNode(value: Int): this.type =
    super.setMinInstancesPerNode(value)

  override def setMinInfoGain(value: Double): this.type = super.setMinInfoGain(value)

  override def setMaxMemoryInMB(value: Int): this.type = super.setMaxMemoryInMB(value)

  override def setCacheNodeIds(value: Boolean): this.type = super.setCacheNodeIds(value)

  override def setCheckpointInterval(value: Int): this.type = super.setCheckpointInterval(value)

  /**
   * The impurity setting is ignored for GBT models.
   * Individual trees are built using impurity "Variance."
   */
  override def setImpurity(value: String): this.type = {
    logWarning("GBTRegressor.setImpurity should NOT be used")
    this
  }

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(value: Double): this.type = super.setSubsamplingRate(value)

  override def setSeed(value: Long): this.type = {
    logWarning("The 'seed' parameter is currently ignored by Gradient Boosting.")
    super.setSeed(value)
  }

  // Parameters from GBTParams:

  override def setMaxIter(value: Int): this.type = super.setMaxIter(value)

  override def setStepSize(value: Double): this.type = super.setStepSize(value)

  // Parameters for GBTRegressor:

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "squared" (L2) and "absolute" (L1)
   * (default = squared)
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTRegressor.supportedLossTypes.mkString(", ")}",
    (value: String) => GBTRegressor.supportedLossTypes.contains(value.toLowerCase))

  setDefault(lossType -> "squared")

  /** @group setParam */
  def setLossType(value: String): this.type = set(lossType, value)

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    getLossType match {
      case "squared" => OldSquaredError
      case "absolute" => OldAbsoluteError
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTRegressorParams was given bad loss type: $getLossType")
    }
  }

  override protected def train(dataset: DataFrame): GBTRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val boostingStrategy = super.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Regression)
    val oldGBT = new OldGBT(boostingStrategy)
    val oldModel = oldGBT.run(oldDataset)
    GBTRegressionModel.fromOld(oldModel, this, categoricalFeatures)
  }
}

object GBTRegressor {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: squared (L2), absolute (L1) */
  final val supportedLossTypes: Array[String] = Array("squared", "absolute").map(_.toLowerCase)
}

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)]]
 * model for regression.
 * It supports both continuous and categorical features.
 * @param _trees  Decision trees in the ensemble.
 * @param _treeWeights  Weights for the decision trees in the ensemble.
 */
@AlphaComponent
final class GBTRegressionModel(
    override val parent: GBTRegressor,
    private val _trees: Array[DecisionTreeRegressionModel],
    private val _treeWeights: Array[Double])
  extends PredictionModel[Vector, GBTRegressionModel]
  with TreeEnsembleModel with Serializable {

  require(numTrees > 0, "GBTRegressionModel requires at least 1 tree.")
  require(_trees.length == _treeWeights.length, "GBTRegressionModel given trees, treeWeights of" +
    s" non-matching lengths (${_trees.length}, ${_treeWeights.length}, respectively).")

  override def trees: Array[DecisionTreeModel] = _trees.asInstanceOf[Array[DecisionTreeModel]]

  override def treeWeights: Array[Double] = _treeWeights

  override protected def predict(features: Vector): Double = {
    // TODO: Override transform() to broadcast model. SPARK-7127
    // TODO: When we add a generic Boosting class, handle transform there?  SPARK-7129
    // Classifies by thresholding sum of weighted tree predictions
    val treePredictions = _trees.map(_.rootNode.predict(features))
    val prediction = blas.ddot(numTrees, treePredictions, 1, _treeWeights, 1)
    if (prediction > 0.0) 1.0 else 0.0
  }

  override def copy(extra: ParamMap): GBTRegressionModel = {
    copyValues(new GBTRegressionModel(parent, _trees, _treeWeights), extra)
  }

  override def toString: String = {
    s"GBTRegressionModel with $numTrees trees"
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldGBTModel = {
    new OldGBTModel(OldAlgo.Regression, _trees.map(_.toOld), _treeWeights)
  }
}

private[ml] object GBTRegressionModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
      oldModel: OldGBTModel,
      parent: GBTRegressor,
      categoricalFeatures: Map[Int, Int]): GBTRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression, "Cannot convert GradientBoostedTreesModel" +
      s" with algo=${oldModel.algo} (old API) to GBTRegressionModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent, fittingParamMap for each tree is null since there are no good ways to set these.
      DecisionTreeRegressionModel.fromOld(tree, null, categoricalFeatures)
    }
    new GBTRegressionModel(parent, newTrees, oldModel.treeWeights)
  }
}

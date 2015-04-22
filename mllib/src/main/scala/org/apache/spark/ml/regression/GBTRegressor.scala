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
import org.apache.spark.ml.param.{Params, ParamMap, Param}
import org.apache.spark.ml.tree.{DecisionTreeModel, TreeEnsembleModel}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.loss.{AbsoluteError => OldAbsoluteError, Loss => OldLoss,
  SquaredError => OldSquaredError}
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

  override def setLearningRate(value: Double): this.type = super.setLearningRate(value)

  // Parameters for GBTRegressor:

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "SquaredError" and "AbsoluteError"
   * (default = SquaredError)
   * @group param
   */
  val loss: Param[String] = new Param[String](this, "loss", "Loss function which GBT tries to" +
    " minimize (case-insensitive). Supported options: SquaredError, AbsoluteError")

  setDefault(loss -> "squarederror")

  /** @group setParam */
  def setLoss(value: String): this.type = {
    val lossStr = value.toLowerCase
    require(GBTRegressor.supportedLosses.contains(lossStr), "GBTRegressor was given bad loss:" +
      s" $value. Supported options: ${GBTRegressor.supportedLosses.mkString(", ")}")
    set(loss, lossStr)
    this
  }

  /** @group getParam */
  def getLoss: String = getOrDefault(loss)

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLoss: OldLoss = {
    getLoss match {
      case "squarederror" => OldSquaredError
      case "absoluteerror" => OldAbsoluteError
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTRegressorParams was given bad loss: $getLoss")
    }
  }

  override protected def train(
      dataset: DataFrame,
      paramMap: ParamMap): GBTRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema(paramMap(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset, paramMap)
    val boostingStrategy = super.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Regression)
    val oldGBT = new OldGBT(boostingStrategy)
    val oldModel = oldGBT.run(oldDataset)
    GBTRegressionModel.fromOld(oldModel, this, paramMap, categoricalFeatures)
  }
}

object GBTRegressor {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings */
  final val supportedLosses: Array[String] =
    Array("squarederror", "absoluteerror").map(_.toLowerCase)
}

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)]]
 * model for regression.
 * It supports both continuous and categorical features.
 * @param trees  Decision trees in the ensemble.
 * @param treeWeights  Weights for the decision trees in the ensemble.
 */
@AlphaComponent
final class GBTRegressionModel(
    override val parent: GBTRegressor,
    override val fittingParamMap: ParamMap,
    val trees: Array[DecisionTreeRegressionModel],
    val treeWeights: Array[Double])
  extends PredictionModel[Vector, GBTRegressionModel]
  with TreeEnsembleModel with Serializable {

  require(numTrees > 0, "GBTRegressionModel requires at least 1 tree.")
  require(trees.length == treeWeights.length, "GBTRegressionModel given trees, treeWeights of" +
    s" non-matching lengths (${trees.length}, ${treeWeights.length}, respectively).")

  override def getTrees: Array[DecisionTreeModel] = trees.asInstanceOf[Array[DecisionTreeModel]]

  override def getTreeWeights: Array[Double] = treeWeights

  override protected def predict(features: Vector): Double = {
    // TODO: Override transform() to broadcast model.
    // Classifies by thresholding sum of weighted tree predictions
    val treePredictions = trees.map(_.rootNode.predict(features))
    val prediction = blas.ddot(numTrees, treePredictions, 1, treeWeights, 1)
    if (prediction > 0.0) 1.0 else 0.0
  }

  override protected def copy(): GBTRegressionModel = {
    val m = new GBTRegressionModel(parent, fittingParamMap, trees, treeWeights)
    Params.inheritValues(this.extractParamMap(), this, m)
    m
  }

  override def toString: String = {
    s"GBTRegressionModel with $numTrees trees"
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldGBTModel = {
    new OldGBTModel(OldAlgo.Regression, trees.map(_.toOld), treeWeights)
  }
}

private[ml] object GBTRegressionModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
      oldModel: OldGBTModel,
      parent: GBTRegressor,
      fittingParamMap: ParamMap,
      categoricalFeatures: Map[Int, Int]): GBTRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression, "Cannot convert GradientBoostedTreesModel" +
      s" with algo=${oldModel.algo} (old API) to GBTRegressionModel (new API).")
    val trees = oldModel.trees.map { tree =>
      // parent, fittingParamMap for each tree is null since there are no good ways to set these.
      DecisionTreeRegressionModel.fromOld(tree, null, null, categoricalFeatures)
    }
    new GBTRegressionModel(parent, fittingParamMap, trees, oldModel.treeWeights)
  }
}

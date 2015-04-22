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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor}
import org.apache.spark.ml.impl.tree.{RandomForestParams, TreeRegressorParams}
import org.apache.spark.ml.param.{Params, ParamMap}
import org.apache.spark.ml.tree.{DecisionTreeModel, TreeEnsembleModel}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] learning algorithm for regression.
 * It supports both continuous and categorical features.
 */
@AlphaComponent
final class RandomForestRegressor
  extends Predictor[Vector, RandomForestRegressor, RandomForestRegressionModel]
  with RandomForestParams with TreeRegressorParams {

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

  override def setImpurity(value: String): this.type = super.setImpurity(value)

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(value: Double): this.type = super.setSubsamplingRate(value)

  override def setSeed(value: Long): this.type = super.setSeed(value)

  // Parameters from RandomForestParams:

  override def setNumTrees(value: Int): this.type = super.setNumTrees(value)

  override def setFeaturesPerNode(value: String): this.type = super.setFeaturesPerNode(value)

  override protected def train(
      dataset: DataFrame,
      paramMap: ParamMap): RandomForestRegressionModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema(paramMap(featuresCol)))
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset, paramMap)
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses = 0, OldAlgo.Regression, getOldImpurity)
    val oldModel = OldRandomForest.trainRegressor(
      oldDataset, strategy, getNumTrees, getFeaturesPerNodeStr, getSeed.toInt)
    RandomForestRegressionModel.fromOld(oldModel, this, paramMap, categoricalFeatures)
  }
}

object RandomForestRegressor {
  /** Accessor for supported impurity settings */
  final val supportedImpurities: Array[String] = TreeRegressorParams.supportedImpurities

  /** Accessor for supported featuresPerNode settings */
  final val supportedFeaturesPerNode: Array[String] = RandomForestParams.supportedFeaturesPerNode
}

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] model for regression.
 * It supports both continuous and categorical features.
 * @param trees  Decision trees in the ensemble.
 */
@AlphaComponent
final class RandomForestRegressionModel private[ml] (
    override val parent: RandomForestRegressor,
    override val fittingParamMap: ParamMap,
    val trees: Array[DecisionTreeRegressionModel])
  extends PredictionModel[Vector, RandomForestRegressionModel]
  with TreeEnsembleModel with Serializable {

  require(numTrees > 0, "RandomForestRegressionModel requires at least 1 tree.")

  override def getTrees: Array[DecisionTreeModel] = trees.asInstanceOf[Array[DecisionTreeModel]]

  // Note: We may add support for weights (based on tree performance) later on.
  override lazy val getTreeWeights: Array[Double] = Array.fill[Double](numTrees)(1.0)

  override protected def predict(features: Vector): Double = {
    // TODO: Override transform() to broadcast model.
    // TODO: When we add a generic Bagging class, handle transform there. Skip single-Row predict.
    // Predict average of tree predictions.
    // Ignore the weights since all are 1.0 for now.
    trees.map(_.rootNode.predict(features)).sum / numTrees
  }

  override protected def copy(): RandomForestRegressionModel = {
    val m = new RandomForestRegressionModel(parent, fittingParamMap, trees)
    Params.inheritValues(this.extractParamMap(), this, m)
    m
  }

  override def toString: String = {
    s"RandomForestRegressionModel with $numTrees trees"
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Regression, trees.map(_.toOld))
  }
}

private[ml] object RandomForestRegressionModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
      oldModel: OldRandomForestModel,
      parent: RandomForestRegressor,
      fittingParamMap: ParamMap,
      categoricalFeatures: Map[Int, Int]): RandomForestRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression, "Cannot convert RandomForestModel" +
      s" with algo=${oldModel.algo} (old API) to RandomForestRegressionModel (new API).")
    val trees = oldModel.trees.map { tree =>
      // parent, fittingParamMap for each tree is null since there are no good ways to set these.
      DecisionTreeRegressionModel.fromOld(tree, null, null, categoricalFeatures)
    }
    new RandomForestRegressionModel(parent, fittingParamMap, trees)
  }
}

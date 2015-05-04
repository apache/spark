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

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.Logging
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor}
import org.apache.spark.ml.impl.tree._
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree.{DecisionTreeModel, TreeEnsembleModel}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.loss.{LogLoss => OldLogLoss, Loss => OldLoss}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel => OldGBTModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)]]
 * learning algorithm for classification.
 * It supports binary labels, as well as both continuous and categorical features.
 * Note: Multiclass labels are not currently supported.
 */
@AlphaComponent
final class GBTClassifier
  extends Predictor[Vector, GBTClassifier, GBTClassificationModel]
  with GBTParams with TreeClassifierParams with Logging {

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

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
    logWarning("GBTClassifier.setImpurity should NOT be used")
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

  // Parameters for GBTClassifier:

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "logistic"
   * (default = logistic)
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTClassifier.supportedLossTypes.mkString(", ")}",
    (value: String) => GBTClassifier.supportedLossTypes.contains(value.toLowerCase))

  setDefault(lossType -> "logistic")

  /** @group setParam */
  def setLossType(value: String): this.type = set(lossType, value)

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    getLossType match {
      case "logistic" => OldLogLoss
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTClassifier was given bad loss type: $getLossType")
    }
  }

  override protected def train(dataset: DataFrame): GBTClassificationModel = {
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => n
      case None => throw new IllegalArgumentException("GBTClassifier was given input" +
        s" with invalid label column ${$(labelCol)}, without the number of classes" +
        " specified. See StringIndexer.")
      // TODO: Automatically index labels: SPARK-7126
    }
    require(numClasses == 2,
      s"GBTClassifier only supports binary classification but was given numClasses = $numClasses")
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
    val boostingStrategy = super.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Classification)
    val oldGBT = new OldGBT(boostingStrategy)
    val oldModel = oldGBT.run(oldDataset)
    GBTClassificationModel.fromOld(oldModel, this, categoricalFeatures)
  }
}

object GBTClassifier {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: logistic */
  final val supportedLossTypes: Array[String] = Array("logistic").map(_.toLowerCase)
}

/**
 * :: AlphaComponent ::
 *
 * [[http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)]]
 * model for classification.
 * It supports binary labels, as well as both continuous and categorical features.
 * Note: Multiclass labels are not currently supported.
 * @param _trees  Decision trees in the ensemble.
 * @param _treeWeights  Weights for the decision trees in the ensemble.
 */
@AlphaComponent
final class GBTClassificationModel(
    override val parent: GBTClassifier,
    private val _trees: Array[DecisionTreeRegressionModel],
    private val _treeWeights: Array[Double])
  extends PredictionModel[Vector, GBTClassificationModel]
  with TreeEnsembleModel with Serializable {

  require(numTrees > 0, "GBTClassificationModel requires at least 1 tree.")
  require(_trees.length == _treeWeights.length, "GBTClassificationModel given trees, treeWeights" +
    s" of non-matching lengths (${_trees.length}, ${_treeWeights.length}, respectively).")

  override def trees: Array[DecisionTreeModel] = _trees.asInstanceOf[Array[DecisionTreeModel]]

  override def treeWeights: Array[Double] = _treeWeights

  override protected def predict(features: Vector): Double = {
    // TODO: Override transform() to broadcast model: SPARK-7127
    // TODO: When we add a generic Boosting class, handle transform there?  SPARK-7129
    // Classifies by thresholding sum of weighted tree predictions
    val treePredictions = _trees.map(_.rootNode.predict(features))
    val prediction = blas.ddot(numTrees, treePredictions, 1, _treeWeights, 1)
    if (prediction > 0.0) 1.0 else 0.0
  }

  override def copy(extra: ParamMap): GBTClassificationModel = {
    copyValues(new GBTClassificationModel(parent, _trees, _treeWeights), extra)
  }

  override def toString: String = {
    s"GBTClassificationModel with $numTrees trees"
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldGBTModel = {
    new OldGBTModel(OldAlgo.Classification, _trees.map(_.toOld), _treeWeights)
  }
}

private[ml] object GBTClassificationModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
      oldModel: OldGBTModel,
      parent: GBTClassifier,
      categoricalFeatures: Map[Int, Int]): GBTClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification, "Cannot convert GradientBoostedTreesModel" +
      s" with algo=${oldModel.algo} (old API) to GBTClassificationModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent, fittingParamMap for each tree is null since there are no good ways to set these.
      DecisionTreeRegressionModel.fromOld(tree, null, categoricalFeatures)
    }
    new GBTClassificationModel(parent, newTrees, oldModel.treeWeights)
  }
}

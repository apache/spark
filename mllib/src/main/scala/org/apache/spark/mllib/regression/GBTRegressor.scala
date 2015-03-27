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

package org.apache.spark.mllib.regression

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.{GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo,
  BoostingStrategy => OldBoostingStrategy}
import org.apache.spark.mllib.tree.loss.{AbsoluteError => OldAbsoluteError, Loss => OldLoss,
  SquaredError => OldSquaredError}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel => OldGradientBoostedTreesModel}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD


class GBTRegressor
  extends TreeRegressorWithValidate[GBTRegressionModel]
  with GBTParams[GBTRegressor]
  with TreeRegressorParams[GBTRegressor]
  with Logging {

  protected var lossStr: String = "squarederror"

  /**
   * Loss function which GBT tries to minimize.
   * Supported: "SquaredError" and "AbsoluteError"
   * (default = SquaredError)
   * @param loss  String for loss (case-insensitive)
   * @group setParam
   */
  def setLoss(loss: String): GBTRegressor = {
    val lossStr = loss.toLowerCase
    require(GBTRegressor.supportedLosses.contains(lossStr),
      s"GBTRegressor was given bad loss: $loss." +
        s"  Supported options: ${GBTRegressor.supportedLosses.mkString(", ")}")
    this.lossStr = lossStr
    this
  }

  /**
   * Loss function which GBT tries to minimize.
   * Supported: "SquaredError" and "AbsoluteError"
   * (default = SquaredError)
   * @group getParam
   */
  def getLossStr: String = lossStr

  /** Convert new loss to old loss. */
  override protected def getOldLoss: OldLoss = {
    lossStr match {
      case "squarederror" => OldSquaredError
      case "absoluteerror" => OldAbsoluteError
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTRegressorParams was given bad loss: $lossStr")
    }
  }

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeRegressorParams:

  override def setMaxDepth(maxDepth: Int): GBTRegressor = super.setMaxDepth(maxDepth)

  override def setMaxBins(maxBins: Int): GBTRegressor = super.setMaxBins(maxBins)

  override def setMinInstancesPerNode(minInstancesPerNode: Int): GBTRegressor =
    super.setMinInstancesPerNode(minInstancesPerNode)

  override def setMinInfoGain(minInfoGain: Double): GBTRegressor =
    super.setMinInfoGain(minInfoGain)

  override def setMaxMemoryInMB(maxMemoryInMB: Int): GBTRegressor =
    super.setMaxMemoryInMB(maxMemoryInMB)

  override def setCacheNodeIds(cacheNodeIds: Boolean): GBTRegressor =
    super.setCacheNodeIds(cacheNodeIds)

  override def setCheckpointInterval(checkpointInterval: Int): GBTRegressor =
    super.setCheckpointInterval(checkpointInterval)

  /**
   * The impurity setting is ignored for GBT models.
   * Individual trees are built using impurity "Variance."
   */
  override def setImpurity(impurity: String): GBTRegressor = {
    logWarning("GBTRegressor.setImpurity should NOT be used")
    this
  }

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(subsamplingRate: Double): GBTRegressor =
    super.setSubsamplingRate(subsamplingRate)

  /** WARNING: This parameter is currently ignored by Gradient Boosting. It will be added later. */
  override def setSeed(seed: Long): GBTRegressor = super.setSeed(seed)

  // Parameters from GBTParams:

  override def setNumIterations(numIterations: Int): GBTRegressor =
    super.setNumIterations(numIterations)

  override def setLearningRate(learningRate: Double): GBTRegressor =
    super.setLearningRate(learningRate)

  override def setValidationTol(validationTol: Double): GBTRegressor =
    super.setValidationTol(validationTol)

  override def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int]): GBTRegressionModel = {
    val boostingStrategy = getOldBoostingStrategy(categoricalFeatures)
    val oldGBT = new OldGBT(boostingStrategy)
    val oldModel = oldGBT.run(input)
    GBTRegressionModel.fromOld(oldModel)
  }

  override def runWithValidation(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int]): GBTRegressionModel = {
    val boostingStrategy = getOldBoostingStrategy(categoricalFeatures)
    val oldGBT = new OldGBT(boostingStrategy)
    val oldModel = oldGBT.runWithValidation(input, validationInput)
    GBTRegressionModel.fromOld(oldModel)
  }

  /**
   * Create a BoostingStrategy instance to use with the old API.
   * TODO: Remove once we move implementation to new API.
   */
  override private[mllib] def getOldBoostingStrategy(
      categoricalFeatures: Map[Int, Int]): OldBoostingStrategy = {
    val strategy = super.getOldBoostingStrategy(categoricalFeatures)
    strategy.treeStrategy.algo = OldAlgo.Regression
    strategy
  }
}

object GBTRegressor {

  // The losses below should be lowercase.
  /** Accessor for supported loss settings */
  final val supportedLosses: Array[String] = Array("squarederror", "absoluteerror")
}

class GBTRegressionModel(
    val trees: Array[DecisionTreeRegressionModel],
    val treeWeights: Array[Double])
  extends TreeEnsembleModel with Serializable with Saveable {

  override def getTrees: Array[DecisionTreeModel] = trees.asInstanceOf[Array[DecisionTreeModel]]

  override def getTreeWeights: Array[Double] = treeWeights

  override def predict(features: Vector): Double = {
    // Classifies by thresholding sum of weighted tree predictions
    val treePredictions = trees.map(_.predict(features))
    val prediction = blas.ddot(numTrees, treePredictions, 1, treeWeights, 1)
    if (prediction > 0.0) 1.0 else 0.0
  }

  override def toString: String = {
    s"GBTRegressionModel with $numTrees trees"
  }

  override def save(sc: SparkContext, path: String): Unit = {
    this.toOld.save(sc, path)
  }

  override protected def formatVersion: String = OldGradientBoostedTreesModel.formatVersion

  /** Convert to a model in the old API */
  private[mllib] def toOld: OldGradientBoostedTreesModel = {
    new OldGradientBoostedTreesModel(OldAlgo.Regression, trees.map(_.toOld), treeWeights)
  }
}

object GBTRegressionModel extends Loader[GBTRegressionModel] {

  override def load(sc: SparkContext, path: String): GBTRegressionModel = {
    GBTRegressionModel.fromOld(OldGradientBoostedTreesModel.load(sc, path))
  }

  private[mllib] def fromOld(oldModel: OldGradientBoostedTreesModel): GBTRegressionModel = {
    require(oldModel.algo == OldAlgo.Regression,
      s"Cannot convert non-regression GradientBoostedTreesModel (old API) to" +
        s" GBTRegressionModel (new API).  Algo is: ${oldModel.algo}")
    new GBTRegressionModel(oldModel.trees.map(DecisionTreeRegressionModel.fromOld),
      oldModel.treeWeights)
  }
}

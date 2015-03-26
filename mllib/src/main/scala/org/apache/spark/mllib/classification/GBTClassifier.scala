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

package org.apache.spark.mllib.classification

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.Logging
import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{DecisionTreeRegressionModel, LabeledPoint}
import org.apache.spark.mllib.tree.{GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.loss.{Loss => OldLoss, LogLoss => OldLogLoss}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel => OldGradientBoostedTreesModel}
import org.apache.spark.rdd.RDD


class GBTClassifier
  extends TreeClassifierWithValidate[GBTClassificationModel]
  with GBTParams[GBTClassifier] with TreeClassifierParams[GBTClassifier]
  with Logging {

  protected var lossStr: String = "LogLoss"

  /**
   * Loss function which GBT tries to minimize.
   * Supported: "LogLoss"
   * (default = LogLoss)
   * @param loss  String for loss (case-insensitive)
   * @group setParam
   */
  def setLoss(loss: String): GBTClassifier = {
    val lossStr = loss.toLowerCase
    require(GBTClassifier.supportedLosses.contains(lossStr),
      s"GBTClassifier was given bad loss: $loss." +
        s"  Supported options: ${GBTClassifier.supportedLosses.mkString(", ")}")
    this.lossStr = lossStr
    this
  }

  /**
   * Loss function which GBT tries to minimize.
   * Supported: "LogLoss"
   * (default = LogLoss)
   * @group getParam
   */
  def getLossStr: String = lossStr

  /** Convert new loss to old loss. */
  override protected def getOldLoss: OldLoss = {
    lossStr match {
      case "logloss" => OldLogLoss
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTClassifierParams was given bad loss: $lossStr")
    }
  }

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  override def setMaxDepth(maxDepth: Int): GBTClassifier = super.setMaxDepth(maxDepth)

  override def setMaxBins(maxBins: Int): GBTClassifier = super.setMaxBins(maxBins)

  override def setMinInstancesPerNode(minInstancesPerNode: Int): GBTClassifier =
    super.setMinInstancesPerNode(minInstancesPerNode)

  override def setMinInfoGain(minInfoGain: Double): GBTClassifier =
    super.setMinInfoGain(minInfoGain)

  override def setMaxMemoryInMB(maxMemoryInMB: Int): GBTClassifier =
    super.setMaxMemoryInMB(maxMemoryInMB)

  override def setCacheNodeIds(cacheNodeIds: Boolean): GBTClassifier =
    super.setCacheNodeIds(cacheNodeIds)

  override def setCheckpointInterval(checkpointInterval: Int): GBTClassifier =
    super.setCheckpointInterval(checkpointInterval)

  /**
   * The impurity setting is ignored for GBT models.
   * Individual trees are built using impurity "Variance."
   */
  override def setImpurity(impurity: String): GBTClassifier = {
    logWarning("GBTClassifier.setImpurity should NOT be used")
    this
  }

  // Parameters from TreeEnsembleParams:

  override def setSubsamplingRate(subsamplingRate: Double): GBTClassifier =
    super.setSubsamplingRate(subsamplingRate)

  /** WARNING: This parameter is currently ignored by Gradient Boosting. It will be added later. */
  override def setSeed(seed: Long): GBTClassifier = super.setSeed(seed)

  // Parameters from GBTParams:

  override def setNumIterations(numIterations: Int): GBTClassifier =
    super.setNumIterations(numIterations)

  override def setLearningRate(learningRate: Double): GBTClassifier =
    super.setLearningRate(learningRate)

  override def setValidationTol(validationTol: Double): GBTClassifier =
    super.setValidationTol(validationTol)

  override def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): GBTClassificationModel = {
    require(numClasses == 2,
      s"GBTClassifier only supports binary classification but was given numClasses = $numClasses")
    val boostingStrategy = getOldBoostingStrategy(categoricalFeatures)
    val oldGBT = new OldGBT(boostingStrategy)
    val oldModel = oldGBT.run(input)
    GBTClassificationModel.fromOld(oldModel)
  }

  override def runWithValidation(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): GBTClassificationModel = {
    require(numClasses == 2,
      s"GBTClassifier only supports binary classification but was given numClasses = $numClasses")
    val boostingStrategy = getOldBoostingStrategy(categoricalFeatures)
    val oldGBT = new OldGBT(boostingStrategy)
    val oldModel = oldGBT.runWithValidation(input, validationInput)
    GBTClassificationModel.fromOld(oldModel)
  }
}

object GBTClassifier {

  /** Accessor for supported loss settings */
  final val supportedLosses: Array[String] = Array("logloss")
}

class GBTClassificationModel(
    val trees: Array[DecisionTreeRegressionModel],
    val treeWeights: Array[Double])
  extends TreeEnsembleModel with Serializable {

  override def getTrees: Array[DecisionTreeModel] = trees.asInstanceOf[Array[DecisionTreeModel]]

  override def getTreeWeights: Array[Double] = treeWeights

  override def predict(features: Vector): Double = {
    // Classifies by thresholding sum of weighted tree predictions
    val treePredictions = trees.map(_.predict(features))
    val prediction = blas.ddot(numTrees, treePredictions, 1, treeWeights, 1)
    if (prediction > 0.0) 1.0 else 0.0
  }

  override def toString: String = {
    s"GBTClassificationModel with $numTrees trees"
  }
}

private[mllib] object GBTClassificationModel {

  def fromOld(oldModel: OldGradientBoostedTreesModel): GBTClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification,
      s"Cannot convert non-classification GradientBoostedTreesModel (old API) to" +
        s" GBTClassificationModel (new API).  Algo is: ${oldModel.algo}")
    new GBTClassificationModel(oldModel.trees.map(DecisionTreeRegressionModel.fromOld),
      oldModel.treeWeights)
  }
}

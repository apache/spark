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

package org.apache.spark.mllib.tree

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.Logging
import org.apache.spark.mllib.tree.impl.TimeTracker
import org.apache.spark.mllib.tree.loss.Losses
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{WeightedEnsembleModel, DecisionTreeModel}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.tree.configuration.EnsembleCombiningStrategy.Sum

/**
 * :: Experimental ::
 * A class that implements gradient boosting for regression problems.
 * @param boostingStrategy Parameters for the gradient boosting algorithm
 */
@Experimental
class GradientBoosting (
    private val boostingStrategy: BoostingStrategy) extends Serializable with Logging {

  /**
   * Method to train a gradient boosting model
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return GradientBoostingModel that can be used for prediction
   */
  def train(input: RDD[LabeledPoint]): WeightedEnsembleModel = {
    val algo = boostingStrategy.algo
    algo match {
      case Regression => GradientBoosting.boost(input, boostingStrategy)
      case Classification =>
        val remappedInput = input.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        GradientBoosting.boost(remappedInput, boostingStrategy)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by the gradient boosting.")
    }
  }

}


object GradientBoosting extends Logging {

  /**
   * Method to train a gradient boosting model.
   *
   * Note: Using [[org.apache.spark.mllib.tree.GradientBoosting#trainRegressor]]
   *       is recommended to clearly specify regression.
   *       Using [[org.apache.spark.mllib.tree.GradientBoosting#trainClassifier]]
   *       is recommended to clearly specify regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return GradientBoostingModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): WeightedEnsembleModel = {
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param numEstimators Number of estimators used in boosting stages. In other words,
   *                      number of boosting iterations performed.
   * @param loss Loss function used for minimization during gradient boosting.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and
   *                                the number of discrete values they take. For example,
   *                                an entry (n -> k) implies the feature n is categorical with k
   *                                categories 0, 1, 2, ... , k-1. It's important to note that
   *                                features are zero-indexed.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      categoricalFeaturesInfo: Map[Int, Int]): WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      maxDepth, learningRate, subsample, checkpointPeriod, 2,
      categoricalFeaturesInfo = categoricalFeaturesInfo)
    new GradientBoosting(boostingStrategy).train(input)
  }


  /**
   * Method to train a gradient boosting binary classification model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param numEstimators Number of estimators used in boosting stages. In other words,
   *                      number of boosting iterations performed.
   * @param loss Loss function used for minimization during gradient boosting.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param numClassesForClassification Number of classes for classification.
   *                                    (Ignored for regression.)
   *                                    Default value is 2 (binary classification).
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and
   *                                the number of discrete values they take. For example,
   *                                an entry (n -> k) implies the feature n is categorical with k
   *                                categories 0, 1, 2, ... , k-1. It's important to note that
   *                                features are zero-indexed.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      numClassesForClassification: Int,
      categoricalFeaturesInfo: Map[Int, Int]): WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      maxDepth, learningRate, subsample, checkpointPeriod, numClassesForClassification,
      categoricalFeaturesInfo = categoricalFeaturesInfo)
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.GradientBoosting#trainRegressor]]
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param numEstimators Number of estimators used in boosting stages. In other words,
   *                      number of boosting iterations performed.
   * @param loss Loss function used for minimization during gradient boosting.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and
   *                                the number of discrete values they take. For example,
   *                                an entry (n -> k) implies the feature n is categorical with k
   *                                categories 0, 1, 2, ... , k-1. It's important to note that
   *                                features are zero-indexed.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: JavaRDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer])
      : WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      maxDepth, learningRate, subsample, checkpointPeriod, 2, categoricalFeaturesInfo =
        categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.GradientBoosting#trainClassifier]]
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param numEstimators Number of estimators used in boosting stages. In other words,
   *                      number of boosting iterations performed.
   * @param loss Loss function used for minimization during gradient boosting.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param numClassesForClassification Number of classes for classification.
   *                                    (Ignored for regression.)
   *                                    Default value is 2 (binary classification).
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and
   *                                the number of discrete values they take. For example,
   *                                an entry (n -> k) implies the feature n is categorical with k
   *                                categories 0, 1, 2, ... , k-1. It's important to note that
   *                                features are zero-indexed.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassifier(
      input: JavaRDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      numClassesForClassification: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer])
      : WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      maxDepth, learningRate, subsample, checkpointPeriod,
      numClassesForClassification, categoricalFeaturesInfo =
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param numEstimators Number of estimators used in boosting stages. In other words,
   *                      number of boosting iterations performed.
   * @param loss Loss function used for minimization during gradient boosting.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double): WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      maxDepth, learningRate, subsample)
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Method to train a gradient boosting binary classification model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param numEstimators Number of estimators used in boosting stages. In other words,
   *                      number of boosting iterations performed.
   * @param loss Loss function used for minimization during gradient boosting.
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double): WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      maxDepth, learningRate, subsample)
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): WeightedEnsembleModel = {
    val algo = boostingStrategy.algo
    require(algo == Regression, s"Only Regression algo supported. Provided algo is $algo.")
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Method to train a gradient boosting classification model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): WeightedEnsembleModel = {
    val algo = boostingStrategy.algo
    require(algo == Classification, s"Only Classification algo supported. Provided algo is $algo.")
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Internal method for performing regression using trees as base learners.
   * @param input training dataset
   * @param boostingStrategy boosting parameters
   * @return
   */
  private def boost(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): WeightedEnsembleModel = {

    val timer = new TimeTracker()

    timer.start("total")
    timer.start("init")


    // Initialize gradient boosting parameters
    val numEstimators = boostingStrategy.numEstimators
    val baseLearners = new Array[DecisionTreeModel](numEstimators)
    val baseLearnerWeights = new Array[Double](numEstimators)
    val loss = boostingStrategy.loss
    val learningRate = boostingStrategy.learningRate
    val checkpointingPeriod = boostingStrategy.checkpointPeriod
    val strategy = boostingStrategy.strategy

    // Cache input
    input.persist(StorageLevel.MEMORY_AND_DISK)
    // Dataset reference for keeping track of last cached dataset in memory.
    var lastCachedData = input
    // Dataset reference for noting dataset marked for unpersisting.
    var unpersistData = lastCachedData

    timer.stop("init")

    logDebug("##########")
    logDebug("Building tree 0")
    logDebug("##########")
    var data = input

    // 1. Initialize tree
    timer.start("building tree 0")
    val firstModel = new DecisionTree(strategy).train(data)
    timer.stop("building tree 0")
    baseLearners(0) = firstModel
    baseLearnerWeights(0) = 1.0
    logDebug("error of tree = " + loss.computeError(firstModel, data))

    // psuedo-residual for second iteration
    data = data.map(point => LabeledPoint(loss.lossGradient(firstModel, point,
      learningRate), point.features))

    var m = 1
    while (m < numEstimators) {
      timer.start(s"building tree $m")
      logDebug("###################################################")
      logDebug("Gradient boosting tree iteration " + m)
      logDebug("###################################################")
      val model = new DecisionTree(strategy).train(data)
      timer.stop(s"building tree $m")
      baseLearners(m) = model
      baseLearnerWeights(m) = learningRate
      logDebug("error of tree = " + loss.computeError(model, data))
      // Update data with pseudo-residuals
      data = data.map(point => LabeledPoint(loss.lossGradient(model, point, learningRate),
        point.features))
      // Unpersist last cached dataset since a newer one has been cached in the previous iteration.
      if (m % checkpointingPeriod == 1 && m != 1) {
        logDebug(s"Unpersisting old cached dataset in iteration $m.")
        unpersistData.unpersist()
      }
      // Checkpoint
      if (m % checkpointingPeriod == 0) {
        logDebug(s"Persisting new dataset in iteration $m.")
        unpersistData = lastCachedData
        data = data.persist(StorageLevel.MEMORY_AND_DISK)
        lastCachedData = data
      }
      m += 1
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")


    // 3. Output classifier
    new WeightedEnsembleModel(baseLearners, baseLearnerWeights, boostingStrategy.algo, Sum)

  }

}

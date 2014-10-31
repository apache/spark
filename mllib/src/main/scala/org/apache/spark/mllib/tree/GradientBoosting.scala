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
import org.apache.spark.mllib.tree.configuration.{Strategy, BoostingStrategy}
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
 * A class that implements gradient boosting for regression and binary classification problems.
 * @param boostingStrategy Parameters for the gradient boosting algorithm
 */
@Experimental
class GradientBoosting (
    private val boostingStrategy: BoostingStrategy) extends Serializable with Logging {

  /**
   * Method to train a gradient boosting model
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return WeightedEnsembleModel that can be used for prediction
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
   * Note: Using [[org.apache.spark.mllib.tree.GradientBoosting$#trainRegressor]]
   *       is recommended to clearly specify regression.
   *       Using [[org.apache.spark.mllib.tree.GradientBoosting$#trainClassifier]]
   *       is recommended to clearly specify regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return WeightedEnsembleModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): WeightedEnsembleModel = {
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Method to train a gradient boosting classification model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return WeightedEnsembleModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): WeightedEnsembleModel = {
    val algo = boostingStrategy.algo
    require(algo == Classification, s"Only Classification algo supported. Provided algo is $algo.")
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return WeightedEnsembleModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): WeightedEnsembleModel = {
    val algo = boostingStrategy.algo
    require(algo == Regression, s"Only Regression algo supported. Provided algo is $algo.")
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
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsamplingRate  Fraction of the training data used for learning the decision tree.
   * @param numClassesForClassification Number of classes for classification.
   *                                    (Ignored for regression.)
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and
   *                                the number of discrete values they take. For example,
   *                                an entry (n -> k) implies the feature n is categorical with k
   *                                categories 0, 1, 2, ... , k-1. It's important to note that
   *                                features are zero-indexed.
   * @param weakLearnerParams Parameters for the weak learner. (Currently only decision tree is
   *                          supported.)
   * @return WeightedEnsembleModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      learningRate: Double,
      subsamplingRate: Double,
      numClassesForClassification: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      weakLearnerParams: Strategy): WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Classification, numEstimators, lossType,
      learningRate, subsamplingRate, numClassesForClassification, categoricalFeaturesInfo,
      weakLearnerParams)
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
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsamplingRate  Fraction of the training data used for learning the decision tree.
   * @param numClassesForClassification Number of classes for classification.
   *                                    (Ignored for regression.)
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and
   *                                the number of discrete values they take. For example,
   *                                an entry (n -> k) implies the feature n is categorical with k
   *                                categories 0, 1, 2, ... , k-1. It's important to note that
   *                                features are zero-indexed.
   * @param weakLearnerParams Parameters for the weak learner. (Currently only decision tree is
   *                          supported.)
   * @return WeightedEnsembleModel that can be used for prediction
   */
  def trainRegressor(
       input: RDD[LabeledPoint],
       numEstimators: Int,
       loss: String,
       learningRate: Double,
       subsamplingRate: Double,
       numClassesForClassification: Int,
       categoricalFeaturesInfo: Map[Int, Int],
       weakLearnerParams: Strategy): WeightedEnsembleModel = {
    val lossType = Losses.fromString(loss)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      learningRate, subsamplingRate, numClassesForClassification, categoricalFeaturesInfo,
      weakLearnerParams)
    new GradientBoosting(boostingStrategy).train(input)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.GradientBoosting$#trainClassifier]]
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      learningRate: Double,
      subsamplingRate: Double,
      numClassesForClassification: Int,
      categoricalFeaturesInfo:java.util.Map[java.lang.Integer, java.lang.Integer],
      weakLearnerParams: Strategy): WeightedEnsembleModel = {
    trainClassifier(input, numEstimators, loss, learningRate, subsamplingRate,
      numClassesForClassification,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      weakLearnerParams)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.GradientBoosting$#trainRegressor]]
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      learningRate: Double,
      subsamplingRate: Double,
      numClassesForClassification: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      weakLearnerParams: Strategy): WeightedEnsembleModel = {
    trainRegressor(input, numEstimators, loss, learningRate, subsamplingRate,
      numClassesForClassification,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      weakLearnerParams)
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
    val strategy = boostingStrategy.weakLearnerParams

    // Cache input
    input.persist(StorageLevel.MEMORY_AND_DISK)

    timer.stop("init")

    logDebug("##########")
    logDebug("Building tree 0")
    logDebug("##########")
    var data = input

    // 1. Initialize tree
    timer.start("building tree 0")
    val firstTreeModel = new DecisionTree(strategy).train(data)
    baseLearners(0) = firstTreeModel
    baseLearnerWeights(0) = 1.0
    val startingModel = new WeightedEnsembleModel(Array(firstTreeModel), Array(1.0), Regression,
      Sum)
    logDebug("error of gbt = " + loss.computeError(startingModel, input))
    // Note: A model of type regression is used since we require raw prediction
    timer.stop("building tree 0")

    // psuedo-residual for second iteration
    data = input.map(point => LabeledPoint(loss.gradient(startingModel, point),
      point.features))

    var m = 1
    while (m < numEstimators) {
      timer.start(s"building tree $m")
      logDebug("###################################################")
      logDebug("Gradient boosting tree iteration " + m)
      logDebug("###################################################")
      val model = new DecisionTree(strategy).train(data)
      timer.stop(s"building tree $m")
      // Create partial model
      baseLearners(m) = model
      baseLearnerWeights(m) = learningRate
      // Note: A model of type regression is used since we require raw prediction
      val partialModel = new WeightedEnsembleModel(baseLearners.slice(0, m + 1),
        baseLearnerWeights.slice(0, m + 1), Regression, Sum)
      logDebug("error of gbt = " + loss.computeError(partialModel, input))
      // Update data with pseudo-residuals
      data = input.map(point => LabeledPoint(-loss.gradient(partialModel, point),
        point.features))
      m += 1
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")


    // 3. Output classifier
    new WeightedEnsembleModel(baseLearners, baseLearnerWeights, boostingStrategy.algo, Sum)

  }

}

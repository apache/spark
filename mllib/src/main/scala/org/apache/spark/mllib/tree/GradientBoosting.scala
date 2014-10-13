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

import org.apache.spark.SparkContext._
import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.tree.configuration.{QuantileStrategy, BoostingStrategy}
import org.apache.spark.Logging
import org.apache.spark.mllib.tree.impl.TimeTracker
import org.apache.spark.mllib.tree.impurity.Impurities
import org.apache.spark.mllib.tree.loss.Losses
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostingModel, DecisionTreeModel}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.storage.StorageLevel

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
  def train(input: RDD[LabeledPoint]): GradientBoostingModel = {
    val strategy = boostingStrategy.strategy
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

  // TODO: Add javadoc
  /**
   * Method to train a gradient boosting model.
   *
   * Note: Using [[org.apache.spark.mllib.tree.GradientBoosting#trainRegressor]]
   *       is recommended to clearly specify regression.
   *       Using [[org.apache.spark.mllib.tree.GradientBoosting#trainClassifier]]
   *       is recommended to clearly specify regression.
   *
   * @return GradientBoostingModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
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
   * @param impurity Criterion used for information gain calculation.
   *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
   *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
   *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param maxBins Maximum number of bins used for discretizing continuous features and
   *                for choosing how to split on features at each node.
   *                More bins give higher granularity.
   * @param quantileCalculationStrategy Algorithm for calculating quantiles.  Supported:
   *                             [[org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort]]
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
   *                                number of discrete values they take. For example, an entry (n ->
   *                                k) implies the feature n is categorical with k categories 0,
   *                                1, 2, ... , k-1. It's important to note that features are
   *                                zero-indexed.
   * @param minInstancesPerNode Minimum number of instances each child must have after split.
   *                            Default value is 1. If a split cause left or right child
   *                            to have less than minInstancesPerNode,
   *                            this split will not be considered as a valid split.
   * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
   *                    If a split has less information gain than minInfoGain,
   *                    this split will not be considered as a valid split.
   * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is
   *                      256 MB.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      impurity: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      maxBins: Int,
      quantileCalculationStrategy: String,
      categoricalFeaturesInfo: Map[Int, Int],
      minInstancesPerNode: Int,
      minInfoGain: Double,
      maxMemoryInMB: Int): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val quantileCalculationStrategyType = {
      quantileCalculationStrategy match {
        case "sort" => QuantileStrategy.Sort
        case _ =>   throw new IllegalArgumentException(s"Did not recognize Loss name: $quantileCalculationStrategy")
      }
    }
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      impurityType, maxDepth, learningRate, subsample, checkpointPeriod, 2, maxBins,
      quantileCalculationStrategyType, categoricalFeaturesInfo, minInstancesPerNode, minInfoGain,
      maxMemoryInMB)
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
   * @param impurity Criterion used for information gain calculation.
   *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
   *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
   *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param numClassesForClassification Number of classes for classification.
   *                                    (Ignored for regression.)
   *                                    Default value is 2 (binary classification).
   * @param maxBins Maximum number of bins used for discretizing continuous features and
   *                for choosing how to split on features at each node.
   *                More bins give higher granularity.
   * @param quantileCalculationStrategy Algorithm for calculating quantiles.  Supported:
   *                             [[org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort]]
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
   *                                number of discrete values they take. For example, an entry (n ->
   *                                k) implies the feature n is categorical with k categories 0,
   *                                1, 2, ... , k-1. It's important to note that features are
   *                                zero-indexed.
   * @param minInstancesPerNode Minimum number of instances each child must have after split.
   *                            Default value is 1. If a split cause left or right child
   *                            to have less than minInstancesPerNode,
   *                            this split will not be considered as a valid split.
   * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
   *                    If a split has less information gain than minInfoGain,
   *                    this split will not be considered as a valid split.
   * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is
   *                      256 MB.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      impurity: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      numClassesForClassification: Int,
      maxBins: Int,
      quantileCalculationStrategy: String,
      categoricalFeaturesInfo: Map[Int, Int],
      minInstancesPerNode: Int,
      minInfoGain: Double,
      maxMemoryInMB: Int): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val quantileCalculationStrategyType = {
      quantileCalculationStrategy match {
        case "sort" => QuantileStrategy.Sort
        case _ =>   throw new IllegalArgumentException(s"Did not recognize Loss name: $quantileCalculationStrategy")
      }
    }
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      impurityType, maxDepth, learningRate, subsample, checkpointPeriod,
      numClassesForClassification, maxBins, quantileCalculationStrategyType,
      categoricalFeaturesInfo, minInstancesPerNode, minInfoGain, maxMemoryInMB)
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
   * @param impurity Criterion used for information gain calculation.
   *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
   *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
   *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param maxBins Maximum number of bins used for discretizing continuous features and
   *                for choosing how to split on features at each node.
   *                More bins give higher granularity.
   * @param quantileCalculationStrategy Algorithm for calculating quantiles.  Supported:
   *                             [[org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort]]
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
   *                                number of discrete values they take. For example, an entry (n ->
   *                                k) implies the feature n is categorical with k categories 0,
   *                                1, 2, ... , k-1. It's important to note that features are
   *                                zero-indexed.
   * @param minInstancesPerNode Minimum number of instances each child must have after split.
   *                            Default value is 1. If a split cause left or right child
   *                            to have less than minInstancesPerNode,
   *                            this split will not be considered as a valid split.
   * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
   *                    If a split has less information gain than minInfoGain,
   *                    this split will not be considered as a valid split.
   * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is
   *                      256 MB.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: JavaRDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      impurity: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      maxBins: Int,
      quantileCalculationStrategy: String,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      minInstancesPerNode: Int,
      minInfoGain: Double,
      maxMemoryInMB: Int): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val quantileCalculationStrategyType = {
      quantileCalculationStrategy match {
        case "sort" => QuantileStrategy.Sort
        case _ =>   throw new IllegalArgumentException(s"Did not recognize Loss name: $quantileCalculationStrategy")
      }
    }
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      impurityType, maxDepth, learningRate, subsample, checkpointPeriod, 2, maxBins,
      quantileCalculationStrategyType, categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int,
        Int]].asScala.toMap, minInstancesPerNode, minInfoGain, maxMemoryInMB)
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
   * @param impurity Criterion used for information gain calculation.
   *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
   *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
   *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param learningRate Learning rate for shrinking the contribution of each estimator. The
   *                     learning rate should be between in the interval (0, 1]
   * @param subsample  Fraction of the training data used for learning the decision tree.
   * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
   * @param numClassesForClassification Number of classes for classification.
   *                                    (Ignored for regression.)
   *                                    Default value is 2 (binary classification).
   * @param maxBins Maximum number of bins used for discretizing continuous features and
   *                for choosing how to split on features at each node.
   *                More bins give higher granularity.
   * @param quantileCalculationStrategy Algorithm for calculating quantiles.  Supported:
   *                             [[org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort]]
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
   *                                number of discrete values they take. For example, an entry (n ->
   *                                k) implies the feature n is categorical with k categories 0,
   *                                1, 2, ... , k-1. It's important to note that features are
   *                                zero-indexed.
   * @param minInstancesPerNode Minimum number of instances each child must have after split.
   *                            Default value is 1. If a split cause left or right child
   *                            to have less than minInstancesPerNode,
   *                            this split will not be considered as a valid split.
   * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
   *                    If a split has less information gain than minInfoGain,
   *                    this split will not be considered as a valid split.
   * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is
   *                      256 MB.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassifier(
      input: JavaRDD[LabeledPoint],
      numEstimators: Int,
      loss: String,
      impurity: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double,
      checkpointPeriod: Int,
      numClassesForClassification: Int,
      maxBins: Int,
      quantileCalculationStrategy: String,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      minInstancesPerNode: Int,
      minInfoGain: Double,
      maxMemoryInMB: Int): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val quantileCalculationStrategyType = {
      quantileCalculationStrategy match {
        case "sort" => QuantileStrategy.Sort
        case _ =>   throw new IllegalArgumentException(s"Did not recognize Loss name: $quantileCalculationStrategy")
      }
    }
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      impurityType, maxDepth, learningRate, subsample, checkpointPeriod,
      numClassesForClassification, maxBins, quantileCalculationStrategyType,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap, minInstancesPerNode, minInfoGain, maxMemoryInMB)
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
   * @param impurity Criterion used for information gain calculation.
   *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
   *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
   *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
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
      impurity: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      impurityType, maxDepth, learningRate, subsample)
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
   * @param impurity Criterion used for information gain calculation.
   *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
   *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
   *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
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
      impurity: String,
      maxDepth: Int,
      learningRate: Double,
      subsample: Double): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val boostingStrategy = new BoostingStrategy(Regression, numEstimators, lossType,
      impurityType, maxDepth, learningRate, subsample)
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
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
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
  def trainClassification(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
    val algo = boostingStrategy.algo
    require(algo == Classification, s"Only Classification algo supported. Provided algo is $algo.")
    new GradientBoosting(boostingStrategy).train(input)
  }

  // TODO: java friendly API for classification and regression


  /**
   * Internal method for performing regression using trees as base learners.
   * @param input training dataset
   * @param boostingStrategy boosting parameters
   * @return
   */
  private def boost(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {

    val timer = new TimeTracker()

    timer.start("total")
    timer.start("init")


    // Initialize gradient boosting parameters
    val numEstimators = boostingStrategy.numEstimators
    val trees = new Array[DecisionTreeModel](numEstimators + 1)
    val loss = boostingStrategy.loss
    val learningRate = boostingStrategy.learningRate
    // TODO: Implement Stochastic gradient boosting using BaggedPoint
    val subsample = boostingStrategy.subsample
    val checkpointingPeriod = boostingStrategy.checkpointPeriod
    val strategy = boostingStrategy.strategy

    // Cache input
    input.persist(StorageLevel.MEMORY_AND_DISK)
    var lastCachedData = input

    timer.stop("init")

    logDebug("##########")
    logDebug("Building tree 0")
    logDebug("##########")
    var data = input

    // 1. Initialize tree
    timer.start("building tree 0")
    val firstModel = new DecisionTree(strategy).train(data)
    timer.stop("building tree 0")
    trees(0) = firstModel
    logDebug("error of tree = " + meanSquaredError(firstModel, data))

    // psuedo-residual for second iteration
    data = data.map(point => LabeledPoint(loss.lossGradient(firstModel, point,
      learningRate), point.features))


    var m = 1
    while (m <= numEstimators) {
      timer.start(s"building tree $m")
      logDebug("###################################################")
      logDebug("Gradient boosting tree iteration " + m)
      logDebug("###################################################")
      val model = new DecisionTree(strategy).train(data)
      timer.stop(s"building tree $m")
      trees(m) = model
      logDebug("error of tree = " + meanSquaredError(model, data))
      // Update data with pseudo-residuals
      data = data.map(point => LabeledPoint(loss.lossGradient(model, point, learningRate),
        point.features))
      if (m % checkpointingPeriod == 1 && m != 1) {
        lastCachedData.unpersist()
      }
      // Checkpoint
      if (m % checkpointingPeriod == 0) {
        data = data.persist(StorageLevel.MEMORY_AND_DISK)
        lastCachedData = data
      }
      m += 1
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")


    // 3. Output classifier
    new GradientBoostingModel(trees, strategy.algo)

  }

  /**
   * Calculates the mean squared error for regression.
   */
  private def meanSquaredError(tree: DecisionTreeModel, data: RDD[LabeledPoint]): Double = {
    data.map { y =>
      val err = tree.predict(y.features) - y.label
      err * err
    }.mean()
  }


}

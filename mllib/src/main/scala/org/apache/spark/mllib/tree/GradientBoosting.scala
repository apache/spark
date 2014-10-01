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
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostingModel, DecisionTreeModel}
import org.apache.spark.mllib.tree.configuration.Algo._

/**
 * :: Experimental ::
 * A class that implements gradient boosting for regression problems.
 * @param strategy Parameters for the underlying decision tree estimators
 * @param boostingStrategy Parameters for the gradient boosting algorithm
 */
@Experimental
class GradientBoosting (
    private val strategy: Strategy,
    private val boostingStrategy: BoostingStrategy) extends Serializable with Logging {

  /**
   * Method to train a gradient boosting model
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return GradientBoostingModel that can be used for prediction
   */
  def train(input: RDD[LabeledPoint]): GradientBoostingModel = {
    val algo = strategy.algo
    algo match {
      case Regression => GradientBoosting.regression(input, strategy, boostingStrategy)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by the gradient boosting.")
    }
  }

}


object GradientBoosting extends Logging {

  /**
   * Method to train a gradient boosting model.
   * The method currently only supports regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.GradientBoosting#trainRegressor]]
   *       is recommended to clearly specify regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The configuration parameters for the underlying tree-based estimators
   *                 which specify the type of algorithm (classification, regression, etc.),
   *                 feature type (continuous, categorical), depth of the tree,
   *                 quantile calculation strategy, etc.
   * @return GradientBoostingModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }


  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The configuration parameters for the underlying tree-based estimators
   *                 which specify the type of algorithm (classification, regression, etc.),
   *                 feature type (continuous, categorical), depth of the tree,
   *                 quantile calculation strategy, etc.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
             input: RDD[LabeledPoint],
             strategy: Strategy,
             boostingStrategy: BoostingStrategy): GradientBoostingModel = {
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }

  /**
   * Internal method for performing regression using trees as base learners.
   * @param input training dataset
   * @param strategy tree parameters
   * @param boostingStrategy boosting parameters
   * @return
   */
  private def regression(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {

    // Initialize gradient boosting parameters
    val numEstimators = boostingStrategy.numEstimators
    val trees = new Array[DecisionTreeModel](numEstimators + 1)
    val loss = boostingStrategy.loss
    val learningRate = boostingStrategy.learningRate
    // TODO: Implement Stochastic gradient boosting using BaggedPoint
    val subSample = boostingStrategy.subSample

    // Cache input
    input.cache()

    logDebug("##########")
    logDebug("Building tree 0")
    logDebug("##########")
    var data = input

    // 1. Initialize tree
    val firstModel = new DecisionTree(strategy).train(data)
    trees(0) = firstModel
    logDebug("error of tree = " + meanSquaredError(firstModel, data))
    logDebug(data.first.toString)

    // psuedo-residual for second iteration
    data = data.map(point => LabeledPoint(loss.lossGradient(firstModel, point,
      learningRate), point.features))


    var m = 1
    while (m <= numEstimators) {
      logDebug("###################################################")
      logDebug("Gradient boosting tree iteration " + m)
      logDebug("###################################################")
      val model = new DecisionTree(strategy).train(data)
      trees(m) = model
      logDebug("error of tree = " + meanSquaredError(model, data))
      //update data with pseudo-residuals
      data = data.map(point => LabeledPoint(loss.lossGradient(model, point, learningRate),
        point.features))
      // Checkpoint
      val checkpointingPeriod = boostingStrategy.checkpointPeriod
      // TODO: Need to find good defaults for checkpointPeriod
      if (m % checkpointingPeriod == 0) {
        //data.checkpoint()
      }
      m += 1
    }

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
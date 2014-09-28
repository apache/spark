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
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostingModel, DecisionTreeModel}
import org.apache.spark.mllib.tree.configuration.Algo._

class GradientBoosting (
    private val strategy: Strategy,
    private val boostingStrategy: BoostingStrategy) extends Serializable with Logging {

  def train(input: RDD[LabeledPoint]): GradientBoostingModel = {
    strategy.algo match {
      // case Classification => GradientBoost.classification(input, strategy)
      case Regression => GradientBoosting.regression(input, strategy, boostingStrategy)
    }
  }

}

object GradientBoosting extends Logging {

  def train(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
    //val weightedInput = input.map(x => WeightedLabeledPoint(x.label, x.features))
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }

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
    data = data.map(point => LabeledPoint(loss.calculateResidual(firstModel, point,
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
      data = data.map(point => LabeledPoint(loss.calculateResidual(model, point, learningRate),
        point.features))
      // Checkpoint
      val checkpointingPeriod = boostingStrategy.checkpointPeriod
      // TODO: Need to find good defaults for checkpointPeriod
      if (m % checkpointingPeriod == 0) {
        //data.checkpoint()
      }
      logDebug(data.first.toString)
      m += 1
    }

    // 3. Output classifier
    new GradientBoostingModel(trees, strategy.algo)

  }

  // TODO: Port this method to a generic metrics package
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
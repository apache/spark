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

package org.apache.spark.ml.tree.impl

import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
import org.apache.spark.mllib.impl.PeriodicRDDCheckpointer
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy => OldBoostingStrategy}
import org.apache.spark.mllib.tree.impurity.{Variance => OldVariance}
import org.apache.spark.mllib.tree.loss.{Loss => OldLoss}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


private[spark] object GradientBoostedTrees extends Logging {

  /**
   * Method to train a gradient boosting model
   * @param input Training dataset: RDD of `LabeledPoint`.
   * @param seed Random seed.
   * @return tuple of ensemble models and weights:
   *         (array of decision tree models, array of model weights)
   */
  def run(
      input: RDD[LabeledPoint],
      boostingStrategy: OldBoostingStrategy,
      seed: Long): (Array[DecisionTreeRegressionModel], Array[Double]) = {
    val algo = boostingStrategy.treeStrategy.algo
    algo match {
      case OldAlgo.Regression =>
        GradientBoostedTrees.boost(input, input, boostingStrategy, validate = false, seed)
      case OldAlgo.Classification =>
        // Map labels to -1, +1 so binary classification can be treated as regression.
        val remappedInput = input.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        GradientBoostedTrees.boost(remappedInput, remappedInput, boostingStrategy, validate = false,
          seed)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by gradient boosting.")
    }
  }

  /**
   * Method to validate a gradient boosting model
   * @param input Training dataset: RDD of `LabeledPoint`.
   * @param validationInput Validation dataset.
   *                        This dataset should be different from the training dataset,
   *                        but it should follow the same distribution.
   *                        E.g., these two datasets could be created from an original dataset
   *                        by using `org.apache.spark.rdd.RDD.randomSplit()`
   * @param seed Random seed.
   * @return tuple of ensemble models and weights:
   *         (array of decision tree models, array of model weights)
   */
  def runWithValidation(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint],
      boostingStrategy: OldBoostingStrategy,
      seed: Long): (Array[DecisionTreeRegressionModel], Array[Double]) = {
    val algo = boostingStrategy.treeStrategy.algo
    algo match {
      case OldAlgo.Regression =>
        GradientBoostedTrees.boost(input, validationInput, boostingStrategy, validate = true, seed)
      case OldAlgo.Classification =>
        // Map labels to -1, +1 so binary classification can be treated as regression.
        val remappedInput = input.map(
          x => new LabeledPoint((x.label * 2) - 1, x.features))
        val remappedValidationInput = validationInput.map(
          x => new LabeledPoint((x.label * 2) - 1, x.features))
        GradientBoostedTrees.boost(remappedInput, remappedValidationInput, boostingStrategy,
          validate = true, seed)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by the gradient boosting.")
    }
  }

  /**
   * Compute the initial predictions and errors for a dataset for the first
   * iteration of gradient boosting.
   * @param data: training data.
   * @param initTreeWeight: learning rate assigned to the first tree.
   * @param initTree: first DecisionTreeModel.
   * @param loss: evaluation metric.
   * @return an RDD with each element being a zip of the prediction and error
   *         corresponding to every sample.
   */
  def computeInitialPredictionAndError(
      data: RDD[LabeledPoint],
      initTreeWeight: Double,
      initTree: DecisionTreeRegressionModel,
      loss: OldLoss): RDD[(Double, Double)] = {
    data.map { lp =>
      val pred = updatePrediction(lp.features, 0.0, initTree, initTreeWeight)
      val error = loss.computeError(pred, lp.label)
      (pred, error)
    }
  }

  /**
   * Update a zipped predictionError RDD
   * (as obtained with computeInitialPredictionAndError)
   * @param data: training data.
   * @param predictionAndError: predictionError RDD
   * @param treeWeight: Learning rate.
   * @param tree: Tree using which the prediction and error should be updated.
   * @param loss: evaluation metric.
   * @return an RDD with each element being a zip of the prediction and error
   *         corresponding to each sample.
   */
  def updatePredictionError(
      data: RDD[LabeledPoint],
      predictionAndError: RDD[(Double, Double)],
      treeWeight: Double,
      tree: DecisionTreeRegressionModel,
      loss: OldLoss): RDD[(Double, Double)] = {

    val newPredError = data.zip(predictionAndError).mapPartitions { iter =>
      iter.map { case (lp, (pred, error)) =>
        val newPred = updatePrediction(lp.features, pred, tree, treeWeight)
        val newError = loss.computeError(newPred, lp.label)
        (newPred, newError)
      }
    }
    newPredError
  }

  /**
   * Add prediction from a new boosting iteration to an existing prediction.
   *
   * @param features Vector of features representing a single data point.
   * @param prediction The existing prediction.
   * @param tree New Decision Tree model.
   * @param weight Tree weight.
   * @return Updated prediction.
   */
  def updatePrediction(
      features: Vector,
      prediction: Double,
      tree: DecisionTreeRegressionModel,
      weight: Double): Double = {
    prediction + tree.rootNode.predictImpl(features).prediction * weight
  }

  /**
   * Method to calculate error of the base learner for the gradient boosting calculation.
   * Note: This method is not used by the gradient boosting algorithm but is useful for debugging
   * purposes.
   * @param data Training dataset: RDD of `LabeledPoint`.
   * @param trees Boosted Decision Tree models
   * @param treeWeights Learning rates at each boosting iteration.
   * @param loss evaluation metric.
   * @return Measure of model error on data
   */
  def computeError(
      data: RDD[LabeledPoint],
      trees: Array[DecisionTreeRegressionModel],
      treeWeights: Array[Double],
      loss: OldLoss): Double = {
    data.map { lp =>
      val predicted = trees.zip(treeWeights).foldLeft(0.0) { case (acc, (model, weight)) =>
        updatePrediction(lp.features, acc, model, weight)
      }
      loss.computeError(predicted, lp.label)
    }.mean()
  }

  /**
   * Method to compute error or loss for every iteration of gradient boosting.
   *
   * @param data RDD of `LabeledPoint`
   * @param trees Boosted Decision Tree models
   * @param treeWeights Learning rates at each boosting iteration.
   * @param loss evaluation metric.
   * @param algo algorithm for the ensemble, either Classification or Regression
   * @return an array with index i having the losses or errors for the ensemble
   *         containing the first i+1 trees
   */
  def evaluateEachIteration(
      data: RDD[LabeledPoint],
      trees: Array[DecisionTreeRegressionModel],
      treeWeights: Array[Double],
      loss: OldLoss,
      algo: OldAlgo.Value): Array[Double] = {

    val sc = data.sparkContext
    val remappedData = algo match {
      case OldAlgo.Classification => data.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
      case _ => data
    }

    val broadcastTrees = sc.broadcast(trees)
    val localTreeWeights = treeWeights
    val treesIndices = trees.indices

    val dataCount = remappedData.count()
    val evaluation = remappedData.map { point =>
      treesIndices.map { idx =>
        val prediction = broadcastTrees.value(idx)
          .rootNode
          .predictImpl(point.features)
          .prediction
        prediction * localTreeWeights(idx)
      }
      .scanLeft(0.0)(_ + _).drop(1)
      .map(prediction => loss.computeError(prediction, point.label))
    }
    .aggregate(treesIndices.map(_ => 0.0))(
      (aggregated, row) => treesIndices.map(idx => aggregated(idx) + row(idx)),
      (a, b) => treesIndices.map(idx => a(idx) + b(idx)))
    .map(_ / dataCount)

    broadcastTrees.destroy()
    evaluation.toArray
  }

  /**
   * Internal method for performing regression using trees as base learners.
   * @param input training dataset
   * @param validationInput validation dataset, ignored if validate is set to false.
   * @param boostingStrategy boosting parameters
   * @param validate whether or not to use the validation dataset.
   * @param seed Random seed.
   * @return tuple of ensemble models and weights:
   *         (array of decision tree models, array of model weights)
   */
  def boost(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint],
      boostingStrategy: OldBoostingStrategy,
      validate: Boolean,
      seed: Long): (Array[DecisionTreeRegressionModel], Array[Double]) = {
    val timer = new TimeTracker()
    timer.start("total")
    timer.start("init")

    boostingStrategy.assertValid()

    // Initialize gradient boosting parameters
    val numIterations = boostingStrategy.numIterations
    val baseLearners = new Array[DecisionTreeRegressionModel](numIterations)
    val baseLearnerWeights = new Array[Double](numIterations)
    val loss = boostingStrategy.loss
    val learningRate = boostingStrategy.learningRate
    // Prepare strategy for individual trees, which use regression with variance impurity.
    val treeStrategy = boostingStrategy.treeStrategy.copy
    val validationTol = boostingStrategy.validationTol
    treeStrategy.algo = OldAlgo.Regression
    treeStrategy.impurity = OldVariance
    treeStrategy.assertValid()

    // Cache input
    val persistedInput = if (input.getStorageLevel == StorageLevel.NONE) {
      input.persist(StorageLevel.MEMORY_AND_DISK)
      true
    } else {
      false
    }

    // Prepare periodic checkpointers
    val predErrorCheckpointer = new PeriodicRDDCheckpointer[(Double, Double)](
      treeStrategy.getCheckpointInterval, input.sparkContext)
    val validatePredErrorCheckpointer = new PeriodicRDDCheckpointer[(Double, Double)](
      treeStrategy.getCheckpointInterval, input.sparkContext)

    timer.stop("init")

    logDebug("##########")
    logDebug("Building tree 0")
    logDebug("##########")

    // Initialize tree
    timer.start("building tree 0")
    val firstTree = new DecisionTreeRegressor().setSeed(seed)
    val firstTreeModel = firstTree.train(input, treeStrategy)
    val firstTreeWeight = 1.0
    baseLearners(0) = firstTreeModel
    baseLearnerWeights(0) = firstTreeWeight

    var predError: RDD[(Double, Double)] =
      computeInitialPredictionAndError(input, firstTreeWeight, firstTreeModel, loss)
    predErrorCheckpointer.update(predError)
    logDebug("error of gbt = " + predError.values.mean())

    // Note: A model of type regression is used since we require raw prediction
    timer.stop("building tree 0")

    var validatePredError: RDD[(Double, Double)] =
      computeInitialPredictionAndError(validationInput, firstTreeWeight, firstTreeModel, loss)
    if (validate) validatePredErrorCheckpointer.update(validatePredError)
    var bestValidateError = if (validate) validatePredError.values.mean() else 0.0
    var bestM = 1

    var m = 1
    var doneLearning = false
    while (m < numIterations && !doneLearning) {
      // Update data with pseudo-residuals
      val data = predError.zip(input).map { case ((pred, _), point) =>
        LabeledPoint(-loss.gradient(pred, point.label), point.features)
      }

      timer.start(s"building tree $m")
      logDebug("###################################################")
      logDebug("Gradient boosting tree iteration " + m)
      logDebug("###################################################")
      val dt = new DecisionTreeRegressor().setSeed(seed + m)
      val model = dt.train(data, treeStrategy)
      timer.stop(s"building tree $m")
      // Update partial model
      baseLearners(m) = model
      // Note: The setting of baseLearnerWeights is incorrect for losses other than SquaredError.
      //       Technically, the weight should be optimized for the particular loss.
      //       However, the behavior should be reasonable, though not optimal.
      baseLearnerWeights(m) = learningRate

      predError = updatePredictionError(
        input, predError, baseLearnerWeights(m), baseLearners(m), loss)
      predErrorCheckpointer.update(predError)
      logDebug("error of gbt = " + predError.values.mean())

      if (validate) {
        // Stop training early if
        // 1. Reduction in error is less than the validationTol or
        // 2. If the error increases, that is if the model is overfit.
        // We want the model returned corresponding to the best validation error.

        validatePredError = updatePredictionError(
          validationInput, validatePredError, baseLearnerWeights(m), baseLearners(m), loss)
        validatePredErrorCheckpointer.update(validatePredError)
        val currentValidateError = validatePredError.values.mean()
        if (bestValidateError - currentValidateError < validationTol * Math.max(
          currentValidateError, 0.01)) {
          doneLearning = true
        } else if (currentValidateError < bestValidateError) {
          bestValidateError = currentValidateError
          bestM = m + 1
        }
      }
      m += 1
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    predErrorCheckpointer.deleteAllCheckpoints()
    validatePredErrorCheckpointer.deleteAllCheckpoints()
    if (persistedInput) input.unpersist()

    if (validate) {
      (baseLearners.slice(0, bestM), baseLearnerWeights.slice(0, bestM))
    } else {
      (baseLearners, baseLearnerWeights)
    }
  }
}

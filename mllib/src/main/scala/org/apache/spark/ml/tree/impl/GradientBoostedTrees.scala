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

import scala.reflect.ClassTag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.util.{Instrumentation, MLUtils}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy => OldBoostingStrategy}
import org.apache.spark.mllib.tree.impurity.{Variance => OldVariance}
import org.apache.spark.mllib.tree.loss.{Loss => OldLoss}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.util.PeriodicRDDCheckpointer
import org.apache.spark.storage.StorageLevel


private[spark] object GradientBoostedTrees extends Logging {

  /**
   * Method to train a gradient boosting model
   * @param input Training dataset: RDD of `Instance`.
   * @param seed Random seed.
   * @return tuple of ensemble models and weights:
   *         (array of decision tree models, array of model weights)
   */
  def run(
      input: RDD[Instance],
      boostingStrategy: OldBoostingStrategy,
      seed: Long,
      featureSubsetStrategy: String,
      instr: Option[Instrumentation] = None):
        (Array[DecisionTreeRegressionModel], Array[Double]) = {
    val algo = boostingStrategy.treeStrategy.algo
    algo match {
      case OldAlgo.Regression =>
        GradientBoostedTrees.boost(input, input, boostingStrategy, validate = false,
          seed, featureSubsetStrategy, instr)
      case OldAlgo.Classification =>
        // Map labels to -1, +1 so binary classification can be treated as regression.
        val remappedInput = input.map(x => Instance((x.label * 2) - 1, x.weight, x.features))
        GradientBoostedTrees.boost(remappedInput, remappedInput, boostingStrategy, validate = false,
          seed, featureSubsetStrategy, instr)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by gradient boosting.")
    }
  }

  /**
   * Method to validate a gradient boosting model
   * @param input Training dataset: RDD of `Instance`.
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
      input: RDD[Instance],
      validationInput: RDD[Instance],
      boostingStrategy: OldBoostingStrategy,
      seed: Long,
      featureSubsetStrategy: String,
      instr: Option[Instrumentation] = None):
        (Array[DecisionTreeRegressionModel], Array[Double]) = {
    val algo = boostingStrategy.treeStrategy.algo
    algo match {
      case OldAlgo.Regression =>
        GradientBoostedTrees.boost(input, validationInput, boostingStrategy,
          validate = true, seed, featureSubsetStrategy, instr)
      case OldAlgo.Classification =>
        // Map labels to -1, +1 so binary classification can be treated as regression.
        val remappedInput = input.map(
          x => Instance((x.label * 2) - 1, x.weight, x.features))
        val remappedValidationInput = validationInput.map(
          x => Instance((x.label * 2) - 1, x.weight, x.features))
        GradientBoostedTrees.boost(remappedInput, remappedValidationInput, boostingStrategy,
          validate = true, seed, featureSubsetStrategy, instr)
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
  def computeInitialPredictionAndError[B: Integral](
      data: RDD[TreePoint[B]],
      initTreeWeight: Double,
      initTree: DecisionTreeRegressionModel,
      loss: OldLoss,
      bcSplits: Broadcast[Array[Array[Split]]]): RDD[(Double, Double)] = {
    data.map { treePoint =>
      val pred = updatePrediction[B](treePoint, 0.0, initTree, initTreeWeight, bcSplits.value)
      val error = loss.computeError(pred, treePoint.label)
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
  def updatePredictionError[B: Integral](
      data: RDD[TreePoint[B]],
      predictionAndError: RDD[(Double, Double)],
      treeWeight: Double,
      tree: DecisionTreeRegressionModel,
      loss: OldLoss,
      bcSplits: Broadcast[Array[Array[Split]]]): RDD[(Double, Double)] = {
    data.zip(predictionAndError).map { case (treePoint, (pred, _)) =>
      val newPred = updatePrediction[B](treePoint, pred, tree, treeWeight, bcSplits.value)
      val newError = loss.computeError(newPred, treePoint.label)
      (newPred, newError)
    }
  }

  /**
   * Add prediction from a new boosting iteration to an existing prediction.
   *
   * @param treePoint Binned vector of features representing a single data point.
   * @param prediction The existing prediction.
   * @param tree New Decision Tree model.
   * @param weight Tree weight.
   * @return Updated prediction.
   */
  def updatePrediction[@specialized(Byte, Short, Int) B: Integral](
      treePoint: TreePoint[B],
      prediction: Double,
      tree: DecisionTreeRegressionModel,
      weight: Double,
      splits: Array[Array[Split]]): Double = {
    prediction +
      tree.rootNode.predictBinned[B](treePoint.binnedFeatures, splits).prediction * weight
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
   * @param data Training dataset: RDD of `Instance`.
   * @param trees Boosted Decision Tree models
   * @param treeWeights Learning rates at each boosting iteration.
   * @param loss evaluation metric.
   * @return Measure of model error on data
   */
  def computeWeightedError(
      data: RDD[Instance],
      trees: Array[DecisionTreeRegressionModel],
      treeWeights: Array[Double],
      loss: OldLoss): Double = {
    val (errSum, weightSum) = data.map { case Instance(label, weight, features) =>
      val predicted = trees.zip(treeWeights).foldLeft(0.0) { case (acc, (model, weight)) =>
        updatePrediction(features, acc, model, weight)
      }
      (loss.computeError(predicted, label) * weight, weight)
    }.treeReduce { case ((err1, weight1), (err2, weight2)) =>
        (err1 + err2, weight1 + weight2)
    }
    errSum / weightSum
  }

  /**
   * Method to calculate error of the base learner for the gradient boosting calculation.
   * @param data Training dataset: RDD of `TreePoint`.
   * @param predError Prediction and error.
   * @return Measure of model error on data
   */
  def computeWeightedError[B: Integral](
      data: RDD[TreePoint[B]],
      predError: RDD[(Double, Double)]): Double = {
    val (errSum, weightSum) = data.zip(predError).map {
      case (treePoint, (_, err)) =>
        (err * treePoint.weight, treePoint.weight)
    }.treeReduce { case ((err1, weight1), (err2, weight2)) =>
      (err1 + err2, weight1 + weight2)
    }
    errSum / weightSum
  }

  /**
   * Method to compute error or loss for every iteration of gradient boosting.
   *
   * @param data RDD of `Instance`
   * @param trees Boosted Decision Tree models
   * @param treeWeights Learning rates at each boosting iteration.
   * @param loss evaluation metric.
   * @param algo algorithm for the ensemble, either Classification or Regression
   * @return an array with index i having the losses or errors for the ensemble
   *         containing the first i+1 trees
   */
  def evaluateEachIteration(
      data: RDD[Instance],
      trees: Array[DecisionTreeRegressionModel],
      treeWeights: Array[Double],
      loss: OldLoss,
      algo: OldAlgo.Value): Array[Double] = {
    val remappedData = algo match {
      case OldAlgo.Classification =>
        data.map(x => Instance((x.label * 2) - 1, x.weight, x.features))
      case _ => data
    }

    val numTrees = trees.length
    val (errSum, weightSum) = remappedData.mapPartitions { iter =>
      iter.map { case Instance(label, weight, features) =>
        val pred = Array.tabulate(numTrees) { i =>
          trees(i).rootNode.predictImpl(features)
            .prediction * treeWeights(i)
        }
        val err = pred.scanLeft(0.0)(_ + _).drop(1)
          .map(p => loss.computeError(p, label) * weight)
        (err, weight)
      }
    }.treeReduce { case ((err1, weight1), (err2, weight2)) =>
      (0 until numTrees).foreach(i => err1(i) += err2(i))
      (err1, weight1 + weight2)
    }

    errSum.map(_ / weightSum)
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
      input: RDD[Instance],
      validationInput: RDD[Instance],
      boostingStrategy: OldBoostingStrategy,
      validate: Boolean,
      seed: Long,
      featureSubsetStrategy: String,
      instr: Option[Instrumentation] = None):
        (Array[DecisionTreeRegressionModel], Array[Double]) = {
    MLUtils.registerKryoClasses(input.sparkContext.getConf)

    val timer = new TimeTracker()
    timer.start("total")

    timer.start("check")
    boostingStrategy.assertValid()
    // Prepare strategy for individual trees, which use regression with variance impurity.
    val treeStrategy = boostingStrategy.treeStrategy.copy
    treeStrategy.algo = OldAlgo.Regression
    treeStrategy.impurity = OldVariance
    treeStrategy.assertValid()
    timer.stop("check")

    timer.start("buildMetadata")
    val retaggedInput = input.retag(classOf[Instance])
    val metadata = DecisionTreeMetadata.buildMetadata(retaggedInput, treeStrategy,
      numTrees = 1, featureSubsetStrategy)
    timer.stop("buildMetadata")

    timer.start("findSplits")
    val splits = RandomForest.findSplits(retaggedInput, metadata, seed)
    timer.stop("findSplits")

    val trees = if (metadata.maxBins < Byte.MaxValue) {
      boostWithClassTag[Byte](input, validationInput, metadata, splits,
        boostingStrategy, validate, seed, featureSubsetStrategy, instr)
    } else if (metadata.maxBins < Short.MaxValue) {
      boostWithClassTag[Short](input, validationInput, metadata, splits,
        boostingStrategy, validate, seed, featureSubsetStrategy, instr)
    } else {
      boostWithClassTag[Int](input, validationInput, metadata, splits,
        boostingStrategy, validate, seed, featureSubsetStrategy, instr)
    }

    timer.stop("total")
    trees
  }

  def boostWithClassTag[B: Integral: ClassTag](
      input: RDD[Instance],
      validationInput: RDD[Instance],
      metadata: DecisionTreeMetadata,
      splits: Array[Array[Split]],
      boostingStrategy: OldBoostingStrategy,
      validate: Boolean,
      seed: Long,
      featureSubsetStrategy: String,
      instr: Option[Instrumentation] = None):
  (Array[DecisionTreeRegressionModel], Array[Double]) = {
    val timer = new TimeTracker()
    timer.start("init")

    val sc = input.sparkContext

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
    require(!treeStrategy.bootstrap, "GradientBoostedTrees does not need bootstrap sampling")
    treeStrategy.assertValid()

    val bcSplits = sc.broadcast(splits)

    // Prepare periodic checkpointers
    // Note: this is checkpointing the unweighted training error
    val predErrorCheckpointer = new PeriodicRDDCheckpointer[(Double, Double)](
      treeStrategy.getCheckpointInterval, sc)

    timer.stop("init")

    logDebug("##########")
    logDebug("Building tree 0")
    logDebug("##########")

    // Initialize tree
    timer.start("building tree 0")

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treePoints = TreePoint.convertToTreeRDD[B](
      input.retag(classOf[Instance]), splits, metadata)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName(s"binned tree points [${implicitly[ClassTag[B]]}]")

    val firstCounts = BaggedPoint
      .convertToBaggedRDD(treePoints, treeStrategy.subsamplingRate, numSubsamples = 1,
        treeStrategy.bootstrap, (tp: TreePoint[_]) => tp.weight, seed = seed)
      .map { bagged =>
        require(bagged.subsampleCounts.length == 1)
        require(bagged.sampleWeight == bagged.datum.weight)
        bagged.subsampleCounts.head
      }.persist(StorageLevel.MEMORY_AND_DISK)
      .setName("firstCounts at iter=0")

    val firstBagged = treePoints.zip(firstCounts)
      .map { case (treePoint, count) =>
        // according to current design, treePoint.weight == baggedPoint.sampleWeight
        new BaggedPoint[TreePoint[B]](treePoint, Array(count), treePoint.weight)
    }

    val firstTreeModel = RandomForest.runBagged[B](baggedInput = firstBagged,
      metadata = metadata, bcSplits = bcSplits, strategy = treeStrategy,
      featureSubsetStrategy = featureSubsetStrategy, seed = seed, instr = instr,
      parentUID = None)
      .head.asInstanceOf[DecisionTreeRegressionModel]

    firstCounts.unpersist()

    val firstTreeWeight = 1.0
    baseLearners(0) = firstTreeModel
    baseLearnerWeights(0) = firstTreeWeight

    var predError = computeInitialPredictionAndError[B](
      treePoints, firstTreeWeight, firstTreeModel, loss, bcSplits)
    predErrorCheckpointer.update(predError)
    logDebug(s"error of gbt = ${computeWeightedError[B](treePoints, predError)}")

    // Note: A model of type regression is used since we require raw prediction
    timer.stop("building tree 0")

    var validationTreePoints: RDD[TreePoint[B]] = null
    var validatePredError: RDD[(Double, Double)] = null
    var validatePredErrorCheckpointer: PeriodicRDDCheckpointer[(Double, Double)] = null
    var bestValidateError = 0.0
    if (validate) {
      timer.start("init validation")
      validationTreePoints = TreePoint.convertToTreeRDD(
        validationInput.retag(classOf[Instance]), splits, metadata)
        .persist(StorageLevel.MEMORY_AND_DISK)
      validatePredError = computeInitialPredictionAndError(
        validationTreePoints, firstTreeWeight, firstTreeModel, loss, bcSplits)
      validatePredErrorCheckpointer = new PeriodicRDDCheckpointer[(Double, Double)](
        treeStrategy.getCheckpointInterval, sc, StorageLevel.MEMORY_AND_DISK)
      validatePredErrorCheckpointer.update(validatePredError)
      bestValidateError = computeWeightedError(validationTreePoints, validatePredError)
      timer.stop("init validation")
    }

    var bestM = 1

    var iter = 1
    var doneLearning = false
    while (iter < numIterations && !doneLearning) {
      timer.start(s"building tree $iter")
      logDebug("###################################################")
      logDebug("Gradient boosting tree iteration " + iter)
      logDebug("###################################################")

      // (label: Double, count: Int)
      val labelWithCounts = BaggedPoint
        .convertToBaggedRDD(treePoints, treeStrategy.subsamplingRate, numSubsamples = 1,
          treeStrategy.bootstrap, (tp: TreePoint[_]) => tp.weight, seed = seed + iter)
        .zip(predError)
        .map { case (bagged, (pred, _)) =>
          require(bagged.subsampleCounts.length == 1)
          require(bagged.sampleWeight == bagged.datum.weight)
          // Update labels with pseudo-residuals
          val newLabel = -loss.gradient(pred, bagged.datum.label)
          (newLabel, bagged.subsampleCounts.head)
        }.persist(StorageLevel.MEMORY_AND_DISK)
        .setName(s"labelWithCounts at iter=$iter")

      val bagged = treePoints.zip(labelWithCounts)
        .map { case (treePoint, (newLabel, count)) =>
          val newTreePoint = new TreePoint(newLabel, treePoint.binnedFeatures, treePoint.weight)
          // according to current design, treePoint.weight == baggedPoint.sampleWeight
          new BaggedPoint[TreePoint[B]](newTreePoint, Array(count), treePoint.weight)
        }.setName(s"bagged points [${implicitly[ClassTag[B]]}]")

      val model = RandomForest.runBagged[B](baggedInput = bagged,
        metadata = metadata, bcSplits = bcSplits, strategy = treeStrategy,
        featureSubsetStrategy = featureSubsetStrategy, seed = seed + iter,
        instr = None, parentUID = None)
        .head.asInstanceOf[DecisionTreeRegressionModel]

      labelWithCounts.unpersist()

      timer.stop(s"building tree $iter")
      // Update partial model
      baseLearners(iter) = model
      // Note: The setting of baseLearnerWeights is incorrect for losses other than SquaredError.
      //       Technically, the weight should be optimized for the particular loss.
      //       However, the behavior should be reasonable, though not optimal.
      baseLearnerWeights(iter) = learningRate

      predError = updatePredictionError[B](
        treePoints, predError, baseLearnerWeights(iter),
        baseLearners(iter), loss, bcSplits)
      predErrorCheckpointer.update(predError)
      logDebug(s"error of gbt = ${computeWeightedError[B](treePoints, predError)}")

      if (validate) {
        // Stop training early if
        // 1. Reduction in error is less than the validationTol or
        // 2. If the error increases, that is if the model is overfit.
        // We want the model returned corresponding to the best validation error.

        validatePredError = updatePredictionError[B](
          validationTreePoints, validatePredError, baseLearnerWeights(iter),
          baseLearners(iter), loss, bcSplits)
        validatePredErrorCheckpointer.update(validatePredError)
        val currentValidateError =
          computeWeightedError[B](validationTreePoints, validatePredError)
        if (bestValidateError - currentValidateError < validationTol * Math.max(
          currentValidateError, 0.01)) {
          doneLearning = true
        } else if (currentValidateError < bestValidateError) {
          bestValidateError = currentValidateError
          bestM = iter + 1
        }
      }
      iter += 1
    }

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    bcSplits.destroy()
    treePoints.unpersist()
    predErrorCheckpointer.unpersistDataSet()
    predErrorCheckpointer.deleteAllCheckpoints()
    if (validate) {
      validationTreePoints.unpersist()
      validatePredErrorCheckpointer.unpersistDataSet()
      validatePredErrorCheckpointer.deleteAllCheckpoints()
    }

    if (validate) {
      (baseLearners.slice(0, bestM), baseLearnerWeights.slice(0, bestM))
    } else {
      (baseLearners, baseLearnerWeights)
    }
  }
}

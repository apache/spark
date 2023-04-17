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

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.tree.impl.{GradientBoostedTrees => NewGBT}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD

/**
 * A class that implements
 * <a href="http://en.wikipedia.org/wiki/Gradient_boosting">Stochastic Gradient Boosting</a>
 * for regression and binary classification.
 *
 * The implementation is based upon:
 *   J.H. Friedman.  "Stochastic Gradient Boosting."  1999.
 *
 * Notes on Gradient Boosting vs. TreeBoost:
 *  - This implementation is for Stochastic Gradient Boosting, not for TreeBoost.
 *  - Both algorithms learn tree ensembles by minimizing loss functions.
 *  - TreeBoost (Friedman, 1999) additionally modifies the outputs at tree leaf nodes
 *    based on the loss function, whereas the original gradient boosting method does not.
 *     - When the loss is SquaredError, these methods give the same result, but they could differ
 *       for other loss functions.
 *
 * @param boostingStrategy Parameters for the gradient boosting algorithm.
 * @param seed Random seed.
 */
@Since("1.2.0")
class GradientBoostedTrees private[spark] (
    private val boostingStrategy: BoostingStrategy,
    private val seed: Int)
  extends Serializable with Logging {

  /**
   * @param boostingStrategy Parameters for the gradient boosting algorithm.
   */
  @Since("1.2.0")
  def this(boostingStrategy: BoostingStrategy) = this(boostingStrategy, seed = 0)

  /**
   * Method to train a gradient boosting model
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return GradientBoostedTreesModel that can be used for prediction.
   */
  @Since("1.2.0")
  def run(input: RDD[LabeledPoint]): GradientBoostedTreesModel = {
    val algo = boostingStrategy.treeStrategy.algo
    val (trees, treeWeights) = NewGBT.run(input.map {
      case LabeledPoint(label, features) =>
        Instance(label, 1.0, features.asML)
    }, boostingStrategy, seed.toLong, "all")
    new GradientBoostedTreesModel(algo, trees.map(_.toOld), treeWeights)
  }

  /**
   * Java-friendly API for `org.apache.spark.mllib.tree.GradientBoostedTrees.run`.
   */
  @Since("1.2.0")
  def run(input: JavaRDD[LabeledPoint]): GradientBoostedTreesModel = {
    run(input.rdd)
  }

  /**
   * Method to validate a gradient boosting model
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @param validationInput Validation dataset.
   *                        This dataset should be different from the training dataset,
   *                        but it should follow the same distribution.
   *                        E.g., these two datasets could be created from an original dataset
   *                        by using `org.apache.spark.rdd.RDD.randomSplit()`
   * @return GradientBoostedTreesModel that can be used for prediction.
   */
  @Since("1.4.0")
  def runWithValidation(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint]): GradientBoostedTreesModel = {
    val algo = boostingStrategy.treeStrategy.algo
    val (trees, treeWeights) = NewGBT.runWithValidation(input.map {
      case LabeledPoint(label, features) =>
        Instance(label, 1.0, features.asML)
    }, validationInput.map {
      case LabeledPoint(label, features) =>
        Instance(label, 1.0, features.asML)
    }, boostingStrategy, seed.toLong, "all")
    new GradientBoostedTreesModel(algo, trees.map(_.toOld), treeWeights)
  }

  /**
   * Java-friendly API for `org.apache.spark.mllib.tree.GradientBoostedTrees.runWithValidation`.
   */
  @Since("1.4.0")
  def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint]): GradientBoostedTreesModel = {
    runWithValidation(input.rdd, validationInput.rdd)
  }
}

@Since("1.2.0")
object GradientBoostedTrees extends Logging {

  /**
   * Method to train a gradient boosting model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param boostingStrategy Configuration options for the boosting algorithm.
   * @return GradientBoostedTreesModel that can be used for prediction.
   */
  @Since("1.2.0")
  def train(
      input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): GradientBoostedTreesModel = {
    new GradientBoostedTrees(boostingStrategy, seed = 0).run(input)
  }

  /**
   * Java-friendly API for `org.apache.spark.mllib.tree.GradientBoostedTrees.train`
   */
  @Since("1.2.0")
  def train(
      input: JavaRDD[LabeledPoint],
      boostingStrategy: BoostingStrategy): GradientBoostedTreesModel = {
    train(input.rdd, boostingStrategy)
  }
}

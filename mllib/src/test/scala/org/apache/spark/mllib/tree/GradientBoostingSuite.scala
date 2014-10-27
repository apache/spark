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

import org.scalatest.FunSuite

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.mllib.tree.impurity.{Variance, Gini}
import org.apache.spark.mllib.tree.loss.{SquaredError, LogLoss}
import org.apache.spark.mllib.tree.model.{WeightedEnsembleModel, DecisionTreeModel}

import org.apache.spark.mllib.util.LocalSparkContext

/**
 * Test suite for [[GradientBoosting]].
 */
class GradientBoostingSuite extends FunSuite with LocalSparkContext {

  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. GradientBoosting (numEstimators = 1)") {

    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numEstimators = 1

    val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
    val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)

    val dt = DecisionTree.train(remappedInput, treeStrategy)

    val boostingStrategy = new BoostingStrategy(algo = Classification,
      numEstimators = numEstimators, loss = LogLoss, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)

    val gbt = GradientBoosting.trainClassifier(rdd, boostingStrategy)
    assert(gbt.baseLearners.size === 1)
    val gbtTree = gbt.baseLearners(0)


    EnsembleTestHelper.validateClassifier(gbt, arr, 0.9)

    // Make sure trees are the same.
    assert(gbtTree.toString == dt.toString)
  }

  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. GradientBoosting (numEstimators = 10)") {

    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numEstimators = 10

    val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
    val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)

    val dt = DecisionTree.train(remappedInput, treeStrategy)

    val boostingStrategy = new BoostingStrategy(algo = Classification,
      numEstimators = numEstimators, loss = LogLoss, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)

    val gbt = GradientBoosting.trainClassifier(rdd, boostingStrategy)
    assert(gbt.baseLearners.size === 10)
    val gbtTree = gbt.baseLearners(0)


    EnsembleTestHelper.validateClassifier(gbt, arr, 0.9)

    // Make sure trees are the same.
    assert(gbtTree.toString == dt.toString)
  }

  test("Binary classification with continuous features:" +
    " Stochastic GradientBoosting (numEstimators = 10, learning rate = 0.9, subsample = 0.75)") {

    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numEstimators = 10

    val boostingStrategy = new BoostingStrategy(algo = Classification,
      numEstimators = numEstimators, loss = LogLoss, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      subsample = 0.75)

    val gbt = GradientBoosting.trainClassifier(rdd, boostingStrategy)
    assert(gbt.baseLearners.size === 10)

    EnsembleTestHelper.validateClassifier(gbt, arr, 0.9)

  }



  test("Regression with continuous features:" +
    " comparing DecisionTree vs. GradientBoosting (numEstimators = 1)") {

    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numEstimators = 1

    val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      maxBins = 10)

    val dt = DecisionTree.train(rdd, treeStrategy)

    val boostingStrategy = new BoostingStrategy(algo = Regression,
      numEstimators = numEstimators, loss = SquaredError, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      maxBins = 10)

    val gbt = GradientBoosting.trainRegressor(rdd, boostingStrategy)
    assert(gbt.baseLearners.size === 1)
    val gbtTree = gbt.baseLearners(0)

    // Make sure trees are the same.
    assert(gbtTree.toString == dt.toString)
    assert(gbt.baseLearnerWeights(0) === 1)

    EnsembleTestHelper.validateRegressor(gbt, arr, 0.01)
    DecisionTreeSuite.validateRegressor(dt, arr, 0.01)

  }

  test("Regression with continuous features:" +
    " comparing DecisionTree vs. GradientBoosting (numEstimators = 10)") {

    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numEstimators = 50

    val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      maxBins = 10)

    val dt = DecisionTree.train(rdd, treeStrategy)

    val boostingStrategy = new BoostingStrategy(algo = Regression,
      numEstimators = numEstimators, loss = SquaredError, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      maxBins = 10, learningRate = 1)

    val gbt = GradientBoosting.trainRegressor(rdd, boostingStrategy)
    assert(gbt.baseLearners.size === numEstimators)
    val gbtTree = gbt.baseLearners(0)

    // Make sure trees are the same.
    assert(gbtTree.toString == dt.toString)
    assert(gbt.baseLearnerWeights(0) === 1)
    assert(gbt.baseLearnerWeights(1) === 1)

    EnsembleTestHelper.validateRegressor(gbt, arr, 0.01)
    DecisionTreeSuite.validateRegressor(dt, arr, 0.01)

  }

  test("Regression with continuous features:" +
    " Stochastic GradientBoosting (numEstimators = 100, learning rate = 1, subsample = 0.75)") {

    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numEstimators = 50
    val learningRate = 1
    val subsample = 0.75

    val boostingStrategy = new BoostingStrategy(algo = Regression,
      numEstimators = numEstimators, loss = SquaredError, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      maxBins = 10, learningRate = learningRate, subsample = subsample)

    val gbt = GradientBoosting.trainRegressor(rdd, boostingStrategy)
    assert(gbt.baseLearners.size === numEstimators)
    val gbtTree = gbt.baseLearners(0)

    // Make sure trees are the same.
    assert(gbt.baseLearnerWeights(0) === 1)
    assert(gbt.baseLearnerWeights(1) === learningRate)

    EnsembleTestHelper.validateRegressor(gbt, arr, 0.01)

  }

}

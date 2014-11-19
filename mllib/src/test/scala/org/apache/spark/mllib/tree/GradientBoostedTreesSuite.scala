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
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.tree.loss.{AbsoluteError, SquaredError, LogLoss}

import org.apache.spark.mllib.util.MLlibTestSparkContext

/**
 * Test suite for [[GradientBoostedTrees]].
 */
class GradientBoostedTreesSuite extends FunSuite with MLlibTestSparkContext {

  test("Regression with continuous features: SquaredError") {
    GradientBoostedTreesSuite.testCombinations.foreach {
      case (numIterations, learningRate, subsamplingRate) =>
        val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100)
        val rdd = sc.parallelize(arr, 2)

        val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
          categoricalFeaturesInfo = Map.empty, subsamplingRate = subsamplingRate)
        val boostingStrategy =
          new BoostingStrategy(treeStrategy, SquaredError, numIterations, learningRate)

        val gbt = GradientBoostedTrees.train(rdd, boostingStrategy)

        assert(gbt.trees.size === numIterations)
        EnsembleTestHelper.validateRegressor(gbt, arr, 0.03)

        val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        val dt = DecisionTree.train(remappedInput, treeStrategy)

        // Make sure trees are the same.
        assert(gbt.trees.head.toString == dt.toString)
    }
  }

  test("Regression with continuous features: Absolute Error") {
    GradientBoostedTreesSuite.testCombinations.foreach {
      case (numIterations, learningRate, subsamplingRate) =>
        val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100)
        val rdd = sc.parallelize(arr, 2)

        val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
          categoricalFeaturesInfo = Map.empty, subsamplingRate = subsamplingRate)
        val boostingStrategy =
          new BoostingStrategy(treeStrategy, AbsoluteError, numIterations, learningRate)

        val gbt = GradientBoostedTrees.train(rdd, boostingStrategy)

        assert(gbt.trees.size === numIterations)
        EnsembleTestHelper.validateRegressor(gbt, arr, 0.85, "mae")

        val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        val dt = DecisionTree.train(remappedInput, treeStrategy)

        // Make sure trees are the same.
        assert(gbt.trees.head.toString == dt.toString)
    }
  }

  test("Binary classification with continuous features: Log Loss") {
    GradientBoostedTreesSuite.testCombinations.foreach {
      case (numIterations, learningRate, subsamplingRate) =>
        val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100)
        val rdd = sc.parallelize(arr, 2)

        val treeStrategy = new Strategy(algo = Classification, impurity = Variance, maxDepth = 2,
          numClassesForClassification = 2, categoricalFeaturesInfo = Map.empty,
          subsamplingRate = subsamplingRate)
        val boostingStrategy =
          new BoostingStrategy(treeStrategy, LogLoss, numIterations, learningRate)

        val gbt = GradientBoostedTrees.train(rdd, boostingStrategy)

        assert(gbt.trees.size === numIterations)
        EnsembleTestHelper.validateClassifier(gbt, arr, 0.9)

        val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        val ensembleStrategy = treeStrategy.copy
        ensembleStrategy.algo = Regression
        ensembleStrategy.impurity = Variance
        val dt = DecisionTree.train(remappedInput, ensembleStrategy)

        // Make sure trees are the same.
        assert(gbt.trees.head.toString == dt.toString)
    }
  }

}

object GradientBoostedTreesSuite {

  // Combinations for estimators, learning rates and subsamplingRate
  val testCombinations = Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 1.0, 0.75), (10, 0.1, 0.75))
}

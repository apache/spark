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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.{Gini, Variance}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils


/**
 * Test suite for [[RandomForest]].
 */
class RandomForestSuite extends SparkFunSuite with MLlibTestSparkContext {
  def binaryClassificationTestWithContinuousFeatures(strategy: Strategy): Unit = {
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val numTrees = 1

    val rf = RandomForest.trainClassifier(rdd, strategy, numTrees = numTrees,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf.trees.length === 1)
    val rfTree = rf.trees(0)

    val dt = DecisionTree.train(rdd, strategy)

    EnsembleTestHelper.validateClassifier(rf, arr, 0.9)
    DecisionTreeSuite.validateClassifier(dt, arr, 0.9)

    // Make sure trees are the same.
    assert(rfTree.toString == dt.toString)
  }

  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)
    binaryClassificationTestWithContinuousFeatures(strategy)
  }

  test("Binary classification with continuous features and node Id cache :" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      useNodeIdCache = true)
    binaryClassificationTestWithContinuousFeatures(strategy)
  }

  def regressionTestWithContinuousFeatures(strategy: Strategy): Unit = {
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
    val rdd = sc.parallelize(arr)
    val numTrees = 1

    val rf = RandomForest.trainRegressor(rdd, strategy, numTrees = numTrees,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf.trees.length === 1)
    val rfTree = rf.trees(0)

    val dt = DecisionTree.train(rdd, strategy)

    EnsembleTestHelper.validateRegressor(rf, arr, 0.01)
    DecisionTreeSuite.validateRegressor(dt, arr, 0.01)

    // Make sure trees are the same.
    assert(rfTree.toString == dt.toString)
  }

  test("Regression with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new Strategy(algo = Regression, impurity = Variance,
      maxDepth = 2, maxBins = 10, numClasses = 2,
      categoricalFeaturesInfo = categoricalFeaturesInfo)
    regressionTestWithContinuousFeatures(strategy)
  }

  test("Regression with continuous features and node Id cache :" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new Strategy(algo = Regression, impurity = Variance,
      maxDepth = 2, maxBins = 10, numClasses = 2,
      categoricalFeaturesInfo = categoricalFeaturesInfo, useNodeIdCache = true)
    regressionTestWithContinuousFeatures(strategy)
  }

  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0))
    arr(1) = new LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0))
    arr(2) = new LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0))
    arr(3) = new LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
    val categoricalFeaturesInfo = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val input = sc.parallelize(arr)

    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClasses = 3, categoricalFeaturesInfo = categoricalFeaturesInfo)
    val model = RandomForest.trainClassifier(input, strategy, numTrees = 2,
      featureSubsetStrategy = "sqrt", seed = 12345)
  }

  test("subsampling rate in RandomForest") {
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(5, 20)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = Map.empty[Int, Int],
      useNodeIdCache = true)

    val rf1 = RandomForest.trainClassifier(rdd, strategy, numTrees = 3,
      featureSubsetStrategy = "auto", seed = 123)
    strategy.subsamplingRate = 0.5
    val rf2 = RandomForest.trainClassifier(rdd, strategy, numTrees = 3,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf1.toDebugString != rf2.toDebugString)
  }

  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Array(Classification, Regression).foreach { algo =>
      val trees = Range(0, 3).map(_ => DecisionTreeSuite.createModel(algo)).toArray
      val model = new RandomForestModel(algo, trees)

      // Save model, load it back, and compare.
      try {
        model.save(sc, path)
        val sameModel = RandomForestModel.load(sc, path)
        assert(model.algo == sameModel.algo)
        model.trees.zip(sameModel.trees).foreach { case (treeA, treeB) =>
          DecisionTreeSuite.checkEqual(treeA, treeB)
        }
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }

}

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
import org.apache.spark.internal.{MDC, MessageWithContext}
import org.apache.spark.internal.LogKey.{LEARNING_RATE, NUM_ITERATIONS, SUBSAMPLING_RATE}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.tree.loss.{AbsoluteError, LogLoss, SquaredError}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Test suite for [[GradientBoostedTrees]].
 */
class GradientBoostedTreesSuite extends SparkFunSuite with MLlibTestSparkContext {

  private def buildErrorLog(
      numIterations: Int,
      learningRate: Double,
      subsamplingRate: Double): MessageWithContext = {
    log"FAILED for numIterations=${MDC(NUM_ITERATIONS, numIterations)}, " +
      log"learningRate=${MDC(LEARNING_RATE, learningRate)}, " +
      log"subsamplingRate=${MDC(SUBSAMPLING_RATE, subsamplingRate)}"
  }

  test("Regression with continuous features: SquaredError") {
    GradientBoostedTreesSuite.testCombinations.foreach {
      case (numIterations, learningRate, subsamplingRate) =>
        val rdd = sc.parallelize(GradientBoostedTreesSuite.data.toImmutableArraySeq, 2)

        val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
          categoricalFeaturesInfo = Map.empty, subsamplingRate = subsamplingRate)
        val boostingStrategy =
          new BoostingStrategy(treeStrategy, SquaredError, numIterations, learningRate)

        val gbt = GradientBoostedTrees.train(rdd, boostingStrategy)

        assert(gbt.trees.length === numIterations)
        try {
          EnsembleTestHelper.validateRegressor(
            gbt, GradientBoostedTreesSuite.data.toImmutableArraySeq, 0.06)
        } catch {
          case e: java.lang.AssertionError =>
            logError(buildErrorLog(numIterations, learningRate, subsamplingRate))
            throw e
        }

        val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        val dt = DecisionTree.train(remappedInput, treeStrategy)

        // Make sure trees are the same.
        assert(gbt.trees.head.toString == dt.toString)
    }
  }

  test("Regression with continuous features: Absolute Error") {
    GradientBoostedTreesSuite.testCombinations.foreach {
      case (numIterations, learningRate, subsamplingRate) =>
        val rdd = sc.parallelize(GradientBoostedTreesSuite.data.toImmutableArraySeq, 2)

        val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
          categoricalFeaturesInfo = Map.empty, subsamplingRate = subsamplingRate)
        val boostingStrategy =
          new BoostingStrategy(treeStrategy, AbsoluteError, numIterations, learningRate)

        val gbt = GradientBoostedTrees.train(rdd, boostingStrategy)

        assert(gbt.trees.length === numIterations)
        try {
          EnsembleTestHelper.validateRegressor(
            gbt, GradientBoostedTreesSuite.data.toImmutableArraySeq, 0.85, "mae")
        } catch {
          case e: java.lang.AssertionError =>
            logError(buildErrorLog(numIterations, learningRate, subsamplingRate))
            throw e
        }

        val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        val dt = DecisionTree.train(remappedInput, treeStrategy)

        // Make sure trees are the same.
        assert(gbt.trees.head.toString == dt.toString)
    }
  }

  test("Binary classification with continuous features: Log Loss") {
    GradientBoostedTreesSuite.testCombinations.foreach {
      case (numIterations, learningRate, subsamplingRate) =>
        val rdd = sc.parallelize(GradientBoostedTreesSuite.data.toImmutableArraySeq, 2)

        val treeStrategy = new Strategy(algo = Classification, impurity = Variance, maxDepth = 2,
          numClasses = 2, categoricalFeaturesInfo = Map.empty,
          subsamplingRate = subsamplingRate)
        val boostingStrategy =
          new BoostingStrategy(treeStrategy, LogLoss, numIterations, learningRate)

        val gbt = GradientBoostedTrees.train(rdd, boostingStrategy)

        assert(gbt.trees.length === numIterations)
        try {
          EnsembleTestHelper.validateClassifier(
            gbt, GradientBoostedTreesSuite.data.toImmutableArraySeq, 0.9)
        } catch {
          case e: java.lang.AssertionError =>
            logError(buildErrorLog(numIterations, learningRate, subsamplingRate))
            throw e
        }

        val remappedInput = rdd.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        val ensembleStrategy = treeStrategy.copy
        ensembleStrategy.algo = Regression
        ensembleStrategy.impurity = Variance
        val dt = DecisionTree.train(remappedInput, ensembleStrategy)

        // Make sure trees are the same.
        assert(gbt.trees.head.toString == dt.toString)
    }
  }

  test("SPARK-5496: BoostingStrategy.defaultParams should recognize Classification") {
    for (algo <- Seq("classification", "Classification", "regression", "Regression")) {
      BoostingStrategy.defaultParams(algo)
    }
  }

  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    val trees = Range(0, 3).map(_ => DecisionTreeSuite.createModel(Regression)).toArray
    val treeWeights = Array(0.1, 0.3, 1.1)

    Array(Classification, Regression).foreach { algo =>
      val model = new GradientBoostedTreesModel(algo, trees, treeWeights)

      // Save model, load it back, and compare.
      try {
        model.save(sc, path)
        val sameModel = GradientBoostedTreesModel.load(sc, path)
        assert(model.algo == sameModel.algo)
        model.trees.zip(sameModel.trees).foreach { case (treeA, treeB) =>
          DecisionTreeSuite.checkEqual(treeA, treeB)
        }
        assert(model.treeWeights === sameModel.treeWeights)
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }

  test("Checkpointing") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    sc.setCheckpointDir(path)

    val rdd = sc.parallelize(GradientBoostedTreesSuite.data.toImmutableArraySeq, 2)

    val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
      categoricalFeaturesInfo = Map.empty, checkpointInterval = 2)
    val boostingStrategy = new BoostingStrategy(treeStrategy, SquaredError, 5, 0.1)

    val gbt = GradientBoostedTrees.train(rdd, boostingStrategy)

    sc.checkpointDir = None
    Utils.deleteRecursively(tempDir)
  }

}

private[spark] object GradientBoostedTreesSuite {

  // Combinations for estimators, learning rates and subsamplingRate
  val testCombinations = Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 0.5, 0.75), (10, 0.1, 0.75))

  val data = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100)
  val trainData = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 120)
  val validateData = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 80)
}

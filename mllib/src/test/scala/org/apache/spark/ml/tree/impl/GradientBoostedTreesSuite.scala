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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.loss.{Loss, AbsoluteError, LogLoss, SquaredError}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.tree.{GradientBoostedTreesSuite => OldGBTSuite}
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils

/**
 * Test suite for [[GradientBoostedTrees]].
 */
class GradientBoostedTreesSuite extends SparkFunSuite with MLlibTestSparkContext with Logging {

  test("runWithValidation stops early and performs better on a validation dataset") {
    import GradientBoostedTreesSuite.predict
    // Set numIterations large enough so that it stops early.
    val numIterations = 20
    val trainRdd = sc.parallelize(OldGBTSuite.trainData, 2)
    val validateRdd = sc.parallelize(OldGBTSuite.validateData, 2)
    val trainDF = sqlContext.createDataFrame(trainRdd)
    val validateDF = sqlContext.createDataFrame(validateRdd)

    val algos = Array(Regression, Regression, Classification)
    val losses = Array(SquaredError, AbsoluteError, LogLoss)
    algos.zip(losses).foreach { case (algo, loss) =>
      val treeStrategy = new Strategy(algo = algo, impurity = Variance, maxDepth = 2,
        categoricalFeaturesInfo = Map.empty)
      val boostingStrategy =
        new BoostingStrategy(treeStrategy, loss, numIterations, validationTol = 0.0)
      val (validateTrees, validateTreeWeights) = GradientBoostedTrees
        .runWithValidation(trainRdd, validateRdd, boostingStrategy, 42L)
      val numTrees = validateTrees.length
      assert(numTrees !== numIterations)

      // Test that it performs better on the validation dataset.
      val (trees, treeWeights) = GradientBoostedTrees.run(trainRdd, boostingStrategy, 42L)
      val (errorWithoutValidation, errorWithValidation) = {
        if (algo == Classification) {
          val remappedRdd = validateRdd.map(x => new LabeledPoint(2 * x.label - 1, x.features))
          (remappedRdd.map(lp => loss.computeError(predict(lp.features, trees, treeWeights))).mean(),
            remappedRdd.map(lp => predict(lp.features, validateTrees, validateTreeWeights)).mean())
        } else {
          (validateRdd.map(lp => predict(lp.features, trees, treeWeights)).mean(),
            validateRdd.map(lp => predict(lp.features, validateTrees, validateTreeWeights)).mean())
        }
      }
      assert(errorWithValidation <= errorWithoutValidation)

      // Test that results from evaluateEachIteration comply with runWithValidation.
      // Note that convergenceTol is set to 0.0
      val evaluationArray = gbt.evaluateEachIteration(validateRdd, loss)
      assert(evaluationArray.length === numIterations)
      assert(evaluationArray(numTrees) > evaluationArray(numTrees - 1))
      var i = 1
      while (i < numTrees) {
        assert(evaluationArray(i) <= evaluationArray(i - 1))
        i += 1
      }
    }
  }

  test("Checkpointing") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    sc.setCheckpointDir(path)

    val rdd = sc.parallelize(OldGBTSuite.data, 2)

    val treeStrategy = new Strategy(algo = Regression, impurity = Variance, maxDepth = 2,
      categoricalFeaturesInfo = Map.empty, checkpointInterval = 2)
    val boostingStrategy = new BoostingStrategy(treeStrategy, SquaredError, 5, 0.1)

    val gbt = GradientBoostedTrees.run(rdd, boostingStrategy, 42L)

    sc.checkpointDir = None
    Utils.deleteRecursively(tempDir)
  }

}

private object GradientBoostedTreesSuite {

  def computeError(
      data: RDD[LabeledPoint],
      trees: Array[DecisionTreeRegressionModel],
      treeWeights: Array[Double],
      loss: Loss): Double = {
    data.map { lp =>
      val predicted = trees.zip(treeWeights).foldLeft(0.0) { case (acc, (model, weight)) =>
        acc + model.rootNode.predictImpl(lp.features).prediction * weight
      }
      loss.computeError(predicted, lp.label)
    }.mean()
  }

  def predict(features: Vector, trees: Array[DecisionTreeRegressionModel],
              treeWeights: Array[Double]): Double = {
    trees.zip(treeWeights).foldLeft(0.0) { case (acc, (model, weight)) =>
      acc + model.rootNode.predictImpl(features).prediction * weight
    }
  }
}

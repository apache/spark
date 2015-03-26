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

package org.apache.spark.mllib.regression

import org.scalatest.FunSuite
import org.apache.spark.mllib.impl.TreeTests
import org.apache.spark.mllib.tree.{EnsembleTestHelper, GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD


/**
 * Test suite for [[GBTRegressor]].
 */
class GBTRegressorSuite extends FunSuite with MLlibTestSparkContext {

  import GBTRegressorSuite.compareAPIs

  // Combinations for estimators, learning rates and subsamplingRate
  private val testCombinations =
    Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 0.5, 0.75), (10, 0.1, 0.75))

  private var data: RDD[LabeledPoint] = _
  private var trainData: RDD[LabeledPoint] = _
  private var validationData: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    data = sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100), 2)
    trainData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 120), 2)
    validationData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 80), 2)
  }

  test("Regression with continuous features: SquaredError") {
    val categoricalFeatures = Map.empty[Int, Int]
    GBTRegressor.supportedLosses.foreach { loss =>
      testCombinations.foreach {
        case (numIterations, learningRate, subsamplingRate) =>
          val gbt = new GBTRegressor()
            .setMaxDepth(2)
            .setSubsamplingRate(subsamplingRate)
            .setLoss(loss)
            .setNumIterations(numIterations)
            .setLearningRate(learningRate)
          compareAPIs(data, None, gbt, categoricalFeatures)
      }
    }
  }

  // TODO: test("model save/load")

  test("runWithValidation stops early and performs better on a validation dataset") {
    val categoricalFeatures = Map.empty[Int, Int]
    // Set numIterations large enough so that it stops early.
    val numIterations = 20
    GBTRegressor.supportedLosses.foreach { loss =>
      val gbt = new GBTRegressor()
        .setNumIterations(numIterations)
        .setMaxDepth(2)
        .setLoss(loss)
        .setValidationTol(0.0)
      compareAPIs(trainData, None, gbt, categoricalFeatures)
      compareAPIs(trainData, Some(validationData), gbt, categoricalFeatures)
    }
  }
}

private object GBTRegressorSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      validationData: Option[RDD[LabeledPoint]],
      gbt: GBTRegressor,
      categoricalFeatures: Map[Int, Int]): Unit = {
    val oldBoostingStrategy = gbt.getOldBoostingStrategy(categoricalFeatures)
    val oldGBT = new OldGBT(oldBoostingStrategy)
    val (oldModel, newModel) = validationData match {
      case None =>
        (oldGBT.run(data),
          gbt.run(data, categoricalFeatures))
      case Some(valData) =>
        (oldGBT.runWithValidation(data, valData),
          gbt.runWithValidation(data, valData, categoricalFeatures))
    }
    val oldModelAsNew = GBTRegressionModel.fromOld(oldModel)
    TreeTests.checkEqual(oldModelAsNew, newModel)
  }
}

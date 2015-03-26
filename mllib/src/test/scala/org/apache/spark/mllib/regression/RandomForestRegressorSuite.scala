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
import org.apache.spark.mllib.tree.EnsembleTestHelper
import org.apache.spark.mllib.tree.{DecisionTreeSuite => OldDecisionTreeSuite,
  RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
 * Test suite for [[RandomForestRegressor]].
 */
class RandomForestRegressorSuite extends FunSuite with MLlibTestSparkContext {

  import RandomForestRegressorSuite.compareAPIs

  private var orderedLabeledPoints50_1000: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    orderedLabeledPoints50_1000 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////

  def regressionTestWithContinuousFeatures(rf: RandomForestRegressor) {
    val rdd = orderedLabeledPoints50_1000
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val newRF = rf
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(10)
      .setNumTrees(1)
      .setFeaturesPerNode("auto")
      .setSeed(123)
    compareAPIs(orderedLabeledPoints50_1000, newRF, categoricalFeaturesInfo)
  }

  test("Regression with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestRegressor()
    regressionTestWithContinuousFeatures(rf)
  }

  test("Regression with continuous features and node Id cache :" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestRegressor()
      .setCacheNodeIds(cacheNodeIds = true)
    regressionTestWithContinuousFeatures(rf)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    val trees = Range(0, 3).map(_ => OldDecisionTreeSuite.createModel(OldAlgo.Regression)).toArray
    val oldModel = new OldRandomForestModel(OldAlgo.Regression, trees)
    val newModel = RandomForestRegressionModel.fromOld(oldModel)

    // Save model, load it back, and compare.
    try {
      newModel.save(sc, path)
      val sameNewModel = RandomForestRegressionModel.load(sc, path)
      TreeTests.checkEqual(newModel, sameNewModel)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

private object RandomForestRegressorSuite extends FunSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      rf: RandomForestRegressor,
      categoricalFeatures: Map[Int, Int]): Unit = {
    val oldStrategy = rf.getOldStrategy(categoricalFeatures)
    val oldModel = OldRandomForest.trainRegressor(
      data, oldStrategy, rf.getNumTrees, rf.getFeaturesPerNodeStr, rf.getSeed.toInt)
    val newModel = rf.run(data, categoricalFeatures)
    val oldModelAsNew = RandomForestRegressionModel.fromOld(oldModel)
    TreeTests.checkEqual(oldModelAsNew, newModel)
  }
}

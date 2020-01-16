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

package org.apache.spark.ml.regression

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.tree.EnsembleTestHelper
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Test suite for [[ExtraTreesRegressor]].
 */
class ExtraTreesRegressorSuite extends MLTest with DefaultReadWriteTest{

  import testImplicits._

  private var orderedLabeledPoints50_1000: RDD[LabeledPoint] = _
  private var linearRegressionData: DataFrame = _
  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()
    orderedLabeledPoints50_1000 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000)
        .map(_.asML))

    linearRegressionData = sc.parallelize(LinearDataGenerator.generateLinearInput(
      intercept = 6.3, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
      xVariance = Array(0.7, 1.2), nPoints = 1000, seed, eps = 0.5), 2).map(_.asML).toDF()
  }

  test("params") {
    ParamsSuite.checkParams(new ExtraTreesRegressor)
    val model = new ExtraTreesRegressionModel("etr",
      Array(new DecisionTreeRegressionModel("dtr", new LeafNode(0.0, 0.0, null), 1)), 2)
    ParamsSuite.checkParams(model)
  }

  test("tree params") {
    val etr = new ExtraTreesRegressor()
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(10)
      .setNumTrees(3)
      .setSeed(seed)

    val df = orderedLabeledPoints50_1000.toDF()
    val model = etr.fit(df)

    model.trees.foreach (i => {
      assert(i.getMaxDepth === model.getMaxDepth)
      assert(i.getSeed === model.getSeed)
      assert(i.getImpurity === model.getImpurity)
      assert(i.getMaxBins === model.getMaxBins)
    })
  }

  test("Regression with continuous features") {
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(orderedLabeledPoints50_1000,
      categoricalFeatures, numClasses = 0)

    val etr = new ExtraTreesRegressor()
      .setMaxDepth(8)
      .setSeed(seed)
    val model = etr.fit(df)

    MLTestingUtils.validateRegressor(model,
      orderedLabeledPoints50_1000.collect, 0.013, "mse")
  }

  test("Regression with continuous features and node Id cache") {
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(orderedLabeledPoints50_1000,
      categoricalFeatures, numClasses = 0)

    val etr = new ExtraTreesRegressor()
      .setMaxDepth(8)
      .setSeed(seed)
      .setCacheNodeIds(true)
    val model = etr.fit(df)

    MLTestingUtils.validateRegressor(model,
      orderedLabeledPoints50_1000.collect, 0.013, "mse")
  }

  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0))
    arr(1) = LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0))
    arr(2) = LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0))
    arr(3) = LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
    val categoricalFeatures = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val df = TreeTests.setMetadata(sc.parallelize(arr), categoricalFeatures, numClasses = 0)

    val etc = new ExtraTreesRegressor()
      .setFeatureSubsetStrategy("sqrt")
      .setMaxDepth(7)
    val model = etc.fit(df)

    MLTestingUtils.validateRegressor(model, arr, 0.08, "mse")
  }

  test("prediction on single instance") {
    val df = orderedLabeledPoints50_1000.toDF()

    val etr = new ExtraTreesRegressor()
      .setImpurity("variance")
      .setMaxDepth(2)
      .setMaxBins(10)
      .setNumTrees(1)
      .setFeatureSubsetStrategy("auto")
      .setSeed(123)
    val model = etr.fit(df)
    testPredictionModelSinglePrediction(model, df)
  }

  test("Feature importance with toy data") {
    // In this data, feature 1 is very important.
    val data = TreeTests.featureImportanceData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(data, categoricalFeatures, 0)

    val etr = new ExtraTreesRegressor()
      .setImpurity("variance")
      .setMaxDepth(3)
      .setNumTrees(3)
      .setFeatureSubsetStrategy("all")
      .setSeed(123)
    val model = etr.fit(df)

    MLTestingUtils.checkCopyAndUids(etr, model)

    val importances = model.featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  test("model support predict leaf index") {
    val model0 = new DecisionTreeRegressionModel("dtr", TreeTests.root0, 3)
    val model1 = new DecisionTreeRegressionModel("dtr", TreeTests.root1, 3)
    val model = new ExtraTreesRegressionModel("etr", Array(model0, model1), 3)
    model.setLeafCol("predictedLeafId")
      .setPredictionCol("")

    val data = TreeTests.getTwoTreesLeafData
    data.foreach { case (leafId, vec) => assert(leafId === model.predictLeaf(vec)) }

    val df = sc.parallelize(data, 1).toDF("leafId", "features")
    model.transform(df).select("leafId", "predictedLeafId")
      .collect()
      .foreach { case Row(leafId: Vector, predictedLeafId: Vector) =>
        assert(leafId === predictedLeafId)
    }
  }

  test("should support all NumericType labels and not support other types") {
    val etr = new ExtraTreesRegressor()
      .setMaxDepth(1)

    MLTestingUtils.checkNumericTypes[ExtraTreesRegressionModel, ExtraTreesRegressor](
      etr, spark, isClassification = false) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  test("training with sample weights") {
    val df = linearRegressionData
    val numClasses = 0
    // (numTrees, maxDepth, fractionInTol)
    val testParams = Seq(
      (50, 5, 0.75),
      (50, 10, 0.75),
      (50, 10, 0.78)
    )

    for ((numTrees, maxDepth, tol) <- testParams) {
      val estimator = new ExtraTreesRegressor()
        .setNumTrees(numTrees)
        .setMaxDepth(maxDepth)
        .setSeed(seed)
        .setMinWeightFractionPerNode(0.05)

      MLTestingUtils.testArbitrarilyScaledWeights[ExtraTreesRegressionModel,
        ExtraTreesRegressor](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ ~= _ relTol 0.2, tol))
      MLTestingUtils.testOutliersWithSmallWeights[ExtraTreesRegressionModel,
        ExtraTreesRegressor](df.as[LabeledPoint], estimator,
        numClasses, MLTestingUtils.modelPredictionEquals(df, _ ~= _ relTol 0.2, tol),
        outlierRatio = 2)
      MLTestingUtils.testOversamplingVsWeighting[ExtraTreesRegressionModel,
        ExtraTreesRegressor](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ ~= _ relTol 0.2, tol), seed)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("read/write") {
    def checkModelData(
        model: ExtraTreesRegressionModel,
        model2: ExtraTreesRegressionModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
    }

    val etr = new ExtraTreesRegressor().setNumTrees(2)
    val rdd = TreeTests.getTreeReadWriteData(sc)

    val allParamSettings = TreeTests.allParamSettings ++ Map("impurity" -> "variance")

    val continuousData =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 0)
    testEstimatorAndModelReadWrite(etr, continuousData, allParamSettings,
      allParamSettings, checkModelData)
  }
}

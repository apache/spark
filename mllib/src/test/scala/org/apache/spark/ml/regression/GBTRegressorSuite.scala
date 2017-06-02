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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{EnsembleTestHelper, GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.Utils

/**
 * Test suite for [[GBTRegressor]].
 */
class GBTRegressorSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  import GBTRegressorSuite.compareAPIs
  import testImplicits._

  // Combinations for estimators, learning rates and subsamplingRate
  private val testCombinations =
    Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 0.5, 0.75), (10, 0.1, 0.75))

  private var data: RDD[LabeledPoint] = _
  private var trainData: RDD[LabeledPoint] = _
  private var validationData: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    data = sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100), 2)
      .map(_.asML)
    trainData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 120), 2)
        .map(_.asML)
    validationData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 80), 2)
        .map(_.asML)
  }

  test("Regression with continuous features") {
    val categoricalFeatures = Map.empty[Int, Int]
    GBTRegressor.supportedLossTypes.foreach { loss =>
      testCombinations.foreach {
        case (maxIter, learningRate, subsamplingRate) =>
          val gbt = new GBTRegressor()
            .setMaxDepth(2)
            .setSubsamplingRate(subsamplingRate)
            .setLossType(loss)
            .setMaxIter(maxIter)
            .setStepSize(learningRate)
            .setSeed(123)
          compareAPIs(data, None, gbt, categoricalFeatures)
      }
    }
  }

  test("GBTRegressor behaves reasonably on toy data") {
    val df = Seq(
      LabeledPoint(10, Vectors.dense(1, 2, 3, 4)),
      LabeledPoint(-5, Vectors.dense(6, 3, 2, 1)),
      LabeledPoint(11, Vectors.dense(2, 2, 3, 4)),
      LabeledPoint(-6, Vectors.dense(6, 4, 2, 1)),
      LabeledPoint(9, Vectors.dense(1, 2, 6, 4)),
      LabeledPoint(-4, Vectors.dense(6, 3, 2, 2))
    ).toDF()
    val gbt = new GBTRegressor()
      .setMaxDepth(2)
      .setMaxIter(2)
    val model = gbt.fit(df)

    MLTestingUtils.checkCopyAndUids(gbt, model)
    val preds = model.transform(df)
    val predictions = preds.select("prediction").rdd.map(_.getDouble(0))
    // Checks based on SPARK-8736 (to ensure it is not doing classification)
    assert(predictions.max() > 2)
    assert(predictions.min() < -1)
  }

  test("Checkpointing") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    sc.setCheckpointDir(path)

    val df = data.toDF()
    val gbt = new GBTRegressor()
      .setMaxDepth(2)
      .setMaxIter(5)
      .setStepSize(0.1)
      .setCheckpointInterval(2)
      .setSeed(123)
    val model = gbt.fit(df)

    sc.checkpointDir = None
    Utils.deleteRecursively(tempDir)
  }

  test("should support all NumericType labels and not support other types") {
    val gbt = new GBTRegressor().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[GBTRegressionModel, GBTRegressor](
      gbt, spark, isClassification = false) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  // TODO: Reinstate test once runWithValidation is implemented  SPARK-7132
  /*
  test("runWithValidation stops early and performs better on a validation dataset") {
    val categoricalFeatures = Map.empty[Int, Int]
    // Set maxIter large enough so that it stops early.
    val maxIter = 20
    GBTRegressor.supportedLossTypes.foreach { loss =>
      val gbt = new GBTRegressor()
        .setMaxIter(maxIter)
        .setMaxDepth(2)
        .setLossType(loss)
        .setValidationTol(0.0)
      compareAPIs(trainData, None, gbt, categoricalFeatures)
      compareAPIs(trainData, Some(validationData), gbt, categoricalFeatures)
    }
  }
  */

  /////////////////////////////////////////////////////////////////////////////
  // Tests of feature importance
  /////////////////////////////////////////////////////////////////////////////
  test("Feature importance with toy data") {
    val gbt = new GBTRegressor()
      .setMaxDepth(3)
      .setMaxIter(5)
      .setSubsamplingRate(1.0)
      .setStepSize(0.5)
      .setSeed(123)

    // In this data, feature 1 is very important.
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, 0)

    val importances = gbt.fit(df).featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("model save/load") {
    def checkModelData(
        model: GBTRegressionModel,
        model2: GBTRegressionModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
    }

    val gbt = new GBTRegressor()
    val rdd = TreeTests.getTreeReadWriteData(sc)

    val allParamSettings = TreeTests.allParamSettings ++ Map("lossType" -> "squared")
    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 0)
    testEstimatorAndModelReadWrite(gbt, continuousData, allParamSettings,
      allParamSettings, checkModelData)
  }
}

private object GBTRegressorSuite extends SparkFunSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      validationData: Option[RDD[LabeledPoint]],
      gbt: GBTRegressor,
      categoricalFeatures: Map[Int, Int]): Unit = {
    val numFeatures = data.first().features.size
    val oldBoostingStrategy = gbt.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Regression)
    val oldGBT = new OldGBT(oldBoostingStrategy, gbt.getSeed.toInt)
    val oldModel = oldGBT.run(data.map(OldLabeledPoint.fromML))
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 0)
    val newModel = gbt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldModelAsNew = GBTRegressionModel.fromOld(
      oldModel, newModel.parent.asInstanceOf[GBTRegressor], categoricalFeatures, numFeatures)
    TreeTests.checkEqual(oldModelAsNew, newModel)
    assert(newModel.numFeatures === numFeatures)
    assert(oldModelAsNew.numFeatures === numFeatures)
  }
}

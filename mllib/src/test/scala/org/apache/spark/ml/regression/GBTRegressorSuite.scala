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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.tree.impl.{GradientBoostedTrees, TreeTests}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{EnsembleTestHelper, GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.Utils

/**
 * Test suite for [[GBTRegressor]].
 */
class GBTRegressorSuite extends MLTest with DefaultReadWriteTest {

  import GBTRegressorSuite.compareAPIs
  import testImplicits._

  // Combinations for estimators, learning rates and subsamplingRate
  private val testCombinations =
    Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 0.5, 0.75), (10, 0.1, 0.75))

  private var data: RDD[LabeledPoint] = _
  private var trainData: RDD[LabeledPoint] = _
  private var validationData: RDD[LabeledPoint] = _
  private var linearRegressionData: DataFrame = _
  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100), 2)
      .map(_.asML)
    trainData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 120), 2)
        .map(_.asML)
    validationData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 80), 2)
        .map(_.asML)
    linearRegressionData = sc.parallelize(LinearDataGenerator.generateLinearInput(
      intercept = 6.3, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
      xVariance = Array(0.7, 1.2), nPoints = 1000, seed, eps = 0.5), 2).map(_.asML).toDF()
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

    testTransformerByGlobalCheckFunc[(Double, Vector)](df, model, "prediction") {
      case rows: Seq[Row] =>
        val predictions = rows.map(_.getDouble(0))
        // Checks based on SPARK-8736 (to ensure it is not doing classification)
        assert(predictions.max > 2)
        assert(predictions.min < -1)
    }
  }

  test("prediction on single instance") {
    val gbt = new GBTRegressor()
      .setMaxDepth(2)
      .setMaxIter(2)
    val model = gbt.fit(trainData.toDF())
    testPredictionModelSinglePrediction(model, validationData.toDF)
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

  test("model support predict leaf index") {
    val model0 = new DecisionTreeRegressionModel("dtc", TreeTests.root0, 3)
    val model1 = new DecisionTreeRegressionModel("dtc", TreeTests.root1, 3)
    val model = new GBTRegressionModel("gbtr", Array(model0, model1), Array(1.0, 1.0), 3)
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
  // Tests of feature subset strategy
  /////////////////////////////////////////////////////////////////////////////
  test("Tests of feature subset strategy") {
    val numClasses = 2
    val gbt = new GBTRegressor()
      .setMaxDepth(3)
      .setMaxIter(5)
      .setSeed(seed)
      .setFeatureSubsetStrategy("all")

    // In this data, feature 1 is very important.
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)

    val importances = gbt.fit(df).featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)

    // GBT with different featureSubsetStrategy
    val gbtWithFeatureSubset = gbt.setFeatureSubsetStrategy("1")
    val importanceFeatures = gbtWithFeatureSubset.fit(df).featureImportances
    val mostIF = importanceFeatures.argmax
    assert(mostIF === 1)
    assert(importances(mostImportantFeature) !== importanceFeatures(mostIF))
  }

  test("model evaluateEachIteration") {
    for (lossType <- GBTRegressor.supportedLossTypes) {
      val gbt = new GBTRegressor()
        .setSeed(1L)
        .setMaxDepth(2)
        .setMaxIter(3)
        .setLossType(lossType)
      val model3 = gbt.fit(trainData.toDF)
      val model1 = new GBTRegressionModel("gbt-reg-model-test1",
        model3.trees.take(1), model3.treeWeights.take(1), model3.numFeatures)
      val model2 = new GBTRegressionModel("gbt-reg-model-test2",
        model3.trees.take(2), model3.treeWeights.take(2), model3.numFeatures)

      for (evalLossType <- GBTRegressor.supportedLossTypes) {
        val evalArr = model3.evaluateEachIteration(validationData.toDF, evalLossType)
        val lossErr1 = GradientBoostedTrees.computeWeightedError(validationData.map(_.toInstance),
          model1.trees, model1.treeWeights, model1.convertToOldLossType(evalLossType))
        val lossErr2 = GradientBoostedTrees.computeWeightedError(validationData.map(_.toInstance),
          model2.trees, model2.treeWeights, model2.convertToOldLossType(evalLossType))
        val lossErr3 = GradientBoostedTrees.computeWeightedError(validationData.map(_.toInstance),
          model3.trees, model3.treeWeights, model3.convertToOldLossType(evalLossType))

        assert(evalArr(0) ~== lossErr1 relTol 1E-3)
        assert(evalArr(1) ~== lossErr2 relTol 1E-3)
        assert(evalArr(2) ~== lossErr3 relTol 1E-3)
      }
    }
  }

  test("runWithValidation stops early and performs better on a validation dataset") {
    val validationIndicatorCol = "validationIndicator"
    val trainDF = trainData.toDF().withColumn(validationIndicatorCol, lit(false))
    val validationDF = validationData.toDF().withColumn(validationIndicatorCol, lit(true))

    val numIter = 20
    for (lossType <- GBTRegressor.supportedLossTypes) {
      val gbt = new GBTRegressor()
        .setSeed(123)
        .setMaxDepth(2)
        .setLossType(lossType)
        .setMaxIter(numIter)
      val modelWithoutValidation = gbt.fit(trainDF)

      gbt.setValidationIndicatorCol(validationIndicatorCol)
      val modelWithValidation = gbt.fit(trainDF.union(validationDF))

      assert(modelWithoutValidation.getNumTrees === numIter)
      // early stop
      assert(modelWithValidation.getNumTrees < numIter)

      val errorWithoutValidation = GradientBoostedTrees.computeWeightedError(
        validationData.map(_.toInstance),
        modelWithoutValidation.trees, modelWithoutValidation.treeWeights,
        modelWithoutValidation.getOldLossType)
      val errorWithValidation = GradientBoostedTrees.computeWeightedError(
        validationData.map(_.toInstance),
        modelWithValidation.trees, modelWithValidation.treeWeights,
        modelWithValidation.getOldLossType)

      assert(errorWithValidation < errorWithoutValidation)

      val evaluationArray = GradientBoostedTrees
        .evaluateEachIteration(validationData.map(_.toInstance), modelWithoutValidation.trees,
          modelWithoutValidation.treeWeights, modelWithoutValidation.getOldLossType,
          OldAlgo.Regression)
      assert(evaluationArray.length === numIter)
      assert(evaluationArray(modelWithValidation.getNumTrees) >
        evaluationArray(modelWithValidation.getNumTrees - 1))
      var i = 1
      while (i < modelWithValidation.getNumTrees) {
        assert(evaluationArray(i) <= evaluationArray(i - 1))
        i += 1
      }
    }
  }

  test("tree params") {
    val gbt = new GBTRegressor()
      .setMaxDepth(2)
      .setCheckpointInterval(5)
      .setSeed(123)
    val model = gbt.fit(trainData.toDF)

    model.trees.foreach (i => {
      assert(i.getMaxDepth === model.getMaxDepth)
      assert(i.getCheckpointInterval === model.getCheckpointInterval)
      assert(i.getSeed === model.getSeed)
    })
  }

  test("training with sample weights") {
    val df = linearRegressionData
    val numClasses = 0
    // (maxIter, maxDepth, subsamplingRate, fractionInTol)
    val testParams = Seq(
      (5, 5, 1.0, 0.98),
      (5, 10, 1.0, 0.98),
      (5, 10, 0.95, 0.6)
    )

    for ((maxIter, maxDepth, subsamplingRate, tol) <- testParams) {
      val estimator = new GBTRegressor()
        .setMaxIter(maxIter)
        .setMaxDepth(maxDepth)
        .setSubsamplingRate(subsamplingRate)
        .setSeed(seed)
        .setMinWeightFractionPerNode(0.1)

      MLTestingUtils.testArbitrarilyScaledWeights[GBTRegressionModel,
        GBTRegressor](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ ~= _ relTol 0.1, tol))
      MLTestingUtils.testOutliersWithSmallWeights[GBTRegressionModel,
        GBTRegressor](df.as[LabeledPoint], estimator, numClasses,
        MLTestingUtils.modelPredictionEquals(df, _ ~= _ relTol 0.1, tol),
        outlierRatio = 2)
      MLTestingUtils.testOversamplingVsWeighting[GBTRegressionModel,
        GBTRegressor](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ ~= _ relTol 0.1, tol), seed)
    }
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

  test("SPARK-33398: Load GBTRegressionModel prior to Spark 3.0") {
    val path = testFile("ml-models/gbtr-2.4.7")
    val model = GBTRegressionModel.load(path)
    assert(model.numFeatures === 692)
    assert(model.totalNumNodes === 6)
    assert(model.trees.map(_.numNodes) === Array(5, 1))

    val metadata = spark.read.json(s"$path/metadata")
    val sparkVersionStr = metadata.select("sparkVersion").first().getString(0)
    assert(sparkVersionStr === "2.4.7")
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

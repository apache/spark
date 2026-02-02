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

package org.apache.spark.ml.classification

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.LinearSVCSuite.generateSVMInput
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.{GradientBoostedTrees, TreeTests}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{EnsembleTestHelper, GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.loss.LogLoss
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Test suite for [[GBTClassifier]].
 */
class GBTClassifierSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._
  import GBTClassifierSuite.compareAPIs

  // Combinations for estimators, learning rates and subsamplingRate
  private val testCombinations =
    Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 0.5, 0.75), (10, 0.1, 0.75))

  private var data: RDD[LabeledPoint] = _
  private var trainData: RDD[LabeledPoint] = _
  private var validationData: RDD[LabeledPoint] = _
  private var binaryDataset: DataFrame = _
  private val eps: Double = 1e-5
  private val absEps: Double = 1e-8
  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()
    data = sc
      .parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 10, 100)
        .toImmutableArraySeq, 2)
      .map(_.asML)
    trainData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 120)
          .toImmutableArraySeq, 2)
        .map(_.asML)
    validationData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 80)
          .toImmutableArraySeq, 2)
        .map(_.asML)
    binaryDataset = generateSVMInput(0.01, Array[Double](-1.5, 1.0), 1000, seed).toDF()
  }

  test("params") {
    ParamsSuite.checkParams(new GBTClassifier)
    val model = new GBTClassificationModel("gbtc",
      Array(new DecisionTreeRegressionModel("dtr", new LeafNode(0.0, 0.0, null), 1)),
      Array(1.0), 1, 2)
    ParamsSuite.checkParams(model)
  }

  test("GBTClassifier: default params") {
    val gbt = new GBTClassifier
    assert(gbt.getLabelCol === "label")
    assert(gbt.getFeaturesCol === "features")
    assert(gbt.getPredictionCol === "prediction")
    assert(gbt.getRawPredictionCol === "rawPrediction")
    assert(gbt.getProbabilityCol === "probability")
    assert(gbt.getFeatureSubsetStrategy === "all")
    val df = trainData.toDF()
    val model = gbt.fit(df)
    model.transform(df)
      .select("label", "probability", "prediction", "rawPrediction")
      .collect()
    intercept[NoSuchElementException] {
      model.getThresholds
    }
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.getRawPredictionCol === "rawPrediction")
    assert(model.getProbabilityCol === "probability")
    assert(model.getFeatureSubsetStrategy === "all")
    assert(model.hasParent)

    MLTestingUtils.checkCopyAndUids(gbt, model)
  }

  test("GBTClassifier validate input dataset") {
    testInvalidClassificationLabels(new GBTClassifier().fit(_), Some(2))
    testInvalidWeights(new GBTClassifier().setWeightCol("weight").fit(_))
    testInvalidVectors(new GBTClassifier().fit(_))
  }

  test("setThreshold, getThreshold") {
    val gbt = new GBTClassifier

    // default
    withClue("GBTClassifier should not have thresholds set by default.") {
      intercept[NoSuchElementException] {
        gbt.getThresholds
      }
    }

    // Set via thresholds
    val gbt2 = new GBTClassifier
    val threshold = Array(0.3, 0.7)
    gbt2.setThresholds(threshold)
    assert(gbt2.getThresholds === threshold)
  }

  test("thresholds prediction") {
    val gbt = new GBTClassifier
    val df = trainData.toDF()
    val binaryModel = gbt.fit(df)

    // should predict all zeros
    binaryModel.setThresholds(Array(0.0, 1.0))
    testTransformer[(Double, Vector)](df, binaryModel, "prediction") {
      case Row(prediction: Double) => prediction === 0.0
    }

    // should predict all ones
    binaryModel.setThresholds(Array(1.0, 0.0))
    testTransformer[(Double, Vector)](df, binaryModel, "prediction") {
      case Row(prediction: Double) => prediction === 1.0
    }

    val gbtBase = new GBTClassifier
    val model = gbtBase.fit(df)
    val basePredictions = model.transform(df).select("prediction").collect()

    // constant threshold scaling is the same as no thresholds
    binaryModel.setThresholds(Array(1.0, 1.0))
    testTransformerByGlobalCheckFunc[(Double, Vector)](df, binaryModel, "prediction") {
      scaledPredictions: Seq[Row] =>
        assert(scaledPredictions.zip(basePredictions).forall { case (scaled, base) =>
          scaled.getDouble(0) === base.getDouble(0)
        })
    }

    // force it to use the predict method
    model.setRawPredictionCol("").setProbabilityCol("").setThresholds(Array(0, 1))
    testTransformer[(Double, Vector)](df, model, "prediction") {
      case Row(prediction: Double) => prediction === 0.0
    }
  }

  test("GBTClassifier: Predictor, Classifier methods") {
    val labelCol = "label"
    val featuresCol = "features"

    val gbt = new GBTClassifier().setSeed(123)
    val trainingDataset = trainData.toDF(labelCol, featuresCol)
    val gbtModel = gbt.fit(trainingDataset)
    assert(gbtModel.numClasses === 2)
    val numFeatures = trainingDataset.select(featuresCol).first().getAs[Vector](0).size
    assert(gbtModel.numFeatures === numFeatures)

    val validationDataset = validationData.toDF(labelCol, featuresCol)
    testTransformer[(Double, Vector)](validationDataset, gbtModel,
      "rawPrediction", "features", "probability", "prediction") {
      case Row(raw: Vector, features: Vector, prob: Vector, pred: Double) =>
        assert(raw.size === 2)
        // check that raw prediction is tree predictions dot tree weights
        val treePredictions = gbtModel.trees.map(_.rootNode.predictImpl(features).prediction)
        val prediction = BLAS.nativeBLAS.ddot(gbtModel.getNumTrees, treePredictions, 1,
          gbtModel.treeWeights, 1)
        assert(raw ~== Vectors.dense(-prediction, prediction) relTol eps)

        // Compare rawPrediction with probability
        assert(prob.size === 2)
        // Note: we should check other loss types for classification if they are added
        val predFromRaw = raw.toDense.values.map(value => LogLoss.computeProbability(value))
        assert(prob(0) ~== predFromRaw(0) relTol eps)
        assert(prob(1) ~== predFromRaw(1) relTol eps)
        assert(prob(0) + prob(1) ~== 1.0 absTol absEps)

        // Compare prediction with probability
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, GBTClassificationModel](this, gbtModel, validationDataset)
  }

  test("prediction on single instance") {

    val gbt = new GBTClassifier().setSeed(123)
    val trainingDataset = trainData.toDF("label", "features")
    val gbtModel = gbt.fit(trainingDataset)

    testPredictionModelSinglePrediction(gbtModel, trainingDataset)
    testClassificationModelSingleRawPrediction(gbtModel, trainingDataset)
    testProbClassificationModelSingleProbPrediction(gbtModel, trainingDataset)
  }

  test("GBT parameter stepSize should be in interval (0, 1]") {
    withClue("GBT parameter stepSize should be in interval (0, 1]") {
      intercept[IllegalArgumentException] {
        new GBTClassifier().setStepSize(10)
      }
    }
  }

  test("Binary classification with continuous features: Log Loss") {
    val categoricalFeatures = Map.empty[Int, Int]
    testCombinations.foreach {
      case (maxIter, learningRate, subsamplingRate) =>
        val gbt = new GBTClassifier()
          .setMaxDepth(2)
          .setSubsamplingRate(subsamplingRate)
          .setLossType("logistic")
          .setMaxIter(maxIter)
          .setStepSize(learningRate)
          .setSeed(123)
        compareAPIs(data, None, gbt, categoricalFeatures)
    }
  }

  test("Checkpointing") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    sc.setCheckpointDir(path)

    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 2)
    val gbt = new GBTClassifier()
      .setMaxDepth(2)
      .setLossType("logistic")
      .setMaxIter(5)
      .setStepSize(0.1)
      .setCheckpointInterval(2)
      .setSeed(123)
    val model = gbt.fit(df)

    MLTestingUtils.checkCopyAndUids(gbt, model)

    sc.checkpointDir = None
    Utils.deleteRecursively(tempDir)
  }

  test("model support predict leaf index") {
    val model0 = new DecisionTreeRegressionModel("dtc", TreeTests.root0, 3)
    val model1 = new DecisionTreeRegressionModel("dtc", TreeTests.root1, 3)
    val model = new GBTClassificationModel("gbtc", Array(model0, model1), Array(1.0, 1.0), 3, 2)
    model.setLeafCol("predictedLeafId")
      .setRawPredictionCol("")
      .setPredictionCol("")
      .setProbabilityCol("")

    val data = TreeTests.getTwoTreesLeafData
    data.foreach { case (leafId, vec) => assert(leafId === model.predictLeaf(vec)) }

    val df = sc.parallelize(data.toImmutableArraySeq, 1).toDF("leafId", "features")
    model.transform(df).select("leafId", "predictedLeafId")
      .collect()
      .foreach { case Row(leafId: Vector, predictedLeafId: Vector) =>
        assert(leafId === predictedLeafId)
    }
  }

  test("should support all NumericType labels and not support other types") {
    val gbt = new GBTClassifier().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[GBTClassificationModel, GBTClassifier](
      gbt, spark) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  // TODO: Reinstate test once runWithValidation is implemented   SPARK-7132
  /*
  test("runWithValidation stops early and performs better on a validation dataset") {
    val categoricalFeatures = Map.empty[Int, Int]
    // Set maxIter large enough so that it stops early.
    val maxIter = 20
    GBTClassifier.supportedLossTypes.foreach { loss =>
      val gbt = new GBTClassifier()
        .setMaxIter(maxIter)
        .setMaxDepth(2)
        .setLossType(loss)
        .setValidationTol(0.0)
      compareAPIs(trainData, None, gbt, categoricalFeatures)
      compareAPIs(trainData, Some(validationData), gbt, categoricalFeatures)
    }
  }
  */

  test("Fitting without numClasses in metadata") {
    val df: DataFrame = TreeTests.featureImportanceData(sc).toDF()
    val gbt = new GBTClassifier().setMaxDepth(1).setMaxIter(1)
    gbt.fit(df)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of feature importance
  /////////////////////////////////////////////////////////////////////////////
  test("Feature importance with toy data") {
    val numClasses = 2
    val gbt = new GBTClassifier()
      .setImpurity("Gini")
      .setMaxDepth(3)
      .setMaxIter(5)
      .setSubsamplingRate(1.0)
      .setStepSize(0.5)
      .setSeed(123)

    // In this data, feature 1 is very important.
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)

    val importances = gbt.fit(df).featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of feature subset  strategy
  /////////////////////////////////////////////////////////////////////////////
  test("Tests of feature subset strategy") {
    val numClasses = 2
    val gbt = new GBTClassifier()
      .setSeed(seed)
      .setMaxDepth(3)
      .setMaxIter(5)
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
    val gbt = new GBTClassifier()
      .setSeed(1L)
      .setMaxDepth(2)
      .setMaxIter(3)
      .setLossType("logistic")
    val model3 = gbt.fit(trainData.toDF())
    val model1 = new GBTClassificationModel("gbt-cls-model-test1",
      model3.trees.take(1), model3.treeWeights.take(1), model3.numFeatures, model3.numClasses)
    val model2 = new GBTClassificationModel("gbt-cls-model-test2",
      model3.trees.take(2), model3.treeWeights.take(2), model3.numFeatures, model3.numClasses)

    val evalArr = model3.evaluateEachIteration(validationData.toDF())
    val remappedValidationData = validationData.map {
      case LabeledPoint(label, features) =>
        Instance(label * 2 - 1, 1.0, features)
    }
    val lossErr1 = GradientBoostedTrees.computeWeightedError(remappedValidationData,
      model1.trees, model1.treeWeights, model1.getOldLossType)
    val lossErr2 = GradientBoostedTrees.computeWeightedError(remappedValidationData,
      model2.trees, model2.treeWeights, model2.getOldLossType)
    val lossErr3 = GradientBoostedTrees.computeWeightedError(remappedValidationData,
      model3.trees, model3.treeWeights, model3.getOldLossType)

    assert(evalArr(0) ~== lossErr1 relTol 1E-3)
    assert(evalArr(1) ~== lossErr2 relTol 1E-3)
    assert(evalArr(2) ~== lossErr3 relTol 1E-3)
  }

  test("runWithValidation stops early and performs better on a validation dataset") {
    val validationIndicatorCol = "validationIndicator"
    val trainDF = trainData.toDF().withColumn(validationIndicatorCol, lit(false))
    val validationDF = validationData.toDF().withColumn(validationIndicatorCol, lit(true))

    val numIter = 20
    for (lossType <- GBTClassifier.supportedLossTypes) {
      val gbt = new GBTClassifier()
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

      val (errorWithoutValidation, errorWithValidation) = {
        val remappedRdd = validationData.map {
          case LabeledPoint(label, features) =>
            Instance(label * 2 - 1, 1.0, features)
        }
        (GradientBoostedTrees.computeWeightedError(remappedRdd, modelWithoutValidation.trees,
          modelWithoutValidation.treeWeights, modelWithoutValidation.getOldLossType),
          GradientBoostedTrees.computeWeightedError(remappedRdd, modelWithValidation.trees,
            modelWithValidation.treeWeights, modelWithValidation.getOldLossType))
      }
      assert(errorWithValidation < errorWithoutValidation)

      val evaluationArray = GradientBoostedTrees
        .evaluateEachIteration(validationData.map(_.toInstance), modelWithoutValidation.trees,
          modelWithoutValidation.treeWeights, modelWithoutValidation.getOldLossType,
          OldAlgo.Classification)
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
    val categoricalFeatures = Map.empty[Int, Int]
    val df: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 2)
    val gbt = new GBTClassifier()
      .setMaxDepth(2)
      .setCheckpointInterval(5)
      .setSeed(123)
    val model = gbt.fit(df)
    model.setLeafCol("predictedLeafId")

    val transformed = model.transform(df)
    checkNominalOnDF(transformed, "prediction", model.numClasses)
    checkVectorSizeOnDF(transformed, "predictedLeafId", model.trees.length)
    checkVectorSizeOnDF(transformed, "rawPrediction", model.numClasses)
    checkVectorSizeOnDF(transformed, "probability", model.numClasses)

    model.trees.foreach (i => {
      assert(i.getMaxDepth === model.getMaxDepth)
      assert(i.getCheckpointInterval === model.getCheckpointInterval)
      assert(i.getSeed === model.getSeed)
    })
  }

  test("training with sample weights") {
    val df = binaryDataset
    val numClasses = 2
    // (maxIter, maxDepth, subsamplingRate, fractionInTol)
    val testParams = Seq(
      (5, 5, 1.0, 0.99),
      (5, 10, 1.0, 0.99),
      (5, 10, 0.95, 0.9)
    )

    for ((maxIter, maxDepth, subsamplingRate, tol) <- testParams) {
      val estimator = new GBTClassifier()
        .setMaxIter(maxIter)
        .setMaxDepth(maxDepth)
        .setSubsamplingRate(subsamplingRate)
        .setSeed(seed)
        .setMinWeightFractionPerNode(0.049)

      MLTestingUtils.testArbitrarilyScaledWeights[GBTClassificationModel,
        GBTClassifier](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ == _, tol))
      MLTestingUtils.testOutliersWithSmallWeights[GBTClassificationModel,
        GBTClassifier](df.as[LabeledPoint], estimator,
        numClasses, MLTestingUtils.modelPredictionEquals(df, _ == _, tol),
        outlierRatio = 2)
      MLTestingUtils.testOversamplingVsWeighting[GBTClassificationModel,
        GBTClassifier](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ == _, tol), seed)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("model save/load") {
    def checkModelData(
        model: GBTClassificationModel,
        model2: GBTClassificationModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
      assert(model.featureImportances == model2.featureImportances)
    }

    val gbt = new GBTClassifier()
    val rdd = TreeTests.getTreeReadWriteData(sc)

    val allParamSettings = TreeTests.allParamSettings ++ Map("lossType" -> "logistic")

    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)
    testEstimatorAndModelReadWrite(gbt, continuousData, allParamSettings,
      allParamSettings, checkModelData)
  }

  test("SPARK-33398: Load GBTClassificationModel prior to Spark 3.0") {
    val path = testFile("ml-models/gbtc-2.4.7")
    val model = GBTClassificationModel.load(path)
    assert(model.numClasses === 2)
    assert(model.numFeatures === 692)
    assert(model.getNumTrees === 2)
    assert(model.totalNumNodes === 22)
    assert(model.trees.map(_.numNodes) === Array(5, 17))

    val metadata = spark.read.json(s"$path/metadata")
    val sparkVersionStr = metadata.select("sparkVersion").first().getString(0)
    assert(sparkVersionStr === "2.4.7")
  }
}

private object GBTClassifierSuite extends SparkFunSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      validationData: Option[RDD[LabeledPoint]],
      gbt: GBTClassifier,
      categoricalFeatures: Map[Int, Int]): Unit = {
    val numFeatures = data.first().features.size
    val oldBoostingStrategy =
      gbt.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Classification)
    val oldGBT = new OldGBT(oldBoostingStrategy, gbt.getSeed.toInt)
    val oldModel = oldGBT.run(data.map(OldLabeledPoint.fromML))
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 2)
    val newModel = gbt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldModelAsNew = GBTClassificationModel.fromOld(
      oldModel, newModel.parent.asInstanceOf[GBTClassifier], categoricalFeatures,
      numFeatures, numClasses = 2)
    TreeTests.checkEqual(oldModelAsNew, newModel)
    assert(newModel.numFeatures === numFeatures)
    assert(oldModelAsNew.numFeatures === numFeatures)
  }
}

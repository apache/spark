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

import org.apache.spark.ml.classification.LinearSVCSuite.generateSVMInput
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.mllib.tree.EnsembleTestHelper
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Test suite for [[ExtraTreesClassifier]].
 */

class ExtraTreesClassifierSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private var orderedLabeledPoints50_1000: RDD[LabeledPoint] = _
  private var orderedLabeledPoints5_20: RDD[LabeledPoint] = _
  private var binaryDataset: DataFrame = _
  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()
    orderedLabeledPoints50_1000 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000))
        .map(_.asML)
    orderedLabeledPoints5_20 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 5, 20))
        .map(_.asML)
    binaryDataset = generateSVMInput(0.01, Array[Double](-1.5, 1.0), 1000, seed).toDF()
  }

  test("params") {
    ParamsSuite.checkParams(new ExtraTreesClassifier)
    val model = new ExtraTreesClassificationModel("etc",
      Array(new DecisionTreeClassificationModel("dtc", new LeafNode(0.0, 0.0, null), 1, 2)), 2, 2)
    ParamsSuite.checkParams(model)
  }

  test("Binary classification with continuous features") {
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(orderedLabeledPoints50_1000,
      categoricalFeatures, numClasses = 2)

    val etc = new ExtraTreesClassifier()
      .setImpurity("gini")
      .setMaxDepth(5)
      .setSeed(seed)
    val model = etc.fit(df)

    MLTestingUtils.validateClassifier(model,
      orderedLabeledPoints50_1000.collect, 0.98)
  }

  test("Binary classification with continuous features and node Id cache") {
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(orderedLabeledPoints50_1000,
      categoricalFeatures, numClasses = 2)

    val etc = new ExtraTreesClassifier()
      .setImpurity("gini")
      .setMaxDepth(5)
      .setSeed(seed)
      .setCacheNodeIds(true)
    assert(!etc.getBootstrap)
    val model = etc.fit(df)

    MLTestingUtils.validateClassifier(model,
      orderedLabeledPoints50_1000.collect, 0.98)
  }

  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0))
    arr(1) = LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0))
    arr(2) = LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0))
    arr(3) = LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
    val categoricalFeatures = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val df = TreeTests.setMetadata(sc.parallelize(arr), categoricalFeatures, numClasses = 3)

    val etc = new ExtraTreesClassifier()
      .setImpurity("gini")
      .setFeatureSubsetStrategy("sqrt")
      .setMaxDepth(5)
      .setSeed(seed)
    val model = etc.fit(df)

    MLTestingUtils.validateClassifier(model, arr, 1.0)
  }

  test("predictRaw and predictProbability") {
    val rdd = orderedLabeledPoints5_20
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses = 2)

    val etc = new ExtraTreesClassifier()
      .setImpurity("Gini")
      .setMaxDepth(3)
      .setNumTrees(3)
      .setSeed(seed)
    val model = etc.fit(df)

    MLTestingUtils.checkCopyAndUids(etc, model)

    testTransformer[(Vector, Double, Double)](df, model, "prediction", "rawPrediction",
      "probability") { case Row(pred: Double, rawPred: Vector, probPred: Vector) =>
      assert(pred === rawPred.argmax,
        s"Expected prediction $pred but calculated ${rawPred.argmax} from rawPrediction.")
      val sum = rawPred.toArray.sum
      assert(Vectors.dense(rawPred.toArray.map(_ / sum)) === probPred,
        "probability prediction mismatch")
      assert(probPred.toArray.sum ~== 1.0 relTol 1E-5)
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, ExtraTreesClassificationModel](this, model, df)
  }

  test("prediction on single instance") {
    val rdd = orderedLabeledPoints5_20
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses = 2)

    val etc = new ExtraTreesClassifier()
      .setImpurity("Gini")
      .setMaxDepth(3)
      .setNumTrees(3)
      .setSeed(123)
    val model = etc.fit(df)

    testPredictionModelSinglePrediction(model, df)
    testClassificationModelSingleRawPrediction(model, df)
    testProbClassificationModelSingleProbPrediction(model, df)
  }

  test("Fitting without numClasses in metadata") {
    val df = TreeTests.featureImportanceData(sc).toDF()
    val etc = new ExtraTreesClassifier()
      .setMaxDepth(1)
      .setNumTrees(1)
    etc.fit(df)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of feature importance
  /////////////////////////////////////////////////////////////////////////////
  test("Feature importance with toy data") {
    val categoricalFeatures = Map.empty[Int, Int]
    // In this data, feature 1 is very important.
    val data = TreeTests.featureImportanceData(sc)
    val df = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 2)

    val etc = new ExtraTreesClassifier()
      .setImpurity("Gini")
      .setMaxDepth(3)
      .setNumTrees(3)
      .setFeatureSubsetStrategy("all")
      .setSeed(123)

    val importances = etc.fit(df).featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  test("model support predict leaf index") {
    val model0 = new DecisionTreeClassificationModel("dtc", TreeTests.root0, 3, 2)
    val model1 = new DecisionTreeClassificationModel("dtc", TreeTests.root1, 3, 2)
    val model = new ExtraTreesClassificationModel("etc", Array(model0, model1), 3, 2)
    model.setLeafCol("predictedLeafId")
      .setRawPredictionCol("")
      .setPredictionCol("")
      .setProbabilityCol("")

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
    val etc = new ExtraTreesClassifier().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[ExtraTreesClassificationModel, ExtraTreesClassifier](
      etc, spark) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  test("tree params") {
    val rdd = orderedLabeledPoints5_20
    val categoricalFeatures = Map.empty[Int, Int]
    val df = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses = 2)

    val etc = new ExtraTreesClassifier()
      .setImpurity("entropy")
      .setMaxDepth(3)
      .setNumTrees(3)
      .setSeed(123)
    val model = etc.fit(df)
    model.setLeafCol("predictedLeafId")

    val transformed = model.transform(df)
    checkNominalOnDF(transformed, "prediction", model.numClasses)
    checkVectorSizeOnDF(transformed, "predictedLeafId", model.trees.length)
    checkVectorSizeOnDF(transformed, "rawPrediction", model.numClasses)
    checkVectorSizeOnDF(transformed, "probability", model.numClasses)

    model.trees.foreach (i => {
      assert(i.getMaxDepth === model.getMaxDepth)
      assert(i.getSeed === model.getSeed)
      assert(i.getImpurity === model.getImpurity)
    })
  }

  test("training with sample weights") {
    val df = binaryDataset
    val numClasses = 2
    // (numTrees, maxDepth, fractionInTol)
    val testParams = Seq(
      (20, 5, 0.96),
      (20, 10, 0.96),
      (20, 10, 0.96)
    )

    for ((numTrees, maxDepth, tol) <- testParams) {
      val estimator = new ExtraTreesClassifier()
        .setNumTrees(numTrees)
        .setMaxDepth(maxDepth)
        .setSeed(seed)
        .setMinWeightFractionPerNode(0.049)

      MLTestingUtils.testArbitrarilyScaledWeights[ExtraTreesClassificationModel,
        ExtraTreesClassifier](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ == _, tol))
      MLTestingUtils.testOutliersWithSmallWeights[ExtraTreesClassificationModel,
        ExtraTreesClassifier](df.as[LabeledPoint], estimator,
        numClasses, MLTestingUtils.modelPredictionEquals(df, _ == _, tol),
        outlierRatio = 2)
      MLTestingUtils.testOversamplingVsWeighting[ExtraTreesClassificationModel,
        ExtraTreesClassifier](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, _ == _, tol), seed)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("read/write") {
    def checkModelData(
        model: ExtraTreesClassificationModel,
        model2: ExtraTreesClassificationModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
      assert(model.numClasses === model2.numClasses)
    }

    val rdd = TreeTests.getTreeReadWriteData(sc)
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2
    val continuousData = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses)

    val allParamSettings = TreeTests.allParamSettings ++ Map("impurity" -> "entropy")

    val etc = new ExtraTreesClassifier()
      .setNumTrees(2)

    testEstimatorAndModelReadWrite(etc, continuousData, allParamSettings,
      allParamSettings, checkModelData)
  }
}


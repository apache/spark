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
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree,
  DecisionTreeSuite => OldDecisionTreeSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.ArrayImplicits._

class DecisionTreeClassifierSuite extends MLTest with DefaultReadWriteTest {

  import DecisionTreeClassifierSuite.compareAPIs
  import testImplicits._

  private var categoricalDataPointsRDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel0RDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel1RDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var continuousDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassForOrderedFeaturesRDD: RDD[LabeledPoint] = _

  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()
    categoricalDataPointsRDD =
      sc.parallelize(
        OldDecisionTreeSuite.generateCategoricalDataPoints().toImmutableArraySeq).map(_.asML)
    orderedLabeledPointsWithLabel0RDD =
      sc.parallelize(
        OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel0().toImmutableArraySeq)
        .map(_.asML)
    orderedLabeledPointsWithLabel1RDD =
      sc.parallelize(
        OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel1().toImmutableArraySeq)
        .map(_.asML)
    categoricalDataPointsForMulticlassRDD =
      sc.parallelize(
        OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlass().toImmutableArraySeq)
        .map(_.asML)
    continuousDataPointsForMulticlassRDD =
      sc.parallelize(
        OldDecisionTreeSuite.generateContinuousDataPointsForMulticlass().toImmutableArraySeq)
        .map(_.asML)
    categoricalDataPointsForMulticlassForOrderedFeaturesRDD = sc.parallelize(
      OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
        .toImmutableArraySeq)
      .map(_.asML)
  }

  test("params") {
    ParamsSuite.checkParams(new DecisionTreeClassifier)
    val model = new DecisionTreeClassificationModel("dtc", new LeafNode(0.0, 0.0, null), 1, 2)
    ParamsSuite.checkParams(model)
  }

  test("DecisionTreeClassifier validate input dataset") {
    testInvalidClassificationLabels(new DecisionTreeClassifier().fit(_), None)
    testInvalidWeights(new DecisionTreeClassifier().setWeightCol("weight").fit(_))
    testInvalidVectors(new DecisionTreeClassifier().fit(_))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////

  test("Binary classification stump with ordered categorical features") {
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(2)
      .setMaxBins(100)
      .setSeed(1)
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    val numClasses = 2
    compareAPIs(categoricalDataPointsRDD, dt, categoricalFeatures, numClasses)
  }

  test("Binary classification stump with fixed labels 0,1 for Entropy,Gini") {
    val dt = new DecisionTreeClassifier()
      .setMaxDepth(3)
      .setMaxBins(100)
    val numClasses = 2
    Array(orderedLabeledPointsWithLabel0RDD, orderedLabeledPointsWithLabel1RDD).foreach { rdd =>
      DecisionTreeClassifier.supportedImpurities.foreach { impurity =>
        dt.setImpurity(impurity)
        compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
      }
    }
  }

  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 3
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(3.0)))
    val rdd = sc.parallelize(arr.toImmutableArraySeq)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("Binary classification stump with 2 continuous features") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0)))))
    val rdd = sc.parallelize(arr.toImmutableArraySeq)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("Multiclass classification stump with unordered categorical features," +
    " with just enough bins") {
    val maxBins = 2 * (math.pow(2, 3 - 1).toInt - 1) // just enough bins to allow unordered features
    val rdd = categoricalDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(maxBins)
    val categoricalFeatures = Map(0 -> 3, 1 -> 3)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("Multiclass classification stump with continuous features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("Multiclass classification stump with continuous + unordered categorical features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("Multiclass classification tree with 10-ary (ordered) categorical features," +
    " with just enough bins") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(10)
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("split must satisfy min instances per node requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr.toImmutableArraySeq)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setMinInstancesPerNode(2)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("do not choose split that does not satisfy min instance per node requirements") {
    // if a split does not satisfy min instances per node requirements,
    // this split is invalid, even though the information gain of split is large.
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0, 0.0)))
    val rdd = sc.parallelize(arr.toImmutableArraySeq)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxBins(2)
      .setMaxDepth(2)
      .setMinInstancesPerNode(2)
    val categoricalFeatures = Map(0 -> 2, 1 -> 2)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("split must satisfy min info gain requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr.toImmutableArraySeq)

    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setMinInfoGain(1.0)
    val numClasses = 2
    compareAPIs(rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("predictRaw and predictProbability") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3

    val newData: DataFrame = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses)
    val newTree = dt.fit(newData)
    newTree.setLeafCol("predictedLeafId")

    val transformed = newTree.transform(newData)
    checkNominalOnDF(transformed, "prediction", newTree.numClasses)
    checkNominalOnDF(transformed, "predictedLeafId", newTree.numLeave)
    checkVectorSizeOnDF(transformed, "rawPrediction", newTree.numClasses)
    checkVectorSizeOnDF(transformed, "probability", newTree.numClasses)

    MLTestingUtils.checkCopyAndUids(dt, newTree)

    testTransformer[(Vector, Double, Double)](newData, newTree,
      "prediction", "rawPrediction", "probability") {
      case Row(pred: Double, rawPred: Vector, probPred: Vector) =>
        assert(pred === rawPred.argmax,
          s"Expected prediction $pred but calculated ${rawPred.argmax} from rawPrediction.")
        val sum = rawPred.toArray.sum
        assert(Vectors.dense(rawPred.toArray.map(_ / sum)) === probPred,
          "probability prediction mismatch")
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, DecisionTreeClassificationModel](this, newTree, newData)
  }

  test("prediction on single instance") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3

    val newData: DataFrame = TreeTests.setMetadata(rdd, categoricalFeatures, numClasses)
    val newTree = dt.fit(newData)

    testPredictionModelSinglePrediction(newTree, newData)
    testClassificationModelSingleRawPrediction(newTree, newData)
    testProbClassificationModelSingleProbPrediction(newTree, newData)
  }

  test("training with 1-category categorical feature") {
    val data = sc.parallelize(Seq(
      LabeledPoint(0, Vectors.dense(0, 2, 3)),
      LabeledPoint(1, Vectors.dense(0, 3, 1)),
      LabeledPoint(0, Vectors.dense(0, 2, 2)),
      LabeledPoint(1, Vectors.dense(0, 3, 9)),
      LabeledPoint(0, Vectors.dense(0, 2, 6))
    ))
    val df = TreeTests.setMetadata(data, Map(0 -> 1), 2)
    val dt = new DecisionTreeClassifier().setMaxDepth(3)
    dt.fit(df)
  }

  test("Feature importance with toy data") {
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(3)
      .setSeed(123)

    // In this data, feature 1 is very important.
    val data: RDD[LabeledPoint] = TreeTests.featureImportanceData(sc)
    val numFeatures = data.first().features.size
    val categoricalFeatures = (0 to numFeatures).map(i => (i, 2)).toMap
    val df = TreeTests.setMetadata(data, categoricalFeatures, 2)

    val model = dt.fit(df)

    val importances = model.featureImportances
    val mostImportantFeature = importances.argmax
    assert(mostImportantFeature === 1)
    assert(importances.toArray.sum === 1.0)
    assert(importances.toArray.forall(_ >= 0.0))
  }

  test("model support predict leaf index") {
    val model = new DecisionTreeClassificationModel("dtc", TreeTests.root0, 3, 2)
    model.setLeafCol("predictedLeafId")
      .setRawPredictionCol("")
      .setPredictionCol("")
      .setProbabilityCol("")

    val data = TreeTests.getSingleTreeLeafData
    data.foreach { case (leafId, vec) => assert(leafId === model.predictLeaf(vec)) }

    val df = sc.parallelize(data.toImmutableArraySeq, 1).toDF("leafId", "features")
    model.transform(df).select("leafId", "predictedLeafId")
      .collect()
      .foreach { case Row(leafId: Double, predictedLeafId: Double) =>
        assert(leafId === predictedLeafId)
    }
  }

  test("should support all NumericType labels and not support other types") {
    val dt = new DecisionTreeClassifier().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[DecisionTreeClassificationModel, DecisionTreeClassifier](
      dt, spark) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  test("Fitting without numClasses in metadata") {
    val df: DataFrame = TreeTests.featureImportanceData(sc).toDF()
    val dt = new DecisionTreeClassifier().setMaxDepth(1)
    dt.fit(df)
  }

  test("training with sample weights") {
    val df = {
      val nPoints = 100
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077,
        -0.16624, -0.84355, -0.048509)

      val xMean = Array(5.843, 3.057)
      val xVariance = Array(0.6856, 0.1899)

      val testData = LogisticRegressionSuite.generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      sc.parallelize(testData, 4).toDF()
    }
    val numClasses = 3
    val predEquals = (x: Double, y: Double) => x == y
    // (impurity, maxDepth)
    val testParams = Seq(
      ("gini", 10),
      ("entropy", 10),
      ("gini", 5)
    )
    for ((impurity, maxDepth) <- testParams) {
      val estimator = new DecisionTreeClassifier()
        .setMaxDepth(maxDepth)
        .setSeed(seed)
        .setMinWeightFractionPerNode(0.049)
        .setImpurity(impurity)

      MLTestingUtils.testArbitrarilyScaledWeights[DecisionTreeClassificationModel,
        DecisionTreeClassifier](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, predEquals, 0.7))
      MLTestingUtils.testOutliersWithSmallWeights[DecisionTreeClassificationModel,
        DecisionTreeClassifier](df.as[LabeledPoint], estimator,
        numClasses, MLTestingUtils.modelPredictionEquals(df, predEquals, 0.8),
        outlierRatio = 2)
      MLTestingUtils.testOversamplingVsWeighting[DecisionTreeClassificationModel,
        DecisionTreeClassifier](df.as[LabeledPoint], estimator,
        MLTestingUtils.modelPredictionEquals(df, predEquals, 0.7), seed)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("read/write") {
    def checkModelData(
        model: DecisionTreeClassificationModel,
        model2: DecisionTreeClassificationModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
      assert(model.numClasses === model2.numClasses)
    }

    val dt = new DecisionTreeClassifier()
    val rdd = TreeTests.getTreeReadWriteData(sc)

    val allParamSettings = TreeTests.allParamSettings ++ Map("impurity" -> "entropy")

    // Categorical splits with tree depth 2
    val categoricalData: DataFrame =
      TreeTests.setMetadata(rdd, Map(0 -> 2, 1 -> 3), numClasses = 2)
    testEstimatorAndModelReadWrite(dt, categoricalData, allParamSettings,
      allParamSettings, checkModelData)
    // Continuous splits with tree depth 2
    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)
    testEstimatorAndModelReadWrite(dt, continuousData, allParamSettings,
      allParamSettings, checkModelData)

    // Continuous splits with tree depth 0
    testEstimatorAndModelReadWrite(dt, continuousData, allParamSettings ++ Map("maxDepth" -> 0),
      allParamSettings ++ Map("maxDepth" -> 0), checkModelData)
  }

  test("SPARK-20043: " +
       "ImpurityCalculator builder fails for uppercase impurity type Gini in model read/write") {
    val rdd = TreeTests.getTreeReadWriteData(sc)
    val data: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)

    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
    val model = dt.fit(data)

    testDefaultReadWrite(model)
  }

  test("SPARK-33398: Load DecisionTreeClassificationModel prior to Spark 3.0") {
    val path = testFile("ml-models/dtc-2.4.7")
    val model = DecisionTreeClassificationModel.load(path)
    assert(model.numClasses === 2)
    assert(model.numFeatures === 692)
    assert(model.numNodes === 5)

    val metadata = spark.read.json(s"$path/metadata")
    val sparkVersionStr = metadata.select("sparkVersion").first().getString(0)
    assert(sparkVersionStr === "2.4.7")
  }
}

private[ml] object DecisionTreeClassifierSuite extends SparkFunSuite {

  /**
   * Train 2 decision trees on the given dataset, one using the old API and one using the new API.
   * Convert the old tree to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      dt: DecisionTreeClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): Unit = {
    val numFeatures = data.first().features.size
    val oldStrategy = dt.getOldStrategy(categoricalFeatures, numClasses)
    val oldTree = OldDecisionTree.train(data.map(OldLabeledPoint.fromML), oldStrategy)
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses)
    val newTree = dt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldTreeAsNew = DecisionTreeClassificationModel.fromOld(
      oldTree, newTree.parent.asInstanceOf[DecisionTreeClassifier], categoricalFeatures)
    TreeTests.checkEqual(oldTreeAsNew, newTree)
    assert(newTree.numFeatures === numFeatures)
  }
}

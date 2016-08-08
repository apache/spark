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

import scala.collection.mutable.ArrayBuffer
import org.scalactic.TolerantNumerics
import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.tree.impurity.ApproxBernoulliAggregator
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{EnsembleTestHelper, GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.loss.LogLoss
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.util.Utils

/**
 * Test suite for [[GBTClassifier]].
 */
class GBTClassifierSuite extends SparkFunSuite with MLlibTestSparkContext
  with DefaultReadWriteTest {

  import testImplicits._
  import GBTClassifierSuite.compareAPIs

  // Combinations for estimators, learning rates and subsamplingRate
  private val testCombinations =
    Array((10, 1.0, 1.0), (10, 0.1, 1.0), (10, 0.5, 0.75), (10, 0.1, 0.75))

  private val dataNumFeatures = 10
  private val dataNumCategories = 100
  private var data: RDD[LabeledPoint] = _
  private var trainData: RDD[LabeledPoint] = _
  private var validationData: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    data = sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(
      numFeatures = dataNumFeatures, dataNumCategories), 2).map(_.asML)
    trainData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 120), 2)
        .map(_.asML)
    validationData =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 20, 80), 2)
        .map(_.asML)
  }

  test("params") {
    ParamsSuite.checkParams(new GBTClassifier)
    val model = new GBTClassificationModel("gbtc",
      Array(new DecisionTreeRegressionModel("dtr", new LeafNode(0.0, 0.0, null), 1)),
      Array(1.0), 1)
    ParamsSuite.checkParams(model)
  }

  test("GBT-specific param defaults") {
    val gbt = new GBTClassifier()
    assert(gbt.getImpurity === "loss-based")
    assert(gbt.getLossType === "bernoulli")
  }

  test("GBT-specific param support") {
    val gbt = new GBTClassifier()
    for (impurity <- GBTClassifier.supportedImpurities) {
      gbt.setImpurity(impurity)
    }
    for (lossType <- GBTClassifier.supportedLossTypes) {
      gbt.setLossType(lossType)
    }
  }

  def checkVarianceBasedLogLoss(categoricalFeatures: Map[Int, Int]): Unit = {
    testCombinations.foreach {
      case (maxIter, learningRate, subsamplingRate) =>
        val gbt = new GBTClassifier()
          .setMaxDepth(2)
          .setSubsamplingRate(subsamplingRate)
          .setImpurity("variance")
          .setLossType("logistic")
          .setMaxIter(maxIter)
          .setStepSize(learningRate)
          .setMaxBins(dataNumCategories)
          .setSeed(123)
        compareAPIs(data, None, gbt, categoricalFeatures)
    }
  }

  test("Binary classification with continuous features: Variance-based impurity + Log Loss") {
    val categoricalFeatures = Map.empty[Int, Int]
    checkVarianceBasedLogLoss(categoricalFeatures)
  }

  test("Binary classification with categorical features: Variance-based impurity + Log Loss") {
    val categoricalFeatures = (0 until dataNumFeatures).map(_ -> dataNumCategories).toMap
    checkVarianceBasedLogLoss(categoricalFeatures)
  }

  test("Binary classification with mixed features: Variance-based impurity + Log Loss") {
    val categoricalFeatures = (0 until dataNumFeatures / 2).map(_ -> dataNumCategories).toMap
    checkVarianceBasedLogLoss(categoricalFeatures)
  }

  private def computeCalculator(df: DataFrame, agg: ImpurityAggregator): ImpurityCalculator = {
    implicit val encoder = Encoders.scalaDouble
    val labels = df.select("label").as[Double].collect()
    val stats = new Array[Double](agg.statsSize)
    labels.foreach(label => agg.update(stats, offset = 0, label, instanceWeight = 1))
    agg.getCalculator(stats, offset = 0)
  }

  test("approximate bernoulli impurity") {
    val agg = new ApproxBernoulliAggregator()
    implicit val approxEquals = TolerantNumerics.tolerantDoubleEquality(1e-3)
    val loss = LogLoss
    def grad(pred: Double, label: Double): Double = {
      -4 * label / (1 + math.exp(2 * label * pred))
    }
    def hess(pred: Double, label: Double): Double = {
      val numerator = 8 * math.exp(2 * label * pred) * math.pow(label, 2)
      val denominator = math.pow(1 + Math.exp(2 * label * pred), 2)
      numerator / denominator
    }

    val npoints = 6
    for (cutoff <- 0 to npoints) withClue(s"for cutoff $cutoff: ") {
      val labels = (0 until npoints).map(x => if (x < cutoff) -1.0 else 1.0)
      val prediction = 0
      val psuedoResiduals = labels.map(x => -loss.gradient(prediction, x))
      val df = spark.createDataFrame(psuedoResiduals.map(Tuple1.apply)).toDF("label")

      val variance = computeCalculator(df, new VarianceAggregator).calculate()
      val newtonRaphson =
        -labels.map(grad(prediction, _)).sum / labels.map(hess(prediction, _)).sum

      val calculator = computeCalculator(df, agg)
      assert(calculator.count === npoints)
      assert(calculator.calculate() === variance)
      assert(calculator.predict === newtonRaphson)
    }
  }

  test("Binary classification: Loss-based impurity + Log Loss") {
    val expectedImpurity = new ApproxBernoulliAggregator()
    val loss = LogLoss
    val impurityName = "bernoulli"

    // We create a dataset that can be optimally classified with a root tree
    // and one round of gradient boosting. The first tree will not be a perfect classifier,
    // so the leaf node predictions will differ for different impurity measures. This is expected
    // to be tested on depth-2 trees (7 nodes total). The generated trees should do no
    // sub-sampling.
    //
    // The error is slight enough that the tree structure should not change, but results in
    // nontrivial leaf error estimates. In other words, since the trees should tend towards the
    // hypothesis of empirical risk minimization, and the first tree is not the minimizer,
    // the second tree is going to have the same structure and leaf prediction signs.

    val data = new ArrayBuffer[LabeledPoint]()

    // At depth-1, the trees separate 4 intervals.
    def addPoints(npoints: Int, label: Double, features: Vector): Unit = {
      for (_ <- 0 until npoints) {
        data += new LabeledPoint(label, features)
      }
    }

    // Adds 9 points of label 'label' and 1 point of label '1.0-label'
    def addMixedSection(label: Double, feature0: Double, feature1: Double): Unit = {
      val pointsPerSection = 10
      val offPoints = 1
      val features = Vectors.dense(feature0, feature1)
      val offFeatures = Vectors.dense(feature0, feature1)
      addPoints(pointsPerSection - offPoints, label, features)
      addPoints(offPoints, 1.0 - label, offFeatures)
    }

    for (feature0 <- Seq(0.0, 1.0); feature1 <- Seq(0.0, 1.0)) {
      val xor = if (feature0 == feature1) 0.0 else 1.0
      addMixedSection(label = xor, feature0, feature1)
    }
    addMixedSection(label = 0.0, feature0 = 0.0, feature1 = 0.0)
    addMixedSection(label = 1.0, feature0 = 0.0, feature1 = 1.0)
    addMixedSection(label = 1.0, feature0 = 1.0, feature1 = 0.0)
    addMixedSection(label = 0.0, feature0 = 1.0, feature1 = 1.0)

    // Make splitting on feature 0 slightly more attractive for an initial split
    // than the others by making it a slight decent identity predictor while keeping
    // other features uninformative. Note that feature 2's predictive power, when
    // conditioned on feature 0, is maintained.
    for (feature1 <- Seq(0.0, 1.0); label <- Seq(0.0, 1.0)) {
      addPoints(npoints = 1, label, Vectors.dense(label, feature1))
    }

    // Convert the input dataframe to a more convenient format to check our results against.
    val rawInput = spark.createDataFrame(data)
    val vectorAsArray = udf((v: Vector) => v.toArray)
    val input = rawInput.select(
      col("*"),
      vectorAsArray(col("features"))(0).as("feature0"),
      vectorAsArray(col("features"))(1).as("feature1"))

    val gbt = new GBTClassifier()
      .setMaxDepth(2)
      .setMinInstancesPerNode(1)
      .setMinInfoGain(0)
      .setSubsamplingRate(1.0)
      .setMaxIter(2)
      .setStepSize(1)
      .setLossType(impurityName)
    val model = gbt.fit(rawInput)

    assert(model.trees.length === 2)
    assert(model.treeWeights === Array(1.0, 1.0))

    // A "feature" with index below 0 is a label
    def pointFilter(featureMap: Seq[(Int, Int)]): String = {
      if (featureMap.isEmpty) return "true"
      val sqlConditions = featureMap.map({
        case (idx, value) if idx < 0 => s"label = $value"
        case (idx, value) => s"feature$idx = $value"})
      sqlConditions.mkString(" and ")
    }

    var relabeledDF: DataFrame = null

    def verifyImpurity(actualImpurity: ImpurityCalculator, featureMap: Seq[(Int, Int)]): Unit = {
      implicit val approxEquals = TolerantNumerics.tolerantDoubleEquality(1e-3)
      val df = relabeledDF.where(pointFilter(featureMap))
      val expectedCalculator = computeCalculator(df, expectedImpurity)
      withClue(s"actualImpurity $actualImpurity\nexpectedImpurity $expectedCalculator\n\n") {
        assert(actualImpurity.count === expectedCalculator.count)
        assert(actualImpurity.calculate() === expectedCalculator.calculate())
        assert(actualImpurity.predict === expectedCalculator.predict)
      }
    }

    def withNode[T <: Node](node: Node, featureMap: Seq[(Int, Int)])(f: T => Unit): T = {
      withClue(s"node $node\n\nlocation ${featureMap.mkString(" ")}\n\n") {
        assert(node.isInstanceOf[T])
        val t = node.asInstanceOf[T]
        f(t)
        t
      }
    }

    def verifyInternalNode(node: Node, feature: Int, featureMap: (Int, Int)*): InternalNode = {
      withNode[InternalNode](node, featureMap) { internal =>
        assert(internal.split.featureIndex === feature)
        verifyImpurity(internal.impurityStats, featureMap)
      }
    }

    def verifyLeafNode(node: Node, featureMap: (Int, Int)*): Unit = {
      withNode[LeafNode](node, featureMap) { leaf =>
        verifyImpurity(leaf.impurityStats, featureMap)
      }
    }

    def verifyFeature0First(dtree: DecisionTreeModel, idx: Int): Unit = {
      withClue(s"Tree $idx:\n\n${dtree.toDebugString}\n") {
        val root = verifyInternalNode(dtree.rootNode, 0)
        val left = verifyInternalNode(root.leftChild, 1, 0 -> 0)
        val right = verifyInternalNode(root.rightChild, 1, 0 -> 1)
        verifyLeafNode(left.leftChild, 0 -> 0, 1 -> 0)
        verifyLeafNode(left.rightChild, 0 -> 0, 1 -> 1)
        verifyLeafNode(right.leftChild, 0 -> 1, 1 -> 0)
        verifyLeafNode(right.rightChild, 0 -> 1, 1 -> 1)
      }
    }

    val oldLabel = input.withColumnRenamed("label", "oldlabel")
    relabeledDF = oldLabel.withColumn("label", col("oldlabel") * 2 - 1)
    verifyFeature0First(model.trees.head, 0)

    val gradient = udf((pred: Double, label: Double) => -loss.gradient(pred, label))
    relabeledDF = model.trees.head.transform(oldLabel)
      .withColumn("label", gradient(col("prediction"), col("oldlabel") * 2 - 1))
    verifyFeature0First(model.trees.last, 1)
  }

  // TODO regression: similar test breakup
  // TODO regression: cont features loss based expected result
  // TODO regression: defaults, param support

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

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    sc.checkpointDir = None
    Utils.deleteRecursively(tempDir)
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
        .setImpurity("variance")
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

  test("extractLabeledPoints with bad data") {
    def getTestData(labels: Seq[Double]): DataFrame = {
      labels.map { label: Double => LabeledPoint(label, Vectors.dense(0.0)) }.toDF()
    }

    val gbt = new GBTClassifier().setMaxDepth(1).setMaxIter(1)
    // Invalid datasets
    val df1 = getTestData(Seq(0.0, -1.0, 1.0, 0.0))
    withClue("Classifier should fail if label is negative") {
      val e: SparkException = intercept[SparkException] {
        gbt.fit(df1)
      }
      assert(e.getMessage.contains("currently only supports binary classification"))
    }
    val df2 = getTestData(Seq(0.0, 0.1, 1.0, 0.0))
    withClue("Classifier should fail if label is not an integer") {
      val e: SparkException = intercept[SparkException] {
        gbt.fit(df2)
      }
      assert(e.getMessage.contains("currently only supports binary classification"))
    }
    val df3 = getTestData(Seq(0.0, 2.0, 1.0, 0.0))
    withClue("Classifier should fail if label is >= 2") {
      val e: SparkException = intercept[SparkException] {
        gbt.fit(df3)
      }
      assert(e.getMessage.contains("currently only supports binary classification"))
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of feature importance
  /////////////////////////////////////////////////////////////////////////////
  test("Feature importance with toy data") {
    val numClasses = 2
    val gbt = new GBTClassifier()
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
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("model save/load") {
    def checkModelData(
        model: GBTClassificationModel,
        model2: GBTClassificationModel): Unit = {
      TreeTests.checkEqual(model, model2)
      assert(model.numFeatures === model2.numFeatures)
    }

    val gbt = new GBTClassifier()
    val rdd = TreeTests.getTreeReadWriteData(sc)

    // Test for all different impurity types.
    for (impurity <- Seq(Some("loss-based"), Some("variance"), None)) {
      val allParamSettings = TreeTests.allParamSettings ++
        Map("lossType" -> "logistic") ++
        impurity.map("impurity" -> _).toMap

      val continuousData: DataFrame =
        TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)
      testEstimatorAndModelReadWrite(gbt, continuousData, allParamSettings, checkModelData)
    }
  }
}

private object GBTClassifierSuite extends SparkFunSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   *
   * The old API only supports variance-based impurity, so gbt should have that setting.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      validationData: Option[RDD[LabeledPoint]],
      gbt: GBTClassifier,
      categoricalFeatures: Map[Int, Int]): Unit = {
    assert(gbt.getImpurity == "variance")
    val numFeatures = data.first().features.size
    val oldBoostingStrategy =
      gbt.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Classification, Variance)
    val oldGBT = new OldGBT(oldBoostingStrategy, gbt.getSeed.toInt)
    val oldModel = oldGBT.run(data.map(OldLabeledPoint.fromML))
    val newData: DataFrame = TreeTests.setMetadata(data, categoricalFeatures, numClasses = 2)
    val newModel = gbt.fit(newData)
    // Use parent from newTree since this is not checked anyways.
    val oldModelAsNew = GBTClassificationModel.fromOld(
      oldModel, newModel.parent.asInstanceOf[GBTClassifier], categoricalFeatures, numFeatures)
    TreeTests.checkEqual(oldModelAsNew, newModel)
    assert(newModel.numFeatures === numFeatures)
    assert(oldModelAsNew.numFeatures === numFeatures)
  }
}

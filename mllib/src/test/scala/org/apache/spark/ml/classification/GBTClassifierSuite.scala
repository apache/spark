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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.util.{DataGenerator, DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{EnsembleTestHelper, GradientBoostedTrees => OldGBT}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.impurity.{Variance => OldVariance}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.Utils
import org.scalactic.TolerantNumerics

case class RTreeNode(splitVar: Int,
                     splitCodePred: Double,
                     leftNode: Int,
                     rightNode: Int,
                     missingNode: Int,
                     errorReduction: Double,
                     weight: Int)

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

  // We can't verify this against the variance-based model in the same way. Though
  // a true loss-based impurity which finds the absolute optimum prediction at the leaf nodes
  // will perform at least as well as a variance-based model, the absolute optimum is not
  // analytic. The use of a NR numerical optimization prevents us from a direct accuracy
  // comparison. Instead, we just make sure that after removing any randomness from GBT generation
  // the results are what we expect.

  /*


  private def checkTreeSimilarToR(sparkNode: Node,
                                  gbmTree: Array[RTreeNode],
                                  rNodeIdx: Int): Unit = {
    implicit val approxEquals = TolerantNumerics.tolerantDoubleEquality(0.01)
    val rNode = gbmTree(rNodeIdx - 1) // R is 1-indexed

    withClue(s"For RTreeNode of 1-index $rNodeIdx:\n" +
      s"R:     $rNode\n" +
      s"Spark: $sparkNode\n\n") {
      if (rNode.splitVar == -1) {
        assert(sparkNode.isInstanceOf[LeafNode])
        val leaf = sparkNode.asInstanceOf[LeafNode]
        assert(leaf.prediction === rNode.splitCodePred, "prediction differs")
        assert(leaf.impurityStats.count === rNode.weight, "number of items in leaf differs")
      } else {
        assert(sparkNode.isInstanceOf[InternalNode])
        val internal = sparkNode.asInstanceOf[InternalNode]
        assert(internal.split.featureIndex === rNode.splitVar - 1, "feature splits disagree")
        assert(internal.split.asInstanceOf[ContinuousSplit].threshold === rNode.splitCodePred,
          "split thresholds were unequal")
      }
    }

    if (rNode.splitVar != -1) {
      val internal = sparkNode.asInstanceOf[InternalNode]
      checkTreeSimilarToR(internal.leftChild, gbmTree, rNode.leftNode)
      checkTreeSimilarToR(internal.rightChild, gbmTree, rNode.rightNode)
    }
  }

  private def checkApproximatelyEqualToR(tree: DecisionTreeModel,
                                         filename: String): Unit = {
    implicit val encoder: Encoder[RTreeNode] = Encoders.product
    val csvFile = getClass.getClassLoader.getResource(
      "org/apache/spark/ml/classification/" + filename).getFile
    val gbmTree = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(csvFile).as[RTreeNode]

    val gbmTreeTable = gbmTree.showString(gbmTree.count().toInt)
    lazy val sparkTreeString = tree.toDebugString
    withClue(s"GBM tree:\n$gbmTreeTable\n\nSpark tree:\n$sparkTreeString\n") {
      checkTreeSimilarToR(tree.rootNode, gbmTree.collect(), 1)
    }
  }

   */

  test("Binary classification with continuous features: Loss-based impurity + Log Loss") {

    val categoricalFeatures = Map.empty[Int, Int]
    val numFeatures = 4
    val numPoints = 100
    val continuousData = DataGenerator.randomLabeledPoints(
      seed = 123, classification = true, numFeatures, categoricalFeatures, numPoints)
    val input = spark.createDataFrame(continuousData)

    /*
    To get equivalent gbm() results (cached in resources directory for tests):

    Scala

    import org.apache.spark.sql.functions._
    import org.apache.spark.ml.linalg.Vector

    val vectorAsArray = udf((v: Vector) => v.toArray)
    val cols = col("label") +: (0 until numFeatures).map(vectorAsArray(col("features"))(_))
    input.select(cols: _*).coalesce(1).write.option("header", true).csv("/tmp/out")

    R

    data <- read.csv(Sys.glob(paste("/tmp/out/", "*.csv", sep="")), header=TRUE)
    library(gbm)
    set.seed(123)
    gbt <- gbm(formula = formula(data),
               distribution = "bernoulli",
               data = data,
               n.trees = 10,
               interaction.depth = 2,
               bag.fraction = 1.0,
               n.minobsinnode = 5,
               shrinkage = 0.1,
               train.fraction = 1.0)
    for (tree in seq(1, 10)) {
      write.table(pretty.gbm.tree(gbt, tree),
        paste("/tmp/gbm-bernoulli-tree-", tree - 1, ".csv", sep=""),
        row.names=FALSE,
        quote=FALSE,
        sep=",")
    }

    # Then move the tree files to the appropriate resource location:
    # spark/mllib/src/test/resources/org/apache/spark/ml/classification
    */

    val gbt = new GBTClassifier()
      .setMaxDepth(2)
      .setMaxBins(numPoints) // continuous splits for compatibility with R.
      .setMinInstancesPerNode(5)
      .setMinInfoGain(0)
      .setImpurity("loss-based")
      .setSubsamplingRate(1.0)
      .setSeed(0) // should not be used
      .setMaxIter(10)
      .setStepSize(0.1)
      .setLossType("bernoulli")

    val model = gbt.fit(input)
  }

  // categorical + continuous features + compare to variance-based (should be better)
  // compare to gbm test logisitic.

  // TODO regression: cat/cont/mixed features variance compared to old model
  // TODO regression: cat/cont/mixed features  compared to variance
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
      gbt.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Classification, OldVariance)
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

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
import org.apache.spark.ml.tree.{CategoricalSplit, InternalNode, LeafNode}
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree, DecisionTreeSuite => OldDecisionTreeSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

class DecisionTreeClassifierSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import DecisionTreeClassifierSuite.compareAPIs

  private var categoricalDataPointsRDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel0RDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel1RDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var continuousDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassForOrderedFeaturesRDD: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    categoricalDataPointsRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPoints()).map(_.asML)
    orderedLabeledPointsWithLabel0RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()).map(_.asML)
    orderedLabeledPointsWithLabel1RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()).map(_.asML)
    categoricalDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlass()).map(_.asML)
    continuousDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateContinuousDataPointsForMulticlass()).map(_.asML)
    categoricalDataPointsForMulticlassForOrderedFeaturesRDD = sc.parallelize(
      OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures())
      .map(_.asML)
  }

  test("params") {
    ParamsSuite.checkParams(new DecisionTreeClassifier)
    val model = new DecisionTreeClassificationModel("dtc", new LeafNode(0.0, 0.0, null), 1, 2)
    ParamsSuite.checkParams(model)
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
    val rdd = sc.parallelize(arr)
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
    val rdd = sc.parallelize(arr)
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
    val rdd = sc.parallelize(arr)
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
    val rdd = sc.parallelize(arr)
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
    val rdd = sc.parallelize(arr)

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

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(newTree)

    val predictions = newTree.transform(newData)
      .select(newTree.getPredictionCol, newTree.getRawPredictionCol, newTree.getProbabilityCol)
      .collect()

    predictions.foreach { case Row(pred: Double, rawPred: Vector, probPred: Vector) =>
      assert(pred === rawPred.argmax,
        s"Expected prediction $pred but calculated ${rawPred.argmax} from rawPrediction.")
      val sum = rawPred.toArray.sum
      assert(Vectors.dense(rawPred.toArray.map(_ / sum)) === probPred,
        "probability prediction mismatch")
    }
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

  test("Use soft prediction for binary classification with ordered categorical features") {
    // The following dataset is set up such that the best split is {1} vs. {0, 2}.
    // If the hard prediction is used to order the categories, then {0} vs. {1, 2} is chosen.
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)))
    val data = sc.parallelize(arr)
    val df = TreeTests.setMetadata(data, Map(0 -> 3), 2)

    // Must set maxBins s.t. the feature will be treated as an ordered categorical feature.
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(1)
      .setMaxBins(3)
    val model = dt.fit(df)
    model.rootNode match {
      case n: InternalNode =>
        n.split match {
          case s: CategoricalSplit =>
            assert(s.leftCategories === Array(1.0))
          case other =>
            fail(s"All splits should be categorical, but got ${other.getClass.getName}: $other.")
        }
      case other =>
        fail(s"Root node should be an internal node, but got ${other.getClass.getName}: $other.")
    }
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

  test("should support all NumericType labels and not support other types") {
    val dt = new DecisionTreeClassifier().setMaxDepth(1)
    MLTestingUtils.checkNumericTypes[DecisionTreeClassificationModel, DecisionTreeClassifier](
      dt, spark) { (expected, actual) =>
        TreeTests.checkEqual(expected, actual)
      }
  }

  test("Fitting without numClasses in metadata") {
    val df: DataFrame = spark.createDataFrame(TreeTests.featureImportanceData(sc))
    val dt = new DecisionTreeClassifier().setMaxDepth(1)
    dt.fit(df)
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
    testEstimatorAndModelReadWrite(dt, categoricalData, allParamSettings, checkModelData)

    // Continuous splits with tree depth 2
    val continuousData: DataFrame =
      TreeTests.setMetadata(rdd, Map.empty[Int, Int], numClasses = 2)
    testEstimatorAndModelReadWrite(dt, continuousData, allParamSettings, checkModelData)

    // Continuous splits with tree depth 0
    testEstimatorAndModelReadWrite(dt, continuousData, allParamSettings ++ Map("maxDepth" -> 0),
      checkModelData)
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

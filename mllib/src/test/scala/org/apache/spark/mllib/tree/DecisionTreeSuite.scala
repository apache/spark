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

package org.apache.spark.mllib.tree

import scala.collection.JavaConverters._

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impl.{DecisionTreeMetadata, TreePoint}
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Variance}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.apache.spark.mllib.util.LocalSparkContext

class DecisionTreeSuite extends FunSuite with LocalSparkContext {

  def validateClassifier(
      model: DecisionTreeModel,
      input: Seq[LabeledPoint],
      requiredAccuracy: Double) {
    val predictions = input.map(x => model.predict(x.features))
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    val accuracy = (input.length - numOffPredictions).toDouble / input.length
    assert(accuracy >= requiredAccuracy,
      s"validateClassifier calculated accuracy $accuracy but required $requiredAccuracy.")
  }

  def validateRegressor(
      model: DecisionTreeModel,
      input: Seq[LabeledPoint],
      requiredMSE: Double) {
    val predictions = input.map(x => model.predict(x.features))
    val squaredError = predictions.zip(input).map { case (prediction, expected) =>
      val err = prediction - expected.label
      err * err
    }.sum
    val mse = squaredError / input.length
    assert(mse <= requiredMSE, s"validateRegressor calculated MSE $mse but required $requiredMSE.")
  }

  test("Binary classification with continuous features: split and bin calculation") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, 3, 2, 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)
  }

  test("Binary classification with binary (ordered) categorical features:" +
    " split and bin calculation") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,
      numClassesForClassification = 2,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 2, 1-> 2))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)
  }

  test("Binary classification with 3-ary (ordered) categorical features," +
    " with no samples for one category") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,
      numClassesForClassification = 2,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)
  }

  test("extract categories from a number for multiclass classification") {
    val l = DecisionTree.extractMultiClassCategories(13, 10)
    assert(l.length === 3)
    assert(List(3.0, 2.0, 0.0).toSeq === l.toSeq)
  }

  test("Multiclass classification with unordered categorical features:" +
      " split and bin calculations") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,
      numClassesForClassification = 100,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    assert(splits(0).length === 3)
    assert(bins(0).length === 6)

    // Expecting 2^2 - 1 = 3 bins/splits
    assert(splits(0)(0).feature === 0)
    assert(splits(0)(0).threshold === Double.MinValue)
    assert(splits(0)(0).featureType === Categorical)
    assert(splits(0)(0).categories.length === 1)
    assert(splits(0)(0).categories.contains(0.0))
    assert(splits(1)(0).feature === 1)
    assert(splits(1)(0).threshold === Double.MinValue)
    assert(splits(1)(0).featureType === Categorical)
    assert(splits(1)(0).categories.length === 1)
    assert(splits(1)(0).categories.contains(0.0))

    assert(splits(0)(1).feature === 0)
    assert(splits(0)(1).threshold === Double.MinValue)
    assert(splits(0)(1).featureType === Categorical)
    assert(splits(0)(1).categories.length === 1)
    assert(splits(0)(1).categories.contains(1.0))
    assert(splits(1)(1).feature === 1)
    assert(splits(1)(1).threshold === Double.MinValue)
    assert(splits(1)(1).featureType === Categorical)
    assert(splits(1)(1).categories.length === 1)
    assert(splits(1)(1).categories.contains(1.0))

    assert(splits(0)(2).feature === 0)
    assert(splits(0)(2).threshold === Double.MinValue)
    assert(splits(0)(2).featureType === Categorical)
    assert(splits(0)(2).categories.length === 2)
    assert(splits(0)(2).categories.contains(0.0))
    assert(splits(0)(2).categories.contains(1.0))
    assert(splits(1)(2).feature === 1)
    assert(splits(1)(2).threshold === Double.MinValue)
    assert(splits(1)(2).featureType === Categorical)
    assert(splits(1)(2).categories.length === 2)
    assert(splits(1)(2).categories.contains(0.0))
    assert(splits(1)(2).categories.contains(1.0))

    // Check bins.

    assert(bins(0)(0).category === Double.MinValue)
    assert(bins(0)(0).lowSplit.categories.length === 0)
    assert(bins(0)(0).highSplit.categories.length === 1)
    assert(bins(0)(0).highSplit.categories.contains(0.0))
    assert(bins(1)(0).category === Double.MinValue)
    assert(bins(1)(0).lowSplit.categories.length === 0)
    assert(bins(1)(0).highSplit.categories.length === 1)
    assert(bins(1)(0).highSplit.categories.contains(0.0))

    assert(bins(0)(1).category === Double.MinValue)
    assert(bins(0)(1).lowSplit.categories.length === 1)
    assert(bins(0)(1).lowSplit.categories.contains(0.0))
    assert(bins(0)(1).highSplit.categories.length === 1)
    assert(bins(0)(1).highSplit.categories.contains(1.0))
    assert(bins(1)(1).category === Double.MinValue)
    assert(bins(1)(1).lowSplit.categories.length === 1)
    assert(bins(1)(1).lowSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.length === 1)
    assert(bins(1)(1).highSplit.categories.contains(1.0))

    assert(bins(0)(2).category === Double.MinValue)
    assert(bins(0)(2).lowSplit.categories.length === 1)
    assert(bins(0)(2).lowSplit.categories.contains(1.0))
    assert(bins(0)(2).highSplit.categories.length === 2)
    assert(bins(0)(2).highSplit.categories.contains(1.0))
    assert(bins(0)(2).highSplit.categories.contains(0.0))
    assert(bins(1)(2).category === Double.MinValue)
    assert(bins(1)(2).lowSplit.categories.length === 1)
    assert(bins(1)(2).lowSplit.categories.contains(1.0))
    assert(bins(1)(2).highSplit.categories.length === 2)
    assert(bins(1)(2).highSplit.categories.contains(1.0))
    assert(bins(1)(2).highSplit.categories.contains(0.0))

  }

  test("Multiclass classification with ordered categorical features: split and bin calculations") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    assert(arr.length === 3000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,
      numClassesForClassification = 100,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 10, 1-> 10))
    // 2^10 - 1 > 100, so categorical features will be ordered

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)
  }


  test("Binary classification stump with ordered categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      numClassesForClassification = 2,
      maxDepth = 2,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    // no bins or splits pre-computed for ordered categorical features
    assert(splits(0).length === 0)
    assert(bins(0).length === 0)

    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(8), metadata, 0,
      new Array[Node](0), splits, bins, 10)

    val split = bestSplits(0)._1
    assert(split.categories === List(1.0))
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = bestSplits(0)._2
    assert(stats.gain > 0)
    assert(stats.predict === 1)
    assert(stats.prob === 0.6)
    assert(stats.impurity > 0.2)
  }

  test("Regression stump with 3-ary (ordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Regression,
      Variance,
      maxDepth = 2,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(8), metadata, 0,
      new Array[Node](0), splits, bins, 10)

    val split = bestSplits(0)._1
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = bestSplits(0)._2
    assert(stats.gain > 0)
    assert(stats.predict === 0.6)
    assert(stats.impurity > 0.2)
  }

  test("Regression stump with binary (ordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Regression,
      Variance,
      maxDepth = 2,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 2, 1-> 2))
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val model = DecisionTree.train(rdd, strategy)
    validateRegressor(model, arr, 0.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
  }

  test("Binary classification stump with fixed label 0 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, maxDepth = 3,
      numClassesForClassification = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(8), metadata, 0,
      new Array[Node](0), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
  }

  test("Binary classification stump with fixed label 1 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, maxDepth = 3,
      numClassesForClassification = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(2), metadata, 0,
      new Array[Node](0), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
    assert(bestSplits(0)._2.predict === 1)
  }

  test("Binary classification stump with fixed label 0 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClassesForClassification = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(2), metadata, 0,
      new Array[Node](0), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
    assert(bestSplits(0)._2.predict === 0)
  }

  test("Binary classification stump with fixed label 1 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClassesForClassification = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(2), metadata, 0,
      new Array[Node](0), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
    assert(bestSplits(0)._2.predict === 1)
  }

  test("Second level node building with vs. without groups") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, 3, 2, 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    // Train a 1-node model
    val strategyOneNode = new Strategy(Classification, Entropy, 1, 2, 100)
    val modelOneNode = DecisionTree.train(rdd, strategyOneNode)
    val nodes: Array[Node] = new Array[Node](8)
    nodes(1) = modelOneNode.topNode
    nodes(1).leftNode = None
    nodes(1).rightNode = None

    val parentImpurities = Array(0, 0.5, 0.5, 0.5)

    // Single group second level tree construction.
    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, parentImpurities, metadata, 1, nodes,
      splits, bins, 10)
    assert(bestSplits.length === 2)
    assert(bestSplits(0)._2.gain > 0)
    assert(bestSplits(1)._2.gain > 0)

    // maxLevelForSingleGroup parameter is set to 0 to force splitting into groups for second
    // level tree construction.
    val bestSplitsWithGroups = DecisionTree.findBestSplits(treeInput, parentImpurities, metadata, 1,
      nodes, splits, bins, 0)
    assert(bestSplitsWithGroups.length === 2)
    assert(bestSplitsWithGroups(0)._2.gain > 0)
    assert(bestSplitsWithGroups(1)._2.gain > 0)

    // Verify whether the splits obtained using single group and multiple group level
    // construction strategies are the same.
    for (i <- 0 until bestSplits.length) {
      assert(bestSplits(i)._1 === bestSplitsWithGroups(i)._1)
      assert(bestSplits(i)._2.gain === bestSplitsWithGroups(i)._2.gain)
      assert(bestSplits(i)._2.impurity === bestSplitsWithGroups(i)._2.impurity)
      assert(bestSplits(i)._2.leftImpurity === bestSplitsWithGroups(i)._2.leftImpurity)
      assert(bestSplits(i)._2.rightImpurity === bestSplitsWithGroups(i)._2.rightImpurity)
      assert(bestSplits(i)._2.predict === bestSplitsWithGroups(i)._2.predict)
    }
  }

  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 3, categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(strategy.isMulticlassClassification)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(32), metadata, 0,
      new Array[Node](0), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1
    assert(bestSplit.feature === 0)
    assert(bestSplit.categories.length === 1)
    assert(bestSplit.categories.contains(1))
    assert(bestSplit.featureType === Categorical)
  }

  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.dense(0.0))
    arr(1) = new LabeledPoint(1.0, Vectors.dense(1.0))
    arr(2) = new LabeledPoint(1.0, Vectors.dense(2.0))
    arr(3) = new LabeledPoint(1.0, Vectors.dense(3.0))
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 2)

    val model = DecisionTree.train(rdd, strategy)
    validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
  }

  test("Binary classification stump with 2 continuous features") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0))))
    arr(1) = new LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0))))
    arr(2) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0))))
    arr(3) = new LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0))))

    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 2)

    val model = DecisionTree.train(rdd, strategy)
    validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
    assert(model.topNode.split.get.feature === 1)
  }

  test("Multiclass classification stump with unordered categorical features," +
    " with just enough bins") {
    val maxBins = 2 * (math.pow(2, 3 - 1).toInt - 1) // just enough bins to allow unordered features
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 3, maxBins = maxBins,
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))

    val model = DecisionTree.train(rdd, strategy)
    validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(32), metadata, 0,
      new Array[Node](0), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1
    assert(bestSplit.feature === 0)
    assert(bestSplit.categories.length === 1)
    assert(bestSplit.categories.contains(1))
    assert(bestSplit.featureType === Categorical)
    val gain = bestSplits(0)._2
    assert(gain.leftImpurity === 0)
    assert(gain.rightImpurity === 0)
  }

  test("Multiclass classification stump with continuous features") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 3, maxBins = 100)
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)

    val model = DecisionTree.train(rdd, strategy)
    validateClassifier(model, arr, 0.9)

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(32), metadata, 0,
      new Array[Node](0), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1

    assert(bestSplit.feature === 1)
    assert(bestSplit.featureType === Continuous)
    assert(bestSplit.threshold > 1980)
    assert(bestSplit.threshold < 2020)

  }

  test("Multiclass classification stump with continuous + unordered categorical features") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 3, maxBins = 100, categoricalFeaturesInfo = Map(0 -> 3))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))

    val model = DecisionTree.train(rdd, strategy)
    validateClassifier(model, arr, 0.9)

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(32), metadata, 0,
      new Array[Node](0), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1

    assert(bestSplit.feature === 1)
    assert(bestSplit.featureType === Continuous)
    assert(bestSplit.threshold > 1980)
    assert(bestSplit.threshold < 2020)
  }

  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 3, maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val bestSplits = DecisionTree.findBestSplits(treeInput, new Array(32), metadata, 0,
      new Array[Node](0), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1
    assert(bestSplit.feature === 0)
    assert(bestSplit.categories.length === 1)
    assert(bestSplit.categories.contains(1.0))
    assert(bestSplit.featureType === Categorical)
  }

  test("Multiclass classification tree with 10-ary (ordered) categorical features," +
      " with just enough bins") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClassesForClassification = 3, maxBins = 10,
      categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    assert(strategy.isMulticlassClassification)

    val model = DecisionTree.train(rdd, strategy)
    validateClassifier(model, arr, 0.6)
  }

}

object DecisionTreeSuite {

  def generateOrderedLabeledPointsWithLabel0(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val lp = new LabeledPoint(0.0, Vectors.dense(i.toDouble, 1000.0 - i))
      arr(i) = lp
    }
    arr
  }

  def generateOrderedLabeledPointsWithLabel1(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      val lp = new LabeledPoint(1.0, Vectors.dense(i.toDouble, 999.0 - i))
      arr(i) = lp
    }
    arr
  }

  def generateOrderedLabeledPoints(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      if (i < 600) {
        val lp = new LabeledPoint(0.0, Vectors.dense(i.toDouble, 1000.0 - i))
        arr(i) = lp
      } else {
        val lp = new LabeledPoint(1.0, Vectors.dense(i.toDouble, 1000.0 - i))
        arr(i) = lp
      }
    }
    arr
  }

  def generateCategoricalDataPoints(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](1000)
    for (i <- 0 until 1000) {
      if (i < 600) {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(0.0, 1.0))
      } else {
        arr(i) = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0))
      }
    }
    arr
  }

  def generateCategoricalDataPointsAsJavaList(): java.util.List[LabeledPoint] = {
    generateCategoricalDataPoints().toList.asJava
  }

  def generateCategoricalDataPointsForMulticlass(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](3000)
    for (i <- 0 until 3000) {
      if (i < 1000) {
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, 2.0))
      } else if (i < 2000) {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(1.0, 2.0))
      } else {
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, 2.0))
      }
    }
    arr
  }

  def generateContinuousDataPointsForMulticlass(): Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](3000)
    for (i <- 0 until 3000) {
      if (i < 2000) {
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, i))
      } else {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(2.0, i))
      }
    }
    arr
  }

  def generateCategoricalDataPointsForMulticlassForOrderedFeatures():
    Array[LabeledPoint] = {
    val arr = new Array[LabeledPoint](3000)
    for (i <- 0 until 3000) {
      if (i < 1000) {
        arr(i) = new LabeledPoint(2.0, Vectors.dense(2.0, 2.0))
      } else if (i < 2000) {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(1.0, 2.0))
      } else {
        arr(i) = new LabeledPoint(1.0, Vectors.dense(2.0, 2.0))
      }
    }
    arr
  }

}

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
import scala.collection.mutable

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.{QuantileStrategy, Strategy}
import org.apache.spark.mllib.tree.impl.{BaggedPoint, DecisionTreeMetadata, TreePoint}
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Variance}
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils


class DecisionTreeSuite extends FunSuite with MLlibTestSparkContext {

  /////////////////////////////////////////////////////////////////////////////
  // Tests examining individual elements of training
  /////////////////////////////////////////////////////////////////////////////

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
      numClasses = 2,
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
      numClasses = 2,
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

  test("find splits for a continuous feature") {
    // find splits for normal case
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(6), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array.fill(200000)(math.random)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 5)
      assert(fakeMetadata.numSplits(0) === 5)
      assert(fakeMetadata.numBins(0) === 6)
      // check returned splits are distinct
      assert(splits.distinct.length === splits.length)
    }

    // find splits should not return identical splits
    // when there are not enough split candidates, reduce the number of splits in metadata
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(5), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array(1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3).map(_.toDouble)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 3)
      assert(fakeMetadata.numSplits(0) === 3)
      assert(fakeMetadata.numBins(0) === 4)
      // check returned splits are distinct
      assert(splits.distinct.length === splits.length)
    }

    // find splits when most samples close to the minimum
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(3), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array(2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 4, 5).map(_.toDouble)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 2)
      assert(fakeMetadata.numSplits(0) === 2)
      assert(fakeMetadata.numBins(0) === 3)
      assert(splits(0) === 2.0)
      assert(splits(1) === 3.0)
    }

    // find splits when most samples close to the maximum
    {
      val fakeMetadata = new DecisionTreeMetadata(1, 0, 0, 0,
        Map(), Set(),
        Array(3), Gini, QuantileStrategy.Sort,
        0, 0, 0.0, 0, 0
      )
      val featureSamples = Array(0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2).map(_.toDouble)
      val splits = DecisionTree.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 1)
      assert(fakeMetadata.numSplits(0) === 1)
      assert(fakeMetadata.numBins(0) === 2)
      assert(splits(0) === 1.0)
    }
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
      numClasses = 100,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(bins.length === 2)
    assert(splits(0).length === 3)
    assert(bins(0).length === 0)

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

  }

  test("Multiclass classification with ordered categorical features: split and bin calculations") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    assert(arr.length === 3000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 2,
      numClasses = 100,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 10, 1-> 10))
    // 2^(10-1) - 1 > 100, so categorical features will be ordered

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

  test("Avoid aggregation on the last level") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.0, 1.0)))
    val input = sc.parallelize(arr)

    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 1,
      numClasses = 2, categoricalFeaturesInfo = Map(0 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(input, metadata)

    val treeInput = TreePoint.convertToTreeRDD(input, bins, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, false)

    val topNode = Node.emptyNode(nodeIndex = 1)
    assert(topNode.predict.predict === Double.MinValue)
    assert(topNode.impurity === -1.0)
    assert(topNode.isLeaf === false)

    val nodesForGroup = Map((0, Array(topNode)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (topNode.id, new RandomForest.NodeIndexInfo(0, None))
    )))
    val nodeQueue = new mutable.Queue[(Int, Node)]()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(topNode),
      nodesForGroup, treeToNodeToIndexInfo, splits, bins, nodeQueue)

    // don't enqueue leaf nodes into node queue
    assert(nodeQueue.isEmpty)

    // set impurity and predict for topNode
    assert(topNode.predict.predict !== Double.MinValue)
    assert(topNode.impurity !== -1.0)

    // set impurity and predict for child nodes
    assert(topNode.leftNode.get.predict.predict === 0.0)
    assert(topNode.rightNode.get.predict.predict === 1.0)
    assert(topNode.leftNode.get.impurity === 0.0)
    assert(topNode.rightNode.get.impurity === 0.0)
  }

  test("Avoid aggregation if impurity is 0.0") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.0, 1.0)))
    val input = sc.parallelize(arr)

    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClasses = 2, categoricalFeaturesInfo = Map(0 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val (splits, bins) = DecisionTree.findSplitsBins(input, metadata)

    val treeInput = TreePoint.convertToTreeRDD(input, bins, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, false)

    val topNode = Node.emptyNode(nodeIndex = 1)
    assert(topNode.predict.predict === Double.MinValue)
    assert(topNode.impurity === -1.0)
    assert(topNode.isLeaf === false)

    val nodesForGroup = Map((0, Array(topNode)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (topNode.id, new RandomForest.NodeIndexInfo(0, None))
    )))
    val nodeQueue = new mutable.Queue[(Int, Node)]()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(topNode),
      nodesForGroup, treeToNodeToIndexInfo, splits, bins, nodeQueue)

    // don't enqueue a node into node queue if its impurity is 0.0
    assert(nodeQueue.isEmpty)

    // set impurity and predict for topNode
    assert(topNode.predict.predict !== Double.MinValue)
    assert(topNode.impurity !== -1.0)

    // set impurity and predict for child nodes
    assert(topNode.leftNode.get.predict.predict === 0.0)
    assert(topNode.rightNode.get.predict.predict === 1.0)
    assert(topNode.leftNode.get.impurity === 0.0)
    assert(topNode.rightNode.get.impurity === 0.0)
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
    val strategyOneNode = new Strategy(Classification, Entropy, maxDepth = 1,
      numClasses = 2, maxBins = 100)
    val modelOneNode = DecisionTree.train(rdd, strategyOneNode)
    val rootNode1 = modelOneNode.topNode.deepCopy()
    val rootNode2 = modelOneNode.topNode.deepCopy()
    assert(rootNode1.leftNode.nonEmpty)
    assert(rootNode1.rightNode.nonEmpty)

    val treeInput = TreePoint.convertToTreeRDD(rdd, bins, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, false)

    // Single group second level tree construction.
    val nodesForGroup = Map((0, Array(rootNode1.leftNode.get, rootNode1.rightNode.get)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (rootNode1.leftNode.get.id, new RandomForest.NodeIndexInfo(0, None)),
      (rootNode1.rightNode.get.id, new RandomForest.NodeIndexInfo(1, None)))))
    val nodeQueue = new mutable.Queue[(Int, Node)]()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(rootNode1),
      nodesForGroup, treeToNodeToIndexInfo, splits, bins, nodeQueue)
    val children1 = new Array[Node](2)
    children1(0) = rootNode1.leftNode.get
    children1(1) = rootNode1.rightNode.get

    // Train one second-level node at a time.
    val nodesForGroupA = Map((0, Array(rootNode2.leftNode.get)))
    val treeToNodeToIndexInfoA = Map((0, Map(
      (rootNode2.leftNode.get.id, new RandomForest.NodeIndexInfo(0, None)))))
    nodeQueue.clear()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(rootNode2),
      nodesForGroupA, treeToNodeToIndexInfoA, splits, bins, nodeQueue)
    val nodesForGroupB = Map((0, Array(rootNode2.rightNode.get)))
    val treeToNodeToIndexInfoB = Map((0, Map(
      (rootNode2.rightNode.get.id, new RandomForest.NodeIndexInfo(0, None)))))
    nodeQueue.clear()
    DecisionTree.findBestSplits(baggedInput, metadata, Array(rootNode2),
      nodesForGroupB, treeToNodeToIndexInfoB, splits, bins, nodeQueue)
    val children2 = new Array[Node](2)
    children2(0) = rootNode2.leftNode.get
    children2(1) = rootNode2.rightNode.get

    // Verify whether the splits obtained using single group and multiple group level
    // construction strategies are the same.
    for (i <- 0 until 2) {
      assert(children1(i).stats.nonEmpty && children1(i).stats.get.gain > 0)
      assert(children2(i).stats.nonEmpty && children2(i).stats.get.gain > 0)
      assert(children1(i).split === children2(i).split)
      assert(children1(i).stats.nonEmpty && children2(i).stats.nonEmpty)
      val stats1 = children1(i).stats.get
      val stats2 = children2(i).stats.get
      assert(stats1.gain === stats2.gain)
      assert(stats1.impurity === stats2.impurity)
      assert(stats1.leftImpurity === stats2.leftImpurity)
      assert(stats1.rightImpurity === stats2.rightImpurity)
      assert(children1(i).predict.predict === children2(i).predict.predict)
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////

  test("Binary classification stump with ordered categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      numClasses = 2,
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

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.categories === List(1.0))
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = rootNode.stats.get
    assert(stats.gain > 0)
    assert(rootNode.predict.predict === 1)
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

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = rootNode.stats.get
    assert(stats.gain > 0)
    assert(rootNode.predict.predict === 0.6)
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
    DecisionTreeSuite.validateRegressor(model, arr, 0.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
  }

  test("Binary classification stump with fixed label 0 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
  }

  test("Binary classification stump with fixed label 1 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
    assert(rootNode.predict.predict === 1)
  }

  test("Binary classification stump with fixed label 0 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
    assert(rootNode.predict.predict === 0)
  }

  test("Binary classification stump with fixed label 1 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val (splits, bins) = DecisionTree.findSplitsBins(rdd, metadata)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val stats = rootNode.stats.get
    assert(stats.gain === 0)
    assert(stats.leftImpurity === 0)
    assert(stats.rightImpurity === 0)
    assert(rootNode.predict.predict === 1)
  }

  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(strategy.isMulticlassClassification)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.feature === 0)
    assert(split.categories.length === 1)
    assert(split.categories.contains(1))
    assert(split.featureType === Categorical)
  }

  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(3.0)))
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 2)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)
  }

  test("Binary classification stump with 2 continuous features") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0)))))

    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 2)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 1.0)
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
      numClasses = 3, maxBins = maxBins,
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 1.0)
    assert(model.numNodes === 3)
    assert(model.depth === 1)

    val rootNode = model.topNode

    val split = rootNode.split.get
    assert(split.feature === 0)
    assert(split.categories.length === 1)
    assert(split.categories.contains(1))
    assert(split.featureType === Categorical)

    val gain = rootNode.stats.get
    assert(gain.leftImpurity === 0)
    assert(gain.rightImpurity === 0)
  }

  test("Multiclass classification stump with continuous features") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, maxBins = 100)
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 0.9)

    val rootNode = model.topNode

    val split = rootNode.split.get
    assert(split.feature === 1)
    assert(split.featureType === Continuous)
    assert(split.threshold > 1980)
    assert(split.threshold < 2020)

  }

  test("Multiclass classification stump with continuous + unordered categorical features") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, maxBins = 100, categoricalFeaturesInfo = Map(0 -> 3))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 0.9)

    val rootNode = model.topNode

    val split = rootNode.split.get
    assert(split.feature === 1)
    assert(split.featureType === Continuous)
    assert(split.threshold > 1980)
    assert(split.threshold < 2020)
  }

  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    assert(strategy.isMulticlassClassification)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.feature === 0)
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)
  }

  test("Multiclass classification tree with 10-ary (ordered) categorical features," +
      " with just enough bins") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, maxBins = 10,
      categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    assert(strategy.isMulticlassClassification)

    val model = DecisionTree.train(rdd, strategy)
    DecisionTreeSuite.validateClassifier(model, arr, 0.6)
  }

  test("split must satisfy min instances per node requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini,
      maxDepth = 2, numClasses = 2, minInstancesPerNode = 2)

    val model = DecisionTree.train(rdd, strategy)
    assert(model.topNode.isLeaf)
    assert(model.topNode.predict.predict == 0.0)
    val predicts = rdd.map(p => model.predict(p.features)).collect()
    predicts.foreach { predict =>
      assert(predict == 0.0)
    }

    // test when no valid split can be found
    val rootNode = model.topNode

    val gain = rootNode.stats.get
    assert(gain == InformationGainStats.invalidInformationGainStats)
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
    val strategy = new Strategy(algo = Classification, impurity = Gini,
      maxBins = 2, maxDepth = 2, categoricalFeaturesInfo = Map(0 -> 2, 1-> 2),
      numClasses = 2, minInstancesPerNode = 2)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    val gain = rootNode.stats.get
    assert(split.feature == 1)
    assert(gain != InformationGainStats.invalidInformationGainStats)
  }

  test("split must satisfy min info gain requirements") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0)))),
      LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0)))),
      LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0)))))

    val input = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, minInfoGain = 1.0)

    val model = DecisionTree.train(input, strategy)
    assert(model.topNode.isLeaf)
    assert(model.topNode.predict.predict == 0.0)
    val predicts = input.map(p => model.predict(p.features)).collect()
    predicts.foreach { predict =>
      assert(predict == 0.0)
    }

    // test when no valid split can be found
    val rootNode = model.topNode

    val gain = rootNode.stats.get
    assert(gain == InformationGainStats.invalidInformationGainStats)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  test("Node.subtreeIterator") {
    val model = DecisionTreeSuite.createModel(Classification)
    val nodeIds = model.topNode.subtreeIterator.map(_.id).toArray.sorted
    assert(nodeIds === DecisionTreeSuite.createdModelNodeIds)
  }

  test("model save/load") {
    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    Array(Classification, Regression).foreach { algo =>
      val model = DecisionTreeSuite.createModel(algo)
      // Save model, load it back, and compare.
      try {
        model.save(sc, path)
        val sameModel = DecisionTreeModel.load(sc, path)
        DecisionTreeSuite.checkEqual(model, sameModel)
      } finally {
        Utils.deleteRecursively(tempDir)
      }
    }
  }
}

object DecisionTreeSuite extends FunSuite {

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
      val label = if (i < 100) {
        0.0
      } else if (i < 500) {
        1.0
      } else if (i < 900) {
        0.0
      } else {
        1.0
      }
      arr(i) = new LabeledPoint(label, Vectors.dense(i.toDouble, 1000.0 - i))
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

  /** Create a leaf node with the given node ID */
  private def createLeafNode(id: Int): Node = {
    Node(nodeIndex = id, new Predict(0.0, 1.0), impurity = 0.5, isLeaf = true)
  }

  /**
   * Create an internal node with the given node ID and feature type.
   * Note: This does NOT set the child nodes.
   */
  private def createInternalNode(id: Int, featureType: FeatureType): Node = {
    val node = Node(nodeIndex = id, new Predict(0.0, 1.0), impurity = 0.5, isLeaf = false)
    featureType match {
      case Continuous =>
        node.split = Some(new Split(feature = 0, threshold = 0.5, Continuous,
          categories = List.empty[Double]))
      case Categorical =>
        node.split = Some(new Split(feature = 1, threshold = 0.0, Categorical,
          categories = List(0.0, 1.0)))
    }
    // TODO: The information gain stats should be consistent with the same info stored in children.
    node.stats = Some(new InformationGainStats(gain = 0.1, impurity = 0.2,
      leftImpurity = 0.3, rightImpurity = 0.4, new Predict(1.0, 0.4), new Predict(0.0, 0.6)))
    node
  }

  /**
   * Create a tree model.  This is deterministic and contains a variety of node and feature types.
   * TODO: Update this to be a correct tree (with matching probabilities, impurities, etc.)
   */
  private[mllib] def createModel(algo: Algo): DecisionTreeModel = {
    val topNode = createInternalNode(id = 1, Continuous)
    val (node2, node3) = (createLeafNode(id = 2), createInternalNode(id = 3, Categorical))
    val (node6, node7) = (createLeafNode(id = 6), createLeafNode(id = 7))
    topNode.leftNode = Some(node2)
    topNode.rightNode = Some(node3)
    node3.leftNode = Some(node6)
    node3.rightNode = Some(node7)
    new DecisionTreeModel(topNode, algo)
  }

  /** Sorted Node IDs matching the model returned by [[createModel()]] */
  private val createdModelNodeIds = Array(1, 2, 3, 6, 7)

  /**
   * Check if the two trees are exactly the same.
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   * If the trees are not equal, this prints the two trees and throws an exception.
   */
  private[mllib] def checkEqual(a: DecisionTreeModel, b: DecisionTreeModel): Unit = {
    try {
      assert(a.algo === b.algo)
      checkEqual(a.topNode, b.topNode)
    } catch {
      case ex: Exception =>
        throw new AssertionError("checkEqual failed since the two trees were not identical.\n" +
          "TREE A:\n" + a.toDebugString + "\n" +
          "TREE B:\n" + b.toDebugString + "\n", ex)
    }
  }

  /**
   * Return true iff the two nodes and their descendents are exactly the same.
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   */
  private def checkEqual(a: Node, b: Node): Unit = {
    assert(a.id === b.id)
    assert(a.predict === b.predict)
    assert(a.impurity === b.impurity)
    assert(a.isLeaf === b.isLeaf)
    assert(a.split === b.split)
    (a.stats, b.stats) match {
      // TODO: Check other fields besides the infomation gain.
      case (Some(aStats), Some(bStats)) => assert(aStats.gain === bStats.gain)
      case (None, None) =>
      case _ => throw new AssertionError(
          s"Only one instance has stats defined. (a.stats: ${a.stats}, b.stats: ${b.stats})")
    }
    (a.leftNode, b.leftNode) match {
      case (Some(aNode), Some(bNode)) => checkEqual(aNode, bNode)
      case (None, None) =>
      case _ => throw new AssertionError("Only one instance has leftNode defined. " +
        s"(a.leftNode: ${a.leftNode}, b.leftNode: ${b.leftNode})")
    }
    (a.rightNode, b.rightNode) match {
      case (Some(aNode: Node), Some(bNode: Node)) => checkEqual(aNode, bNode)
      case (None, None) =>
      case _ => throw new AssertionError("Only one instance has rightNode defined. " +
        s"(a.rightNode: ${a.rightNode}, b.rightNode: ${b.rightNode})")
    }
  }
}

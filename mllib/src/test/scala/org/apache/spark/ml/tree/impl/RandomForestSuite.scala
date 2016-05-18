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

package org.apache.spark.ml.tree.impl

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.tree._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.tree.{DecisionTreeSuite => OldDTSuite, EnsembleTestHelper}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, QuantileStrategy, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, GiniCalculator}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.collection.OpenHashMap

/**
 * Test suite for [[RandomForest]].
 */
class RandomForestSuite extends SparkFunSuite with MLlibTestSparkContext {

  import RandomForestSuite.mapToVec

  /////////////////////////////////////////////////////////////////////////////
  // Tests for split calculation
  /////////////////////////////////////////////////////////////////////////////

  test("Binary classification with continuous features: split calculation") {
    val arr = OldDTSuite.generateOrderedLabeledPointsWithLabel1().map(_.asML)
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new OldStrategy(OldAlgo.Classification, Gini, 3, 2, 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    val splits = RandomForest.findSplits(rdd, metadata, seed = 42)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
  }

  test("Binary classification with binary (ordered) categorical features: split calculation") {
    val arr = OldDTSuite.generateCategoricalDataPoints().map(_.asML)
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new OldStrategy(OldAlgo.Classification, Gini, maxDepth = 2, numClasses = 2,
      maxBins = 100, categoricalFeaturesInfo = Map(0 -> 2, 1 -> 2))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    val splits = RandomForest.findSplits(rdd, metadata, seed = 42)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    assert(splits.length === 2)
    // no splits pre-computed for ordered categorical features
    assert(splits(0).length === 0)
  }

  test("Binary classification with 3-ary (ordered) categorical features," +
    " with no samples for one category: split calculation") {
    val arr = OldDTSuite.generateCategoricalDataPoints().map(_.asML)
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new OldStrategy(OldAlgo.Classification, Gini, maxDepth = 2, numClasses = 2,
      maxBins = 100, categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val splits = RandomForest.findSplits(rdd, metadata, seed = 42)
    assert(splits.length === 2)
    // no splits pre-computed for ordered categorical features
    assert(splits(0).length === 0)
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
      val splits = RandomForest.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
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
      val splits = RandomForest.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 3)
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
      val splits = RandomForest.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 2)
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
      val splits = RandomForest.findSplitsForContinuousFeature(featureSamples, fakeMetadata, 0)
      assert(splits.length === 1)
      assert(splits(0) === 1.0)
    }
  }

  test("Multiclass classification with unordered categorical features: split calculations") {
    val arr = OldDTSuite.generateCategoricalDataPoints().map(_.asML)
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new OldStrategy(
      OldAlgo.Classification,
      Gini,
      maxDepth = 2,
      numClasses = 100,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(metadata.isUnordered(featureIndex = 0))
    assert(metadata.isUnordered(featureIndex = 1))
    val splits = RandomForest.findSplits(rdd, metadata, seed = 42)
    assert(splits.length === 2)
    assert(splits(0).length === 3)
    assert(metadata.numSplits(0) === 3)
    assert(metadata.numBins(0) === 3)
    assert(metadata.numSplits(1) === 3)
    assert(metadata.numBins(1) === 3)

    // Expecting 2^2 - 1 = 3 splits per feature
    def checkCategoricalSplit(s: Split, featureIndex: Int, leftCategories: Array[Double]): Unit = {
      assert(s.featureIndex === featureIndex)
      assert(s.isInstanceOf[CategoricalSplit])
      val s0 = s.asInstanceOf[CategoricalSplit]
      assert(s0.leftCategories === leftCategories)
      assert(s0.numCategories === 3)  // for this unit test
    }
    // Feature 0
    checkCategoricalSplit(splits(0)(0), 0, Array(0.0))
    checkCategoricalSplit(splits(0)(1), 0, Array(1.0))
    checkCategoricalSplit(splits(0)(2), 0, Array(0.0, 1.0))
    // Feature 1
    checkCategoricalSplit(splits(1)(0), 1, Array(0.0))
    checkCategoricalSplit(splits(1)(1), 1, Array(1.0))
    checkCategoricalSplit(splits(1)(2), 1, Array(0.0, 1.0))
  }

  test("Multiclass classification with ordered categorical features: split calculations") {
    val arr = OldDTSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures().map(_.asML)
    assert(arr.length === 3000)
    val rdd = sc.parallelize(arr)
    val strategy = new OldStrategy(OldAlgo.Classification, Gini, maxDepth = 2, numClasses = 100,
      maxBins = 100, categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    // 2^(10-1) - 1 > 100, so categorical features will be ordered

    val metadata = DecisionTreeMetadata.buildMetadata(rdd, strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))
    val splits = RandomForest.findSplits(rdd, metadata, seed = 42)
    assert(splits.length === 2)
    // no splits pre-computed for ordered categorical features
    assert(splits(0).length === 0)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of other algorithm internals
  /////////////////////////////////////////////////////////////////////////////

  test("extract categories from a number for multiclass classification") {
    val l = RandomForest.extractMultiClassCategories(13, 10)
    assert(l.length === 3)
    assert(Seq(3.0, 2.0, 0.0) === l)
  }

  test("Avoid aggregation on the last level") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.0, 1.0)))
    val input = sc.parallelize(arr)

    val strategy = new OldStrategy(algo = OldAlgo.Classification, impurity = Gini, maxDepth = 1,
      numClasses = 2, categoricalFeaturesInfo = Map(0 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val splits = RandomForest.findSplits(input, metadata, seed = 42)

    val treeInput = TreePoint.convertToTreeRDD(input, splits, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, withReplacement = false)

    val topNode = LearningNode.emptyNode(nodeIndex = 1)
    assert(topNode.isLeaf === false)
    assert(topNode.stats === null)

    val nodesForGroup = Map((0, Array(topNode)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (topNode.id, new RandomForest.NodeIndexInfo(0, None))
    )))
    val nodeQueue = new mutable.Queue[(Int, LearningNode)]()
    RandomForest.findBestSplits(baggedInput, metadata, Array(topNode),
      nodesForGroup, treeToNodeToIndexInfo, splits, nodeQueue)

    // don't enqueue leaf nodes into node queue
    assert(nodeQueue.isEmpty)

    // set impurity and predict for topNode
    assert(topNode.stats !== null)
    assert(topNode.stats.impurity > 0.0)

    // set impurity and predict for child nodes
    assert(topNode.leftChild.get.toNode.prediction === 0.0)
    assert(topNode.rightChild.get.toNode.prediction === 1.0)
    assert(topNode.leftChild.get.stats.impurity === 0.0)
    assert(topNode.rightChild.get.stats.impurity === 0.0)
  }

  test("Avoid aggregation if impurity is 0.0") {
    val arr = Array(
      LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 2.0, 1.0)))
    val input = sc.parallelize(arr)

    val strategy = new OldStrategy(algo = OldAlgo.Classification, impurity = Gini, maxDepth = 5,
      numClasses = 2, categoricalFeaturesInfo = Map(0 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val splits = RandomForest.findSplits(input, metadata, seed = 42)

    val treeInput = TreePoint.convertToTreeRDD(input, splits, metadata)
    val baggedInput = BaggedPoint.convertToBaggedRDD(treeInput, 1.0, 1, withReplacement = false)

    val topNode = LearningNode.emptyNode(nodeIndex = 1)
    assert(topNode.isLeaf === false)
    assert(topNode.stats === null)

    val nodesForGroup = Map((0, Array(topNode)))
    val treeToNodeToIndexInfo = Map((0, Map(
      (topNode.id, new RandomForest.NodeIndexInfo(0, None))
    )))
    val nodeQueue = new mutable.Queue[(Int, LearningNode)]()
    RandomForest.findBestSplits(baggedInput, metadata, Array(topNode),
      nodesForGroup, treeToNodeToIndexInfo, splits, nodeQueue)

    // don't enqueue a node into node queue if its impurity is 0.0
    assert(nodeQueue.isEmpty)

    // set impurity and predict for topNode
    assert(topNode.stats !== null)
    assert(topNode.stats.impurity > 0.0)

    // set impurity and predict for child nodes
    assert(topNode.leftChild.get.toNode.prediction === 0.0)
    assert(topNode.rightChild.get.toNode.prediction === 1.0)
    assert(topNode.leftChild.get.stats.impurity === 0.0)
    assert(topNode.rightChild.get.stats.impurity === 0.0)
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
    val input = sc.parallelize(arr)

    // Must set maxBins s.t. the feature will be treated as an ordered categorical feature.
    val strategy = new OldStrategy(algo = OldAlgo.Classification, impurity = Gini, maxDepth = 1,
      numClasses = 2, categoricalFeaturesInfo = Map(0 -> 3), maxBins = 3)

    val model = RandomForest.run(input, strategy, numTrees = 1, featureSubsetStrategy = "all",
      seed = 42, instr = None).head
    model.rootNode match {
      case n: InternalNode => n.split match {
        case s: CategoricalSplit =>
          assert(s.leftCategories === Array(1.0))
        case _ => throw new AssertionError("model.rootNode.split was not a CategoricalSplit")
      }
      case _ => throw new AssertionError("model.rootNode was not an InternalNode")
    }
  }

  test("Second level node building with vs. without groups") {
    val arr = OldDTSuite.generateOrderedLabeledPoints().map(_.asML)
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    // For tree with 1 group
    val strategy1 =
      new OldStrategy(OldAlgo.Classification, Entropy, 3, 2, 100, maxMemoryInMB = 1000)
    // For tree with multiple groups
    val strategy2 =
      new OldStrategy(OldAlgo.Classification, Entropy, 3, 2, 100, maxMemoryInMB = 0)

    val tree1 = RandomForest.run(rdd, strategy1, numTrees = 1, featureSubsetStrategy = "all",
      seed = 42, instr = None).head
    val tree2 = RandomForest.run(rdd, strategy2, numTrees = 1, featureSubsetStrategy = "all",
      seed = 42, instr = None).head

    def getChildren(rootNode: Node): Array[InternalNode] = rootNode match {
      case n: InternalNode =>
        assert(n.leftChild.isInstanceOf[InternalNode])
        assert(n.rightChild.isInstanceOf[InternalNode])
        Array(n.leftChild.asInstanceOf[InternalNode], n.rightChild.asInstanceOf[InternalNode])
      case _ => throw new AssertionError("rootNode was not an InternalNode")
    }

    // Single group second level tree construction.
    val children1 = getChildren(tree1.rootNode)
    val children2 = getChildren(tree2.rootNode)

    // Verify whether the splits obtained using single group and multiple group level
    // construction strategies are the same.
    for (i <- 0 until 2) {
      assert(children1(i).gain > 0)
      assert(children2(i).gain > 0)
      assert(children1(i).split === children2(i).split)
      assert(children1(i).impurity === children2(i).impurity)
      assert(children1(i).impurityStats.stats === children2(i).impurityStats.stats)
      assert(children1(i).leftChild.impurity === children2(i).leftChild.impurity)
      assert(children1(i).rightChild.impurity === children2(i).rightChild.impurity)
      assert(children1(i).prediction === children2(i).prediction)
    }
  }

  def binaryClassificationTestWithContinuousFeaturesAndSubsampledFeatures(strategy: OldStrategy) {
    val numFeatures = 50
    val arr = EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures, 1000)
    val rdd = sc.parallelize(arr).map(_.asML)

    // Select feature subset for top nodes.  Return true if OK.
    def checkFeatureSubsetStrategy(
        numTrees: Int,
        featureSubsetStrategy: String,
        numFeaturesPerNode: Int): Unit = {
      val seeds = Array(123, 5354, 230, 349867, 23987)
      val maxMemoryUsage: Long = 128 * 1024L * 1024L
      val metadata =
        DecisionTreeMetadata.buildMetadata(rdd, strategy, numTrees, featureSubsetStrategy)
      seeds.foreach { seed =>
        val failString = s"Failed on test with:" +
          s"numTrees=$numTrees, featureSubsetStrategy=$featureSubsetStrategy," +
          s" numFeaturesPerNode=$numFeaturesPerNode, seed=$seed"
        val nodeQueue = new mutable.Queue[(Int, LearningNode)]()
        val topNodes: Array[LearningNode] = new Array[LearningNode](numTrees)
        Range(0, numTrees).foreach { treeIndex =>
          topNodes(treeIndex) = LearningNode.emptyNode(nodeIndex = 1)
          nodeQueue.enqueue((treeIndex, topNodes(treeIndex)))
        }
        val rng = new scala.util.Random(seed = seed)
        val (nodesForGroup: Map[Int, Array[LearningNode]],
        treeToNodeToIndexInfo: Map[Int, Map[Int, RandomForest.NodeIndexInfo]]) =
          RandomForest.selectNodesToSplit(nodeQueue, maxMemoryUsage, metadata, rng)

        assert(nodesForGroup.size === numTrees, failString)
        assert(nodesForGroup.values.forall(_.length == 1), failString) // 1 node per tree

        if (numFeaturesPerNode == numFeatures) {
          // featureSubset values should all be None
          assert(treeToNodeToIndexInfo.values.forall(_.values.forall(_.featureSubset.isEmpty)),
            failString)
        } else {
          // Check number of features.
          assert(treeToNodeToIndexInfo.values.forall(_.values.forall(
            _.featureSubset.get.length === numFeaturesPerNode)), failString)
        }
      }
    }

    checkFeatureSubsetStrategy(numTrees = 1, "auto", numFeatures)
    checkFeatureSubsetStrategy(numTrees = 1, "all", numFeatures)
    checkFeatureSubsetStrategy(numTrees = 1, "sqrt", math.sqrt(numFeatures).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 1, "log2",
      (math.log(numFeatures) / math.log(2)).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 1, "onethird", (numFeatures / 3.0).ceil.toInt)

    val realStrategies = Array(".1", ".10", "0.10", "0.1", "0.9", "1.0")
    for (strategy <- realStrategies) {
      val expected = (strategy.toDouble * numFeatures).ceil.toInt
      checkFeatureSubsetStrategy(numTrees = 1, strategy, expected)
    }

    val integerStrategies = Array("1", "10", "100", "1000", "10000")
    for (strategy <- integerStrategies) {
      val expected = if (strategy.toInt < numFeatures) strategy.toInt else numFeatures
      checkFeatureSubsetStrategy(numTrees = 1, strategy, expected)
    }

    val invalidStrategies = Array("-.1", "-.10", "-0.10", ".0", "0.0", "1.1", "0")
    for (invalidStrategy <- invalidStrategies) {
      intercept[IllegalArgumentException]{
        val metadata =
          DecisionTreeMetadata.buildMetadata(rdd, strategy, numTrees = 1, invalidStrategy)
      }
    }

    checkFeatureSubsetStrategy(numTrees = 2, "all", numFeatures)
    checkFeatureSubsetStrategy(numTrees = 2, "auto", math.sqrt(numFeatures).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "sqrt", math.sqrt(numFeatures).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "log2",
      (math.log(numFeatures) / math.log(2)).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "onethird", (numFeatures / 3.0).ceil.toInt)

    for (strategy <- realStrategies) {
      val expected = (strategy.toDouble * numFeatures).ceil.toInt
      checkFeatureSubsetStrategy(numTrees = 2, strategy, expected)
    }

    for (strategy <- integerStrategies) {
      val expected = if (strategy.toInt < numFeatures) strategy.toInt else numFeatures
      checkFeatureSubsetStrategy(numTrees = 2, strategy, expected)
    }
    for (invalidStrategy <- invalidStrategies) {
      intercept[IllegalArgumentException]{
        val metadata =
          DecisionTreeMetadata.buildMetadata(rdd, strategy, numTrees = 2, invalidStrategy)
      }
    }
  }

  test("Binary classification with continuous features: subsampling features") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new OldStrategy(algo = OldAlgo.Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)
    binaryClassificationTestWithContinuousFeaturesAndSubsampledFeatures(strategy)
  }

  test("Binary classification with continuous features and node Id cache: subsampling features") {
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val strategy = new OldStrategy(algo = OldAlgo.Classification, impurity = Gini, maxDepth = 2,
      numClasses = 2, categoricalFeaturesInfo = categoricalFeaturesInfo,
      useNodeIdCache = true)
    binaryClassificationTestWithContinuousFeaturesAndSubsampledFeatures(strategy)
  }

  test("computeFeatureImportance, featureImportances") {
    /* Build tree for testing, with this structure:
          grandParent
      left2       parent
                left  right
     */
    val leftImp = new GiniCalculator(Array(3.0, 2.0, 1.0))
    val left = new LeafNode(0.0, leftImp.calculate(), leftImp)

    val rightImp = new GiniCalculator(Array(1.0, 2.0, 5.0))
    val right = new LeafNode(2.0, rightImp.calculate(), rightImp)

    val parent = TreeTests.buildParentNode(left, right, new ContinuousSplit(0, 0.5))
    val parentImp = parent.impurityStats

    val left2Imp = new GiniCalculator(Array(1.0, 6.0, 1.0))
    val left2 = new LeafNode(0.0, left2Imp.calculate(), left2Imp)

    val grandParent = TreeTests.buildParentNode(left2, parent, new ContinuousSplit(1, 1.0))
    val grandImp = grandParent.impurityStats

    // Test feature importance computed at different subtrees.
    def testNode(node: Node, expected: Map[Int, Double]): Unit = {
      val map = new OpenHashMap[Int, Double]()
      TreeEnsembleModel.computeFeatureImportance(node, map)
      assert(mapToVec(map.toMap) ~== mapToVec(expected) relTol 0.01)
    }

    // Leaf node
    testNode(left, Map.empty[Int, Double])

    // Internal node with 2 leaf children
    val feature0importance = parentImp.calculate() * parentImp.count -
      (leftImp.calculate() * leftImp.count + rightImp.calculate() * rightImp.count)
    testNode(parent, Map(0 -> feature0importance))

    // Full tree
    val feature1importance = grandImp.calculate() * grandImp.count -
      (left2Imp.calculate() * left2Imp.count + parentImp.calculate() * parentImp.count)
    testNode(grandParent, Map(0 -> feature0importance, 1 -> feature1importance))

    // Forest consisting of (full tree) + (internal node with 2 leafs)
    val trees = Array(parent, grandParent).map { root =>
      new DecisionTreeClassificationModel(root, numFeatures = 2, numClasses = 3)
        .asInstanceOf[DecisionTreeModel]
    }
    val importances: Vector = TreeEnsembleModel.featureImportances(trees, 2)
    val tree2norm = feature0importance + feature1importance
    val expected = Vectors.dense((1.0 + feature0importance / tree2norm) / 2.0,
      (feature1importance / tree2norm) / 2.0)
    assert(importances ~== expected relTol 0.01)
  }

  test("normalizeMapValues") {
    val map = new OpenHashMap[Int, Double]()
    map(0) = 1.0
    map(2) = 2.0
    TreeEnsembleModel.normalizeMapValues(map)
    val expected = Map(0 -> 1.0 / 3.0, 2 -> 2.0 / 3.0)
    assert(mapToVec(map.toMap) ~== mapToVec(expected) relTol 0.01)
  }

}

private object RandomForestSuite {

  def mapToVec(map: Map[Int, Double]): Vector = {
    val size = (map.keys.toSeq :+ 0).max + 1
    val (indices, values) = map.toSeq.sortBy(_._1).unzip
    Vectors.sparse(size, indices.toArray, values.toArray)
  }
}

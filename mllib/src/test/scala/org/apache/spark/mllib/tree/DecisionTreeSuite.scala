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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.tree.impl.DecisionTreeMetadata
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Variance}
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils


class DecisionTreeSuite extends SparkFunSuite with MLlibTestSparkContext {

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
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.categories === List(1.0))
    assert(split.featureType === Categorical)

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
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))

    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)

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
      categoricalFeaturesInfo = Map(0 -> 2, 1 -> 2))
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
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
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    assert(rootNode.impurity === 0)
    assert(rootNode.stats.isEmpty)
    assert(rootNode.predict.predict === 0)
  }

  test("Binary classification stump with fixed label 1 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    assert(rootNode.impurity === 0)
    assert(rootNode.stats.isEmpty)
    assert(rootNode.predict.predict === 1)
  }

  test("Binary classification stump with fixed label 0 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    assert(rootNode.impurity === 0)
    assert(rootNode.stats.isEmpty)
    assert(rootNode.predict.predict === 0)
  }

  test("Binary classification stump with fixed label 1 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, maxDepth = 3,
      numClasses = 2, maxBins = 100)
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
    assert(!metadata.isUnordered(featureIndex = 0))
    assert(!metadata.isUnordered(featureIndex = 1))

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    assert(rootNode.impurity === 0)
    assert(rootNode.stats.isEmpty)
    assert(rootNode.predict.predict === 1)
  }

  test("Multiclass classification stump with 3-ary (unordered) categorical features") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 4,
      numClasses = 3, categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
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
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
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
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
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
    val metadata = DecisionTreeMetadata.buildMetadata(rdd.map(_.asML.toInstance), strategy)
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

    assert(rootNode.stats.isEmpty)
  }

  test("do not choose split that does not satisfy min instance per node requirements") {
    // if a split does not satisfy min instances per node requirements,
    // this split is invalid, even though the information gain of split is large.
    val arr = Array(
      LabeledPoint(1.0, Vectors.dense(0.0, 1.0)),
      LabeledPoint(0.0, Vectors.dense(1.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, 0.0)))

    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini,
      maxBins = 2, maxDepth = 2, categoricalFeaturesInfo = Map(0 -> 2, 1 -> 2),
      numClasses = 2, minInstancesPerNode = 2)

    val rootNode = DecisionTree.train(rdd, strategy).topNode

    val split = rootNode.split.get
    val gainStats = rootNode.stats.get
    assert(split.feature == 1)
    assert(gainStats.gain >= 0)
    assert(gainStats.impurity >= 0)
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
    assert(model.topNode.stats.isEmpty)
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

object DecisionTreeSuite extends SparkFunSuite {

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
      if (i < 1001) {
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
    // TODO: The information gain stats should be consistent with info in children: SPARK-7131
    node.stats = Some(new InformationGainStats(gain = 0.1, impurity = 0.2,
      leftImpurity = 0.3, rightImpurity = 0.4, new Predict(1.0, 0.4), new Predict(0.0, 0.6)))
    node
  }

  /**
   * Create a tree model.  This is deterministic and contains a variety of node and feature types.
   * TODO: Update to be a correct tree (with matching probabilities, impurities, etc.): SPARK-7131
   */
  private[spark] def createModel(algo: Algo): DecisionTreeModel = {
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
        fail("checkEqual failed since the two trees were not identical.\n" +
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
      // TODO: Check other fields besides the information gain.
      case (Some(aStats), Some(bStats)) => assert(aStats.gain === bStats.gain)
      case (None, None) =>
      case _ => fail(s"Only one instance has stats defined. (a.stats: ${a.stats}, " +
        s"b.stats: ${b.stats})")
    }
    (a.leftNode, b.leftNode) match {
      case (Some(aNode), Some(bNode)) => checkEqual(aNode, bNode)
      case (None, None) =>
      case _ =>
        fail("Only one instance has leftNode defined. (a.leftNode: ${a.leftNode}," +
          " b.leftNode: ${b.leftNode})")
    }
    (a.rightNode, b.rightNode) match {
      case (Some(aNode: Node), Some(bNode: Node)) => checkEqual(aNode, bNode)
      case (None, None) =>
      case _ => fail("Only one instance has rightNode defined. (a.rightNode: ${a.rightNode}, " +
        "b.rightNode: ${b.rightNode})")
    }
  }
}

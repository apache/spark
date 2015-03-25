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

package org.apache.spark.mllib.classification

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.impl.tree._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree => OldDecisionTree,
  DecisionTreeSuite => OldDecisionTreeSuite}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD


class DecisionTreeClassifierSuite extends FunSuite with MLlibTestSparkContext {

  private var categoricalDataPointsRDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel0RDD: RDD[LabeledPoint] = _
  private var orderedLabeledPointsWithLabel1RDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var continuousDataPointsForMulticlassRDD: RDD[LabeledPoint] = _
  private var categoricalDataPointsForMulticlassForOrderedFeaturesRDD: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    categoricalDataPointsRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPoints())
    orderedLabeledPointsWithLabel0RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel0())
    orderedLabeledPointsWithLabel1RDD =
      sc.parallelize(OldDecisionTreeSuite.generateOrderedLabeledPointsWithLabel1())
    categoricalDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlass())
    continuousDataPointsForMulticlassRDD =
      sc.parallelize(OldDecisionTreeSuite.generateContinuousDataPointsForMulticlass())
    categoricalDataPointsForMulticlassForOrderedFeaturesRDD = sc.parallelize(
      OldDecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures())
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train() and comparing with the old API
  /////////////////////////////////////////////////////////////////////////////

  test("Binary classification stump with ordered categorical features") {
    val dt = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(2)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 3, 1-> 3)
    val numClasses = 2
    DecisionTreeClassifierSuite.compareAPIs(
      categoricalDataPointsRDD, dt, categoricalFeatures, numClasses)
  }

  test("Binary classification stump with fixed labels 0,1 for Entropy,Gini") {
    val dt = new DecisionTreeClassifier()
      .setMaxDepth(3)
      .setMaxBins(100)
    val numClasses = 2
    Array(orderedLabeledPointsWithLabel0RDD, orderedLabeledPointsWithLabel1RDD).foreach { rdd =>
      DecisionTreeClassifier.supportedImpurities.foreach { impurity =>
        dt.setImpurity(impurity)
        DecisionTreeClassifierSuite.compareAPIs(
          rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
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
    DecisionTreeClassifierSuite.compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("Binary classification stump with 1 continuous feature, to check off-by-1 error") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.dense(0.0))
    arr(1) = new LabeledPoint(1.0, Vectors.dense(1.0))
    arr(2) = new LabeledPoint(1.0, Vectors.dense(2.0))
    arr(3) = new LabeledPoint(1.0, Vectors.dense(3.0))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 2
    DecisionTreeClassifierSuite.compareAPIs(
      rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("Binary classification stump with 2 continuous features") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0))))
    arr(1) = new LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0))))
    arr(2) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0))))
    arr(3) = new LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 2.0))))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
    val numClasses = 2
    DecisionTreeClassifierSuite.compareAPIs(
      rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
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
    DecisionTreeClassifierSuite.compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("Multiclass classification stump with continuous features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val numClasses = 3
    DecisionTreeClassifierSuite.compareAPIs(
      rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("Multiclass classification stump with continuous + unordered categorical features") {
    val rdd = continuousDataPointsForMulticlassRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 3)
    val numClasses = 3
    DecisionTreeClassifierSuite.compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("Multiclass classification stump with 10-ary (ordered) categorical features") {
    val rdd = categoricalDataPointsForMulticlassForOrderedFeaturesRDD
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(4)
      .setMaxBins(100)
    val categoricalFeatures = Map(0 -> 10, 1 -> 10)
    val numClasses = 3
    DecisionTreeClassifierSuite.compareAPIs(rdd, dt, categoricalFeatures, numClasses)
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
    DecisionTreeClassifierSuite.compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("split must satisfy min instances per node requirements") {
    val arr = new Array[LabeledPoint](3)
    arr(0) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0))))
    arr(1) = new LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0))))
    arr(2) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0))))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setMinInstancesPerNode(2)
    val numClasses = 2
    DecisionTreeClassifierSuite.compareAPIs(
      rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  test("do not choose split that does not satisfy min instance per node requirements") {
    // if a split does not satisfy min instances per node requirements,
    // this split is invalid, even though the information gain of split is large.
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.dense(0.0, 1.0))
    arr(1) = new LabeledPoint(1.0, Vectors.dense(1.0, 1.0))
    arr(2) = new LabeledPoint(0.0, Vectors.dense(0.0, 0.0))
    arr(3) = new LabeledPoint(0.0, Vectors.dense(0.0, 0.0))
    val rdd = sc.parallelize(arr)
    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxBins(2)
      .setMaxDepth(2)
      .setMinInstancesPerNode(2)
    val categoricalFeatures = Map(0 -> 2, 1-> 2)
    val numClasses = 2
    DecisionTreeClassifierSuite.compareAPIs(rdd, dt, categoricalFeatures, numClasses)
  }

  test("split must satisfy min info gain requirements") {
    val arr = new Array[LabeledPoint](3)
    arr(0) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 0.0))))
    arr(1) = new LabeledPoint(1.0, Vectors.sparse(2, Seq((1, 1.0))))
    arr(2) = new LabeledPoint(0.0, Vectors.sparse(2, Seq((0, 1.0))))
    val rdd = sc.parallelize(arr)

    val dt = new DecisionTreeClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setMinInfoGain(1.0)
    val numClasses = 2
    DecisionTreeClassifierSuite.compareAPIs(
      rdd, dt, categoricalFeatures = Map.empty[Int, Int], numClasses)
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests of model save/load
  /////////////////////////////////////////////////////////////////////////////

  // TODO: test("model save/load")
}

private object DecisionTreeClassifierSuite extends FunSuite {

  /**
   * Train 2 decision trees on the given dataset, one using the old API and one using the new API.
   * Convert the old tree to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      dt: DecisionTreeClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): Unit = {
    val oldStrategy = dt.getOldStrategy(categoricalFeatures, numClasses)
    val oldTree = OldDecisionTree.train(data, oldStrategy)
    val newTree = dt.run(data, categoricalFeatures, numClasses)
    val oldTreeAsNew = DecisionTreeClassificationModel.fromOld(oldTree)
    checkEqual(oldTreeAsNew, newTree)
  }

  /**
   * Check if the two trees are exactly the same.
   * Note: I hesitate to override Node.equals since it could cause problems if users
   *       make mistakes such as creating loops of Nodes.
   * If the trees are not equal, this prints the two trees and throws an exception.
   */
  def checkEqual(a: DecisionTreeModel, b: DecisionTreeModel): Unit = {
    try {
      checkEqual(a.rootNode, b.rootNode)
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
  def checkEqual(a: Node, b: Node): Unit = {
    assert(a.prediction === b.prediction)
    assert(a.impurity === b.impurity)
    (a, b) match {
      case (aye: InternalNode, bee: InternalNode) =>
        assert(aye.split === bee.split)
        checkEqual(aye.leftChild, bee.leftChild)
        checkEqual(aye.rightChild, bee.rightChild)
      case (aye: LeafNode, bee: LeafNode) => // do nothing
      case _ =>
        throw new AssertionError("Found mismatched nodes")
    }
  }
}

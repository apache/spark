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

import org.apache.spark.mllib.impl.tree.TreeUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.EnsembleTestHelper
import org.apache.spark.mllib.tree.{RandomForest => OldRandomForest}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD


class RandomForestClassifierSuite extends FunSuite with MLlibTestSparkContext {

  import RandomForestClassifierSuite.compareAPIs

  private var orderedLabeledPoints50_1000: RDD[LabeledPoint] = _
  private var orderedLabeledPoints5_20: RDD[LabeledPoint] = _

  override def beforeAll() {
    super.beforeAll()
    orderedLabeledPoints50_1000 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 50, 1000))
    orderedLabeledPoints5_20 =
      sc.parallelize(EnsembleTestHelper.generateOrderedLabeledPoints(numFeatures = 5, 20))
  }

  /////////////////////////////////////////////////////////////////////////////
  // Tests calling train()
  /////////////////////////////////////////////////////////////////////////////

  def binaryClassificationTestWithContinuousFeatures(rf: RandomForestClassifier) {
    val rdd = orderedLabeledPoints50_1000
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2
    val newRF = rf
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setNumTrees(1)
      .setFeaturesPerNode("auto")
      .setSeed(123)
    compareAPIs(orderedLabeledPoints50_1000, newRF, categoricalFeatures, numClasses)
  }

  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
    binaryClassificationTestWithContinuousFeatures(rf)
  }

  test("Binary classification with continuous features and node Id cache:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
      .setCacheNodeIds(cacheNodeIds = true)
    binaryClassificationTestWithContinuousFeatures(rf)
  }

  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0))
    arr(1) = new LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0))
    arr(2) = new LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0))
    arr(3) = new LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
    val rdd = sc.parallelize(arr)
    val categoricalFeatures = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val numClasses = 3

    val rf = new RandomForestClassifier()
      .setImpurity("Gini")
      .setMaxDepth(5)
      .setNumTrees(2)
      .setFeaturesPerNode("sqrt")
      .setSeed(12345)
    compareAPIs(rdd, rf, categoricalFeatures, numClasses)
  }

  test("subsampling rate in RandomForest"){
    val rdd = orderedLabeledPoints5_20
    val categoricalFeatures = Map.empty[Int, Int]
    val numClasses = 2

    val rf1 = new RandomForestClassifier()
      .setImpurity("Gini")
      .setMaxDepth(2)
      .setCacheNodeIds(cacheNodeIds = true)
      .setNumTrees(3)
      .setFeaturesPerNode("auto")
      .setSeed(123)
    compareAPIs(rdd, rf1, categoricalFeatures, numClasses)

    val rf2 = rf1.setSubsamplingRate(0.5)
    compareAPIs(rdd, rf2, categoricalFeatures, numClasses)
  }

  // TODO
  // test("model save/load") {
}

private object RandomForestClassifierSuite extends FunSuite {

  /**
   * Train 2 models on the given dataset, one using the old API and one using the new API.
   * Convert the old model to the new format, compare them, and fail if they are not exactly equal.
   */
  def compareAPIs(
      data: RDD[LabeledPoint],
      rf: RandomForestClassifier,
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): Unit = {
    val oldStrategy = rf.getOldStrategy(categoricalFeatures, numClasses)
    val oldModel = OldRandomForest.trainClassifier(
      data, oldStrategy, rf.getNumTrees, rf.getFeaturesPerNodeStr, rf.getSeed.toInt)
    val newModel = rf.run(data, categoricalFeatures, numClasses)
    val oldModelAsNew = RandomForestClassificationModel.fromOld(oldModel)
    TreeUtils.checkEqual(oldModelAsNew, newModel)
  }
}

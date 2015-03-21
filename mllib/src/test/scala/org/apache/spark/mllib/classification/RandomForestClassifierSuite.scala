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

import org.apache.spark.mllib.classification.tree.ClassificationImpurity.Gini
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.EnsembleTestHelper
import org.apache.spark.mllib.tree.{RandomForest => OldRandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.rdd.RDD


class RandomForestClassifierSuite extends FunSuite with MLlibTestSparkContext {

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
    val newModel = rf
      .setImpurity(Gini)
      .setMaxDepth(2)
      .setNumTrees(1)
      .setFeatureSubsetStrategy("auto")
      .setSeed(123)
      .run(rdd, categoricalFeatures, numClasses)
    val oldStrategy = rf.getOldStrategy(categoricalFeatures, numClasses)
    val oldModel = OldRandomForest.trainClassifier(rdd, oldStrategy, numTrees = 1, "auto", 123)
    RandomForestClassifierSuite.checkEqual(newModel, oldModel)
  }

  test("Binary classification with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
    binaryClassificationTestWithContinuousFeatures(rf)
  }

  test("Binary classification with continuous features and node Id cache:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {
    val rf = new RandomForestClassifier()
      .setCacheNodeIds(true)
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
      .setImpurity(Gini)
      .setMaxDepth(5)
      .setNumTrees(2)
      .setFeatureSubsetStrategy("sqrt")
      .setSeed(12345)

    val newModel = rf.run(rdd, categoricalFeatures, numClasses)
    val oldStrategy = rf.getOldStrategy(categoricalFeatures, numClasses)
    val oldModel = OldRandomForest.trainClassifier(rdd, oldStrategy, numTrees = 2, "sqrt", 12345)
    RandomForestClassifierSuite.checkEqual(newModel, oldModel)
  }

  test("subsampling rate in RandomForest"){
    val rdd = orderedLabeledPoints5_20
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numClasses = 2

    val rf1 = new RandomForestClassifier()
      .setImpurity(Gini)
      .setMaxDepth(2)
      .setCacheNodeIds(true)
      .setNumTrees(3)
      .setFeatureSubsetStrategy("auto")
      .setSeed(123)
    val model1 = rf1.run(rdd, categoricalFeaturesInfo, numClasses)
    val oldStrategy1 = rf1.getOldStrategy(categoricalFeaturesInfo, numClasses)
    val oldModel1 = OldRandomForest.trainClassifier(rdd, oldStrategy1, numTrees = 3, "auto", 123)
    RandomForestClassifierSuite.checkEqual(model1, oldModel1)

    val rf2 = rf1.setSubsamplingRate(0.5)
    val model2 = rf2.run(rdd, categoricalFeaturesInfo, numClasses)
    val oldStrategy2 = rf2.getOldStrategy(categoricalFeaturesInfo, numClasses)
    val oldModel2 = OldRandomForest.trainClassifier(rdd, oldStrategy2, numTrees = 3, "auto", 123)
    RandomForestClassifierSuite.checkEqual(model2, oldModel2)
  }

  // TODO
  // test("model save/load") {
}

private object RandomForestClassifierSuite extends FunSuite {

  /** Check to ensure the two models are exactly equal.  Fail if not equal. */
  def checkEqual(
      newModel: RandomForestClassificationModel,
      oldModel: OldRandomForestModel): Unit = {
    assert(oldModel.algo === OldAlgo.Classification)
    newModel.trees.zip(oldModel.trees).foreach { case (newTree, oldTree) =>
      val oldTreeAsNew = DecisionTreeClassificationModel.fromOld(oldTree)
      DecisionTreeClassifierSuite.checkEqual(newTree, oldTreeAsNew)
    }
    // Do not check treeWeights since they are all 1.0 for RandomForest.
  }
}

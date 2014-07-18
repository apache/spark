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

import org.scalatest.FunSuite

import org.apache.spark.mllib.tree.impurity.{Entropy, Gini, Variance}
import org.apache.spark.mllib.tree.model.Filter
import org.apache.spark.mllib.tree.model.Split
import org.apache.spark.mllib.tree.configuration.{FeatureType, Strategy}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.regression.LabeledPoint

class DecisionTreeSuite extends FunSuite with LocalSparkContext {

  test("split and bin calculation") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, 3, 2, 100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    assert(splits.length === 2)
    assert(bins.length === 2)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)
  }

  test("split and bin calculation for categorical variables") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 3,
      numClassesForClassification = 2,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 2, 1-> 2))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    assert(splits.length === 2)
    assert(bins.length === 2)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)

    // Check splits.

    assert(splits(0)(0).feature === 0)
    assert(splits(0)(0).threshold === Double.MinValue)
    assert(splits(0)(0).featureType === Categorical)
    assert(splits(0)(0).categories.length === 1)
    assert(splits(0)(0).categories.contains(1.0))

    assert(splits(0)(1).feature === 0)
    assert(splits(0)(1).threshold === Double.MinValue)
    assert(splits(0)(1).featureType === Categorical)
    assert(splits(0)(1).categories.length === 2)
    assert(splits(0)(1).categories.contains(1.0))
    assert(splits(0)(1).categories.contains(0.0))

    assert(splits(0)(2) === null)

    assert(splits(1)(0).feature === 1)
    assert(splits(1)(0).threshold === Double.MinValue)
    assert(splits(1)(0).featureType === Categorical)
    assert(splits(1)(0).categories.length === 1)
    assert(splits(1)(0).categories.contains(0.0))

    assert(splits(1)(1).feature === 1)
    assert(splits(1)(1).threshold === Double.MinValue)
    assert(splits(1)(1).featureType === Categorical)
    assert(splits(1)(1).categories.length === 2)
    assert(splits(1)(1).categories.contains(1.0))
    assert(splits(1)(1).categories.contains(0.0))

    assert(splits(1)(2) === null)

    // Check bins.

    assert(bins(0)(0).category === 1.0)
    assert(bins(0)(0).lowSplit.categories.length === 0)
    assert(bins(0)(0).highSplit.categories.length === 1)
    assert(bins(0)(0).highSplit.categories.contains(1.0))

    assert(bins(0)(1).category === 0.0)
    assert(bins(0)(1).lowSplit.categories.length === 1)
    assert(bins(0)(1).lowSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.length === 2)
    assert(bins(0)(1).highSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.contains(0.0))

    assert(bins(0)(2) === null)

    assert(bins(1)(0).category === 0.0)
    assert(bins(1)(0).lowSplit.categories.length === 0)
    assert(bins(1)(0).highSplit.categories.length === 1)
    assert(bins(1)(0).highSplit.categories.contains(0.0))

    assert(bins(1)(1).category === 1.0)
    assert(bins(1)(1).lowSplit.categories.length === 1)
    assert(bins(1)(1).lowSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.length === 2)
    assert(bins(1)(1).highSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.contains(1.0))

    assert(bins(1)(2) === null)
  }

  test("split and bin calculations for categorical variables with no sample for one category") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 3,
      numClassesForClassification = 2,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)

    // Check splits.

    assert(splits(0)(0).feature === 0)
    assert(splits(0)(0).threshold === Double.MinValue)
    assert(splits(0)(0).featureType === Categorical)
    assert(splits(0)(0).categories.length === 1)
    assert(splits(0)(0).categories.contains(1.0))

    assert(splits(0)(1).feature === 0)
    assert(splits(0)(1).threshold === Double.MinValue)
    assert(splits(0)(1).featureType === Categorical)
    assert(splits(0)(1).categories.length === 2)
    assert(splits(0)(1).categories.contains(1.0))
    assert(splits(0)(1).categories.contains(0.0))

    assert(splits(0)(2).feature === 0)
    assert(splits(0)(2).threshold === Double.MinValue)
    assert(splits(0)(2).featureType === Categorical)
    assert(splits(0)(2).categories.length === 3)
    assert(splits(0)(2).categories.contains(1.0))
    assert(splits(0)(2).categories.contains(0.0))
    assert(splits(0)(2).categories.contains(2.0))

    assert(splits(0)(3) === null)

    assert(splits(1)(0).feature === 1)
    assert(splits(1)(0).threshold === Double.MinValue)
    assert(splits(1)(0).featureType === Categorical)
    assert(splits(1)(0).categories.length === 1)
    assert(splits(1)(0).categories.contains(0.0))

    assert(splits(1)(1).feature === 1)
    assert(splits(1)(1).threshold === Double.MinValue)
    assert(splits(1)(1).featureType === Categorical)
    assert(splits(1)(1).categories.length === 2)
    assert(splits(1)(1).categories.contains(1.0))
    assert(splits(1)(1).categories.contains(0.0))

    assert(splits(1)(2).feature === 1)
    assert(splits(1)(2).threshold === Double.MinValue)
    assert(splits(1)(2).featureType === Categorical)
    assert(splits(1)(2).categories.length === 3)
    assert(splits(1)(2).categories.contains(1.0))
    assert(splits(1)(2).categories.contains(0.0))
    assert(splits(1)(2).categories.contains(2.0))

    assert(splits(1)(3) === null)

    // Check bins.

    assert(bins(0)(0).category === 1.0)
    assert(bins(0)(0).lowSplit.categories.length === 0)
    assert(bins(0)(0).highSplit.categories.length === 1)
    assert(bins(0)(0).highSplit.categories.contains(1.0))

    assert(bins(0)(1).category === 0.0)
    assert(bins(0)(1).lowSplit.categories.length === 1)
    assert(bins(0)(1).lowSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.length === 2)
    assert(bins(0)(1).highSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.contains(0.0))

    assert(bins(0)(2).category === 2.0)
    assert(bins(0)(2).lowSplit.categories.length === 2)
    assert(bins(0)(2).lowSplit.categories.contains(1.0))
    assert(bins(0)(2).lowSplit.categories.contains(0.0))
    assert(bins(0)(2).highSplit.categories.length === 3)
    assert(bins(0)(2).highSplit.categories.contains(1.0))
    assert(bins(0)(2).highSplit.categories.contains(0.0))
    assert(bins(0)(2).highSplit.categories.contains(2.0))

    assert(bins(0)(3) === null)

    assert(bins(1)(0).category === 0.0)
    assert(bins(1)(0).lowSplit.categories.length === 0)
    assert(bins(1)(0).highSplit.categories.length === 1)
    assert(bins(1)(0).highSplit.categories.contains(0.0))

    assert(bins(1)(1).category === 1.0)
    assert(bins(1)(1).lowSplit.categories.length === 1)
    assert(bins(1)(1).lowSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.length === 2)
    assert(bins(1)(1).highSplit.categories.contains(0.0))
    assert(bins(1)(1).highSplit.categories.contains(1.0))

    assert(bins(1)(2).category === 2.0)
    assert(bins(1)(2).lowSplit.categories.length === 2)
    assert(bins(1)(2).lowSplit.categories.contains(0.0))
    assert(bins(1)(2).lowSplit.categories.contains(1.0))
    assert(bins(1)(2).highSplit.categories.length === 3)
    assert(bins(1)(2).highSplit.categories.contains(0.0))
    assert(bins(1)(2).highSplit.categories.contains(1.0))
    assert(bins(1)(2).highSplit.categories.contains(2.0))

    assert(bins(1)(3) === null)
  }

  test("extract categories from a number for multiclass classification") {
    val l = DecisionTree.extractMultiClassCategories(13, 10)
    assert(l.length === 3)
    assert(List(3.0, 2.0, 0.0).toSeq == l.toSeq)
  }

  test("split and bin calculations for unordered categorical variables with multiclass " +
    "classification") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 3,
      numClassesForClassification = 100,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)

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

    assert(splits(0)(3) === null)
    assert(splits(1)(3) === null)


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

    assert(bins(0)(3) === null)
    assert(bins(1)(3) === null)

  }

  test("split and bin calculations for ordered categorical variables with multiclass " +
    "classification") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    assert(arr.length === 3000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      maxDepth = 3,
      numClassesForClassification = 100,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 10, 1-> 10))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)

    // 2^10 - 1 > 100, so categorical variables will be ordered

    assert(splits(0)(0).feature === 0)
    assert(splits(0)(0).threshold === Double.MinValue)
    assert(splits(0)(0).featureType === Categorical)
    assert(splits(0)(0).categories.length === 1)
    assert(splits(0)(0).categories.contains(1.0))

    assert(splits(0)(1).feature === 0)
    assert(splits(0)(1).threshold === Double.MinValue)
    assert(splits(0)(1).featureType === Categorical)
    assert(splits(0)(1).categories.length === 2)
    assert(splits(0)(1).categories.contains(2.0))

    assert(splits(0)(2).feature === 0)
    assert(splits(0)(2).threshold === Double.MinValue)
    assert(splits(0)(2).featureType === Categorical)
    assert(splits(0)(2).categories.length === 3)
    assert(splits(0)(2).categories.contains(2.0))
    assert(splits(0)(2).categories.contains(1.0))

    assert(splits(0)(10) === null)
    assert(splits(1)(10) === null)


    // Check bins.

    assert(bins(0)(0).category === 1.0)
    assert(bins(0)(0).lowSplit.categories.length === 0)
    assert(bins(0)(0).highSplit.categories.length === 1)
    assert(bins(0)(0).highSplit.categories.contains(1.0))
    assert(bins(0)(1).category === 2.0)
    assert(bins(0)(1).lowSplit.categories.length === 1)
    assert(bins(0)(1).highSplit.categories.length === 2)
    assert(bins(0)(1).highSplit.categories.contains(1.0))
    assert(bins(0)(1).highSplit.categories.contains(2.0))

    assert(bins(0)(10) === null)

  }


  test("classification stump with all categorical variables") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Classification,
      Gini,
      numClassesForClassification = 2,
      maxDepth = 3,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    val bestSplits = DecisionTree.findBestSplits(rdd, new Array(7), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)

    val split = bestSplits(0)._1
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = bestSplits(0)._2
    assert(stats.gain > 0)
    assert(stats.predict === 1)
    assert(stats.prob == 0.6)
    assert(stats.impurity > 0.2)
  }

  test("regression stump with all categorical variables") {
    val arr = DecisionTreeSuite.generateCategoricalDataPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(
      Regression,
      Variance,
      maxDepth = 3,
      maxBins = 100,
      categoricalFeaturesInfo = Map(0 -> 3, 1-> 3))
    val (splits, bins) = DecisionTree.findSplitsBins(rdd,strategy)
    val bestSplits = DecisionTree.findBestSplits(rdd, new Array(7), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)

    val split = bestSplits(0)._1
    assert(split.categories.length === 1)
    assert(split.categories.contains(1.0))
    assert(split.featureType === Categorical)
    assert(split.threshold === Double.MinValue)

    val stats = bestSplits(0)._2
    assert(stats.gain > 0)
    assert(stats.predict == 0.6)
    assert(stats.impurity > 0.2)
  }

  test("stump with fixed label 0 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, 3, 2, 100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)

    val bestSplits = DecisionTree.findBestSplits(rdd, new Array(7), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._1.threshold === 10)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
  }

  test("stump with fixed label 1 for Gini") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Gini, 3, 2, 100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)

    val bestSplits = DecisionTree.findBestSplits(rdd, Array(0.0), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._1.threshold === 10)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
    assert(bestSplits(0)._2.predict === 1)
  }

  test("stump with fixed label 0 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel0()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, 3, 2, 100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)

    val bestSplits = DecisionTree.findBestSplits(rdd, Array(0.0), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._1.threshold === 10)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
    assert(bestSplits(0)._2.predict === 0)
  }

  test("stump with fixed label 1 for Entropy") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPointsWithLabel1()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, 3, 2, 100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)

    val bestSplits = DecisionTree.findBestSplits(rdd, Array(0.0), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)
    assert(bestSplits.length === 1)
    assert(bestSplits(0)._1.feature === 0)
    assert(bestSplits(0)._1.threshold === 10)
    assert(bestSplits(0)._2.gain === 0)
    assert(bestSplits(0)._2.leftImpurity === 0)
    assert(bestSplits(0)._2.rightImpurity === 0)
    assert(bestSplits(0)._2.predict === 1)
  }

  test("second level node building with/without groups") {
    val arr = DecisionTreeSuite.generateOrderedLabeledPoints()
    assert(arr.length === 1000)
    val rdd = sc.parallelize(arr)
    val strategy = new Strategy(Classification, Entropy, 3, 2, 100)
    val (splits, bins) = DecisionTree.findSplitsBins(rdd, strategy)
    assert(splits.length === 2)
    assert(splits(0).length === 99)
    assert(bins.length === 2)
    assert(bins(0).length === 100)
    assert(splits(0).length === 99)
    assert(bins(0).length === 100)

    val leftFilter = Filter(new Split(0, 400, FeatureType.Continuous, List()), -1)
    val rightFilter = Filter(new Split(0, 400, FeatureType.Continuous, List()) ,1)
    val filters = Array[List[Filter]](List(), List(leftFilter), List(rightFilter))
    val parentImpurities = Array(0.5, 0.5, 0.5)

    // Single group second level tree construction.
    val bestSplits = DecisionTree.findBestSplits(rdd, parentImpurities, strategy, 1, filters,
      splits, bins, 10)
    assert(bestSplits.length === 2)
    assert(bestSplits(0)._2.gain > 0)
    assert(bestSplits(1)._2.gain > 0)

    // maxLevelForSingleGroup parameter is set to 0 to force splitting into groups for second
    // level tree construction.
    val bestSplitsWithGroups = DecisionTree.findBestSplits(rdd, parentImpurities, strategy, 1,
      filters, splits, bins, 0)
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

  test("stump with categorical variables for multiclass classification") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlass()
    val input = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClassesForClassification = 3, categoricalFeaturesInfo = Map(0 -> 3, 1 -> 3))
    assert(strategy.isMulticlassClassification)
    val (splits, bins) = DecisionTree.findSplitsBins(input, strategy)
    val bestSplits = DecisionTree.findBestSplits(input, new Array(31), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1
    assert(bestSplit.feature === 0)
    assert(bestSplit.categories.length === 1)
    assert(bestSplit.categories.contains(1))
    assert(bestSplit.featureType === Categorical)
  }

  test("stump with continuous variables for multiclass classification") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val input = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClassesForClassification = 3)
    assert(strategy.isMulticlassClassification)
    val (splits, bins) = DecisionTree.findSplitsBins(input, strategy)
    val bestSplits = DecisionTree.findBestSplits(input, new Array(31), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1

    assert(bestSplit.feature === 1)
    assert(bestSplit.featureType === Continuous)
    assert(bestSplit.threshold > 1980)
    assert(bestSplit.threshold < 2020)

  }

  test("stump with continuous + categorical variables for multiclass classification") {
    val arr = DecisionTreeSuite.generateContinuousDataPointsForMulticlass()
    val input = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClassesForClassification = 3, categoricalFeaturesInfo = Map(0 -> 3))
    assert(strategy.isMulticlassClassification)
    val (splits, bins) = DecisionTree.findSplitsBins(input, strategy)
    val bestSplits = DecisionTree.findBestSplits(input, new Array(31), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1

    assert(bestSplit.feature === 1)
    assert(bestSplit.featureType === Continuous)
    assert(bestSplit.threshold > 1980)
    assert(bestSplit.threshold < 2020)
  }

  test("stump with categorical variables for ordered multiclass classification") {
    val arr = DecisionTreeSuite.generateCategoricalDataPointsForMulticlassForOrderedFeatures()
    val input = sc.parallelize(arr)
    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClassesForClassification = 3, categoricalFeaturesInfo = Map(0 -> 10, 1 -> 10))
    assert(strategy.isMulticlassClassification)
    val (splits, bins) = DecisionTree.findSplitsBins(input, strategy)
    val bestSplits = DecisionTree.findBestSplits(input, new Array(31), strategy, 0,
      Array[List[Filter]](), splits, bins, 10)

    assert(bestSplits.length === 1)
    val bestSplit = bestSplits(0)._1
    assert(bestSplit.feature === 0)
    assert(bestSplit.categories.length === 1)
    assert(bestSplit.categories.contains(1.0))
    assert(bestSplit.featureType === Categorical)
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

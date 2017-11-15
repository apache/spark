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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.tree.{CategoricalSplit, ContinuousSplit, Split}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.tree.impurity.{Entropy, Impurity}
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.mllib.util.MLlibTestSparkContext

/** Suite exercising helper methods for making split decisions during decision tree training. */
class TreeSplitUtilsSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  /**
   * Get a DTStatsAggregator for sufficient stat collection/impurity calculation populated
   * with the data from the specified training points. Assumes a feature index of 0 and that
   * all training points have the same weights (1.0).
   */
  private def getAggregator(
      metadata: DecisionTreeMetadata,
      values: Array[Int],
      labels: Array[Double],
      featureSplits: Array[Split]): DTStatsAggregator = {
    // Create stats aggregator
    val statsAggregator = new DTStatsAggregator(metadata, featureSubset = None)
    // Update parent impurity stats
    val featureIndex = 0
    val instanceWeights = Array.fill[Double](values.length)(1.0)
    AggUpdateUtils.updateParentImpurity(statsAggregator, indices = values.indices.toArray,
      from = 0, to = values.length, instanceWeights, labels)
    // Update current aggregator's impurity stats
    values.zip(labels).foreach { case (value: Int, label: Double) =>
      if (metadata.isUnordered(featureIndex)) {
        AggUpdateUtils.updateUnorderedFeature(statsAggregator, value, label,
          featureIndex = featureIndex, featureIndexIdx = 0, featureSplits, instanceWeight = 1.0)
      } else {
        AggUpdateUtils.updateOrderedFeature(statsAggregator, value, label, featureIndexIdx = 0,
          instanceWeight = 1.0)
      }
    }
    statsAggregator
  }

  /**
   * Check that left/right impurities match what we'd expect for a split.
   * @param labels Labels whose impurity information should be reflected in stats
   * @param stats ImpurityStats object containing impurity info for the left/right sides of a split
   */
  private def validateImpurityStats(
      impurity: Impurity,
      labels: Array[Double],
      stats: ImpurityStats,
      expectedLeftStats: Array[Double],
      expectedRightStats: Array[Double]): Unit = {
    // Compute impurity for our data points manually
    val numClasses = (labels.max + 1).toInt
    val fullImpurityStatsArray
      = Array.tabulate[Double](numClasses)((label: Int) => labels.count(_ == label).toDouble)
    val fullImpurity = Entropy.calculate(fullImpurityStatsArray, labels.length)
    // Verify that impurity stats were computed correctly for split
    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
    assert(stats.impurity === fullImpurity)
    assert(stats.leftImpurityCalculator.stats === expectedLeftStats)
    assert(stats.rightImpurityCalculator.stats === expectedRightStats)
    assert(stats.valid)
  }

  /* * * * * * * * * * * Choosing Splits  * * * * * * * * * * */

  test("chooseSplit: choose correct type of split (continuous split)") {
    // Construct (binned) continuous data
    val labels = Array(0.0, 0.0, 1.0)
    val values = Array(1, 2, 3)
    val featureIndex = 0
    // Get an array of continuous splits corresponding to values in our binned data
    val splits = TreeTests.getContinuousSplits(thresholds = values.distinct.sorted,
      featureIndex = 0)
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map.empty)
    val statsAggregator = getAggregator(metadata, values, labels, splits)
    // Choose split, check that it's a valid ContinuousSplit
    val (split, stats) = SplitUtils.chooseSplit(statsAggregator, featureIndex, featureIndex,
      splits)
    assert(stats.valid && split.isInstanceOf[ContinuousSplit])
  }

  test("chooseSplit: choose correct type of split (categorical split)") {
    // Construct categorical data
    val labels = Array(0.0, 0.0, 1.0, 1.0, 1.0)
    val featureArity = 3
    val values = Array(0, 0, 1, 2, 2)
    val featureIndex = 0
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map(featureIndex -> featureArity))
    val splits = RandomForest.findUnorderedSplits(metadata, featureIndex)
    val statsAggregator = getAggregator(metadata, values, labels, splits)
    // Choose split, check that it's a valid categorical split
    val (split, stats) = SplitUtils.chooseSplit(statsAggregator = statsAggregator,
      featureIndex = featureIndex, featureIndexIdx = featureIndex, featureSplits = splits)
    assert(stats.valid && split.isInstanceOf[CategoricalSplit])
  }

  test("chooseOrderedCategoricalSplit: basic case") {
    // Helper method for testing ordered categorical split
    def testHelper(
        values: Array[Int],
        labels: Array[Double],
        expectedLeftCategories: Array[Double],
        expectedLeftStats: Array[Double],
        expectedRightStats: Array[Double]): Unit = {
      // Set up metadata for ordered categorical feature
      val featureIndex = 0
      val featureArity = values.max + 1
      val arityMap = Map[Int, Int](featureIndex -> featureArity)
      val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
        numClasses = 2, arityMap, unorderedFeatures = Some(Set.empty))
      // Construct DTStatsAggregator, compute sufficient stats
      val statsAggregator = getAggregator(metadata, values, labels, featureSplits = Array.empty)
      // Choose split
      val (split, stats) =
        SplitUtils.chooseOrderedCategoricalSplit(statsAggregator, featureIndex, featureIndex)
      // Verify that split has the expected left-side/right-side categories
      val expectedRightCategories = Range(0, featureArity)
        .filter(c => !expectedLeftCategories.contains(c)).map(_.toDouble).toArray
      split match {
        case s: CategoricalSplit =>
          assert(s.featureIndex === featureIndex)
          assert(s.leftCategories === expectedLeftCategories)
          assert(s.rightCategories === expectedRightCategories)
        case _ =>
          throw new AssertionError(
            s"Expected CategoricalSplit but got ${split.getClass.getSimpleName}")
      }
      validateImpurityStats(Entropy, labels, stats, expectedLeftStats, expectedRightStats)
    }

    // Test a single split: The left side of our split should contain the two points with label 0,
    // the left side of our split should contain the five points with label 1
    val values = Array(0, 0, 1, 2, 2, 2, 2)
    val labels1 = Array(0, 0, 1, 1, 1, 1, 1).map(_.toDouble)
    testHelper(values, labels1, expectedLeftCategories = Array(0.0),
      expectedLeftStats = Array(2.0, 0.0), expectedRightStats = Array(0.0, 5.0))

    // Test a single split: The left side of our split should contain the three points with label 0,
    // the left side of our split should contain the four points with label 1
    val labels2 = Array(0, 0, 0, 1, 1, 1, 1).map(_.toDouble)
    testHelper(values, labels2, expectedLeftCategories = Array(0.0, 1.0),
      expectedLeftStats = Array(3.0, 0.0), expectedRightStats = Array(0.0, 4.0))
  }

  test("chooseOrderedCategoricalSplit: return bad stats if we should not split") {
    // Construct categorical data
    val featureIndex = 0
    val values = Array(0, 0, 1, 2, 2, 2, 2)
    val featureArity = values.max + 1
    val labels = Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map(featureIndex -> featureArity), unorderedFeatures = Some(Set.empty))
    val statsAggregator = getAggregator(metadata, values, labels, featureSplits = Array.empty)
    // Choose split, verify that it's invalid
    val (_, stats) = SplitUtils.chooseOrderedCategoricalSplit(statsAggregator, featureIndex,
        featureIndex)
    assert(!stats.valid)
  }

  test("chooseUnorderedCategoricalSplit: basic case") {
    val featureIndex = 0
    // Construct data for unordered categorical feature
    // label: 0 --> values: 1
    // label: 1 --> values: 0, 2
    // label: 2 --> values: 2
    // Expected split: feature value 1 on the left, values (0, 2) on the right
    val values = Array(1, 1, 0, 2, 2)
    val featureArity = values.max + 1
    val labels = Array(0.0, 0.0, 1.0, 1.0, 2.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 3, Map(featureIndex -> featureArity))
    val splits = RandomForest.findUnorderedSplits(metadata, featureIndex)
    val statsAggregator = getAggregator(metadata, values, labels, splits)
    // Choose split
    val (split, stats) =
      SplitUtils.chooseUnorderedCategoricalSplit(statsAggregator, featureIndex, featureIndex,
        splits)
    // Verify that split has the expected left-side/right-side categories
    split match {
      case s: CategoricalSplit =>
        assert(s.featureIndex === featureIndex)
        assert(s.leftCategories.toSet === Set(1.0))
        assert(s.rightCategories.toSet === Set(0.0, 2.0))
      case _ =>
        throw new AssertionError(
          s"Expected CategoricalSplit but got ${split.getClass.getSimpleName}")
    }
    validateImpurityStats(Entropy, labels, stats, expectedLeftStats = Array(2.0, 0.0, 0.0),
      expectedRightStats = Array(0.0, 2.0, 1.0))
  }

  test("chooseUnorderedCategoricalSplit: return bad stats if we should not split") {
    // Construct data for unordered categorical feature; all points have label 1
    val featureIndex = 0
    val featureArity = 4
    val values = Array(3, 1, 0, 2, 2)
    val labels = Array(1.0, 1.0, 1.0, 1.0, 1.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map(featureIndex -> featureArity))
    val splits = RandomForest.findUnorderedSplits(metadata, featureIndex)
    val statsAggregator = getAggregator(metadata, values, labels, splits)
    // Choose split, verify that it's invalid
    val (_, stats) = SplitUtils.chooseUnorderedCategoricalSplit(statsAggregator, featureIndex,
      featureIndex, splits)
    assert(!stats.valid)
  }

  test("chooseContinuousSplit: basic case") {
    // Construct data for continuous feature
    val featureIndex = 0
    val thresholds = Array(0, 1, 2, 3)
    val values = thresholds.indices.toArray
    val labels = Array(0.0, 0.0, 1.0, 1.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val splits = TreeTests.getContinuousSplits(thresholds, featureIndex)
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map.empty)
    val statsAggregator = getAggregator(metadata, values, labels, splits)

    // Choose split, verify that it has expected threshold
    val (split, stats) = SplitUtils.chooseContinuousSplit(statsAggregator, featureIndex,
      featureIndex, splits)
    split match {
      case s: ContinuousSplit =>
        assert(s.featureIndex === featureIndex)
        assert(s.threshold === 1)
      case _ =>
        throw new AssertionError(
          s"Expected ContinuousSplit but got ${split.getClass.getSimpleName}")
    }
    // Verify impurity stats of split
    validateImpurityStats(Entropy, labels, stats, expectedLeftStats = Array(2.0, 0.0),
      expectedRightStats = Array(0.0, 2.0))
  }

  test("chooseContinuousSplit: return bad stats if we should not split") {
    // Construct data for continuous feature; all points have label 0
    val featureIndex = 0
    val thresholds = Array(0, 1, 2, 3)
    val values = thresholds.indices.toArray
    val labels = Array(0.0, 0.0, 0.0, 0.0, 0.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val splits = TreeTests.getContinuousSplits(thresholds, featureIndex)
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map.empty[Int, Int])
    val statsAggregator = getAggregator(metadata, values, labels, splits)
    // Choose split, verify that it's invalid
    val (_, stats) = SplitUtils.chooseContinuousSplit(statsAggregator, featureIndex,
      featureIndex, splits)
    assert(!stats.valid)
  }
}

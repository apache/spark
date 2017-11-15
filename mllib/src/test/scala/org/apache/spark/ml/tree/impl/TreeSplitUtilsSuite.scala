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
   * Iterate over feature values and labels for a specific (node, feature), updating stats
   * aggregator for the current node.
   */
  private[impl] def updateAggregator(
      statsAggregator: DTStatsAggregator,
      featureIndex: Int,
      values: Array[Int],
      indices: Array[Int],
      instanceWeights: Array[Double],
      labels: Array[Double],
      from: Int,
      to: Int,
      featureIndexIdx: Int,
      featureSplits: Array[Split]): Unit = {
    val metadata = statsAggregator.metadata
    from.until(to).foreach { idx =>
      val rowIndex = indices(idx)
      if (metadata.isUnordered(featureIndex)) {
        AggUpdateUtils.updateUnorderedFeature(statsAggregator, values(idx), labels(rowIndex),
          featureIndex = featureIndex, featureIndexIdx, featureSplits,
          instanceWeight = instanceWeights(rowIndex))
      } else {
        AggUpdateUtils.updateOrderedFeature(statsAggregator, values(idx), labels(rowIndex),
          featureIndexIdx, instanceWeight = instanceWeights(rowIndex))
      }
    }
  }

  /**
   * Get a DTStatsAggregator for sufficient stat collection/impurity calculation populated
   * with the data from the specified training points.
   */
  private def getAggregator(
      metadata: DecisionTreeMetadata,
      values: Array[Int],
      from: Int,
      to: Int,
      labels: Array[Double],
      featureSplits: Array[Split]): DTStatsAggregator = {

    val featureIndex = 0
    val statsAggregator = new DTStatsAggregator(metadata, featureSubset = None)
    val instanceWeights = Array.fill[Double](values.length)(1.0)
    val indices = values.indices.toArray
    AggUpdateUtils.updateParentImpurity(statsAggregator, indices, from, to, instanceWeights, labels)
    updateAggregator(statsAggregator, featureIndex = 0, values, indices, instanceWeights, labels,
      from, to, featureIndex, featureSplits)
    statsAggregator
  }

  /** Check that left/right impurities match what we'd expect for a split. */
  private def validateImpurityStats(
      impurity: Impurity,
      labels: Array[Double],
      stats: ImpurityStats,
      expectedLeftStats: Array[Double],
      expectedRightStats: Array[Double]): Unit = {
    // Verify that impurity stats were computed correctly for split
    val numClasses = (labels.max + 1).toInt
    val fullImpurityStatsArray
      = Array.tabulate[Double](numClasses)((label: Int) => labels.count(_ == label).toDouble)
    val fullImpurity = Entropy.calculate(fullImpurityStatsArray, labels.length)
    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
    assert(stats.impurity === fullImpurity)
    assert(stats.leftImpurityCalculator.stats === expectedLeftStats)
    assert(stats.rightImpurityCalculator.stats === expectedRightStats)
    assert(stats.valid)
  }

  /* * * * * * * * * * * Choosing Splits  * * * * * * * * * * */

  test("chooseSplit: choose correct type of split (continuous split)") {
    // Construct (binned) continuous data
    val labels = Array(0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0)
    val values = Array(8, 1, 1, 2, 3, 5, 6)
    val featureIndex = 0
    // Get an array of continuous splits corresponding to values in our binned data
    val splits = TreeTests.getContinuousSplits(1.to(8).toArray, featureIndex = 0)
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = 7,
      numFeatures = 1, numClasses = 2, Map.empty)
    val statsAggregator = getAggregator(metadata, values, from = 1, to = 4, labels, splits)
    // Choose split, check that it's a valid ContinuousSplit
    val (split1, stats1) = SplitUtils.chooseSplit(statsAggregator, featureIndex, featureIndex,
      splits)
    assert(stats1.valid && split1.isInstanceOf[ContinuousSplit])
  }

  test("chooseSplit: choose correct type of split (categorical split)") {
    // Construct categorical data
    val labels = Array(0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0)
    val featureArity = 3
    val values = Array(0, 0, 1, 1, 1, 2, 2)
    val featureIndex = 0
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = 7,
      numFeatures = 1, numClasses = 2, Map(featureIndex -> featureArity))
    val splits = RandomForest.findUnorderedSplits(metadata, featureIndex)
    val statsAggregator = getAggregator(metadata, values, from = 1, to = 4, labels, splits)
    // Choose split, check that it's a valid categorical split
    val (split2, stats2) = SplitUtils.chooseSplit(statsAggregator = statsAggregator,
      featureIndex = featureIndex, featureIndexIdx = featureIndex,
      featureSplits = splits)
    assert(stats2.valid && split2.isInstanceOf[CategoricalSplit])
  }

  test("chooseOrderedCategoricalSplit: basic case") {
    // Helper method for testing ordered categorical split
    def testHelper(
        values: Array[Int],
        labels: Array[Double],
        expectedLeftCategories: Array[Double],
        expectedLeftStats: Array[Double],
        expectedRightStats: Array[Double]): Unit = {
      val featureIndex = 0
      // Construct FeatureVector to store categorical data
      val featureArity = values.max + 1
      val arityMap = Map[Int, Int](featureIndex -> featureArity)
      // Construct DTStatsAggregator, compute sufficient stats
      val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
        numClasses = 2, arityMap, unorderedFeatures = Some(Set.empty))
      val statsAggregator = getAggregator(metadata, values, from = 0, to = values.length,
        labels, featureSplits = Array.empty)
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

    val values = Array(0, 0, 1, 2, 2, 2, 2)
    val labels1 = Array(0, 0, 1, 1, 1, 1, 1).map(_.toDouble)
    testHelper(values, labels1, Array(0.0), Array(2.0, 0.0), Array(0.0, 5.0))

    val labels2 = Array(0, 0, 0, 1, 1, 1, 1).map(_.toDouble)
    testHelper(values, labels2, Array(0.0, 1.0), Array(3.0, 0.0), Array(0.0, 4.0))
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
    val statsAggregator = getAggregator(metadata, values, from = 0, to = values.length,
      labels, featureSplits = Array.empty)
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
    val values = Array(1, 1, 0, 2, 2)
    val featureArity = values.max + 1
    val labels = Array(0.0, 0.0, 1.0, 1.0, 2.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 3, Map(featureIndex -> featureArity))
    val splits = RandomForest.findUnorderedSplits(metadata, featureIndex)
    val statsAggregator = getAggregator(metadata, values, from = 0, to = values.length,
      labels, splits)
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
    // Construct data for unordered categorical feature
    val featureIndex = 0
    val featureArity = 4
    val values = Array(3, 1, 0, 2, 2)
    val labels = Array(1.0, 1.0, 1.0, 1.0, 1.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map(featureIndex -> featureArity))
    val splits = RandomForest.findUnorderedSplits(metadata, featureIndex)
    val statsAggregator = getAggregator(metadata, values, from = 0, to = values.length, labels,
      splits)
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
    val statsAggregator = getAggregator(metadata, values, from = 0, to = values.length, labels,
      splits)

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
    // Construct data for continuous feature
    val featureIndex = 0
    val thresholds = Array(0, 1, 2, 3)
    val values = thresholds.indices.toArray
    val labels = Array(0.0, 0.0, 0.0, 0.0, 0.0)
    // Construct DTStatsAggregator, compute sufficient stats
    val splits = TreeTests.getContinuousSplits(thresholds, featureIndex)
    val metadata = TreeTests.getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map.empty[Int, Int])
    val statsAggregator = getAggregator(metadata, values, from = 0, to = values.length, labels,
      splits)
    // Choose split, verify that it's invalid
    val (split, stats) = SplitUtils.chooseContinuousSplit(statsAggregator, featureIndex,
      featureIndex, splits)
    assert(!stats.valid)
  }
}

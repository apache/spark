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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.tree.{CategoricalSplit, ContinuousSplit, LearningNode, Split}
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.collection.BitSet

/** Unit tests for helper classes/methods specific to local tree training */
class LocalTreeUnitSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  private def getDummyActiveNodes(numNodes: Int): Array[LearningNode] = {
    0.until(numNodes).toArray.map(_ => LearningNode.emptyNode(nodeIndex = -1))
  }

  /** Returns a DecisionTreeMetadata instance with hard-coded values for use in tests */
  private def getMetadata(
      numExamples: Int,
      numFeatures: Int,
      numClasses: Int,
      featureArity: Map[Int, Int]): DecisionTreeMetadata = {
    // Assume all categorical features within tests
    // have small enough arity to be treated as unordered
    val unordered = featureArity.keys.toSet
    val maxBins = 4
    val numBins = 0.until(numFeatures).toArray.map(_ => maxBins)
    new DecisionTreeMetadata(numFeatures = numFeatures, numExamples = numExamples,
      numClasses = numClasses, maxBins = maxBins, minInfoGain = 0.0, featureArity = featureArity,
      unorderedFeatures = unordered, numBins = numBins, impurity = Entropy,
      quantileStrategy = null, maxDepth = 5, minInstancesPerNode = 1, numTrees = 1,
      numFeaturesPerNode = 2)
  }

  private def getUnorderedSplits(
      values: Array[Int],
      featureIndex: Int): Array[CategoricalSplit] = {
    val uniqueVals = values.map(_.toDouble).distinct.toList
    def recurse(vals: List[Double]): List[List[Double]] = {
      vals match {
        case Nil => List(List.empty)
        case a :: rest =>
          val remainder = recurse(rest)
          remainder.map(lst => a :: lst) ++ remainder
      }
    }
    recurse(uniqueVals).toArray.map { leftCats =>
      new CategoricalSplit(featureIndex, leftCats.toArray, uniqueVals.length)
    }
  }

  /**
   * Returns an array containing a single element; an array of continuous splits for
   * the feature with index featureIndex and the passed-in set of values. Creates one
   * continuous split per value in the array.
   */
  private def getContinuousSplits(
      values: Array[Int],
      featureIndex: Int): Array[Array[Split]] = {
    val splits = values.map {
      new ContinuousSplit(featureIndex, _).asInstanceOf[Split]
    }
    Array(splits)
  }

  test("Transform row-major training data into a column-major representation") {
    // Transform an empty training dataset
    {
      val result = LocalDecisionTreeUtils.rowToColumnStoreDense(Array.empty)
      val expected: Array[Array[Int]] = Array.empty
      assert(result.sameElements(expected))
    }

    // Transform a training dataset consisting of a single row
    {
      val rowLength = 10
      val data = Array(0.until(rowLength).toArray)
      val transposed = LocalDecisionTreeUtils.rowToColumnStoreDense(data)
      assert(transposed.length == rowLength,
        s"Transpose of row of length ${rowLength} only contained ${transposed.length} columns")
      transposed.foreach { col =>
        assert(col.length == 1, s"Column had length ${col.length}, expected length: 1")
      }
    }

    // Transform a 2x2 dataset
    {
      val data = Array(Array(0, 1), Array(2, 3))
      val expected = Array(Array(0, 2), Array(1, 3))
      val transposed = LocalDecisionTreeUtils.rowToColumnStoreDense(data)
      transposed.zip(expected).foreach { case (resultCol, expectedCol) =>
        assert(resultCol.sameElements(expectedCol), s"Result column" +
          s"${resultCol.mkString(", ")} differed from expected col ${expectedCol.mkString(", ")}")
      }
    }
  }

  test("Methods for sorting columns work properly") {
    val numEntries = 100

    val tempVals = new Array[Int](numEntries)
    val tempIndices = new Array[Int](numEntries)

    // Create a column of numEntries values
    val values = 0.until(numEntries).toArray
    val col = new FeatureVector(-1, 0, values, values.indices.toArray)

    // TODO(smurching): Test different bounds here?
    val from = 0
    val to = numEntries

    // Pick a random subset of indices to split left
    val rng = new Random(seed = 42)
    val leftProb = 0.5
    val (leftIdxs, rightIdxs) = values.indices.partition(i => rng.nextDouble() < leftProb)

    // Determine our expected result after sorting
    val expected = leftIdxs.map(values(_)) ++ rightIdxs.map(values(_))

    // Create a bitset indicating whether each of our values splits left or right
    val instanceBitVector = new BitSet(values.length)
    rightIdxs.foreach(instanceBitVector.set(_))

    LocalDecisionTreeUtils.sortCol(col, from, to, leftIdxs.length, tempVals,
      tempIndices, instanceBitVector)

    assert(col.values.sameElements(expected))
  }

  /* Check that FeatureVector methods produce expected results */
  test("FeatureVector") {
    // Create a feature vector v, modify a deep copy of v, and check that
    // v itself was not modified
    val v = new FeatureVector(1, 0, Array(1, 2, 3), Array(1, 2, 0))
    val vCopy = v.deepCopy()
    vCopy.values(0) = 1000
    assert(v.values(0) !== vCopy.values(0))

    // Check that fromOriginal properly converts an Array representation
    // of a feature vector to an equivalent FeatureVector
    val original = Array(3, 1, 2)
    val v2 = FeatureVector.fromOriginal(1, 0, original)
    assert(v === v2)
  }

  /*
   * Check that the FeatureVector constructor properly sorts a randomly-shuffled
   * column of values
   */
  test("FeatureVectorSortByValue") {
    val values = Array(1, 2, 4, 6, 7, 9, 15, 155)
    val col = Random.shuffle(values.toIterator).toArray
    val unsortedIndices = col.indices
    val sortedIndices = unsortedIndices.sortBy(x => col(x)).toArray
    val featureIndex = 3
    val featureArity = 0
    val fvSorted =
      FeatureVector.fromOriginal(featureIndex, featureArity, col)
    assert(fvSorted.featureIndex === featureIndex)
    assert(fvSorted.featureArity === featureArity)
    assert(fvSorted.values.deep === values.deep)
    assert(fvSorted.indices.deep === sortedIndices.deep)
  }

  /*
   * Check that PartitionInfo correctly updates node offsets & sorts column values during
   * update()
   */
  test("PartitionInfo") {
    val numRows = 4
    val col1 = FeatureVector.fromOriginal(0, 0, Array(8, 2, 1, 6))
    val col2 =
      FeatureVector.fromOriginal(1, 3, Array(0, 1, 0, 2))
    val labels = Array(0, 0, 0, 1, 1, 1, 1).map(_.toDouble)

    val featureArity = Map[Int, Int](1 -> 3)
    val metadata = getMetadata(numExamples = numRows, numFeatures = 2, numClasses = 2,
      featureArity)
    val fullImpurityAgg = metadata.createImpurityAggregator()
    labels.foreach(label => fullImpurityAgg.update(label))

    assert(col1.values.length === numRows)
    assert(col2.values.length === numRows)

    val nodeOffsets = Array((0, numRows))
    val activeNodes = getDummyActiveNodes(1)

    val info = new PartitionInfo(Array(col1, col2), nodeOffsets, activeNodes)

    // Create bitVector for splitting the 4 rows: L, R, L, R
    // New groups are {0, 2}, {1, 3}
    val bitVector = new BitSet(4)
    bitVector.set(1)
    bitVector.set(3)

    // for these tests, use the activeNodes for nodeSplitBitVector
    val newInfo = info.update(bitVector, getDummyActiveNodes(2), labels, metadata)

    assert(newInfo.columns.length === 2)
    val expectedCol1a =
      new FeatureVector(0, 0, Array(1, 8, 2, 6), Array(2, 0, 1, 3))
    val expectedCol1b =
      new FeatureVector(1, 3, Array(0, 0, 1, 2), Array(0, 2, 1, 3))
    assert(newInfo.columns(0) === expectedCol1a)
    assert(newInfo.columns(1) === expectedCol1b)
    assert(newInfo.nodeOffsets === Array((0, 2), (2, 4)))

    // Create 2 bitVectors for splitting into: 0, 2, 1, 3
    val bitVector2 = new BitSet(4)
    bitVector2.set(2) // 2 goes to the right
    bitVector2.set(3) // 3 goes to the right

    val newInfo2 = newInfo.update(bitVector2, getDummyActiveNodes(4), labels, metadata)

    assert(newInfo2.columns.length === 2)
    val expectedCol2a =
      new FeatureVector(0, 0, Array(8, 1, 2, 6), Array(0, 2, 1, 3))
    val expectedCol2b =
      new FeatureVector(1, 3, Array(0, 0, 1, 2), Array(0, 2, 1, 3))
    assert(newInfo2.columns(0) === expectedCol2a)
    assert(newInfo2.columns(1) === expectedCol2b)
    assert(newInfo2.nodeOffsets === Array((0, 1), (1, 2), (2, 3), (3, 4)))
  }

  /* * * * * * * * * * * Choosing Splits  * * * * * * * * * * */

  test("computeBestSplits") {
    // TODO
  }

  test("chooseSplit: choose correct type of split") {
    val labels = Array(0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0)
    val fromOffset = 1
    val toOffset = 4
    val impurity = Entropy
    val metadata = getMetadata(numExamples = 7, numFeatures = 2, numClasses = 2, Map.empty)
    val splits = getContinuousSplits(1.to(8).toArray, featureIndex = 0)
    val fullImpurityAgg = metadata.createImpurityAggregator()
    labels.foreach(label => fullImpurityAgg.update(label))

    val col1 = FeatureVector.fromOriginal(featureIndex = 0, featureArity = 0,
      values = Array(8, 1, 1, 2, 3, 5, 6))
    val (split1, _) = LocalDecisionTree.chooseSplit(col1, labels,
      fromOffset, toOffset, fullImpurityAgg, metadata, splits)
    assert(split1.nonEmpty && split1.get.isInstanceOf[ContinuousSplit])

    val col2 = FeatureVector.fromOriginal(featureIndex = 1, featureArity = 3,
      values = Array(0, 0, 1, 1, 1, 2, 2))
    val (split2, _) = LocalDecisionTree.chooseSplit(col2, labels, fromOffset,
      toOffset, fullImpurityAgg, metadata, splits)
    assert(split2.nonEmpty && split2.get.isInstanceOf[CategoricalSplit])
  }

  test("chooseOrderedCategoricalSplit: basic case") {
    val featureIndex = 0
    val values = Array(0, 0, 1, 2, 2, 2, 2)
    val featureArity = values.max.toInt + 1
    val arityMap = Map[Int, Int](featureIndex -> featureArity)

    def testHelper(
        labels: Array[Double],
        expectedLeftCategories: Array[Double],
        expectedLeftStats: Array[Double],
        expectedRightStats: Array[Double]): Unit = {

      val expectedRightCategories = Range(0, featureArity)
        .filter(c => !expectedLeftCategories.contains(c)).map(_.toDouble).toArray
      val impurity = Entropy
      val metadata = getMetadata(numExamples = values.length, numFeatures = 3,
        numClasses = 2, arityMap)
      val fullImpurityAgg = metadata.createImpurityAggregator()
      labels.foreach(fullImpurityAgg.update(_))
      val (split, stats) =
        LocalDecisionTree.chooseOrderedCategoricalSplit(featureIndex, values,
          values.indices.toArray,
          labels, 0, values.length, featureArity, metadata, fullImpurityAgg)
      split match {
        case Some(s: CategoricalSplit) =>
          assert(s.featureIndex === featureIndex)
          assert(s.leftCategories === expectedLeftCategories)
          assert(s.rightCategories === expectedRightCategories)
        case _ =>
          throw new AssertionError(
            s"Expected CategoricalSplit but got ${split.getClass.getSimpleName}")
      }
      val fullImpurityStatsArray =
        Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
      val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
      assert(stats.gain === fullImpurity)
      assert(stats.impurity === fullImpurity)
      assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
      assert(stats.leftImpurityCalculator.stats === expectedLeftStats)
      assert(stats.rightImpurityCalculator.stats === expectedRightStats)
      assert(stats.valid)
    }

    val labels1 = Array(0, 0, 1, 1, 1, 1, 1).map(_.toDouble)
    testHelper(labels1, Array(0.0), Array(2.0, 0.0), Array(0.0, 5.0))

    val labels2 = Array(0, 0, 0, 1, 1, 1, 1).map(_.toDouble)
    testHelper(labels2, Array(0.0, 1.0), Array(3.0, 0.0), Array(0.0, 4.0))
  }

  test("chooseOrderedCategoricalSplit: return bad split if we should not split") {
    val featureIndex = 0
    val values = Array(0, 0, 1, 2, 2, 2, 2)
    val featureArity = values.max.toInt + 1

    val labels = Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)

    val impurity = Entropy
    val metadata = getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map(featureIndex -> featureArity))
    val fullImpurityAgg = metadata.createImpurityAggregator()
    labels.foreach(fullImpurityAgg.update(_))
    val (split, stats) =
      LocalDecisionTree.chooseOrderedCategoricalSplit(featureIndex, values,
        values.indices.toArray,
        labels, 0, values.length, featureArity, metadata, fullImpurityAgg)

    assert(split.isEmpty)
    val fullImpurityStatsArray =
      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
    assert(stats.gain === 0.0)
    assert(stats.impurity === fullImpurity)
    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
    assert(stats.valid)
  }

  test("chooseUnorderedCategoricalSplit: basic case") {

    val featureIndex = 0
    val featureArity = 4
    val values = Array(3, 1, 0, 2, 2)
    val labels = Array(0.0, 0.0, 1.0, 1.0, 2.0)
    val impurity = Entropy
    val splits = getUnorderedSplits(values, featureIndex)

    val metadata = getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 3, Map(featureIndex -> featureArity))
    val fullImpurityAgg = metadata.createImpurityAggregator()
    labels.foreach(fullImpurityAgg.update(_))
    val (split, _) =
      LocalDecisionTree.chooseUnorderedCategoricalSplit(featureIndex, values,
        values.indices.toArray,
        labels, 0, values.length, featureArity, metadata, splits, fullImpurityAgg)

    split match {
      case Some(s: CategoricalSplit) =>
        assert(s.featureIndex === featureIndex)
        assert(s.leftCategories.toSet === Set(1.0, 3.0))
        assert(s.rightCategories.toSet === Set(0.0, 2.0))
      // TODO: test correctness of stats
      case _ =>
        throw new AssertionError(
          s"Expected CategoricalSplit but got ${split.getClass.getSimpleName}")
    }
  }

  test("chooseUnorderedCategoricalSplit: return bad split if we should not split") {
    val featureIndex = 0
    val featureArity = 4
    val values = Array(3, 1, 0, 2, 2)
    val labels = Array(1.0, 1.0, 1.0, 1.0, 1.0)
    val impurity = Entropy
    val splits = getUnorderedSplits(values, featureIndex)

    val metadata = getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map(featureIndex -> featureArity))
    val fullImpurityAgg = metadata.createImpurityAggregator()
    labels.foreach(fullImpurityAgg.update(_))
    val (split, stats) =
      LocalDecisionTree.chooseUnorderedCategoricalSplit(featureIndex, values,
        values.indices.toArray,
        labels, 0, values.length, featureArity, metadata, splits, fullImpurityAgg)

    val fullImpurityStatsArray =
      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
    assert(stats.gain === -1.0)
    assert(stats.impurity === fullImpurity)
    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
    assert(stats.valid)
  }

  test("chooseContinuousSplit: basic case") {
    val featureIndex = 0
    val thresholds = Array(1, 2, 3, 4, 5)
    val values = thresholds.indices.toArray
    val splits = getContinuousSplits(thresholds, featureIndex)
    val labels = Array(0.0, 0.0, 1.0, 1.0, 1.0)
    val impurity = Entropy
    val metadata = getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map.empty[Int, Int])
    val fullImpurityAgg = metadata.createImpurityAggregator()
    labels.foreach(label => fullImpurityAgg.update(label))

    val (split, stats) = LocalDecisionTree.chooseContinuousSplit(featureIndex, values,
      values.indices.toArray, labels, 0, values.length, metadata, splits, fullImpurityAgg)
    split match {
      case Some(s: ContinuousSplit) =>
        assert(s.featureIndex === featureIndex)
        assert(s.threshold === 2)
      case _ =>
        throw new AssertionError(
          s"Expected ContinuousSplit but got ${split.getClass.getSimpleName}")
    }
    val fullImpurityStatsArray =
      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
    assert(stats.gain === fullImpurity)
    assert(stats.impurity === fullImpurity)
    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
    assert(stats.leftImpurityCalculator.stats === Array(2.0, 0.0))
    assert(stats.rightImpurityCalculator.stats === Array(0.0, 3.0))
    assert(stats.valid)
  }

  test("chooseContinuousSplit: return bad split if we should not split") {
    val featureIndex = 0
    val thresholds = Array(1, 2, 3, 4, 5)
    val values = thresholds.indices.toArray
    val splits = getContinuousSplits(thresholds, featureIndex)
    val labels = Array(0.0, 0.0, 0.0, 0.0, 0.0)
    val impurity = Entropy
    val metadata = getMetadata(numExamples = values.length, numFeatures = 1,
      numClasses = 2, Map.empty[Int, Int])
    val fullImpurityAgg = metadata.createImpurityAggregator()
    labels.foreach(label => fullImpurityAgg.update(label))

    val (split, stats) = LocalDecisionTree.chooseContinuousSplit(featureIndex, values,
      values.indices.toArray, labels, 0, values.length, metadata, splits, fullImpurityAgg)
    // split should be None
    assert(split.isEmpty)
    // stats for parent node should be correct
    val fullImpurityStatsArray =
      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
    assert(stats.gain === 0.0)
    assert(stats.impurity === fullImpurity)
    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
  }

  /* * * * * * * * * * * Bit subvectors * * * * * * * * * * */

  test("bitSubvectorFromSplit: 1 node") {
    val featureIndex = 0
    val thresholds = Array(1, 2, 4, 6, 7)
    val values = thresholds.indices.toArray
    val splits = getContinuousSplits(thresholds, featureIndex)
    val col = FeatureVector.fromOriginal(0, 0, values)
    val fromOffset = 0
    val toOffset = col.values.length
    val numRows = toOffset
    val split = new ContinuousSplit(0, threshold = 5)
    val bitv = LocalDecisionTreeUtils.bitVectorFromSplit(col, fromOffset, toOffset, split, splits)
    assert(bitv.toArray.toSet === Set(3, 4))
  }

  test("bitSubvectorFromSplit: 2 nodes") {
    // Initially, 1 split, rows: (0, 2, 4) | (1, 3)
    val thresholds = Array(1, 2, 4, 6, 7)
    val values = thresholds.indices.toArray
    val splits = getContinuousSplits(thresholds, featureIndex = 0)
    val col = new FeatureVector(0, 0, values,
      Array(4, 2, 0, 1, 3))

    def checkSplit(
        fromOffset: Int,
        toOffset: Int,
        threshold: Double,
        expectedRight: Set[Int]): Unit = {
      val split = new ContinuousSplit(0, threshold)
      val numRows = col.values.length
      val bitv = LocalDecisionTreeUtils.bitVectorFromSplit(col, fromOffset, toOffset, split, splits)
      assert(bitv.toArray.toSet === expectedRight)
    }

    // Left child node
    checkSplit(0, 3, 0.5, Set(0, 2, 4))
    checkSplit(0, 3, 1.5, Set(0, 2))
    checkSplit(0, 3, 2, Set(0))
    checkSplit(0, 3, 5, Set())
    // Right child node
    checkSplit(3, 5, 1, Set(1, 3))
    checkSplit(3, 5, 6.5, Set(3))
    checkSplit(3, 5, 8, Set())
  }

}

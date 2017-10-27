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
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.collection.BitSet

/** Suite exercising data structures (FeatureVector, TrainingInfo) for local tree training. */
class LocalTreeDataSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("FeatureVector: updating columns for split") {
    val vecLength = 100
    // Create a column of vecLength values
    val values = 0.until(vecLength).toArray
    val col = FeatureColumn(-1, values)
    // Pick a random subset of indices to split left
    val rng = new Random(seed = 42)
    val leftProb = 0.5
    val (leftIdxs, rightIdxs) = values.indices.partition(_ => rng.nextDouble() < leftProb)
    // Determine our expected result after updating for split
    val expected = leftIdxs.map(values(_)) ++ rightIdxs.map(values(_))
    // Create a bitset indicating whether each of our values splits left or right
    val instanceBitVector = new BitSet(values.length)
    rightIdxs.foreach(instanceBitVector.set)
    // Update column, compare new values to expected result
    val tempVals = new Array[Int](vecLength)
    val tempIndices = new Array[Int](vecLength)
    LocalDecisionTreeUtils.updateArrayForSplit(col.values, from = 0, to = vecLength,
      leftIdxs.length, tempVals, instanceBitVector)
    assert(col.values.sameElements(expected))
  }

  /* Check that FeatureVector methods produce expected results */
  test("FeatureVector: constructor and deepCopy") {
    // Create a feature vector v, modify a deep copy of v, and check that
    // v itself was not modified
    val v = new FeatureColumn(1, Array(1, 2, 3))
    val vCopy = v.deepCopy()
    vCopy.values(0) = 1000
    assert(v.values(0) !== vCopy.values(0))
  }

  // Get common TrainingInfo for tests
  // Data:
  // Feature 0 (continuous): [3, 2, 0, 1]
  // Feature 1 (categorical):[0, 0, 2, 1]
  private def getTrainingInfo(): TrainingInfo = {
    val numRows = 4
    // col1 is continuous features
    val col1 = FeatureColumn(featureIndex = 0, Array(3, 2, 0, 1))
    // col2 is categorical features
    val catFeatureIdx = 1
    val col2 = FeatureColumn(featureIndex = catFeatureIdx, values = Array(0, 0, 2, 1))

    val nodeOffsets = Array((0, numRows))
    val activeNodes = Array(LearningNode.emptyNode(nodeIndex = -1))
    TrainingInfo(Array(col1, col2), nodeOffsets, activeNodes)
  }

   // Check that TrainingInfo correctly updates node offsets, sorts column values during update()
  test("TrainingInfo.update(): correctness when splitting on continuous features") {
    // Get TrainingInfo
    // Feature 0 (continuous): [3, 2, 0, 1]
    // Feature 1 (categorical):[0, 0, 2, 1]
    val info = getTrainingInfo()
    val activeNodes = info.currentLevelActiveNodes
    val contFeatureIdx = 0

    // For continuous feature, active node has a split with threshold 1
    val contNode = activeNodes(contFeatureIdx)
    contNode.split = Some(new ContinuousSplit(contFeatureIdx, threshold = 1))

    // Update TrainingInfo for continuous split
    val contValues = info.columns(contFeatureIdx).values
    val splits = Array(LocalTreeTests.getContinuousSplits(contValues, contFeatureIdx))
    val newInfo = info.update(splits, newActiveNodes = Array(contNode))

    assert(newInfo.columns.length === 2)
    // Continuous split should send feature values [0, 1] to the left, [3, 2] to the right
    // ==> row indices (2, 3) should split left, row indices (0, 1) should split right
    val expectedContCol = new FeatureColumn(0, values = Array(0, 1, 3, 2))
    val expectedCatCol = new FeatureColumn(1, values = Array(2, 1, 0, 0))
    val expectedIndices = Array(2, 3, 0, 1)
    assert(newInfo.columns(0) === expectedContCol)
    assert(newInfo.columns(1) === expectedCatCol)
    assert(newInfo.indices === expectedIndices)
    // Check that node offsets were updated properly
    assert(newInfo.nodeOffsets === Array((0, 2), (2, 4)))
  }

  test("TrainingInfo.update(): correctness when splitting on categorical features") {
    // Get TrainingInfo
    // Feature 0 (continuous): [3, 2, 0, 1]
    // Feature 1 (categorical):[0, 0, 2, 1]
    val info = getTrainingInfo()
    val activeNodes = info.currentLevelActiveNodes
    val catFeatureIdx = 1

    // For categorical feature, active node puts category 2 on left side of split
    val catNode = activeNodes(0)
    val catSplit = new CategoricalSplit(catFeatureIdx, _leftCategories = Array(2),
      numCategories = 3)
    catNode.split = Some(catSplit)

    // Update TrainingInfo for categorical split
    val splits: Array[Array[Split]] = Array(Array.empty, Array(catSplit))
    val newInfo = info.update(splits, newActiveNodes = Array(catNode))

    assert(newInfo.columns.length === 2)
    // Categorical split should send feature values [2] to the left, [0, 1] to the right
    // ==> row 2 should split left, rows [0, 1, 3] should split right
    val expectedContCol = new FeatureColumn(0, values = Array(0, 3, 2, 1))
    val expectedCatCol = new FeatureColumn(1, values = Array(2, 0, 0, 1))
    val expectedIndices = Array(2, 0, 1, 3)
    assert(newInfo.columns(0) === expectedContCol)
    assert(newInfo.columns(1) === expectedCatCol)
    assert(newInfo.indices === expectedIndices)
    // Check that node offsets were updated properly
    assert(newInfo.nodeOffsets === Array((0, 1), (1, 4)))
  }

  private def getSetBits(bitset: BitSet): Set[Int] = {
    Range(0, bitset.capacity).filter(bitset.get).toSet
  }

  test("TrainingInfo.bitSetFromSplit correctness: splitting a single node") {
    val featureIndex = 0
    val thresholds = Array(1, 2, 4, 6, 7)
    val values = thresholds.indices.toArray
    val splits = LocalTreeTests.getContinuousSplits(thresholds, featureIndex)
    val col = FeatureColumn(0, values)
    val fromOffset = 0
    val toOffset = col.values.length
    val numRows = toOffset
    // Create split; first three rows (with feature values [1, 2, 4]) should split left, as they
    // have feature values <= 5. Last two rows (feature values [6, 7]) should split right.
    val split = new ContinuousSplit(0, threshold = 5)
    val bitset = TrainingInfo.bitSetFromSplit(col, fromOffset, toOffset, split, splits)
    // Check that the last two rows (row indices [3, 4] within the set of rows being split)
    // fall on the right side of the split.
    assert(getSetBits(bitset) === Set(3, 4))
  }

  test("TrainingInfo.bitSetFromSplit correctness: splitting 2 nodes") {
    // Assume there was already 1 split, which split rows (represented by row index) as:
    // (0, 2, 4) | (1, 3)
    val thresholds = Array(1, 2, 4, 6, 7)
    val values = thresholds.indices.toArray
    val splits = LocalTreeTests.getContinuousSplits(thresholds, featureIndex = 0)
    val col = new FeatureColumn(0, values)

    /**
     * Computes a bitset for splitting rows in with indices in [fromOffset, toOffset) using a
     * continuous split with the specified threshold. Then, checks that right side of the split
     * contains the row indices in expectedRight.
     */
    def checkSplit(
        fromOffset: Int,
        toOffset: Int,
        threshold: Double,
        expectedRight: Set[Int]): Unit = {
      val split = new ContinuousSplit(0, threshold)
      val numRows = col.values.length
      val bitset = TrainingInfo.bitSetFromSplit(col, fromOffset, toOffset, split, splits)
      assert(getSetBits(bitset) === expectedRight)
    }

    // Split rows corresponding to left child node (rows [0, 2, 4])
    checkSplit(fromOffset = 0, toOffset = 3, threshold = 0.5, expectedRight = Set(0, 1, 2))
    checkSplit(fromOffset = 0, toOffset = 3, threshold = 1.5, expectedRight = Set(1, 2))
    checkSplit(fromOffset = 0, toOffset = 3, threshold = 2, expectedRight = Set(2))
    checkSplit(fromOffset = 0, toOffset = 3, threshold = 5, expectedRight = Set())
    // Split rows corresponding to right child node (rows [1, 3])
    checkSplit(fromOffset = 3, toOffset = 5, threshold = 1, expectedRight = Set(0, 1))
    checkSplit(fromOffset = 3, toOffset = 5, threshold = 6.5, expectedRight = Set(1))
    checkSplit(fromOffset = 3, toOffset = 5, threshold = 8, expectedRight = Set())
  }

}

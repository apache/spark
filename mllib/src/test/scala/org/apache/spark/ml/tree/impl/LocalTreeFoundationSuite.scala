///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.ml.tree.impl
//
//import scala.util.Random
//import org.apache.spark.SparkFunSuite
//import org.apache.spark.ml.tree.{CategoricalSplit, ContinuousSplit, LearningNode, Split}
//import org.apache.spark.ml.tree.impl.Yggdrasil.{FeatureVector, PartitionInfo, YggdrasilMetadata}
//import org.apache.spark.ml.tree.impl.{Yggdrasil, YggdrasilClassification, YggdrasilRegression}
//import org.apache.spark.ml.util.DefaultReadWriteTest
//import org.apache.spark.mllib.tree.impurity.Entropy
//import org.apache.spark.mllib.util.MLlibTestSparkContext
//import org.apache.spark.util.collection.BitSet
//
///* * * * * * * * * * * Helper classes * * * * * * * * * * */
//class LocalTreeUnitSuite
//  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {
//
//  test("FeatureVector") {
//    val v = new FeatureVector(1, 0, Array(1, 2, 3), Array(1, 2, 0))
//    val vCopy = v.deepCopy()
//    vCopy.values(0) = 1000
//    assert(v.values(0) !== vCopy.values(0))
//
//    val original = Array(1, 2, 3)
//    val v2 = FeatureVector.fromOriginal(1, 0, original)
//    assert(v === v2)
//  }
//
//  test("FeatureVectorSortByValue") {
//    val values = Array(1, 2, 4, 6, 7, 9, 15, 155)
//    val col = Random.shuffle(values.toIterator).toArray
//    val unsortedIndices = col.indices
//    val sortedIndices = unsortedIndices.sortBy(x => col(x)).toArray
//    val featureIndex = 3
//    val featureArity = 0
//    val fvSorted =
//      FeatureVector.fromOriginal(featureIndex, featureArity, col)
//    assert(fvSorted.featureIndex === featureIndex)
//    assert(fvSorted.featureArity === featureArity)
//    assert(fvSorted.values.deep === values.deep)
//    assert(fvSorted.indices.deep === sortedIndices.deep)
//  }
//
//  test("PartitionInfo") {
//    val numRows = 4
//    val col1 = FeatureVector.fromOriginal(0, 0, Array(8, 2, 1, 6))
//    val col2 =
//      FeatureVector.fromOriginal(1, 3, Array(0, 1, 0, 2))
//    val labels = Array(0, 0, 0, 1, 1, 1, 1).map(_.toDouble)
//
//    val metadata = new DecisionTreeMetadata(numFeatures = 2, numExamples = 4, numClasses = 2,
//      maxBins = 4, minInfoGain = 0.0, featureArity = Map(1 -> 3), unorderedFeatures = Set(1),
//      numBins = null, impurity = Entropy, quantileStrategy = null,  maxDepth = 2,
//      minInstancesPerNode = 1, numTrees = 1, numFeaturesPerNode = 2)
//
//    val fullImpurityAgg = metadata.createImpurityAggregator()
//    labels.foreach(label => fullImpurityAgg.update(label))
//
//    assert(col1.values.length === numRows)
//    assert(col2.values.length === numRows)
//
//    val nodeOffsets = Array((0, numRows))
//    val activeNodes = Array(LearningNode.emptyNode(nodeIndex = -1))
//
//    val info = new PartitionInfo(Array(col1, col2), nodeOffsets, activeNodes)
//
//    // Create bitVector for splitting the 4 rows: L, R, L, R
//    // New groups are {0, 2}, {1, 3}
//    val bitVector = new BitSet(4)
//    bitVector.set(1)
//    bitVector.set(3)
//
//    // for these tests, use the activeNodes for nodeSplitBitVector
//    val newInfo = info.update(bitVector, newActiveNodes, labels, metadata)
//
//    assert(newInfo.columns.length === 2)
//    val expectedCol1a =
//      new FeatureVector(0, 0, Array(1, 8, 2, 6), Array(2, 0, 1, 3))
//    val expectedCol1b =
//      new FeatureVector(1, 3, Array(0, 0, 1, 2), Array(0, 2, 1, 3))
//    assert(newInfo.columns(0) === expectedCol1a)
//    assert(newInfo.columns(1) === expectedCol1b)
//    assert(newInfo.nodeOffsets === Array(0, 2, 4))
//    assert(newInfo.activeNodes.iterator.toSet === Set(0, 1))
//
//    // stats for the two child nodes should be correct
//    //    val fullImpurityStatsArray =
//    //      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
//    //    val fullImpurity = Entropy.calculate(fullImpurityStatsArray, labels.length)
//    //    val stats = newInfo.fullImpurityAggs(0).getCalculator.
//    //    assert(stats.gain === 0.0)
//    //    assert(stats.impurity === fullImpurity)
//    //    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
//
//    // Create 2 bitVectors for splitting into: 0, 2, 1, 3
//    val bitVector2 = new BitSet(4)
//    bitVector2.set(2) // 2 goes to the right
//    bitVector2.set(3) // 3 goes to the right
//
//    val newInfo2 = newInfo.update(bitVector2, newActiveNodes, labels, metadata)
//
//    assert(newInfo2.columns.length === 2)
//    val expectedCol2a =
//      new FeatureVector(0, 0, Array(8, 1, 2, 6), Array(0, 2, 1, 3))
//    val expectedCol2b =
//      new FeatureVector(1, 3, Array(0, 0, 1, 2), Array(0, 2, 1, 3))
//    assert(newInfo2.columns(0) === expectedCol2a)
//    assert(newInfo2.columns(1) === expectedCol2b)
//    assert(newInfo2.nodeOffsets === Array(0, 1, 2, 3, 4))
//    assert(newInfo2.activeNodes.iterator.toSet === Set(0, 1, 2, 3))
//  }
//
//  /* * * * * * * * * * * Choosing Splits  * * * * * * * * * * */
//
//  test("computeBestSplits") {
//    // TODO
//  }
//
//  test("chooseSplit: choose correct type of split") {
//    val labels = Array(0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0)
//    val labelsAsBytes = labels.map(_.toByte)
//    val fromOffset = 1
//    val toOffset = 4
//    val impurity = Entropy
//    val metadata = new DecisionTreeMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0, impurity, Map(1 -> 3))
//    val fullImpurityAgg = metadata.createImpurityAggregator()
//    labels.foreach(label => fullImpurityAgg.update(label))
//
//    val col1 = FeatureVector.fromOriginal(featureIndex = 0, featureArity = 0,
//      values = Array(0.8, 0.1, 0.1, 0.2, 0.3, 0.5, 0.6))
//    val (split1, _) = YggdrasilClassification.chooseSplit(col1, labelsAsBytes, fromOffset, toOffset, fullImpurityAgg, metadata)
//    assert(split1.nonEmpty && split1.get.isInstanceOf[ContinuousSplit])
//
//    val col2 = FeatureVector.fromOriginal(featureIndex = 1, featureArity = 3,
//      values = Array(0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 2.0))
//    val (split2, _) = YggdrasilRegression.chooseSplit(col2, labels, fromOffset, toOffset, fullImpurityAgg, metadata)
//    assert(split2.nonEmpty && split2.get.isInstanceOf[CategoricalSplit])
//  }
//
//  test("chooseOrderedCategoricalSplit: basic case") {
//    val featureIndex = 0
//    val values = Array(0, 0, 1, 2, 2, 2, 2).map(_.toDouble)
//    val featureArity = values.max.toInt + 1
//
//    def testHelper(
//                    labels: Array[Byte],
//                    expectedLeftCategories: Array[Double],
//                    expectedLeftStats: Array[Double],
//                    expectedRightStats: Array[Double]): Unit = {
//      val expectedRightCategories = Range(0, featureArity)
//        .filter(c => !expectedLeftCategories.contains(c)).map(_.toDouble).toArray
//      val impurity = Entropy
//      val metadata = new YggdrasilMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0,
//        impurity, Map.empty[Int, Int])
//      val (split, stats) =
//        YggdrasilClassification.chooseOrderedCategoricalSplit(featureIndex, values, values.indices.toArray,
//          labels, 0, values.length, metadata, featureArity)
//      split match {
//        case Some(s: CategoricalSplit) =>
//          assert(s.featureIndex === featureIndex)
//          assert(s.leftCategories === expectedLeftCategories)
//          assert(s.rightCategories === expectedRightCategories)
//        case _ =>
//          throw new AssertionError(
//            s"Expected CategoricalSplit but got ${split.getClass.getSimpleName}")
//      }
//      val fullImpurityStatsArray =
//        Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
//      val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
//      assert(stats.gain === fullImpurity)
//      assert(stats.impurity === fullImpurity)
//      assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
//      assert(stats.leftImpurityCalculator.stats === expectedLeftStats)
//      assert(stats.rightImpurityCalculator.stats === expectedRightStats)
//      assert(stats.valid)
//    }
//
//    val labels1 = Array(0, 0, 1, 1, 1, 1, 1).map(_.toByte)
//    testHelper(labels1, Array(0.0), Array(2.0, 0.0), Array(0.0, 5.0))
//
//    val labels2 = Array(0, 0, 0, 1, 1, 1, 1).map(_.toByte)
//    testHelper(labels2, Array(0.0, 1.0), Array(3.0, 0.0), Array(0.0, 4.0))
//  }
//
//  test("chooseOrderedCategoricalSplit: return bad split if we should not split") {
//    val featureIndex = 0
//    val values = Array(0, 0, 1, 2, 2, 2, 2).map(_.toDouble)
//    val featureArity = values.max.toInt + 1
//
//    val labels = Array(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
//
//    val impurity = Entropy
//    val metadata = new YggdrasilMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0, impurity,
//      Map(featureIndex -> featureArity))
//    val (split, stats) =
//      YggdrasilRegression.chooseOrderedCategoricalSplit(featureIndex, values, values.indices.toArray,
//        labels, 0, values.length, metadata, featureArity)
//    assert(split.isEmpty)
//    val fullImpurityStatsArray =
//      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
//    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
//    assert(stats.gain === 0.0)
//    assert(stats.impurity === fullImpurity)
//    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
//    assert(stats.valid)
//  }
//
//  test("chooseUnorderedCategoricalSplit: basic case") {
//    val featureIndex = 0
//    val featureArity = 4
//    val values = Array(3.0, 1.0, 0.0, 2.0, 2.0)
//    val labels = Array(0.0, 0.0, 1.0, 1.0, 2.0)
//    val impurity = Entropy
//    val metadata = new YggdrasilMetadata(numClasses = 3, maxBins = 16, minInfoGain = 0.0, impurity,
//      Map(featureIndex -> featureArity))
//    val allSplits = metadata.getUnorderedSplits(featureIndex)
//    val (split, _) = YggdrasilRegression.chooseUnorderedCategoricalSplit(featureIndex, values, values.indices.toArray,
//      labels, 0, values.length, metadata, featureArity, allSplits)
//    split match {
//      case Some(s: CategoricalSplit) =>
//        assert(s.featureIndex === featureIndex)
//        assert(s.leftCategories.toSet === Set(0.0, 2.0))
//        assert(s.rightCategories.toSet === Set(1.0, 3.0))
//      // TODO: test correctness of stats
//      case _ =>
//        throw new AssertionError(
//          s"Expected CategoricalSplit but got ${split.getClass.getSimpleName}")
//    }
//  }
//
//  test("chooseUnorderedCategoricalSplit: return bad split if we should not split") {
//    val featureIndex = 0
//    val featureArity = 4
//    val values = Array(3.0, 1.0, 0.0, 2.0, 2.0)
//    val labels = Array(1.0, 1.0, 1.0, 1.0, 1.0).map(_.toByte)
//    val impurity = Entropy
//    val metadata = new YggdrasilMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0, impurity,
//      Map(featureIndex -> featureArity))
//    val (split, stats) =
//      YggdrasilClassification.chooseOrderedCategoricalSplit(featureIndex, values, values.indices.toArray,
//        labels, 0, values.length, metadata, featureArity)
//    assert(split.isEmpty)
//    val fullImpurityStatsArray =
//      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
//    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
//    assert(stats.gain === 0.0)
//    assert(stats.impurity === fullImpurity)
//    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
//    assert(stats.valid)
//  }
//
//  test("chooseContinuousSplit: basic case") {
//    val featureIndex = 0
//    val values = Array(0.1, 0.2, 0.3, 0.4, 0.5)
//    val labels = Array(0.0, 0.0, 1.0, 1.0, 1.0)
//    val impurity = Entropy
//    val metadata = new YggdrasilMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0, impurity, Map.empty[Int, Int])
//    val fullImpurityAgg = metadata.createImpurityAggregator()
//    labels.foreach(label => fullImpurityAgg.update(label))
//
//    val (split, stats) = YggdrasilRegression.chooseContinuousSplit(featureIndex, values,
//      values.indices.toArray, labels, 0, values.length, fullImpurityAgg, metadata)
//    split match {
//      case Some(s: ContinuousSplit) =>
//        assert(s.featureIndex === featureIndex)
//        assert(s.threshold === 0.2)
//      case _ =>
//        throw new AssertionError(
//          s"Expected ContinuousSplit but got ${split.getClass.getSimpleName}")
//    }
//    val fullImpurityStatsArray =
//      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
//    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
//    assert(stats.gain === fullImpurity)
//    assert(stats.impurity === fullImpurity)
//    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
//    assert(stats.leftImpurityCalculator.stats === Array(2.0, 0.0))
//    assert(stats.rightImpurityCalculator.stats === Array(0.0, 3.0))
//    assert(stats.valid)
//  }
//
//  test("chooseContinuousSplit: return bad split if we should not split") {
//    val featureIndex = 0
//    val values = Array(0.1, 0.2, 0.3, 0.4, 0.5)
//    val labels = Array(0.0, 0.0, 0.0, 0.0, 0.0).map(_.toByte)
//    val impurity = Entropy
//    val metadata = new YggdrasilMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0, impurity, Map.empty[Int, Int])
//    val fullImpurityAgg = metadata.createImpurityAggregator()
//    labels.foreach(label => fullImpurityAgg.update(label))
//
//    val (split, stats) = YggdrasilClassification.chooseContinuousSplit(featureIndex, values, values.indices.toArray,
//      labels, 0, values.length, fullImpurityAgg, metadata)
//    // split should be None
//    assert(split.isEmpty)
//    // stats for parent node should be correct
//    val fullImpurityStatsArray =
//      Array(labels.count(_ == 0.0).toDouble, labels.count(_ == 1.0).toDouble)
//    val fullImpurity = impurity.calculate(fullImpurityStatsArray, labels.length)
//    assert(stats.gain === 0.0)
//    assert(stats.impurity === fullImpurity)
//    assert(stats.impurityCalculator.stats === fullImpurityStatsArray)
//  }
//
//  /* * * * * * * * * * * Bit subvectors * * * * * * * * * * */
//
//  test("bitSubvectorFromSplit: 1 node") {
//    val col =
//      FeatureVector.fromOriginal(0, 0, Array(0.1, 0.2, 0.4, 0.6, 0.7))
//    val fromOffset = 0
//    val toOffset = col.values.length
//    val numRows = toOffset
//    val split = new ContinuousSplit(0, threshold = 0.5)
//    val bitv = Yggdrasil.bitVectorFromSplit(col, fromOffset, toOffset, split, numRows)
//    assert(bitv.toArray.toSet === Set(3, 4))
//  }
//
//  test("bitSubvectorFromSplit: 2 nodes") {
//    // Initially, 1 split: (0, 2, 4) | (1, 3)
//    val col = new FeatureVector(0, 0, Array(0.1, 0.2, 0.4, 0.6, 0.7),
//      Array(4, 2, 0, 1, 3))
//    def checkSplit(fromOffset: Int, toOffset: Int, threshold: Double,
//                   expectedRight: Set[Int]): Unit = {
//      val split = new ContinuousSplit(0, threshold)
//      val numRows = col.values.length
//      val bitv = Yggdrasil.bitVectorFromSplit(col, fromOffset, toOffset, split, numRows)
//      assert(bitv.toArray.toSet === expectedRight)
//    }
//    // Left child node
//    checkSplit(0, 3, 0.05, Set(0, 2, 4))
//    checkSplit(0, 3, 0.15, Set(0, 2))
//    checkSplit(0, 3, 0.2, Set(0))
//    checkSplit(0, 3, 0.5, Set())
//    // Right child node
//    checkSplit(3, 5, 0.1, Set(1, 3))
//    checkSplit(3, 5, 0.65, Set(3))
//    checkSplit(3, 5, 0.8, Set())
//  }
//
//  test("collectBitVectors with 1 vector") {
//    val col =
//      FeatureVector.fromOriginal(0, 0, Array(0.1, 0.2, 0.4, 0.6, 0.7))
//    val numRows = col.values.length
//    val activeNodes = new BitSet(1)
//    activeNodes.set(0)
//    val labels = Array(0, 0, 0, 1, 1, 1, 1).map(_.toDouble)
//    val metadata = new YggdrasilMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0, Entropy, Map(1 -> 3))
//    val fullImpurityAgg = metadata.createImpurityAggregator()
//    labels.foreach(label => fullImpurityAgg.update(label))
//
//    val info = PartitionInfo(Array(col), Array(0, numRows), activeNodes, Array(fullImpurityAgg))
//    val partitionInfos = sc.parallelize(Seq(info))
//    val bestSplit = new ContinuousSplit(0, threshold = 0.5)
//    val bitVector = Yggdrasil.aggregateBitVector(partitionInfos, Array(Some(bestSplit)), numRows)
//    assert(bitVector.toArray.toSet === Set(3, 4))
//  }
//
//  test("collectBitVectors with 1 vector, with tied threshold") {
//    val col = new FeatureVector(0, 0,
//      Array(-4.0, -4.0, -2.0, -2.0, -1.0, -1.0, 1.0, 1.0),
//      Array(3, 7, 2, 6, 1, 5, 0, 4))
//    val numRows = col.values.length
//    val activeNodes = new BitSet(1)
//    activeNodes.set(0)
//    val labels = Array(0, 0, 0, 1, 1, 1, 1).map(_.toDouble)
//    val metadata = new YggdrasilMetadata(numClasses = 2, maxBins = 4, minInfoGain = 0.0, Entropy, Map(1 -> 3))
//    val fullImpurityAgg = metadata.createImpurityAggregator()
//    labels.foreach(label => fullImpurityAgg.update(label))
//
//    val info = PartitionInfo(Array(col), Array(0, numRows), activeNodes, Array(fullImpurityAgg))
//    val partitionInfos = sc.parallelize(Seq(info))
//    val bestSplit = new ContinuousSplit(0, threshold = -2.0)
//    val bitVector = Yggdrasil.aggregateBitVector(partitionInfos, Array(Some(bestSplit)), numRows)
//    assert(bitVector.toArray.toSet === Set(0, 1, 4, 5))
//  }
//
//  /* * * * * * * * * * * Active nodes * * * * * * * * * * */
//
//  test("computeActiveNodePeriphery") {
//
//    def exactlyEquals(a: ImpurityStats, b: ImpurityStats): Boolean = {
//      a.gain == b.gain && a.impurity == b.impurity &&
//        a.impurityCalculator.stats.sameElements(b.impurityCalculator.stats) &&
//        a.leftImpurityCalculator.stats.sameElements(b.leftImpurityCalculator.stats) &&
//        a.rightImpurityCalculator.stats.sameElements(b.rightImpurityCalculator.stats) &&
//        a.valid == b.valid
//    }
//    // old periphery: 2 nodes
//    val left = LearningNode.emptyNode(id = 1)
//    val right = LearningNode.emptyNode(id = 2)
//    val oldPeriphery: Array[LearningNode] = Array(left, right)
//    // bestSplitsAndGains: Do not split left, but split right node.
//    val lCalc = new EntropyCalculator(Array(8.0, 1.0))
//    val lStats = new ImpurityStats(0.0, lCalc.calculate(),
//      lCalc, lCalc, new EntropyCalculator(Array(0.0, 0.0)))
//
//    val rSplit = new ContinuousSplit(featureIndex = 1, threshold = 0.6)
//    val rCalc = new EntropyCalculator(Array(5.0, 7.0))
//    val rRightChildCalc = new EntropyCalculator(Array(1.0, 5.0))
//    val rLeftChildCalc = new EntropyCalculator(Array(
//      rCalc.stats(0) - rRightChildCalc.stats(0),
//      rCalc.stats(1) - rRightChildCalc.stats(1)))
//    val rGain = {
//      val rightWeight = rRightChildCalc.stats.sum / rCalc.stats.sum
//      val leftWeight = rLeftChildCalc.stats.sum / rCalc.stats.sum
//      rCalc.calculate() -
//        rightWeight * rRightChildCalc.calculate() - leftWeight * rLeftChildCalc.calculate()
//    }
//    val rStats =
//      new ImpurityStats(rGain, rCalc.calculate(), rCalc, rLeftChildCalc, rRightChildCalc)
//
//    val bestSplitsAndGains: Array[(Option[Split], ImpurityStats)] =
//      Array((None, lStats), (Some(rSplit), rStats))
//
//    // Test A: Split right node
//    val newPeriphery1: Array[LearningNode] =
//      Yggdrasil.computeActiveNodePeriphery(oldPeriphery, bestSplitsAndGains, minInfoGain = 0.0)
//    // Expect 2 active nodes
//    assert(newPeriphery1.length === 2)
//    // Confirm right node was updated
//    assert(right.split.get === rSplit)
//    assert(!right.isLeaf)
//    assert(exactlyEquals(right.stats, rStats))
//    assert(right.leftChild.nonEmpty && right.leftChild.get === newPeriphery1(0))
//    assert(right.rightChild.nonEmpty && right.rightChild.get === newPeriphery1(1))
//    // Confirm new active nodes have stats but no children
//    assert(newPeriphery1(0).leftChild.isEmpty && newPeriphery1(0).rightChild.isEmpty &&
//      newPeriphery1(0).split.isEmpty &&
//      newPeriphery1(0).stats.impurityCalculator.stats.sameElements(rLeftChildCalc.stats))
//    assert(newPeriphery1(1).leftChild.isEmpty && newPeriphery1(1).rightChild.isEmpty &&
//      newPeriphery1(1).split.isEmpty &&
//      newPeriphery1(1).stats.impurityCalculator.stats.sameElements(rRightChildCalc.stats))
//
//    // Test B: Increase minInfoGain, so split nothing
//    val newPeriphery2: Array[LearningNode] =
//      Yggdrasil.computeActiveNodePeriphery(oldPeriphery, bestSplitsAndGains, minInfoGain = 1000.0)
//    assert(newPeriphery2.isEmpty)
//  }
//}

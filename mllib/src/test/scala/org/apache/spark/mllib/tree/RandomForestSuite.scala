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

import scala.collection.mutable

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impl.{BaggedPoint, DecisionTreeMetadata}
import org.apache.spark.mllib.tree.impurity.{Gini, Variance}
import org.apache.spark.mllib.tree.model.{Node, RandomForestModel}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.util.StatCounter

/**
 * Test suite for [[RandomForest]].
 */
class RandomForestSuite extends FunSuite with LocalSparkContext {

  test("BaggedPoint RDD: without subsampling") {
    val arr = RandomForestSuite.generateOrderedLabeledPoints(numFeatures = 1)
    val rdd = sc.parallelize(arr)
    val baggedRDD = BaggedPoint.convertToBaggedRDDWithoutSampling(rdd)
    baggedRDD.collect().foreach { baggedPoint =>
      assert(baggedPoint.subsampleWeights.size == 1 && baggedPoint.subsampleWeights(0) == 1)
    }
  }

  test("BaggedPoint RDD: with subsampling") {
    val numSubsamples = 100
    val (expectedMean, expectedStddev) = (1.0, 1.0)

    val seeds = Array(123, 5354, 230, 349867, 23987)
    val arr = RandomForestSuite.generateOrderedLabeledPoints(numFeatures = 1)
    val rdd = sc.parallelize(arr)
    seeds.foreach { seed =>
      val baggedRDD = BaggedPoint.convertToBaggedRDD(rdd, numSubsamples, seed = seed)
      val subsampleCounts: Array[Array[Double]] = baggedRDD.map(_.subsampleWeights).collect()
      RandomForestSuite.testRandomArrays(subsampleCounts, numSubsamples, expectedMean,
        expectedStddev, epsilon = 0.01)
    }
  }

  test("Binary classification with continuous features:" +
      " comparing DecisionTree vs. RandomForest(numTrees = 1)") {

    val arr = RandomForestSuite.generateOrderedLabeledPoints(numFeatures = 50)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numTrees = 1

    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)

    val rf = RandomForest.trainClassifier(rdd, strategy, numTrees = numTrees,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf.trees.size === 1)
    val rfTree = rf.trees(0)

    val dt = DecisionTree.train(rdd, strategy)

    RandomForestSuite.validateClassifier(rf, arr, 0.9)
    DecisionTreeSuite.validateClassifier(dt, arr, 0.9)

    // Make sure trees are the same.
    assert(rfTree.toString == dt.toString)
  }

  test("Regression with continuous features:" +
    " comparing DecisionTree vs. RandomForest(numTrees = 1)") {

    val arr = RandomForestSuite.generateOrderedLabeledPoints(numFeatures = 50)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]
    val numTrees = 1

    val strategy = new Strategy(algo = Regression, impurity = Variance,
      maxDepth = 2, maxBins = 10, numClassesForClassification = 2,
      categoricalFeaturesInfo = categoricalFeaturesInfo)

    val rf = RandomForest.trainRegressor(rdd, strategy, numTrees = numTrees,
      featureSubsetStrategy = "auto", seed = 123)
    assert(rf.trees.size === 1)
    val rfTree = rf.trees(0)

    val dt = DecisionTree.train(rdd, strategy)

    RandomForestSuite.validateRegressor(rf, arr, 0.01)
    DecisionTreeSuite.validateRegressor(dt, arr, 0.01)

    // Make sure trees are the same.
    assert(rfTree.toString == dt.toString)
  }

  test("Binary classification with continuous features: subsampling features") {
    val numFeatures = 50
    val arr = RandomForestSuite.generateOrderedLabeledPoints(numFeatures)
    val rdd = sc.parallelize(arr)
    val categoricalFeaturesInfo = Map.empty[Int, Int]

    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 2,
      numClassesForClassification = 2, categoricalFeaturesInfo = categoricalFeaturesInfo)

    // Select feature subset for top nodes.  Return true if OK.
    def checkFeatureSubsetStrategy(
        numTrees: Int,
        featureSubsetStrategy: String,
        numFeaturesPerNode: Int): Unit = {
      val seeds = Array(123, 5354, 230, 349867, 23987)
      val maxMemoryUsage: Long = 128 * 1024L * 1024L
      val metadata =
        DecisionTreeMetadata.buildMetadata(rdd, strategy, numTrees, featureSubsetStrategy)
      seeds.foreach { seed =>
        val failString = s"Failed on test with:" +
          s"numTrees=$numTrees, featureSubsetStrategy=$featureSubsetStrategy," +
          s" numFeaturesPerNode=$numFeaturesPerNode, seed=$seed"
        val nodeQueue = new mutable.Queue[(Int, Node)]()
        val topNodes: Array[Node] = new Array[Node](numTrees)
        Range(0, numTrees).foreach { treeIndex =>
          topNodes(treeIndex) = Node.emptyNode(nodeIndex = 1)
          nodeQueue.enqueue((treeIndex, topNodes(treeIndex)))
        }
        val rng = new scala.util.Random(seed = seed)
        val (nodesForGroup: Map[Int, Array[Node]],
            treeToNodeToIndexInfo: Map[Int, Map[Int, RandomForest.NodeIndexInfo]]) =
          RandomForest.selectNodesToSplit(nodeQueue, maxMemoryUsage, metadata, rng)

        assert(nodesForGroup.size === numTrees, failString)
        assert(nodesForGroup.values.forall(_.size == 1), failString) // 1 node per tree

        if (numFeaturesPerNode == numFeatures) {
          // featureSubset values should all be None
          assert(treeToNodeToIndexInfo.values.forall(_.values.forall(_.featureSubset.isEmpty)),
            failString)
        } else {
          // Check number of features.
          assert(treeToNodeToIndexInfo.values.forall(_.values.forall(
            _.featureSubset.get.size === numFeaturesPerNode)), failString)
        }
      }
    }

    checkFeatureSubsetStrategy(numTrees = 1, "auto", numFeatures)
    checkFeatureSubsetStrategy(numTrees = 1, "all", numFeatures)
    checkFeatureSubsetStrategy(numTrees = 1, "sqrt", math.sqrt(numFeatures).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 1, "log2",
      (math.log(numFeatures) / math.log(2)).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 1, "onethird", (numFeatures / 3.0).ceil.toInt)

    checkFeatureSubsetStrategy(numTrees = 2, "all", numFeatures)
    checkFeatureSubsetStrategy(numTrees = 2, "auto", math.sqrt(numFeatures).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "sqrt", math.sqrt(numFeatures).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "log2",
      (math.log(numFeatures) / math.log(2)).ceil.toInt)
    checkFeatureSubsetStrategy(numTrees = 2, "onethird", (numFeatures / 3.0).ceil.toInt)
  }

  test("alternating categorical and continuous features with multiclass labels to test indexing") {
    val arr = new Array[LabeledPoint](4)
    arr(0) = new LabeledPoint(0.0, Vectors.dense(1.0, 0.0, 0.0, 3.0, 1.0))
    arr(1) = new LabeledPoint(1.0, Vectors.dense(0.0, 1.0, 1.0, 1.0, 2.0))
    arr(2) = new LabeledPoint(0.0, Vectors.dense(2.0, 0.0, 0.0, 6.0, 3.0))
    arr(3) = new LabeledPoint(2.0, Vectors.dense(0.0, 2.0, 1.0, 3.0, 2.0))
    val categoricalFeaturesInfo = Map(0 -> 3, 2 -> 2, 4 -> 4)
    val input = sc.parallelize(arr)

    val strategy = new Strategy(algo = Classification, impurity = Gini, maxDepth = 5,
      numClassesForClassification = 3, categoricalFeaturesInfo = categoricalFeaturesInfo)
    val model = RandomForest.trainClassifier(input, strategy, numTrees = 2,
      featureSubsetStrategy = "sqrt", seed = 12345)
    RandomForestSuite.validateClassifier(model, arr, 0.0)
  }

}

object RandomForestSuite {

  /**
   * Aggregates all values in data, and tests whether the empirical mean and stddev are within
   * epsilon of the expected values.
   * @param data  Every element of the data should be an i.i.d. sample from some distribution.
   */
  def testRandomArrays(
      data: Array[Array[Double]],
      numCols: Int,
      expectedMean: Double,
      expectedStddev: Double,
      epsilon: Double) {
    val values = new mutable.ArrayBuffer[Double]()
    data.foreach { row =>
      assert(row.size == numCols)
      values ++= row
    }
    val stats = new StatCounter(values)
    assert(math.abs(stats.mean - expectedMean) < epsilon)
    assert(math.abs(stats.stdev - expectedStddev) < epsilon)
  }

  def validateClassifier(
      model: RandomForestModel,
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
      model: RandomForestModel,
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

  def generateOrderedLabeledPoints(numFeatures: Int): Array[LabeledPoint] = {
    val numInstances = 1000
    val arr = new Array[LabeledPoint](numInstances)
    for (i <- 0 until numInstances) {
      val label = if (i < numInstances / 10) {
        0.0
      } else if (i < numInstances / 2) {
        1.0
      } else if (i < numInstances * 0.9) {
        0.0
      } else {
        1.0
      }
      val features = Array.fill[Double](numFeatures)(i.toDouble)
      arr(i) = new LabeledPoint(label, Vectors.dense(features))
    }
    arr
  }

}

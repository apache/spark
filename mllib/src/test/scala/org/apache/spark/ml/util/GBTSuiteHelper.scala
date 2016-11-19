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

package org.apache.spark.ml.util

import scala.collection.mutable.ArrayBuffer

import org.scalactic.TolerantNumerics

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tree.{InternalNode, LeafNode, Node}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.tree.impurity.{ImpurityAggregator, ImpurityCalculator}
import org.apache.spark.mllib.tree.loss.Loss
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object GBTSuiteHelper extends SparkFunSuite {
  /**
   * @param labels set of GBT labels
   * @param agg the aggregator to use
   * @return the calculator from aggregation on the labels
   */
  def computeCalculator(
      labels: Seq[Double],
      agg: ImpurityAggregator): ImpurityCalculator = {
    val stats = new Array[Double](agg.statsSize)
    labels.foreach(label => agg.update(stats, offset = 0, label, instanceWeight = 1))
    agg.getCalculator(stats, offset = 0)
  }

  /**
   * Makes sure that the given aggregator reports the expected impurity for a variety
   * of different label sets.
   *
   * @param actualAgg the aggregator to test
   * @param loss the loss function for the responses
   * @param expectedImpurity a function from the set of responses to the expected impurity
   * @param expectedPrediction a function from the triplet
   *                           (previous prediction, labels, responses) to the
   *                           the expected prediction
   */
  def verifyCalculator(
      actualAgg: ImpurityAggregator,
      loss: Loss,
      expectedImpurity: Seq[Double] => Double,
      expectedPrediction: (Double, Seq[Double], Seq[Double]) => Double): Unit = {
    val npoints = 6
    for (cutoff <- 0 to npoints) withClue(s"for cutoff $cutoff\n") {
      val labels = (0 until npoints).map(x => if (x < cutoff) -1.0 else 1.0)
      val prediction = 0
      val psuedoResiduals = labels.map(x => -loss.gradient(prediction, x))

      val calculator = computeCalculator(psuedoResiduals, actualAgg)
      withClue(s"for calculator $calculator\n") {
        assert(calculator.count === npoints)
        assert(calculator.calculate() ~== expectedImpurity(psuedoResiduals) absTol 1e-3)
        assert(calculator.predict ~==
            expectedPrediction(prediction, labels, psuedoResiduals) absTol 1e-3)
      }
    }
  }

  /**
   * Makes sure that a GBT is constructed in accordance with its impurity and loss function.
   *
   * @param spark the spark session
   * @param classification whether to use a [[GBTClassifier]] or [[GBTRegressor]]
   * @param impurityName name of the impurity to use
   * @param loss loss function for gradients
   * @param expectedImpurity expected impurity aggregator for statistics
   */
  def verifyGBTConstruction(
      spark: SparkSession,
      classification: Boolean,
      impurityName: String,
      loss: Loss,
      expectedImpurity: ImpurityAggregator): Unit = {
    // We create a dataset that can be optimally classified with a root tree
    // and one round of gradient boosting. The first tree will not be a perfect classifier,
    // so the leaf node predictions will differ for different impurity measures. This is expected
    // to be tested on depth-2 trees (7 nodes max). The generated trees should do no
    // sub-sampling.
    //
    // The error is slight enough to force a certain tree structure the first round,
    // but still give nontrivial results in both cases.

    val data = new ArrayBuffer[LabeledPoint]()

    // At depth-1, the trees separate 4 intervals.
    def addPoints(npoints: Int, label: Double, features: Vector): Unit = {
      for (_ <- 0 until npoints) {
        data += new LabeledPoint(label, features)
      }
    }

    // Adds 9 points of label 'label' and 1 point of label '1.0-label'
    def addMixedSection(label: Double, feature0: Double, feature1: Double): Unit = {
      val pointsPerSection = 10
      val offPoints = 1
      val features = Vectors.dense(feature0, feature1)
      val offFeatures = Vectors.dense(feature0, feature1)
      addPoints(pointsPerSection - offPoints, label, features)
      addPoints(offPoints, 1.0 - label, offFeatures)
    }

    for (feature0 <- Seq(0.0, 1.0); feature1 <- Seq(0.0, 1.0)) {
      val xor = if (feature0 == feature1) 0.0 else 1.0
      addMixedSection(label = xor, feature0, feature1)
    }
    addMixedSection(label = 0.0, feature0 = 0.0, feature1 = 0.0)
    addMixedSection(label = 1.0, feature0 = 0.0, feature1 = 1.0)
    addMixedSection(label = 1.0, feature0 = 1.0, feature1 = 0.0)
    addMixedSection(label = 0.0, feature0 = 1.0, feature1 = 1.0)

    // Make splitting on feature 0 slightly more attractive for an initial split
    // than the others by making it a slight decent identity predictor while keeping
    // other features uninformative. Note that feature 2's predictive power, when
    // conditioned on feature 0, is maintained.
    for (feature1 <- Seq(0.0, 1.0); label <- Seq(0.0, 1.0)) {
      addPoints(npoints = 5, label, Vectors.dense(label, feature1))
    }

    // Convert the input dataframe to a more convenient format to check our results against.
    val rawInput = spark.createDataFrame(data)
    val vectorAsArray = udf((v: Vector) => v.toArray)
    val input = rawInput.select(
      col("*"),
      vectorAsArray(col("features"))(0).as("feature0"),
      vectorAsArray(col("features"))(1).as("feature1"))

    // Classification/regression ambivalent tree retrieval
    val infoGain = 1.0 / data.size
    val (trees, treeWeights) = if (classification) {
      val model = new GBTClassifier()
        .setMaxDepth(2)
        .setMinInstancesPerNode(1)
        .setMinInfoGain(infoGain)
        .setSubsamplingRate(1.0)
        .setMaxIter(2)
        .setStepSize(1)
        .setLossType(impurityName)
        .fit(rawInput)
      (model.trees, model.treeWeights)
    } else {
      val model = new GBTRegressor()
        .setMaxDepth(2)
        .setMinInstancesPerNode(1)
        .setMinInfoGain(infoGain)
        .setSubsamplingRate(1.0)
        .setMaxIter(2)
        .setStepSize(1)
        .setLossType(impurityName)
        .fit(rawInput)
      (model.trees, model.treeWeights)
    }

    assert(trees.length === 2)
    assert(treeWeights === Array(1.0, 1.0))

    // A "feature" with index below 0 is a label
    def pointFilter(featureMap: Seq[(Int, Int)]): String = {
      if (featureMap.isEmpty) return "true"
      val sqlConditions = featureMap.map({
        case (idx, value) if idx < 0 => s"label = $value"
        case (idx, value) => s"feature$idx = $value"
      })
      sqlConditions.mkString(" and ")
    }

    var relabeledDF: DataFrame = null
    def trueCalculator(featureMap: (Int, Int)*) = {
      implicit val encoder = Encoders.scalaDouble
      val df = relabeledDF.where(pointFilter(featureMap))
      val labels = df.select("label").as[Double].collect()
      computeCalculator(labels, expectedImpurity)
    }

    def verifyImpurity(actualImpurity: ImpurityCalculator, featureMap: Seq[(Int, Int)]): Unit = {
      implicit val approxEquals = TolerantNumerics.tolerantDoubleEquality(1e-3)
      val expectedCalculator = trueCalculator(featureMap: _*)
      withClue(s"actualImpurity $actualImpurity\nexpectedImpurity $expectedCalculator\n\n") {
        assert(actualImpurity.count === expectedCalculator.count)
        assert(actualImpurity.calculate() ~== expectedCalculator.calculate() absTol 1e-3)
        assert(actualImpurity.predict ~== expectedCalculator.predict absTol 1e-3)
      }
    }

    def verifyInternalNode(node: Node, feature: Int, featureMap: (Int, Int)*): InternalNode = {
      withClue(s"node $node\n\nlocation ${featureMap.mkString(" ")}\n\n") {
        assert(node.isInstanceOf[InternalNode])
        val internal = node.asInstanceOf[InternalNode]
        assert(internal.split.featureIndex === feature)
        verifyImpurity(internal.impurityStats, featureMap)
        internal
      }
    }

    def verifyLeafNode(node: Node, featureMap: (Int, Int)*): Unit = {
      withClue(s"node $node\n\nlocation ${featureMap.mkString(" ")}\n\n") {
        assert(node.isInstanceOf[LeafNode])
        val leaf = node.asInstanceOf[LeafNode]
        verifyImpurity(leaf.impurityStats, featureMap)
      }
    }

    val oldLabel = input.withColumnRenamed("label", "oldlabel")
    val transformedLabel = if (classification) col("oldlabel") * 2 - 1 else col("oldlabel")
    relabeledDF = oldLabel.withColumn("label", transformedLabel)
    withClue(s"Tree 0:\n\n${trees.head.toDebugString}\n") {
      val root = verifyInternalNode(trees.head.rootNode, 0)
      val left = verifyInternalNode(root.leftChild, 1, 0 -> 0)
      val right = verifyInternalNode(root.rightChild, 1, 0 -> 1)
      verifyLeafNode(left.leftChild, 0 -> 0, 1 -> 0)
      verifyLeafNode(left.rightChild, 0 -> 0, 1 -> 1)
      verifyLeafNode(right.leftChild, 0 -> 1, 1 -> 0)
      verifyLeafNode(right.rightChild, 0 -> 1, 1 -> 1)
    }

    def computeGain(splitFeature: Int, featureMap: (Int, Int)*): Double = {
      val preSplit = trueCalculator(featureMap: _*)
      val postSplit = Seq(0, 1).map(value => {
        val half = trueCalculator((splitFeature -> value) +: featureMap: _*)
        half.calculate() * half.count
      }).sum / preSplit.count
      preSplit.calculate() - postSplit
    }

    // The second tree's structure is going to be sensitive to the actual loss
    val gradient = udf((pred: Double, label: Double) => -loss.gradient(pred, label))
    relabeledDF = trees.head.transform(oldLabel)
      .withColumn("label", gradient(col("prediction"), transformedLabel))
    withClue(s"Tree 1:\n\n${trees.last.toDebugString}\n") {
      val rootFeature = Seq(0, 1).maxBy(computeGain(_))
      if (computeGain(rootFeature) < infoGain) {
        verifyLeafNode(trees.last.rootNode)
        return
      }

      val root = verifyInternalNode(trees.last.rootNode, rootFeature)
      val otherFeature = 1 - rootFeature

      for (splitValue <- Seq(0, 1)) {
        val genericChild = if (splitValue == 0) root.leftChild else root.rightChild
        if (computeGain(otherFeature, rootFeature -> splitValue) < infoGain) {
          verifyLeafNode(genericChild, rootFeature -> splitValue)
        } else {
          val child = verifyInternalNode(genericChild, otherFeature, rootFeature -> splitValue)
          verifyLeafNode(child.leftChild, rootFeature -> splitValue, otherFeature -> 0)
          verifyLeafNode(child.rightChild, rootFeature -> splitValue, otherFeature -> 1)
        }
      }
    }
  }
}

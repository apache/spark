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

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.tree.TreeEnsembleParams
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.rdd.RDD

/**
 * Learning and dataset metadata for DecisionTree.
 *
 * @param weightedNumExamples  Weighted count of samples in the tree.
 * @param numClasses    For classification: labels can take values {0, ..., numClasses - 1}.
 *                      For regression: fixed at 0 (no meaning).
 * @param maxBins  Maximum number of bins, for all features.
 * @param featureArity  Map: categorical feature index to arity.
 *                      I.e., the feature takes values in {0, ..., arity - 1}.
 * @param numBins  Number of bins for each feature.
 * @param minWeightFractionPerNode  The minimum fraction of the total sample weight that must be
 *                                  present in a leaf node in order to be considered a valid split.
 */
private[spark] class DecisionTreeMetadata(
    val numFeatures: Int,
    val numExamples: Long,
    val weightedNumExamples: Double,
    val numClasses: Int,
    val maxBins: Int,
    val featureArity: Map[Int, Int],
    val unorderedFeatures: Set[Int],
    val numBins: Array[Int],
    val impurity: Impurity,
    val quantileStrategy: QuantileStrategy,
    val maxDepth: Int,
    val minInstancesPerNode: Int,
    val minWeightFractionPerNode: Double,
    val minInfoGain: Double,
    val numTrees: Int,
    val numFeaturesPerNode: Int) extends Serializable {

  def isUnordered(featureIndex: Int): Boolean = unorderedFeatures.contains(featureIndex)

  def isClassification: Boolean = numClasses >= 2

  def isMulticlass: Boolean = numClasses > 2

  def isMulticlassWithCategoricalFeatures: Boolean = isMulticlass && (featureArity.size > 0)

  def isCategorical(featureIndex: Int): Boolean = featureArity.contains(featureIndex)

  def isContinuous(featureIndex: Int): Boolean = !featureArity.contains(featureIndex)

  def minWeightPerNode: Double = minWeightFractionPerNode * weightedNumExamples

  /**
   * Number of splits for the given feature.
   * For unordered features, there is 1 bin per split.
   * For ordered features, there is 1 more bin than split.
   */
  def numSplits(featureIndex: Int): Int = if (isUnordered(featureIndex)) {
    numBins(featureIndex)
  } else {
    numBins(featureIndex) - 1
  }


  /**
   * Set number of splits for a continuous feature.
   * For a continuous feature, number of bins is number of splits plus 1.
   */
  def setNumSplits(featureIndex: Int, numSplits: Int): Unit = {
    require(isContinuous(featureIndex),
      s"Only number of bin for a continuous feature can be set.")
    numBins(featureIndex) = numSplits + 1
  }

  /**
   * Indicates if feature subsampling is being used.
   */
  def subsamplingFeatures: Boolean = numFeatures != numFeaturesPerNode

}

private[spark] object DecisionTreeMetadata extends Logging {

  /**
   * Construct a [[DecisionTreeMetadata]] instance for this dataset and parameters.
   * This computes which categorical features will be ordered vs. unordered,
   * as well as the number of splits and bins for each feature.
   */
  def buildMetadata(
      input: RDD[Instance],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String): DecisionTreeMetadata = {

    val numFeatures = input.map(_.features.size).take(1).headOption.getOrElse {
      throw new IllegalArgumentException(s"DecisionTree requires size of input RDD > 0, " +
        s"but was given by empty one.")
    }
    require(numFeatures > 0, s"DecisionTree requires number of features > 0, " +
      s"but was given an empty features vector")
    val (numExamples, weightSum) = input.aggregate((0L, 0.0))(
      seqOp = (cw, instance) => (cw._1 + 1L, cw._2 + instance.weight),
      combOp = (cw1, cw2) => (cw1._1 + cw2._1, cw1._2 + cw2._2)
    )

    val numClasses = strategy.algo match {
      case Classification => strategy.numClasses
      case Regression => 0
    }

    val maxPossibleBins = math.min(strategy.maxBins, numExamples).toInt
    if (maxPossibleBins < strategy.maxBins) {
      logWarning(log"DecisionTree reducing maxBins from " +
        log"${MDC(LogKeys.MAX_NUM_BINS, strategy.maxBins)} to " +
        log"${MDC(LogKeys.MAX_NUM_POSSIBLE_BINS, maxPossibleBins)} " +
        log"(= number of training instances)")
    }

    // We check the number of bins here against maxPossibleBins.
    // This needs to be checked here instead of in Strategy since maxPossibleBins can be modified
    // based on the number of training examples.
    if (strategy.categoricalFeaturesInfo.nonEmpty) {
      val maxCategoriesPerFeature = strategy.categoricalFeaturesInfo.values.max
      val maxCategory =
        strategy.categoricalFeaturesInfo.find(_._2 == maxCategoriesPerFeature).get._1
      require(maxCategoriesPerFeature <= maxPossibleBins,
        s"DecisionTree requires maxBins (= $maxPossibleBins) to be at least as large as the " +
        s"number of values in each categorical feature, but categorical feature $maxCategory " +
        s"has $maxCategoriesPerFeature values. Consider removing this and other categorical " +
        "features with a large number of values, or add more training examples.")
    }

    val unorderedFeatures = new mutable.HashSet[Int]()
    val numBins = Array.fill[Int](numFeatures)(maxPossibleBins)
    if (numClasses > 2) {
      // Multiclass classification
      val maxCategoriesForUnorderedFeature =
        ((math.log(maxPossibleBins / 2 + 1) / math.log(2.0)) + 1).floor.toInt
      strategy.categoricalFeaturesInfo.foreach { case (featureIndex, numCategories) =>
        // Hack: If a categorical feature has only 1 category, we treat it as continuous.
        // TODO(SPARK-9957): Handle this properly by filtering out those features.
        if (numCategories > 1) {
          // Decide if some categorical features should be treated as unordered features,
          //  which require 2 * ((1 << numCategories - 1) - 1) bins.
          // We do this check with log values to prevent overflows in case numCategories is large.
          // The next check is equivalent to: 2 * ((1 << numCategories - 1) - 1) <= maxBins
          if (numCategories <= maxCategoriesForUnorderedFeature) {
            unorderedFeatures.add(featureIndex)
            numBins(featureIndex) = numUnorderedBins(numCategories)
          } else {
            numBins(featureIndex) = numCategories
          }
        }
      }
    } else {
      // Binary classification or regression
      strategy.categoricalFeaturesInfo.foreach { case (featureIndex, numCategories) =>
        // If a categorical feature has only 1 category, we treat it as continuous: SPARK-9957
        if (numCategories > 1) {
          numBins(featureIndex) = numCategories
        }
      }
    }

    // Set number of features to use per node (for random forests).
    val _featureSubsetStrategy = featureSubsetStrategy match {
      case "auto" =>
        if (numTrees == 1) {
          "all"
        } else {
          if (strategy.algo == Classification) {
            "sqrt"
          } else {
            "onethird"
          }
        }
      case _ => featureSubsetStrategy
    }

    val numFeaturesPerNode: Int = _featureSubsetStrategy match {
      case "all" => numFeatures
      case "sqrt" => math.sqrt(numFeatures).ceil.toInt
      case "log2" => math.max(1, (math.log(numFeatures) / math.log(2)).ceil.toInt)
      case "onethird" => (numFeatures / 3.0).ceil.toInt
      case _ =>
        Try(_featureSubsetStrategy.toInt).filter(_ > 0).toOption match {
          case Some(value) => math.min(value, numFeatures)
          case None =>
            Try(_featureSubsetStrategy.toDouble).filter(_ > 0).filter(_ <= 1.0).toOption match {
              case Some(value) => math.ceil(value * numFeatures).toInt
              case _ => throw new IllegalArgumentException(s"Supported values:" +
                s" ${TreeEnsembleParams.supportedFeatureSubsetStrategies.mkString(", ")}," +
                s" (0.0-1.0], [1-n].")
            }
        }
    }

    new DecisionTreeMetadata(numFeatures, numExamples, weightSum, numClasses,
      numBins.max, strategy.categoricalFeaturesInfo, unorderedFeatures.toSet, numBins,
      strategy.impurity, strategy.quantileCalculationStrategy, strategy.maxDepth,
      strategy.minInstancesPerNode, strategy.minWeightFractionPerNode, strategy.minInfoGain,
      numTrees, numFeaturesPerNode)
  }

  /**
   * Version of [[DecisionTreeMetadata#buildMetadata]] for DecisionTree.
   */
  def buildMetadata(
      input: RDD[Instance],
      strategy: Strategy): DecisionTreeMetadata = {
    buildMetadata(input, strategy, numTrees = 1, featureSubsetStrategy = "all")
  }

    /**
   * Given the arity of a categorical feature (arity = number of categories),
   * return the number of bins for the feature if it is to be treated as an unordered feature.
   * There is 1 split for every partitioning of categories into 2 disjoint, non-empty sets;
   * there are math.pow(2, arity - 1) - 1 such splits.
   * Each split has 2 corresponding bins.
   */
  def numUnorderedBins(arity: Int): Int = (1 << arity - 1) - 1

}

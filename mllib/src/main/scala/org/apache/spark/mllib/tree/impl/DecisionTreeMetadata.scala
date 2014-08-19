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

package org.apache.spark.mllib.tree.impl

import scala.collection.mutable

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD


/*
 * TODO: Add doc about ordered vs. unordered features.
 * Ensure numBins is always greater than the categories. For multiclass classification,
 * numBins should be greater than math.pow(2, maxCategories - 1) - 1.
 * It's a limitation of the current implementation but a reasonable trade-off since features
 * with large number of categories get favored over continuous features.
 *
 * This needs to be checked here instead of in Strategy since numBins can be determined
 * by the number of training examples.
 */


/**
 * Learning and dataset metadata for DecisionTree.
 *
 * @param numClasses    For classification: labels can take values {0, ..., numClasses - 1}.
 *                      For regression: fixed at 0 (no meaning).
 * @param featureArity  Map: categorical feature index --> arity.
 *                      I.e., the feature takes values in {0, ..., arity - 1}.
 * @param numBins  numBins(featureIndex) = number of bins for feature
 */
private[tree] class DecisionTreeMetadata(
    val numFeatures: Int,
    val numExamples: Long,
    val numClasses: Int,
    val maxBins: Int,
    val featureArity: Map[Int, Int],
    val unorderedFeatures: Set[Int],
    val numBins: Array[Int],
    val impurity: Impurity,
    val quantileStrategy: QuantileStrategy) extends Serializable {

  def isUnordered(featureIndex: Int): Boolean = unorderedFeatures.contains(featureIndex)

  def isClassification: Boolean = numClasses >= 2

  def isMulticlass: Boolean = numClasses > 2

  def isMulticlassWithCategoricalFeatures: Boolean = isMulticlass && (featureArity.size > 0)

  def isCategorical(featureIndex: Int): Boolean = featureArity.contains(featureIndex)

  def isContinuous(featureIndex: Int): Boolean = !featureArity.contains(featureIndex)

  def numSplits(featureIndex: Int): Int = if (isUnordered(featureIndex)) {
    numBins(featureIndex)
  } else {
    numBins(featureIndex) - 1
  }

}

private[tree] object DecisionTreeMetadata {

  def buildMetadata(input: RDD[LabeledPoint], strategy: Strategy): DecisionTreeMetadata = {

    val numFeatures = input.take(1)(0).features.size
    val numExamples = input.count()
    val numClasses = strategy.algo match {
      case Classification => strategy.numClassesForClassification
      case Regression => 0
    }

    val maxPossibleBins = math.min(strategy.maxBins, numExamples).toInt
    val log2MaxPossibleBinsp1 = math.log(maxPossibleBins + 1) / math.log(2.0)

    val unorderedFeatures = new mutable.HashSet[Int]()
    val numBins = Array.fill[Int](numFeatures)(maxPossibleBins)
    if (numClasses > 2) {
      strategy.categoricalFeaturesInfo.foreach { case (f, k) =>
        if (k - 1 < log2MaxPossibleBinsp1) {
          // Note: The above check is equivalent to checking:
          //       numUnorderedBins = (1 << k - 1) - 1 < maxBins
          unorderedFeatures.add(f)
          numBins(f) = numUnorderedBins(k)
        } else {
          // TODO: Check the below k <= maxBins.
          //       Checking k <= maxPossibleBins should work.
          //       However, there may have been a 1-off error later on allocating 1 extra
          //       (unused) bin.
          // TODO: Allow this case, where we simply will know nothing about some categories?
          require(k <= maxPossibleBins,
            s"maxBins (= $maxPossibleBins) should be greater than max categories " +
            s"in categorical features (>= $k)")
          numBins(f) = k
        }
      }
    } else {
      strategy.categoricalFeaturesInfo.foreach { case (f, k) =>
        require(k <= maxPossibleBins,
          s"DecisionTree requires maxBins (= $maxPossibleBins) >= max categories " +
          s"in categorical features (= ${strategy.categoricalFeaturesInfo.values.max})")
        numBins(f) = k
      }
    }

    new DecisionTreeMetadata(numFeatures, numExamples, numClasses, numBins.max,
      strategy.categoricalFeaturesInfo, unorderedFeatures.toSet, numBins,
      strategy.impurity, strategy.quantileCalculationStrategy)
  }

  /**
   * Given the arity of a categorical feature (arity = number of categories),
   * return the number of bins for the feature if it is to be treated as an unordered feature.
   */
  def numUnorderedBins(arity: Int): Int = {
    (1 << arity - 1) - 1
  }

}

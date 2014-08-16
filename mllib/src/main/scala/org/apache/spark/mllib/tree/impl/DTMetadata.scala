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
import org.apache.spark.rdd.RDD


/**
 * Learning and dataset metadata for DecisionTree.
 *
 * @param featureArity  Map: categorical feature index --> arity.
 *                      I.e., the feature takes values in {0, ..., arity - 1}.
 */
private[tree] class DTMetadata(
    val numFeatures: Int,
    val numExamples: Long,
    val numClasses: Int,
    val maxBins: Int,
    val featureArity: Map[Int, Int],
    val unorderedFeatures: Set[Int],
    val impurity: Impurity,
    val quantileStrategy: QuantileStrategy) extends Serializable {

  def isUnordered(featureIndex: Int): Boolean = unorderedFeatures.contains(featureIndex)

  def isClassification: Boolean = numClasses >= 2

  def isMulticlass: Boolean = numClasses > 2

  def isMulticlassWithCategoricalFeatures: Boolean = isMulticlass && (featureArity.size > 0)

  def isCategorical(featureIndex: Int): Boolean = featureArity.contains(featureIndex)

  def isContinuous(featureIndex: Int): Boolean = !featureArity.contains(featureIndex)

}

private[tree] object DTMetadata {

  def buildMetadata(input: RDD[LabeledPoint], strategy: Strategy): DTMetadata = {

    val numFeatures = input.take(1)(0).features.size
    val numExamples = input.count()
    val numClasses = strategy.algo match {
      case Classification => strategy.numClassesForClassification
      case Regression => 0
    }

    val maxBins = math.min(strategy.maxBins, numExamples).toInt

    val unorderedFeatures = new mutable.HashSet[Int]()
    if (numClasses > 2) {
      strategy.categoricalFeaturesInfo.foreach { case (f, k) =>
        val numUnorderedBins = math.pow(2, k - 1) - 1
        if (numUnorderedBins < maxBins) {
          unorderedFeatures.add(f)
        } else {
          // TODO: Allow this case, where we simply will know nothing about some categories?
          require(k < maxBins, "maxBins should be greater than max categories " +
            "in categorical features")
        }
      }
    } else {
      strategy.categoricalFeaturesInfo.foreach { case (f, k) =>
        require(k < maxBins, "maxBins should be greater than max categories " +
          "in categorical features")
      }
    }

    new DTMetadata(numFeatures, numExamples, numClasses, maxBins,
      strategy.categoricalFeaturesInfo, unorderedFeatures.toSet,
      strategy.impurity, strategy.quantileCalculationStrategy)
  }

}

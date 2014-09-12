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

package org.apache.spark.mllib.tree.configuration

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.tree.impurity.{Variance, Entropy, Gini, Impurity}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._

/**
 * :: Experimental ::
 * Stores all the configuration options for tree construction
 * @param algo  Learning goal.  Supported:
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
 * @param impurity Criterion used for information gain calculation.
 *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
 *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
 *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
 * @param maxDepth Maximum depth of the tree.
 *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
 * @param numClassesForClassification Number of classes for classification.
 *                                    (Ignored for regression.)
 *                                    Default value is 2 (binary classification).
 * @param maxBins Maximum number of bins used for discretizing continuous features and
 *                for choosing how to split on features at each node.
 *                More bins give higher granularity.
 * @param quantileCalculationStrategy Algorithm for calculating quantiles.  Supported:
   *                             [[org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort]]
 * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
 *                                number of discrete values they take. For example, an entry (n ->
 *                                k) implies the feature n is categorical with k categories 0,
 *                                1, 2, ... , k-1. It's important to note that features are
 *                                zero-indexed.
 * @param minInstancesPerNode Minimum number of instances each child must have after split.
 *                            Default value is 1. If a split cause left or right child
 *                            to have less than minInstancesPerNode,
 *                            this split will not be considered as a valid split.
 * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
 *                    If a split has less information gain than minInfoGain,
 *                    this split will not be considered as a valid split.
 * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is
 *                      256 MB.
 */
@Experimental
class Strategy (
    val algo: Algo,
    val impurity: Impurity,
    val maxDepth: Int,
    val numClassesForClassification: Int = 2,
    val maxBins: Int = 32,
    val quantileCalculationStrategy: QuantileStrategy = Sort,
    val categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int](),
    val minInstancesPerNode: Int = 1,
    val minInfoGain: Double = 0.0,
    val maxMemoryInMB: Int = 256) extends Serializable {

  if (algo == Classification) {
    require(numClassesForClassification >= 2)
  }
  val isMulticlassClassification =
    algo == Classification && numClassesForClassification > 2
  val isMulticlassWithCategoricalFeatures
    = isMulticlassClassification && (categoricalFeaturesInfo.size > 0)

  /**
   * Java-friendly constructor for [[org.apache.spark.mllib.tree.configuration.Strategy]]
   */
  def this(
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClassesForClassification: Int,
      maxBins: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]) {
    this(algo, impurity, maxDepth, numClassesForClassification, maxBins, Sort,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
  }

  /**
   * Check validity of parameters.
   * Throws exception if invalid.
   */
  private[tree] def assertValid(): Unit = {
    algo match {
      case Classification =>
        require(numClassesForClassification >= 2,
          s"DecisionTree Strategy for Classification must have numClassesForClassification >= 2," +
          s" but numClassesForClassification = $numClassesForClassification.")
        require(Set(Gini, Entropy).contains(impurity),
          s"DecisionTree Strategy given invalid impurity for Classification: $impurity." +
          s"  Valid settings: Gini, Entropy")
      case Regression =>
        require(impurity == Variance,
          s"DecisionTree Strategy given invalid impurity for Regression: $impurity." +
          s"  Valid settings: Variance")
      case _ =>
        throw new IllegalArgumentException(
          s"DecisionTree Strategy given invalid algo parameter: $algo." +
          s"  Valid settings are: Classification, Regression.")
    }
    require(maxDepth >= 0, s"DecisionTree Strategy given invalid maxDepth parameter: $maxDepth." +
      s"  Valid values are integers >= 0.")
    require(maxBins >= 2, s"DecisionTree Strategy given invalid maxBins parameter: $maxBins." +
      s"  Valid values are integers >= 2.")
    categoricalFeaturesInfo.foreach { case (feature, arity) =>
      require(arity >= 2,
        s"DecisionTree Strategy given invalid categoricalFeaturesInfo setting:" +
        s" feature $feature has $arity categories.  The number of categories should be >= 2.")
    }
  }

}

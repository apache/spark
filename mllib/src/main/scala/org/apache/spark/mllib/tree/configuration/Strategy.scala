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

import scala.beans.BeanProperty
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
 * @param numClasses Number of classes for classification.
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
 * @param subsamplingRate Fraction of the training data used for learning decision tree.
 * @param useNodeIdCache If this is true, instead of passing trees to executors, the algorithm will
 *                      maintain a separate RDD of node Id cache for each row.
 * @param checkpointDir If the node Id cache is used, it will help to checkpoint
 *                      the node Id cache periodically. This is the checkpoint directory
 *                      to be used for the node Id cache.
 * @param checkpointInterval How often to checkpoint when the node Id cache gets updated.
 *                           E.g. 10 means that the cache will get checkpointed every 10 updates.
 */
@Experimental
class Strategy (
    @BeanProperty var algo: Algo,
    @BeanProperty var impurity: Impurity,
    @BeanProperty var maxDepth: Int,
    @BeanProperty var numClasses: Int = 2,
    @BeanProperty var maxBins: Int = 32,
    @BeanProperty var quantileCalculationStrategy: QuantileStrategy = Sort,
    @BeanProperty var categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int](),
    @BeanProperty var minInstancesPerNode: Int = 1,
    @BeanProperty var minInfoGain: Double = 0.0,
    @BeanProperty var maxMemoryInMB: Int = 256,
    @BeanProperty var subsamplingRate: Double = 1,
    @BeanProperty var useNodeIdCache: Boolean = false,
    @BeanProperty var checkpointDir: Option[String] = None,
    @BeanProperty var checkpointInterval: Int = 10) extends Serializable {

  def isMulticlassClassification =
    algo == Classification && numClasses > 2
  def isMulticlassWithCategoricalFeatures
    = isMulticlassClassification && (categoricalFeaturesInfo.size > 0)

  /**
   * Java-friendly constructor for [[org.apache.spark.mllib.tree.configuration.Strategy]]
   */
  def this(
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int,
      maxBins: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]) {
    this(algo, impurity, maxDepth, numClasses, maxBins, Sort,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
  }

  /**
   * Sets Algorithm using a String.
   */
  def setAlgo(algo: String): Unit = algo match {
    case "Classification" => setAlgo(Classification)
    case "Regression" => setAlgo(Regression)
  }

  /**
   * Sets categoricalFeaturesInfo using a Java Map.
   */
  def setCategoricalFeaturesInfo(
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer]): Unit = {
    this.categoricalFeaturesInfo =
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap
  }

  /**
   * Check validity of parameters.
   * Throws exception if invalid.
   */
  private[tree] def assertValid(): Unit = {
    algo match {
      case Classification =>
        require(numClasses >= 2,
          s"DecisionTree Strategy for Classification must have numClasses >= 2," +
          s" but numClasses = $numClasses.")
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
    require(minInstancesPerNode >= 1,
      s"DecisionTree Strategy requires minInstancesPerNode >= 1 but was given $minInstancesPerNode")
    require(maxMemoryInMB <= 10240,
      s"DecisionTree Strategy requires maxMemoryInMB <= 10240, but was given $maxMemoryInMB")
    require(subsamplingRate > 0 && subsamplingRate <= 1,
      s"DecisionTree Strategy requires subsamplingRate <=1 and >0, but was given " +
      s"$subsamplingRate")
  }

  /** Returns a shallow copy of this instance. */
  def copy: Strategy = {
    new Strategy(algo, impurity, maxDepth, numClasses, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo, minInstancesPerNode, minInfoGain,
      maxMemoryInMB, subsamplingRate, useNodeIdCache, checkpointDir, checkpointInterval)
  }
}

@Experimental
object Strategy {

  /**
   * Construct a default set of parameters for [[org.apache.spark.mllib.tree.DecisionTree]]
   * @param algo  "Classification" or "Regression"
   */
  def defaultStrategy(algo: String): Strategy = {
    defaultStategy(Algo.fromString(algo))
  }

  /**
   * Construct a default set of parameters for [[org.apache.spark.mllib.tree.DecisionTree]]
   * @param algo Algo.Classification or Algo.Regression
   */
  def defaultStategy(algo: Algo): Strategy = algo match {
    case Algo.Classification =>
      new Strategy(algo = Classification, impurity = Gini, maxDepth = 10,
        numClasses = 2)
    case Algo.Regression =>
      new Strategy(algo = Regression, impurity = Variance, maxDepth = 10,
        numClasses = 0)
  }
}

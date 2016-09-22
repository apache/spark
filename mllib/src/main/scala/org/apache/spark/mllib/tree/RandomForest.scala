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

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.ml.tree.{DecisionTreeModel => NewDTModel, RandomForestParams => NewRFParams}
import org.apache.spark.ml.tree.impl.{RandomForest => NewRandomForest}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity.Impurities
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
 * A class that implements a [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]]
 * learning algorithm for classification and regression.
 * It supports both continuous and categorical features.
 *
 * The settings for featureSubsetStrategy are based on the following references:
 *  - log2: tested in Breiman (2001)
 *  - sqrt: recommended by Breiman manual for random forests
 *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
 *    package.
 *
 * @see [[http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf  Breiman (2001)]]
 * @see [[http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf  Breiman manual for
 *     random forests]]
 * @param strategy The configuration parameters for the random forest algorithm which specify
 *                 the type of random forest (classification or regression), feature type
 *                 (continuous, categorical), depth of the tree, quantile calculation strategy,
 *                 etc.
 * @param numTrees If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
 * @param featureSubsetStrategy Number of features to consider for splits at each node.
 *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
 *                              Supported numerical values: "(0.0-1.0]", "[1-n]".
 *                              If "auto" is set, this parameter is set based on numTrees:
 *                                if numTrees == 1, set to "all";
 *                                if numTrees > 1 (forest) set to "sqrt" for classification and
 *                                  to "onethird" for regression.
 *                              If a real value "n" in the range (0, 1.0] is set,
 *                                use n * number of features.
 *                              If an integer value "n" in the range (1, num features) is set,
 *                                use n features.
 * @param seed Random seed for bootstrapping and choosing feature subsets.
 */
private class RandomForest (
    private val strategy: Strategy,
    private val numTrees: Int,
    featureSubsetStrategy: String,
    private val seed: Int)
  extends Serializable with Logging {

  strategy.assertValid()
  require(numTrees > 0, s"RandomForest requires numTrees > 0, but was given numTrees = $numTrees.")
  require(RandomForest.supportedFeatureSubsetStrategies.contains(featureSubsetStrategy)
    || Try(featureSubsetStrategy.toInt).filter(_ > 0).isSuccess
    || Try(featureSubsetStrategy.toDouble).filter(_ > 0).filter(_ <= 1.0).isSuccess,
    s"RandomForest given invalid featureSubsetStrategy: $featureSubsetStrategy." +
    s" Supported values: ${NewRFParams.supportedFeatureSubsetStrategies.mkString(", ")}," +
    s" (0.0-1.0], [1-n].")

  /**
   * Method to train a decision tree model over an RDD
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return RandomForestModel that can be used for prediction.
   */
  def run(input: RDD[LabeledPoint]): RandomForestModel = {
    val trees: Array[NewDTModel] = NewRandomForest.run(input.map(_.asML), strategy, numTrees,
      featureSubsetStrategy, seed.toLong, None)
    new RandomForestModel(strategy.algo, trees.map(_.toOld))
  }

}

@Since("1.2.0")
object RandomForest extends Serializable with Logging {

  /**
   * Method to train a decision tree model for binary or multiclass classification.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels should take values {0, 1, ..., numClasses-1}.
   * @param strategy Parameters for training each tree in the forest.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "sqrt".
   * @param seed Random seed for bootstrapping and choosing feature subsets.
   * @return RandomForestModel that can be used for prediction.
   */
  @Since("1.2.0")
  def trainClassifier(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Int): RandomForestModel = {
    require(strategy.algo == Classification,
      s"RandomForest.trainClassifier given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }

  /**
   * Method to train a decision tree model for binary or multiclass classification.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels should take values {0, 1, ..., numClasses-1}.
   * @param numClasses Number of classes for classification.
   * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n -> k)
   *                                indicates that feature n is categorical with k categories
   *                                indexed from 0: {0, 1, ..., k-1}.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "sqrt".
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "gini" (recommended) or "entropy".
   * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
   *                 1 internal node + 2 leaf nodes).
   *                 (suggested value: 4)
   * @param maxBins Maximum number of bins used for splitting features
   *                (suggested value: 100)
   * @param seed Random seed for bootstrapping and choosing feature subsets.
   * @return RandomForestModel that can be used for prediction.
   */
  @Since("1.2.0")
  def trainClassifier(
      input: RDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Classification, impurityType, maxDepth,
      numClasses, maxBins, Sort, categoricalFeaturesInfo)
    trainClassifier(input, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.RandomForest$#trainClassifier]]
   */
  @Since("1.2.0")
  def trainClassifier(
      input: JavaRDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int): RandomForestModel = {
    trainClassifier(input.rdd, numClasses,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param strategy Parameters for training each tree in the forest.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "onethird".
   * @param seed Random seed for bootstrapping and choosing feature subsets.
   * @return RandomForestModel that can be used for prediction.
   */
  @Since("1.2.0")
  def trainRegressor(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Int): RandomForestModel = {
    require(strategy.algo == Regression,
      s"RandomForest.trainRegressor given Strategy with invalid algo: ${strategy.algo}")
    val rf = new RandomForest(strategy, numTrees, featureSubsetStrategy, seed)
    rf.run(input)
  }

  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n -> k)
   *                                indicates that feature n is categorical with k categories
   *                                indexed from 0: {0, 1, ..., k-1}.
   * @param numTrees Number of trees in the random forest.
   * @param featureSubsetStrategy Number of features to consider for splits at each node.
   *                              Supported values: "auto", "all", "sqrt", "log2", "onethird".
   *                              If "auto" is set, this parameter is set based on numTrees:
   *                                if numTrees == 1, set to "all";
   *                                if numTrees > 1 (forest) set to "onethird".
   * @param impurity Criterion used for information gain calculation.
   *                 The only supported value for regression is "variance".
   * @param maxDepth Maximum depth of the tree. (e.g., depth 0 means 1 leaf node, depth 1 means
   *                 1 internal node + 2 leaf nodes).
   *                 (suggested value: 4)
   * @param maxBins Maximum number of bins used for splitting features.
   *                (suggested value: 100)
   * @param seed Random seed for bootstrapping and choosing feature subsets.
   * @return RandomForestModel that can be used for prediction.
   */
  @Since("1.2.0")
  def trainRegressor(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt()): RandomForestModel = {
    val impurityType = Impurities.fromString(impurity)
    val strategy = new Strategy(Regression, impurityType, maxDepth,
      0, maxBins, Sort, categoricalFeaturesInfo)
    trainRegressor(input, strategy, numTrees, featureSubsetStrategy, seed)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.RandomForest$#trainRegressor]]
   */
  @Since("1.2.0")
  def trainRegressor(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int): RandomForestModel = {
    trainRegressor(input.rdd,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
  }

  /**
   * List of supported feature subset sampling strategies.
   */
  @Since("1.2.0")
  val supportedFeatureSubsetStrategies: Array[String] = NewRFParams.supportedFeatureSubsetStrategies
}

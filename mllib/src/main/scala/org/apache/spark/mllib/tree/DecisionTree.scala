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

import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD


/**
 * A class which implements a decision tree learning algorithm for classification and regression.
 * It supports both continuous and categorical features.
 *
 * @param strategy The configuration parameters for the tree algorithm which specify the type
 *                 of decision tree (classification or regression), feature type (continuous,
 *                 categorical), depth of the tree, quantile calculation strategy, etc.
 * @param seed Random seed.
 */
@Since("1.0.0")
class DecisionTree private[spark] (private val strategy: Strategy, private val seed: Int)
  extends Serializable with Logging {

  /**
   * @param strategy The configuration parameters for the tree algorithm which specify the type
   *                 of decision tree (classification or regression), feature type (continuous,
   *                 categorical), depth of the tree, quantile calculation strategy, etc.
   */
  @Since("1.0.0")
  def this(strategy: Strategy) = this(strategy, seed = 0)

  strategy.assertValid()

  /**
   * Method to train a decision tree model over an RDD
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return DecisionTreeModel that can be used for prediction.
   */
  @Since("1.2.0")
  def run(input: RDD[LabeledPoint]): DecisionTreeModel = {
    val rf = new RandomForest(strategy, numTrees = 1, featureSubsetStrategy = "all", seed = seed)
    val rfModel = rf.run(input)
    rfModel.trees(0)
  }
}

@Since("1.0.0")
object DecisionTree extends Serializable with Logging {

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The configuration parameters for the tree algorithm which specify the type
   *                 of decision tree (classification or regression), feature type (continuous,
   *                 categorical), depth of the tree, quantile calculation strategy, etc.
   * @return DecisionTreeModel that can be used for prediction.
   */
  @Since("1.0.0")
  def train(input: RDD[LabeledPoint], strategy: Strategy): DecisionTreeModel = {
    new DecisionTree(strategy).run(input)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo Type of decision tree, either classification or regression.
   * @param impurity Criterion used for information gain calculation.
   * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
   *                 1 internal node + 2 leaf nodes).
   * @return DecisionTreeModel that can be used for prediction.
   */
  @Since("1.0.0")
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int): DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth)
    new DecisionTree(strategy).run(input)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo Type of decision tree, either classification or regression.
   * @param impurity Criterion used for information gain calculation.
   * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
   *                 1 internal node + 2 leaf nodes).
   * @param numClasses Number of classes for classification. Default value of 2.
   * @return DecisionTreeModel that can be used for prediction.
   */
  @Since("1.2.0")
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int): DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, numClasses)
    new DecisionTree(strategy).run(input)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo Type of decision tree, either classification or regression.
   * @param impurity Criterion used for information gain calculation.
   * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
   *                 1 internal node + 2 leaf nodes).
   * @param numClasses Number of classes for classification. Default value of 2.
   * @param maxBins Maximum number of bins used for splitting features.
   * @param quantileCalculationStrategy  Algorithm for calculating quantiles.
   * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n -> k)
   *                                indicates that feature n is categorical with k categories
   *                                indexed from 0: {0, 1, ..., k-1}.
   * @return DecisionTreeModel that can be used for prediction.
   */
  @Since("1.0.0")
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int,
      maxBins: Int,
      quantileCalculationStrategy: QuantileStrategy,
      categoricalFeaturesInfo: Map[Int, Int]): DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, numClasses, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo)
    new DecisionTree(strategy).run(input)
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
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "gini" (recommended) or "entropy".
   * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
   *                 1 internal node + 2 leaf nodes).
   *                 (suggested value: 5)
   * @param maxBins Maximum number of bins used for splitting features.
   *                (suggested value: 32)
   * @return DecisionTreeModel that can be used for prediction.
   */
  @Since("1.1.0")
  def trainClassifier(
      input: RDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    val impurityType = Impurities.fromString(impurity)
    train(input, Classification, impurityType, maxDepth, numClasses, maxBins, Sort,
      categoricalFeaturesInfo)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   */
  @Since("1.1.0")
  def trainClassifier(
      input: JavaRDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    trainClassifier(input.rdd, numClasses,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      impurity, maxDepth, maxBins)
  }

  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param categoricalFeaturesInfo Map storing arity of categorical features. An entry (n -> k)
   *                                indicates that feature n is categorical with k categories
   *                                indexed from 0: {0, 1, ..., k-1}.
   * @param impurity Criterion used for information gain calculation.
   *                 The only supported value for regression is "variance".
   * @param maxDepth Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
   *                 1 internal node + 2 leaf nodes).
   *                 (suggested value: 5)
   * @param maxBins Maximum number of bins used for splitting features.
   *                (suggested value: 32)
   * @return DecisionTreeModel that can be used for prediction.
   */
  @Since("1.1.0")
  def trainRegressor(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    val impurityType = Impurities.fromString(impurity)
    train(input, Regression, impurityType, maxDepth, 0, maxBins, Sort, categoricalFeaturesInfo)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   */
  @Since("1.1.0")
  def trainRegressor(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    trainRegressor(input.rdd,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      impurity, maxDepth, maxBins)
  }
}

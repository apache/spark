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

package org.apache.spark.mllib.impl.tree

import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


/**
 * Trait which provides Java-compatible run() methods for Regressors and Classifiers.
 * These methods are somewhat specific to trees since they take the categoricalFeatures param.
 * @tparam M  Concrete class implementing this trait.
 */
private[mllib] abstract class TreeEstimator[M] {

  /**
   * Run this algorithm to train a new model using the given training data.
   * @param input  Training dataset
   * @param categoricalFeatures  Map storing the arity of categorical features.
   *          E.g., an entry (j -> k) indicates that feature j is categorical
   *          with k categories indexed from 0: {0, 1, ..., k-1}.
   *          (default = empty, i.e., all features are numerical)
   * @return Learned model
   * @group run
   */
  def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int] = Map.empty[Int, Int]): M

  /**
   * Java-compatible version of [[run()]].
   * @group run
   */
  def run(input: JavaRDD[LabeledPoint]): M = {
    run(input.rdd)
  }

  /**
   * Java-compatible version of [[run()]].
   * @group run
   */
  def run(
      input: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer]): M = {
    run(input.rdd, categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
  }
}

/**
 * Trait which provides Java-compatible run() methods for Classifiers.
 * These methods are somewhat specific to trees since they take the categoricalFeatures param.
 * @tparam M  Concrete class implementing this trait.
 */
private[mllib] abstract class TreeClassifier[M] extends TreeEstimator[M] {

  /**
   * Run this algorithm to train a new model using the given training data.
   * @param input  Training dataset
   * @param categoricalFeatures  Map storing the arity of categorical features.
   *          E.g., an entry (j -> k) indicates that feature j is categorical
   *          with k categories indexed from 0: {0, 1, ..., k-1}.
   *          (default = empty, i.e., all features are numerical)
   * @param numClasses  Number of classes the label can take,
   *                    indexed from 0: {0, 1, ..., numClasses-1}.
   *                    (default = 2, i.e., binary classification)
   * @return Learned model
   * @group run
   */
  def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): M

  /**
   * Run this algorithm to train a new model using the given training data.
   * This variant of [[run()]] is for binary classification (numClasses = 2).
   * @param input  Training dataset
   * @param categoricalFeatures  Map storing the arity of categorical features.
   *          E.g., an entry (j -> k) indicates that feature j is categorical
   *          with k categories indexed from 0: {0, 1, ..., k-1}.
   *          (default = empty, i.e., all features are numerical)
   * @return Learned model
   * @group run
   */
  override def run(
      input: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int] = Map.empty[Int, Int]): M = {
    run(input, categoricalFeatures, numClasses = 2)
  }

  /**
   * Java-compatible version of [[run()]].
   * @group run
   */
  def run(
      input: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer],
      numClasses: Int): M = {
    run(input.rdd, categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      numClasses)
  }
}

/**
 * Trait which provides Java-compatible run() methods for Regressors.
 * These methods are somewhat specific to trees since they take the categoricalFeatures param.
 * @tparam M  Concrete class implementing this trait.
 */
private[mllib] abstract class TreeRegressor[M] extends TreeEstimator[M]

/** Version of [[TreeEstimator]] for algorithms which support runWithValidation(). */
private[mllib] trait TreeEstimatorWithValidate[M] {

  /**
   * Run this algorithm to train a new model using the given training data.
   * @param input  Training dataset
   * @param validationInput Validation dataset.
   *                        This dataset should be different from the training dataset,
   *                        but it should follow the same distribution.
   *                        E.g., these two datasets could be created from an original dataset
   *                        by using [[org.apache.spark.rdd.RDD.randomSplit()]]
   * @param categoricalFeatures  Map storing the arity of categorical features.
   *          E.g., an entry (j -> k) indicates that feature j is categorical
   *          with k categories indexed from 0: {0, 1, ..., k-1}.
   *          (default = empty, i.e., all features are numerical)
   * @return Learned model
   * @group run
   */
  def runWithValidation(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int] = Map.empty[Int, Int]): M

  /**
   * Java-compatible version of [[runWithValidation()]].
   * @group run
   */
  def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint]): M

  /**
   * Java-compatible version of [[runWithValidation()]].
   * @group run
   */
  def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer]): M
}

/** Version of [[TreeClassifier]] for algorithms which support runWithValidation(). */
private[mllib] abstract class TreeClassifierWithValidate[M]
  extends TreeClassifier[M]
  with TreeEstimatorWithValidate[M] {

  /**
   * Run this algorithm to train a new model using the given training data.
   * @param input  Training dataset
   * @param categoricalFeatures  Map storing the arity of categorical features.
   *          E.g., an entry (j -> k) indicates that feature j is categorical
   *          with k categories indexed from 0: {0, 1, ..., k-1}.
   *          (default = empty, i.e., all features are numerical)
   * @param numClasses  Number of classes the label can take,
   *                    indexed from 0: {0, 1, ..., numClasses-1}.
   *                    (default = 2, i.e., binary classification)
   * @return Learned model
   * @group run
   */
  def runWithValidation(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): M

  /**
   * Run this algorithm to train a new model using the given training data.
   * This variant of [[run()]] is for binary classification (numClasses = 2).
   * @param input  Training dataset
   * @param categoricalFeatures  Map storing the arity of categorical features.
   *          E.g., an entry (j -> k) indicates that feature j is categorical
   *          with k categories indexed from 0: {0, 1, ..., k-1}.
   *          (default = empty, i.e., all features are numerical)
   * @return Learned model
   * @group run
   */
  override def runWithValidation(
      input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int] = Map.empty[Int, Int]): M = {
    runWithValidation(input, validationInput, categoricalFeatures, numClasses = 2)
  }

  override def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint]): M = {
    runWithValidation(input.rdd, validationInput.rdd)
  }

  override def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer]): M = {
    runWithValidation(input.rdd, validationInput.rdd,
      categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
  }

  /**
   * Java-compatible version of [[run()]].
   * @group run
   */
  def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer],
      numClasses: Int): M = {
    runWithValidation(input.rdd, validationInput.rdd,
      categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap, numClasses)
  }
}

/** Version of [[TreeRegressor]] for algorithms which support runWithValidation(). */
private[mllib] abstract class TreeRegressorWithValidate[M]
  extends TreeRegressor[M]
  with TreeEstimatorWithValidate[M] {

  override def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint]): M = {
    runWithValidation(input.rdd, validationInput.rdd)
  }

  override def runWithValidation(
      input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint],
      categoricalFeatures: java.util.Map[java.lang.Integer, java.lang.Integer]): M = {
    runWithValidation(input.rdd, validationInput.rdd,
      categoricalFeatures.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap)
  }
}

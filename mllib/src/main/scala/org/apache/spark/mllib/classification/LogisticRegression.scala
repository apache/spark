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

package org.apache.spark.mllib.classification

import scala.math.round

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.util.DataValidators

import org.jblas.DoubleMatrix

/**
 * Classification model trained using Logistic Regression.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class LogisticRegressionModel(
    override val weights: Array[Double],
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept)
  with ClassificationModel with Serializable {

  override def predictPoint(dataMatrix: DoubleMatrix, weightMatrix: DoubleMatrix,
      intercept: Double) = {
    val margin = dataMatrix.mmul(weightMatrix).get(0) + intercept
    round(1.0/ (1.0 + math.exp(margin * -1)))
  }
}

/**
 * Train a classification model for Logistic Regression using Stochastic Gradient Descent.
 * NOTE: Labels used in Logistic Regression should be {0, 1}
 */
class LogisticRegressionWithSGD private (
    var stepSize: Double,
    var numIterations: Int,
    var regParam: Double,
    var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel]
  with Serializable {

  val gradient = new LogisticGradient()
  val updater = new SimpleUpdater()
  override val optimizer = new GradientDescent(gradient, updater)
      .setStepSize(stepSize)
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setMiniBatchFraction(miniBatchFraction)
  override val validators = List(DataValidators.classificationLabels)

  /**
   * Construct a LogisticRegression object with default parameters
   */
  def this() = this(1.0, 100, 0.0, 1.0)

  def createModel(weights: Array[Double], intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling Logistic Regression.
 * NOTE: Labels used in Logistic Regression should be {0, 1}
 */
object LogisticRegressionWithSGD {
  // NOTE(shivaram): We use multiple train methods instead of default arguments to support
  // Java programs.

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to 
   *        the number of features in the data.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Array[Double])
    : LogisticRegressionModel =
  {
    new LogisticRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction).run(
      input, initialWeights)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.

   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double)
    : LogisticRegressionModel =
  {
    new LogisticRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction).run(
      input)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. We use the entire data
   * set to update the gradient in each iteration.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.

   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LogisticRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double)
    : LogisticRegressionModel =
  {
    train(input, numIterations, stepSize, 1.0)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using a step size of 1.0. We use the entire data set
   * to update the gradient in each iteration.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LogisticRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int)
    : LogisticRegressionModel =
  {
    train(input, numIterations, 1.0, 1.0)
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: LogisticRegression <master> <input_dir> <step_size> " +
        "<niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "LogisticRegression")
    val data = MLUtils.loadLabeledData(sc, args(1))
    val model = LogisticRegressionWithSGD.train(data, args(3).toInt, args(2).toDouble)

    sc.stop()
  }
}

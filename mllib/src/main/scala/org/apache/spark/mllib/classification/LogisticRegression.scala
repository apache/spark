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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.classification.impl.GLMClassificationModel
import org.apache.spark.mllib.linalg.BLAS.dot
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{DataValidators, Saveable, Loader}
import org.apache.spark.rdd.RDD


/**
 * Classification model trained using Multinomial/Binary Logistic Regression.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model. (Only used in Binary Logistic Regression.
 *                  In Multinomial Logistic Regression, the intercepts will not be a single value,
 *                  so the intercepts will be part of the weights.)
 * @param numFeatures the dimension of the features.
 * @param numClasses the number of possible outcomes for k classes classification problem in
 *                   Multinomial Logistic Regression. By default, it is binary logistic regression
 *                   so numClasses will be set to 2.
 */
class LogisticRegressionModel (
    override val weights: Vector,
    override val intercept: Double,
    val numFeatures: Int,
    val numClasses: Int)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable
  with Saveable {

  if (numClasses == 2) {
    require(weights.size == numFeatures,
      s"LogisticRegressionModel with numClasses = 2 was given non-matching values:" +
      s" numFeatures = $numFeatures, but weights.size = ${weights.size}")
  } else {
    val weightsSizeWithoutIntercept = (numClasses - 1) * numFeatures
    val weightsSizeWithIntercept = (numClasses - 1) * (numFeatures + 1)
    require(weights.size == weightsSizeWithoutIntercept || weights.size == weightsSizeWithIntercept,
      s"LogisticRegressionModel.load with numClasses = $numClasses and numFeatures = $numFeatures" +
      s" expected weights of length $weightsSizeWithoutIntercept (without intercept)" +
      s" or $weightsSizeWithIntercept (with intercept)," +
      s" but was given weights of length ${weights.size}")
  }

  /**
   * Constructs a [[LogisticRegressionModel]] with weights and intercept for binary classification.
   */
  def this(weights: Vector, intercept: Double) = this(weights, intercept, weights.size, 2)

  private var threshold: Option[Double] = Some(0.5)

  /**
   * :: Experimental ::
   * Sets the threshold that separates positive predictions from negative predictions
   * in Binary Logistic Regression. An example with prediction score greater than or equal to
   * this threshold is identified as an positive, and negative otherwise. The default value is 0.5.
   */
  @Experimental
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
   * :: Experimental ::
   * Returns the threshold (if any) used for converting raw prediction scores into 0/1 predictions.
   */
  @Experimental
  def getThreshold: Option[Double] = threshold

  /**
   * :: Experimental ::
   * Clears the threshold so that `predict` will output raw prediction scores.
   */
  @Experimental
  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(
      dataMatrix: Vector,
      weightMatrix: Vector,
      intercept: Double) = {
    require(dataMatrix.size == numFeatures)

    // If dataMatrix and weightMatrix have the same dimension, it's binary logistic regression.
    if (numClasses == 2) {
      require(numFeatures == weightMatrix.size)
      val margin = dot(weightMatrix, dataMatrix) + intercept
      val score = 1.0 / (1.0 + math.exp(-margin))
      threshold match {
        case Some(t) => if (score > t) 1.0 else 0.0
        case None => score
      }
    } else {
      val dataWithBiasSize = weightMatrix.size / (numClasses - 1)

      val weightsArray = weightMatrix match {
        case dv: DenseVector => dv.values
        case _ =>
          throw new IllegalArgumentException(
            s"weights only supports dense vector but got type ${weightMatrix.getClass}.")
      }

      val margins = (0 until numClasses - 1).map { i =>
        var margin = 0.0
        dataMatrix.foreachActive { (index, value) =>
          if (value != 0.0) margin += value * weightsArray((i * dataWithBiasSize) + index)
        }
        // Intercept is required to be added into margin.
        if (dataMatrix.size + 1 == dataWithBiasSize) {
          margin += weightsArray((i * dataWithBiasSize) + dataMatrix.size)
        }
        margin
      }

      /**
       * Find the one with maximum margins. If the maxMargin is negative, then the prediction
       * result will be the first class.
       *
       * PS, if you want to compute the probabilities for each outcome instead of the outcome
       * with maximum probability, remember to subtract the maxMargin from margins if maxMargin
       * is positive to prevent overflow.
       */
      var bestClass = 0
      var maxMargin = 0.0
      var i = 0
      while(i < margins.size) {
        if (margins(i) > maxMargin) {
          maxMargin = margins(i)
          bestClass = i + 1
        }
        i += 1
      }
      bestClass.toDouble
    }
  }

  override def save(sc: SparkContext, path: String): Unit = {
    GLMClassificationModel.SaveLoadV1_0.save(sc, path, this.getClass.getName,
      numFeatures, numClasses, weights, intercept, threshold)
  }

  override protected def formatVersion: String = "1.0"
}

object LogisticRegressionModel extends Loader[LogisticRegressionModel] {

  override def load(sc: SparkContext, path: String): LogisticRegressionModel = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    // Hard-code class name string in case it changes in the future
    val classNameV1_0 = "org.apache.spark.mllib.classification.LogisticRegressionModel"
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val (numFeatures, numClasses) = ClassificationModel.getNumFeaturesClasses(metadata)
        val data = GLMClassificationModel.SaveLoadV1_0.loadData(sc, path, classNameV1_0)
        // numFeatures, numClasses, weights are checked in model initialization
        val model =
          new LogisticRegressionModel(data.weights, data.intercept, numFeatures, numClasses)
        data.threshold match {
          case Some(t) => model.setThreshold(t)
          case None => model.clearThreshold()
        }
        model
      case _ => throw new Exception(
        s"LogisticRegressionModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }
}

/**
 * Train a classification model for Binary Logistic Regression
 * using Stochastic Gradient Descent. By default L2 regularization is used,
 * which can be changed via [[LogisticRegressionWithSGD.optimizer]].
 * NOTE: Labels used in Logistic Regression should be {0, 1, ..., k - 1}
 * for k classes multi-label classification problem.
 * Using [[LogisticRegressionWithLBFGS]] is recommended over this.
 */
class LogisticRegressionWithSGD private[mllib] (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  private val gradient = new LogisticGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  /**
   * Construct a LogisticRegression object with default parameters: {stepSize: 1.0,
   * numIterations: 100, regParm: 0.01, miniBatchFraction: 1.0}.
   */
  def this() = this(1.0, 100, 0.01, 1.0)

  override protected[mllib] def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept)
  }
}

/**
 * Top-level methods for calling Logistic Regression using Stochastic Gradient Descent.
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
      initialWeights: Vector): LogisticRegressionModel = {
    new LogisticRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
      .run(input, initialWeights)
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
      miniBatchFraction: Double): LogisticRegressionModel = {
    new LogisticRegressionWithSGD(stepSize, numIterations, 0.0, miniBatchFraction)
      .run(input)
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
      stepSize: Double): LogisticRegressionModel = {
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
      numIterations: Int): LogisticRegressionModel = {
    train(input, numIterations, 1.0, 1.0)
  }
}

/**
 * Train a classification model for Multinomial/Binary Logistic Regression using
 * Limited-memory BFGS. Standard feature scaling and L2 regularization are used by default.
 * NOTE: Labels used in Logistic Regression should be {0, 1, ..., k - 1}
 * for k classes multi-label classification problem.
 */
class LogisticRegressionWithLBFGS
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  this.setFeatureScaling(true)

  override val optimizer = new LBFGS(new LogisticGradient, new SquaredL2Updater)

  override protected val validators = List(multiLabelValidator)

  private def multiLabelValidator: RDD[LabeledPoint] => Boolean = { data =>
    if (numOfLinearPredictor > 1) {
      DataValidators.multiLabelValidator(numOfLinearPredictor + 1)(data)
    } else {
      DataValidators.binaryLabelValidator(data)
    }
  }

  /**
   * :: Experimental ::
   * Set the number of possible outcomes for k classes classification problem in
   * Multinomial Logistic Regression.
   * By default, it is binary logistic regression so k will be set to 2.
   */
  @Experimental
  def setNumClasses(numClasses: Int): this.type = {
    require(numClasses > 1)
    numOfLinearPredictor = numClasses - 1
    if (numClasses > 2) {
      optimizer.setGradient(new LogisticGradient(numClasses))
    }
    this
  }

  override protected def createModel(weights: Vector, intercept: Double) = {
    new LogisticRegressionModel(weights, intercept, numFeatures, numOfLinearPredictor + 1)
  }
}

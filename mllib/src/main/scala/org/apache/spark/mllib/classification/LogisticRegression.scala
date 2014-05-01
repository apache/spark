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
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{BinaryLabelParser, DataValidators, MLUtils}
import org.apache.spark.rdd.RDD

/**
 * Classification model trained using Logistic Regression.
 *
 * @param weights Weights computed for every feature.
 * @param intercept Intercept computed for this model.
 */
class LogisticRegressionModel(
    override val weights: Vector,
    override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable {

  private var threshold: Option[Double] = Some(0.5)

  /**
   * Sets the threshold that separates positive predictions from negative predictions. An example
   * with prediction score greater than or equal to this threshold is identified as an positive,
   * and negative otherwise. The default value is 0.5.
   */
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
   * Clears the threshold so that `predict` will output raw prediction scores.
   */
  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(dataMatrix: Vector, weightMatrix: Vector,
      intercept: Double) = {
    val margin = weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
    val score = 1.0/ (1.0 + math.exp(-margin))
    threshold match {
      case Some(t) => if (score < t) 0.0 else 1.0
      case None => score
    }
  }
}

/**
 * Train a classification model for Logistic Regression using Stochastic Gradient Descent.
 * NOTE: Labels used in Logistic Regression should be {0, 1}
 */
class LogisticRegressionWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double,
    private var isL1Reg: Boolean = false)
  extends GeneralizedLinearAlgorithm[LogisticRegressionModel] with Serializable {

  private val gradient = new LogisticGradient()
  private val updater = (regParam, isL1Reg) match {
    case (0.0, _) => new SimpleUpdater()
    case (regValue, true) => new L1Updater()
    case _ => new SquaredL2Updater()
  }
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  /**
   * Construct a LogisticRegression object with default parameters
   */
  def this() = this(1.0, 100, 0.0, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
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

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed
   * number of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. Can choose to
   * use stochastic gradient descent or just gradient descent by scanning data sequentially. In
   * the latter case, the `miniBatchFraction` fraction is ignored. Either no regularization or
   * l1, l2 regularization can be enabled. A model with non-zero intercept can be trained. The
   * weights used in gradient descent are initialized using the initial weights provided.
   * NOTE: Labels used in Logistic Regression should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param addIntercept train a model with non-zero intercept.
   * @param stochastic apply gradient descent on sampled data
   * @param l1 amount of l1 regularization
   * @param l2 amount of l2 regularization
   */
  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      addIntercept: Boolean,
      stochastic: Boolean,
      l1: Option[Double],
      l2: Option[Double]): LogisticRegressionModel = {
    val regParam = (l1, l2) match {
      case (None, None) => 0.0
      case (Some(v), _) => v
      case (_, Some(v)) => v
    }
    val trainer = new LogisticRegressionWithSGD(stepSize, numIterations,
      regParam, miniBatchFraction, l1.isDefined)
    trainer.setIntercept(addIntercept)
    trainer.optimizer.setStochastic(stochastic)
    trainer.run(input)
  }

  def main(args: Array[String]) {

    def extractFlagValue(flag: String): Option[String] = {
      args.flatMap { x =>
        if (x.startsWith(flag)) {
          Some(x.stripPrefix(flag))
        } else {
          None
        }
      }.headOption
    }

    if (args.length <= 4) {
      println("Usage: LogisticRegression <master> <input_dir> <model_dir>\n" +
          "       [<step_size> <niters> [--svmlight] [--l1=reg] [--l2=reg] [--stochastic] " +
          "[--add_intercept] [--num_features=n] [--minibatch=fraction]] # training\n" +
          "       [<prediction_dir> --testonly] # testing")
      System.exit(1)
    }
    val sparkMaster = args(0)
    val inputDir = args(1)
    val modelDir = args(2)
    val sc = new SparkContext(sparkMaster, "LogisticRegression")

    val numFeatures = extractFlagValue("--num_features=").getOrElse("-1").toInt

    val data = if (args.contains("--svmlight"))
      MLUtils.loadLibSVMData(sc, inputDir, BinaryLabelParser, numFeatures)
    else
      MLUtils.loadLabeledData(sc, inputDir)

    if (args.contains("--testonly")) {
      val predictionFile = args(3)

      // read model
      val rModel: Seq[(String, Double)]= sc.textFile(modelDir).map { line =>
        val parts = line.split(":")
        (parts(0), parts(1).toDouble)
      } .collect()

      val numFeatures = rModel(0)._2.toInt
      val intercept = rModel(1)._2
      val weights = Array.fill(numFeatures) { 0.0d }
      rModel.slice(2, rModel.length).foreach { kv =>
        weights(kv._1.toInt) = kv._2
      }
      val model = new LogisticRegressionModel(Vectors.dense(weights), intercept)

      val dataLabel = data.map { _.features }
      data.map { _.label }
        .zip(model.predict(dataLabel))
        .map { lp => lp._1 + "," + lp._2 }
        .saveAsTextFile(predictionFile)
    } else {
      // train a model
      val stepSize = args(3).toDouble
      val pass = args(4).toInt
      val addIntercept = args.contains("--add_intercept")
      val stochastic = args.contains("--stochastic")
      val l1 = extractFlagValue("--l1=").map { _.toDouble }
      val l2 = extractFlagValue("--l2=").map { _.toDouble }
      val minibatch = extractFlagValue("--minibatch=").map { _.toDouble } .getOrElse(1.0)
      val model = LogisticRegressionWithSGD.train(data, pass, stepSize, minibatch, addIntercept,
        stochastic, l1, l2)

      // create and RDD[Text] and save it to output
      sc.makeRDD(model.readableModel).saveAsTextFile(modelDir)
    }

    sc.stop()
  }
}

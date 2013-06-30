package spark.ml.regression

import spark.{Logging, RDD, SparkContext}
import spark.ml.optimization._
import spark.ml.util.MLUtils

import org.jblas.DoubleMatrix

/**
 * Logistic Regression using Stochastic Gradient Descent.
 * Based on Matlab code written by John Duchi.
 */
class LogisticRegressionModel(
  val weights: DoubleMatrix,
  val intercept: Double,
  val losses: Array[Double]) extends RegressionModel {

  override def predict(testData: spark.RDD[Array[Double]]) = {
    testData.map { x =>
      val margin = new DoubleMatrix(1, x.length, x:_*).mmul(this.weights).get(0) + this.intercept
      1.0/ (1.0 + math.exp(margin * -1))
    }
  }

  override def predict(testData: Array[Double]): Double = {
    val dataMat = new DoubleMatrix(1, testData.length, testData:_*)
    val margin = dataMat.mmul(this.weights).get(0) + this.intercept
    1.0/ (1.0 + math.exp(margin * -1))
  }
}

class LogisticRegression private (var stepSize: Double, var miniBatchFraction: Double,
    var numIters: Int)
  extends Logging {

  /**
   * Construct a LogisticRegression object with default parameters
   */
  def this() = this(1.0, 1.0, 100)

  /**
   * Set the step size per-iteration of SGD. Default 1.0.
   */
  def setStepSize(step: Double) = {
    this.stepSize = step
    this
  }

  /**
   * Set fraction of data to be used for each SGD iteration. Default 1.0.
   */
  def setMiniBatchFraction(fraction: Double) = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int) = {
    this.numIters = iters
    this
  }

  def train(input: RDD[(Double, Array[Double])]): LogisticRegressionModel = {
    // Add a extra variable consisting of all 1.0's for the intercept.
    val data = input.map { case (y, features) =>
      (y, Array(1.0, features:_*))
    }

    val (weights, losses) = GradientDescent.runMiniBatchSGD(
      data, new LogisticGradient(), new SimpleUpdater(), stepSize, numIters, miniBatchFraction)

    val weightsScaled = weights.getRange(1, weights.length)
    val intercept = weights.get(0)

    val model = new LogisticRegressionModel(weightsScaled, intercept, losses)

    logInfo("Final model weights " + model.weights)
    logInfo("Final model intercept " + model.intercept)
    logInfo("Last 10 losses " + model.losses.takeRight(10).mkString(", "))
    model
  }
}

/**
 * Top-level methods for calling Logistic Regression.
 */
object LogisticRegression {

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   */
  def train(
      input: RDD[(Double, Array[Double])],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double)
    : LogisticRegressionModel =
  {
    new LogisticRegression(stepSize, miniBatchFraction, numIterations).train(input)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to update
   * the gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LogisticRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[(Double, Array[Double])],
      numIterations: Int,
      stepSize: Double)
    : LogisticRegressionModel =
  {
    train(input, numIterations, stepSize, 1.0)
  }

  /**
   * Train a logistic regression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to update
   * the gradient in each iteration.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a LogisticRegressionModel which has the weights and offset from training.
   */
  def train(
      input: RDD[(Double, Array[Double])],
      numIterations: Int)
    : LogisticRegressionModel =
  {
    train(input, numIterations, 1.0, 1.0)
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: LogisticRegression <master> <input_dir> <step_size> <niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "LogisticRegression")
    val data = MLUtils.loadData(sc, args(1))
    val model = LogisticRegression.train(data, args(3).toInt, args(2).toDouble)

    sc.stop()
  }
}

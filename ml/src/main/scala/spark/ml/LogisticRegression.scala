package spark.ml

import spark.{Logging, RDD, SparkContext}

import org.jblas.DoubleMatrix

/**
 * Logistic Regression using Stochastic Gradient Descent.
 * Based on Matlab code written by John Duchi.
 */
class LogisticRegressionModel(
  val weights: DoubleMatrix,
  val intercept: Double,
  val losses: Array[Double]) extends RegressionModel {

  override def predict(test_data: spark.RDD[Array[Double]]) = {
    test_data.map { x =>
      val margin = new DoubleMatrix(1, x.length, x:_*).mmul(this.weights).get(0) + this.intercept
      1.0/ (1.0 + math.exp(margin * -1))
    }
  }
}

class LogisticRegression(var stepSize: Double, var miniBatchFraction: Double, var numIters: Int)
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
    val nfeatures: Int = input.take(1)(0)._2.length
    val nexamples: Long = input.count()

    val (yMean, xColMean, xColSd) = MLUtils.computeStats(input, nfeatures, nexamples)

    // Shift only the features for LogisticRegression
    val data = input.map { case(y, features) =>
      val featuresMat = new DoubleMatrix(nfeatures, 1, features:_*)
      val featuresNormalized = featuresMat.sub(xColMean).divi(xColSd)
      (y, featuresNormalized.toArray)
    }

    val (weights, losses) = GradientDescent.runMiniBatchSGD(
      data, new LogisticGradient(), new SimpleUpdater(), stepSize, numIters, miniBatchFraction)

    val weightsScaled = weights.div(xColSd)
    val intercept = -1.0 * weights.transpose().mmul(xColMean.div(xColSd)).get(0)

    val model = new LogisticRegressionModel(weightsScaled, intercept, losses)

    logInfo("Final model weights " + model.weights)
    logInfo("Final model intercept " + model.intercept)
    logInfo("Last 10 losses " + model.losses.takeRight(10).mkString(", "))
    model
  }
}

object LogisticRegression {

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage: LogisticRegression <master> <input_dir> <step_size> <niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "LogisticRegression")
    val data = MLUtils.loadData(sc, args(1))
    val lr = new LogisticRegression().setStepSize(args(2).toDouble)
                                     .setNumIterations(args(3).toInt)
    val model = lr.train(data)
    sc.stop()
  }
}

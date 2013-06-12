package spark.ml

import spark.{Logging, RDD, SparkContext}

import org.jblas.DoubleMatrix

/**
 * Logistic Regression using Stochastic Gradient Descent.
 */
class LogisticRegressionModel(
  weights: DoubleMatrix,
  intercept: Double,
  val losses: Array[Double]) extends RegressionModel(weights, intercept) {

  override def predict(test_data: spark.RDD[Array[Double]]) = {
    test_data.map { x =>
      val margin = new DoubleMatrix(1, x.length, x:_*).mmul(this.weights).get(0) + this.intercept
      1.0/ (1.0 + math.exp(margin * -1))
    }
  }
}

class LogisticRegressionData(data: RDD[(Double, Array[Double])]) extends RegressionData(data) {
  override def normalizeData() = {
    // Shift only the features for LogisticRegression
    data.map { case(y, features) =>
      val featuresNormalized = Array.tabulate(nfeatures) { column =>
        (features(column) - xColMean(column)) / xColSd(column)
      }
      (y, featuresNormalized)
    }
  }

  override def scaleModel(m: RegressionModel) = {
    val model = m.asInstanceOf[LogisticRegressionModel]
    val colSdMat = new DoubleMatrix(xColSd.length, 1, xColSd:_*)
    val colMeanMat = new DoubleMatrix(xColMean.length, 1, xColMean:_*)

    val weights = model.weights.div(colSdMat)
    val intercept = -1.0 * model.weights.transpose().mmul(colMeanMat.div(colSdMat)).get(0)

    new LogisticRegressionModel(weights, intercept, model.losses)
  }
}

class LogisticRegression(stepSize: Double, miniBatchFraction: Double, numIters: Int)
  extends Regression with Logging {

  override def train(input: RDD[(Double, Array[Double])]): RegressionModel = {
    input.cache()

    val lrData = new LogisticRegressionData(input)
    val data = lrData.normalizeData()
    val (weights, losses) = GradientDescent.runMiniBatchSGD(
      data, new LogisticGradient(), new SimpleUpdater(), stepSize, numIters, numIters)

    val computedModel = new LogisticRegressionModel(weights, 0, losses)
    val model = lrData.scaleModel(computedModel)
    logInfo("Final model weights " + model.weights)
    logInfo("Final model intercept " + model.intercept)
    logInfo("Last 10 losses " + model.losses.takeRight(10).mkString(", "))
    model
  }
}

/**
 * Helper classes to build a LogisticRegression object.
 */
object LogisticRegression {

  /**
   * Build a logistic regression object with default arguments:
   *
   * @param stepSize as 1.0
   * @param miniBatchFraction as 1.0
   * @param numIters as 100
   */
  def builder() = {
    new LogisticRegressionBuilder(1.0, 1.0, 100)
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: LogisticRegression <master> <input_dir> <niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "LogisticRegression")
    val data = MLUtils.loadData(sc, args(1))
    val lr = LogisticRegression.builder()
                               .setStepSize(2.0)
                               .setNumIterations(args(2).toInt)
                               .build()
    val model = lr.train(data)
    sc.stop()
  }
}

class LogisticRegressionBuilder(stepSize: Double, miniBatchFraction: Double, numIters: Int) {

  /**
   * Set the step size per-iteration of SGD. Default 1.0.
   */
  def setStepSize(step: Double) = {
    new LogisticRegressionBuilder(step, this.miniBatchFraction, this.numIters)
  }

  /**
   * Set fraction of data to be used for each SGD iteration. Default 1.0.
   */
  def setMiniBatchFraction(fraction: Double) = {
    new LogisticRegressionBuilder(this.stepSize, fraction, this.numIters)
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int) = {
    new LogisticRegressionBuilder(this.stepSize, this.miniBatchFraction, iters)
  }

  /**
   * Build a Logistic regression object.
   */
  def build() = {
    new LogisticRegression(stepSize, miniBatchFraction, numIters)
  }
}

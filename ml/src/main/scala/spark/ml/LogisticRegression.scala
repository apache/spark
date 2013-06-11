package spark.ml

import spark.{Logging, RDD, SparkContext}
import spark.SparkContext._

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
      val margin = (new DoubleMatrix(1, x.length, x:_*).mmul(this.weights)).get(0) + this.intercept
      1.0/(1.0 + math.exp(margin * -1))
    }
  }
}

class LogisticRegressionData(data: RDD[(Double, Array[Double])]) extends RegressionData(data) {
  override def normalizeData() = {
    // Shift only the features for LogisticRegression
    data.map { case(y, features) =>
      val featuresNormalized = (0 until nfeatures).map(
        column => (features(column) - xColMean(column)) / xColSd(column)
      ).toArray
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

object LogisticRegression extends Logging {
  val STEP_SIZE = 1.0
  val MINI_BATCH_FRACTION = 1.0

  def train(input: RDD[(Double, Array[Double])], numIters: Int) = {
    input.cache()

    val lrData = new LogisticRegressionData(input)
    val data = lrData.normalizeData()
    val (weights, losses) = GradientDescent.runMiniBatchSGD(
        data, new LogisticGradient(), new SimpleUpdater(), STEP_SIZE, numIters, MINI_BATCH_FRACTION)
  
    val computedModel = new LogisticRegressionModel(weights, 0, losses)
    val model = lrData.scaleModel(computedModel)
    logInfo("Final model weights " + model.weights)
    logInfo("Final model intercept " + model.intercept)
    logInfo("Last 10 losses " + model.losses.takeRight(10).mkString(","))
    model
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: LogisticRegression <master> <input_dir> <niters>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "LogisticRegression")
    val data = MLUtils.loadData(sc, args(1))
    val model = train(data, args(2).toInt)
    sc.stop()
  }
}

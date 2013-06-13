package spark.ml

import spark.{Logging, RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix
import org.jblas.Solve

/**
 * Ridge Regression from Joseph Gonzalez's implementation in MLBase
 */
class RidgeRegressionModel(
    weights: DoubleMatrix,
    intercept: Double,
    val lambdaOpt: Double,
    val lambdas: List[(Double, Double, DoubleMatrix)])
  extends RegressionModel(weights, intercept) {

  override def predict(test_data: RDD[Array[Double]]) = {
    test_data.map { x =>
      (new DoubleMatrix(1, x.length, x:_*).mmul(this.weights)).get(0) + this.intercept
    }
  }
}

class RidgeRegressionData(data: RDD[(Double, Array[Double])]) extends RegressionData(data) {
  override def normalizeData() = {
    data.map { case(y, features) =>
      val yNormalized = y - yMean
      val featuresNormalized = Array.tabulate(nfeatures) { column =>
        (features(column) - xColMean(column)) / xColSd(column)
      }.toArray
      (yNormalized, featuresNormalized)
    }
  }

  override def scaleModel(m: RegressionModel) = {
    val model = m.asInstanceOf[RidgeRegressionModel]
    val colSdMat = new DoubleMatrix(xColSd.length, 1, xColSd:_*)
    val colMeanMat = new DoubleMatrix(xColMean.length, 1, xColMean:_*)

    val weights = model.weights.div(colSdMat)
    val intercept = yMean - model.weights.transpose().mmul(colMeanMat.div(colSdMat)).get(0)

    new RidgeRegressionModel(weights, intercept, model.lambdaOpt, model.lambdas)
  }
}

class RidgeRegression(var lambdaLow: Double, var lambdaHigh: Double) 
  extends Regression with Logging {

  def this() = this(0.0, 100.0)

  /**
   * Set the lower bound on binary search for lambda's. Default is 0.
   */
  def setLowLambda(low: Double) = {
    this.lambdaLow = low
    this
  }

  /**
   * Set the upper bound on binary search for lambda's. Default is 100.0.
   */
  def setHighLambda(hi: Double) = {
    this.lambdaHigh = hi
    this
  }

  def train(inputData: RDD[(Double, Array[Double])]): RegressionModel = {
    inputData.cache()
    val ridgeData = new RidgeRegressionData(inputData)
    val data = ridgeData.normalizeData()
    val nfeatures: Int = ridgeData.nfeatures
    val nexamples: Long = ridgeData.nexamples

    // Compute XtX - Size of XtX is nfeatures by nfeatures
    val XtX: DoubleMatrix = data.map { case (y, features) =>
      val x = new DoubleMatrix(1, features.length, features:_*)
      x.transpose().mmul(x)
    }.reduce(_.add(_))

    // Compute Xt*y - Size of Xty is nfeatures by 1
    val Xty: DoubleMatrix = data.map { case (y, features) =>
      new DoubleMatrix(features.length, 1, features:_*).mul(y)
    }.reduce(_.add(_))

    // Define a function to compute the leave one out cross validation error
    // for a single example
    def crossValidate(lambda: Double): (Double, Double, DoubleMatrix) = {
      // Compute the MLE ridge regression parameter value

      // Ridge Regression parameter = inv(XtX + \lambda*I) * Xty
      val XtXlambda = DoubleMatrix.eye(nfeatures).muli(lambda).addi(XtX)
      val w = Solve.solveSymmetric(XtXlambda, Xty)

      val invXtX = Solve.solveSymmetric(XtXlambda, DoubleMatrix.eye(nfeatures))

      // compute the generalized cross validation score
      val cverror = data.map {
        case (y, features) =>
          val x = new DoubleMatrix(features.length, 1, features:_*)
          val yhat = w.transpose().mmul(x).get(0)
          val H_ii = x.transpose().mmul(invXtX).mmul(x).get(0)
          val residual = (y - yhat) / (1.0 - H_ii)
          residual * residual
      }.reduce(_ + _) / nexamples

      (lambda, cverror, w)
    }

    // Binary search for the best assignment to lambda.
    def binSearch(low: Double, high: Double): List[(Double, Double, DoubleMatrix)] = {
      val mid = (high - low) / 2 + low
      val lowValue = crossValidate((mid - low) / 2 + low)
      val highValue = crossValidate((high - mid) / 2 + mid)
      val (newLow, newHigh) = if (lowValue._2 < highValue._2) {
        (low, mid + (high-low)/4)
      } else {
        (mid - (high-low)/4, high)
      }
      if (newHigh - newLow > 1.0E-7) {
        // :: is list prepend in Scala.
        lowValue :: highValue :: binSearch(newLow, newHigh)
      } else {
        List(lowValue, highValue)
      }
    }

    // Actually compute the best lambda
    val lambdas = binSearch(lambdaLow, lambdaHigh).sortBy(_._1)

    // Find the best parameter set by taking the lowest cverror.
    val (lambdaOpt, cverror, weights) = lambdas.reduce((a, b) => if (a._2 < b._2) a else b)

    // Return the model which contains the solution
    val trainModel = new RidgeRegressionModel(weights, 0.0, lambdaOpt, lambdas)
    val normModel = ridgeData.scaleModel(trainModel)

    logInfo("RidgeRegression: optimal lambda " + normModel.lambdaOpt)
    logInfo("RidgeRegression: optimal weights " + normModel.weights)
    logInfo("RidgeRegression: optimal intercept " + normModel.intercept)
    logInfo("RidgeRegression: cross-validation error " + cverror)

    normModel
  }
}

object RidgeRegression {

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: RidgeRegression <master> <input_dir>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "RidgeRegression")
    val data = MLUtils.loadData(sc, args(1))
    val ridgeReg = new RidgeRegression().setLowLambda(0)
                                        .setHighLambda(1000)

    val model = ridgeReg.train(data)
    sc.stop()
  }
}

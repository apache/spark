package spark.ml

import spark._
import spark.SparkContext._

import org.apache.commons.math3.distribution.NormalDistribution
import org.jblas.DoubleMatrix
import org.jblas.Solve

/**
 * Ridge Regression from Joseph Gonzalez's implementation in MLBase
 */

class RidgeRegressionModel(
  val wOpt: DoubleMatrix,
  val lambdaOpt: Double,
  val lambdas: List[(Double, Double, DoubleMatrix)]) {

  def predict(test_data: spark.RDD[Array[Double]]) = {
    test_data.map(x => new DoubleMatrix(1, x.length, x:_*).mmul(this.wOpt))
  }
}

object RidgeRegression extends Logging {

  def train(data: spark.RDD[(Double, Array[Double])],
    lambdaLow: Double = 0.0, 
    lambdaHigh: Double = 10000.0) = {

    data.cache()
    val nfeatures = data.take(1)(0)._2.length
    val nexamples = data.count

    // Compute XtX - Size of XtX is nfeatures by nfeatures
    val XtX = data.map {
      case (y, features) =>
        val x = new DoubleMatrix(1, features.length, features:_*)
        x.transpose().mmul(x)
    }.reduce(_.add(_))

    // Compute Xt*y - Size of Xty is nfeatures by 1
    val Xty = data.map {
      case (y, features) => 
        new DoubleMatrix(features.length, 1, features:_*).mul(y)
    }.reduce(_.add(_))

    // Define a function to compute the leave one out cross validation error
    // for a single example
    def crossValidate(lambda: Double) = {
      // Compute the MLE ridge regression parameter value 

      // Ridge Regression parameter = inv(XtX + \lambda*I) * Xty
      val XtXlambda = DoubleMatrix.eye(nfeatures).muli(lambda).addi(XtX)
      val w = Solve.solveSymmetric(XtXlambda, Xty)

      val invXtX = Solve.solveSymmetric(XtXlambda,
          DoubleMatrix.eye(nfeatures))
          
      // compute the leave one out cross validation score
      val cvError = data.map {
        case (y, features) =>
          val x = new DoubleMatrix(features.length, 1, features:_*)
          val yhat = w.transpose().mmul(x).get(0)
          val H_ii = x.transpose().mmul(invXtX).mmul(x).get(0)
          val residual = (y - yhat) / (1.0 - H_ii)
          residual * residual
      }.reduce(_ + _)
      (lambda, cvError, w)
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
        lowValue :: highValue :: binSearch(newLow, newHigh)
      } else {
        List(lowValue, highValue)
      }
    }

    // Actually compute the best lambda
    val lambdas = binSearch(lambdaLow, lambdaHigh).sortBy(_._1)

    // Find the best parameter set
    val (lambdaOpt, cverror, wOpt) = lambdas.reduce((a, b) => if (a._2 < b._2) a else b)

    logInfo("RidgeRegression: optimal lambda " + lambdaOpt)

    // Return the model which contains the solution 
    new RidgeRegressionModel(wOpt, lambdaOpt, lambdas)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: RidgeRegression <master> <input_dir>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "RidgeRegression")
    val data = RidgeRegressionGenerator.loadData(sc, args(1))
    val model = train(data, 0, 100)
    sc.stop()
  }
}

package spark.ml

import spark.{Logging, RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix
import org.jblas.Solve

/**
 * Ridge Regression from Joseph Gonzalez's implementation in MLBase
 */
class RidgeRegressionModel(
    val weights: DoubleMatrix,
    val intercept: Double,
    val lambdaOpt: Double,
    val lambdas: List[(Double, Double, DoubleMatrix)])
  extends RegressionModel {

  override def predict(test_data: RDD[Array[Double]]) = {
    test_data.map { x =>
      (new DoubleMatrix(1, x.length, x:_*).mmul(this.weights)).get(0) + this.intercept
    }
  }
}

class RidgeRegression(var lambdaLow: Double, var lambdaHigh: Double) 
  extends Logging {

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

  def train(input: RDD[(Double, Array[Double])]): RidgeRegressionModel = {
    val nfeatures: Int = input.take(1)(0)._2.length
    val nexamples: Long = input.count()

    val (yMean, xColMean, xColSd) = MLUtils.computeStats(input, nfeatures, nexamples)

    val data = input.map { case(y, features) =>
      val yNormalized = y - yMean
      val featuresMat = new DoubleMatrix(nfeatures, 1, features:_*)
      val featuresNormalized = featuresMat.sub(xColMean).divi(xColSd)
      (yNormalized, featuresNormalized.toArray)
    }

    // Compute XtX - Size of XtX is nfeatures by nfeatures
    val XtX: DoubleMatrix = data.map { case (y, features) =>
      val x = new DoubleMatrix(1, features.length, features:_*)
      x.transpose().mmul(x)
    }.reduce(_.addi(_))

    // Compute Xt*y - Size of Xty is nfeatures by 1
    val Xty: DoubleMatrix = data.map { case (y, features) =>
      new DoubleMatrix(features.length, 1, features:_*).mul(y)
    }.reduce(_.addi(_))

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
    val weightsScaled = weights.div(xColSd)
    val intercept = yMean - (weights.transpose().mmul(xColMean.div(xColSd)).get(0))
    val model = new RidgeRegressionModel(weightsScaled, intercept, lambdaOpt, lambdas)

    logInfo("RidgeRegression: optimal lambda " + model.lambdaOpt)
    logInfo("RidgeRegression: optimal weights " + model.weights)
    logInfo("RidgeRegression: optimal intercept " + model.intercept)
    logInfo("RidgeRegression: cross-validation error " + cverror)

    model
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

package spark.ml

import spark.{Logging, RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix
import org.jblas.Solve

/**
 * Ridge Regression from Joseph Gonzalez's implementation in MLBase
 */

class RidgeRegressionModel(
  val wOpt: DoubleMatrix,
  val bOpt: Double,
  val lambdaOpt: Double,
  val lambdas: List[(Double, Double, DoubleMatrix)]) {

  def predict(test_data: spark.RDD[Array[Double]]) = {
    test_data.map(x => (new DoubleMatrix(1, x.length, x:_*).mmul(this.wOpt)).get(0) + this.bOpt)
  }
}

case class RidgeRegressionData(
  val data: RDD[(Double, Array[Double])],
  val normalizedData: RDD[(Double, Array[Double])],
  val yMean: Double,
  val xColMean: Array[Double],
  val xColSd: Array[Double]
)

object RidgeRegression extends Logging {

  def train(inputData: RDD[(Double, Array[Double])],
    lambdaLow: Double = 0.0,
    lambdaHigh: Double = 10000.0) = {

    val ridgeData = normalize(inputData)
    val data = ridgeData.normalizedData

    data.cache()
    val nfeatures: Int = data.take(1)(0)._2.length
    val nexamples: Long = data.count()

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
      // TODO: Is there a way to calculate this using one pass on the data ?
      val H_ii_mean = data.map {
        case (y, features) =>
          val x = new DoubleMatrix(features.length, 1, features:_*)
          val H_ii = x.transpose().mmul(invXtX).mmul(x).get(0)
          H_ii
      }.reduce(_ + _) / nexamples

      val gcv = data.map {
        case (y, features) =>
          val x = new DoubleMatrix(features.length, 1, features:_*)
          val yhat = w.transpose().mmul(x).get(0)
          val residual = (y - yhat) / (1.0 - H_ii_mean)
          residual * residual
      }.reduce(_ + _) / nexamples

      (lambda, gcv, w)
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
    val (lambdaOpt, cverror, wOpt) = lambdas.reduce((a, b) => if (a._2 < b._2) a else b)

    // Return the model which contains the solution
    val trainModel = new RidgeRegressionModel(wOpt, 0.0, lambdaOpt, lambdas)
    val normModel = normalizeModel(trainModel, ridgeData.xColSd, ridgeData.xColMean, ridgeData.yMean)

    logInfo("RidgeRegression: optimal lambda " + normModel.lambdaOpt)
    logInfo("RidgeRegression: optimal weights " + normModel.wOpt)
    logInfo("RidgeRegression: optimal intercept " + normModel.bOpt)
    logInfo("RidgeRegression: optimal GCV " + cverror)

    normModel
  }

  /**
   * yMu = Mean[Y]
   * xMuVec = Mean[X]
   * xSigmaVec = StdDev[X]
   *
   * // Shift the data
   * Xtrain = (X - xMuVec) / xSigmaVec
   * Ytrain = Y - yMu
   */
  def normalize(data: RDD[(Double, Array[Double])]) = {
    data.cache()

    val nexamples: Long = data.count()
    val nfeatures: Int = data.take(1)(0)._2.length

    // Calculate the mean for Y
    val yMean: Double = data.map { case (y, features) => y }.reduce(_ + _) / nexamples

    // NOTE: We shuffle X by column here to compute column sum and sum of squares.
    val xColSumSq: RDD[(Int, (Double, Double))] = data.flatMap { case(y, features) =>
      val nCols = features.length
      // Traverse over every column and emit (col, value, value^2)
      (0 until nCols).map(i => (i, (features(i), features(i)*features(i))))
    }.reduceByKey { case(x1, x2) =>
      (x1._1 + x2._1, x1._2 + x2._2)
    }
    val xColSumsMap = xColSumSq.collectAsMap()

    // Compute mean and unbiased variance using column sums
    val xColMeans = (0 until nfeatures).map(x => xColSumsMap(x)._1 / nexamples).toArray
    val xColSd = (0 until nfeatures).map {x =>
      val v = (xColSumsMap(x)._2 - (math.pow(xColSumsMap(x)._1, 2) / nexamples)) / (nexamples)
      math.sqrt(v)
    }.toArray

    // Shift the data
    val normalizedData = data.map { case(y, features) =>
      val yNormalized = y - yMean
      val featuresNormalized = (0 until nfeatures).map(
        column => (features(column) - xColMeans(column)) / xColSd(column)
      ).toArray
      (yNormalized, featuresNormalized)
    }
    new RidgeRegressionData(data, normalizedData, yMean, xColMeans, xColSd)
  }

  /**
   * Augment and return then final model (derivation):
   *   y = w' ( (xPred - xMu) / xSigma ) + yMu
   *   y = w' xPred/sigma + (yMu - w' (xMu/ xSigmaVec)
   * Note that the / operator is point wise divions
   *
   * model.w = w' / sigma     // point wise division
   * model.b = yMu - w' * (xMu / xSigmaVec)  // scalar offset
   *
   * // Make predictions
   * yPred = model.w' * xPred + model.b
   */
  def normalizeModel(model: RidgeRegressionModel,
      xColSd: Array[Double], xColMeans: Array[Double],
      yMean: Double) = {
    val colSdMat = new DoubleMatrix(xColSd.length, 1, xColSd:_*)
    val colMeanMat = new DoubleMatrix(xColMeans.length, 1, xColMeans:_*)

    val wOpt = model.wOpt.div(colSdMat)
    val bOpt = yMean - model.wOpt.transpose().mmul(colMeanMat.div(colSdMat)).get(0)

    new RidgeRegressionModel(wOpt, bOpt, model.lambdaOpt, model.lambdas)
  }


  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: RidgeRegression <master> <input_dir>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "RidgeRegression")
    val data = RidgeRegressionGenerator.loadData(sc, args(1))
    val model = train(data, 0, 1000)
    sc.stop()
  }
}

package spark.ml

import spark.{Logging, RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer


object GradientDescent {

  def runMiniBatchSGD(
    data: RDD[(Double, Array[Double])],
    gradient: Gradient,
    updater: Updater,
    stepSize: Double,
    numIters: Int,
    miniBatchFraction: Double=1.0) : (DoubleMatrix, Array[Double]) = {

    val lossHistory = new ArrayBuffer[Double]

    val nfeatures: Int = data.take(1)(0)._2.length
    val nexamples: Long = data.count()
    val miniBatchSize = nexamples * miniBatchFraction

    var weights = DoubleMatrix.ones(nfeatures)
    var reg_val = 0.0

    for (i <- 1 to numIters) {
      val (gradientSum, lossSum) = data.sample(false, miniBatchFraction, 42).map {
        case (y, features) =>
          val featuresRow = new DoubleMatrix(features.length, 1, features:_*)
          val (grad, loss) = gradient.compute(featuresRow, y, weights)
          (grad, loss)
      }.reduce((a, b) => (a._1.add(b._1), a._2 + b._2))

      lossHistory.append(lossSum / miniBatchSize + reg_val)
      val update = updater.compute(weights, gradientSum.div(miniBatchSize), stepSize, i)
      weights = update._1
      reg_val = update._2
    }

    (weights, lossHistory.toArray)
  }
}

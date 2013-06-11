package spark.ml

import java.io._

import spark.{RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix

abstract class RegressionModel(
  val weights: DoubleMatrix,
  val intercept: Double) {

  def predict(test_data: RDD[Array[Double]]): RDD[Double]
}

abstract class RegressionData(val data: RDD[(Double, Array[Double])]) extends Serializable {
  var yMean: Double = 0.0
  var xColMean: Array[Double] = null
  var xColSd: Array[Double] = null
  var nfeatures: Int = 0
  var nexamples: Long = 0

  // This will populate yMean, xColMean and xColSd
  calculateStats()

  def normalizeData(): RDD[(Double, Array[Double])]
  def scaleModel(model: RegressionModel): RegressionModel

  def calculateStats() {
    this.nexamples = data.count()
    this.nfeatures = data.take(1)(0)._2.length
    
    this.yMean = data.map { case (y, features) => y }.reduce(_ + _) / nexamples
    
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
    this.xColMean = (0 until nfeatures).map(x => xColSumsMap(x)._1 / nexamples).toArray
    this.xColSd = (0 until nfeatures).map {x =>
      val v = (xColSumsMap(x)._2 - (math.pow(xColSumsMap(x)._1, 2) / nexamples)) / (nexamples)
      math.sqrt(v)
    }.toArray
  }
}

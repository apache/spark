package spark.ml

import java.io._

import spark.{RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix

abstract class RegressionModel(val weights: DoubleMatrix, val intercept: Double) {
  def predict(test_data: RDD[Array[Double]]): RDD[Double]
}

abstract class RegressionData(val data: RDD[(Double, Array[Double])]) extends Serializable {
  val nfeatures: Int = data.take(1)(0)._2.length
  val nexamples: Long = data.count()

  val yMean: Double = data.map { case (y, features) => y }.reduce(_ + _) / nexamples

  // NOTE: We shuffle X by column here to compute column sum and sum of squares.
  private val xColSumSq: RDD[(Int, (Double, Double))] = data.flatMap { case(y, features) =>
    val nCols = features.length
    // Traverse over every column and emit (col, value, value^2)
    Iterator.tabulate(nCols) { i =>
      (i, (features(i), features(i)*features(i)))
    }
  }.reduceByKey { case(x1, x2) =>
    (x1._1 + x2._1, x1._2 + x2._2)
  }
  private val xColSumsMap = xColSumSq.collectAsMap()

  // Compute mean and unbiased variance using column sums
  val xColMean: Array[Double] = Array.tabulate(nfeatures) { x =>
    xColSumsMap(x)._1 / nexamples
  }
  val xColSd: Array[Double] = Array.tabulate(nfeatures) { x =>
    val v = (xColSumsMap(x)._2 - (math.pow(xColSumsMap(x)._1, 2) / nexamples)) / (nexamples)
    math.sqrt(v)
  }

  /**
   * Normalize the provided input data. This function is typically called before
   * training a classifier on the input dataset and should be used to center of scale the data
   * appropriately.
   *
   * @return RDD containing the normalized data
   */
  def normalizeData(): RDD[(Double, Array[Double])]

  /**
   * Scale the trained regression model. This function is usually called after training
   * to adjust the model based on the normalization performed before.
   *
   * @return Regression model that can be used for prediction
   */
  def scaleModel(model: RegressionModel): RegressionModel
}

trait Regression {

  /**
   * Train a model on the provided input dataset. Input data is an RDD of (Label, [Features])
   *
   * @return RegressionModel representing the model built.
   */
  def train(input: RDD[(Double, Array[Double])]): RegressionModel
}

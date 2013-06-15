package spark.ml.util

import spark.{RDD, SparkContext}
import spark.SparkContext._

import org.jblas.DoubleMatrix

/**
 * Helper methods to load and save data
 * Data format:
 * <l>, <f1> <f2> ...
 * where <f1>, <f2> are feature values in Double and <l> is the corresponding label as Double.
 */
object MLUtils {

  /**
   * @param sc SparkContext
   * @param dir Directory to the input data files.
   * @return An RDD of tuples. For each tuple, the first element is the label, and the second
   *         element represents the feature values (an array of Double).
   */
  def loadData(sc: SparkContext, dir: String): RDD[(Double, Array[Double])] = {
    sc.textFile(dir).map { line =>
      val parts = line.split(",")
      val label = parts(0).toDouble
      val features = parts(1).trim().split(" ").map(_.toDouble)
      (label, features)
    }
  }

  def saveData(data: RDD[(Double, Array[Double])], dir: String) {
    val dataStr = data.map(x => x._1 + "," + x._2.mkString(" "))
    dataStr.saveAsTextFile(dir)
  }

  /**
   * Utility function to compute mean and standard deviation on a given dataset.
   *
   * @param data - input data set whose statistics are computed
   * @param nfeatures - number of features
   * @param nexamples - number of examples in input dataset
   *
   * @return (yMean, xColMean, xColSd) - Tuple consisting of
   *     yMean - mean of the labels
   *     xColMean - Row vector with mean for every column (or feature) of the input data
   *     xColSd - Row vector standard deviation for every column (or feature) of the input data.
   */
  def computeStats(data: RDD[(Double, Array[Double])], nfeatures: Int, nexamples: Long):
      (Double, DoubleMatrix, DoubleMatrix) = {
    val yMean: Double = data.map { case (y, features) => y }.reduce(_ + _) / nexamples

    // NOTE: We shuffle X by column here to compute column sum and sum of squares.
    val xColSumSq: RDD[(Int, (Double, Double))] = data.flatMap { case(y, features) =>
      val nCols = features.length
      // Traverse over every column and emit (col, value, value^2)
      Iterator.tabulate(nCols) { i =>
        (i, (features(i), features(i)*features(i)))
      }
    }.reduceByKey { case(x1, x2) =>
      (x1._1 + x2._1, x1._2 + x2._2)
    }
    val xColSumsMap = xColSumSq.collectAsMap()

    val xColMean = DoubleMatrix.zeros(nfeatures, 1)
    val xColSd = DoubleMatrix.zeros(nfeatures, 1)

    // Compute mean and unbiased variance using column sums
    var col = 0
    while (col < nfeatures) {
      xColMean.put(col, xColSumsMap(col)._1 / nexamples)
      val variance =
        (xColSumsMap(col)._2 - (math.pow(xColSumsMap(col)._1, 2) / nexamples)) / (nexamples)
      xColSd.put(col, math.sqrt(variance))
      col += 1
    }

    (yMean, xColMean, xColSd)
  }
}

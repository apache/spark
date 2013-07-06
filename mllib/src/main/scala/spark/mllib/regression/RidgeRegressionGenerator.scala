package spark.mllib.regression

import scala.util.Random

import org.jblas.DoubleMatrix

import spark.{RDD, SparkContext}
import spark.mllib.util.MLUtils


object RidgeRegressionGenerator {

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: RidgeRegressionGenerator " +
        "<master> <output_dir> <num_examples> <num_features> <num_partitions>")
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 100
    val parts: Int = if (args.length > 4) args(4).toInt else 2
    val eps = 10

    org.jblas.util.Random.seed(42)
    val sc = new SparkContext(sparkMaster, "RidgeRegressionGenerator")

    // Random values distributed uniformly in [-0.5, 0.5]
    val w = DoubleMatrix.rand(nfeatures, 1).subi(0.5)
    w.put(0, 0, 10)
    w.put(1, 0, 10)

    val data: RDD[(Double, Array[Double])] = sc.parallelize(0 until parts, parts).flatMap { p =>
      org.jblas.util.Random.seed(42 + p)
      val examplesInPartition = nexamples / parts

      val X = DoubleMatrix.rand(examplesInPartition, nfeatures)
      val y = X.mmul(w)

      val rnd = new Random(42 + p)

      val normalValues = Array.fill[Double](examplesInPartition)(rnd.nextGaussian() * eps)
      val yObs = new DoubleMatrix(normalValues).addi(y)

      Iterator.tabulate(examplesInPartition) { i =>
        (yObs.get(i, 0), X.getRow(i).toArray)
      }
    }

    MLUtils.saveData(data, outputPath)
    sc.stop()
  }
}

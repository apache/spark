package spark.ml.regression

import spark.{RDD, SparkContext}
import spark.ml.util.MLUtils

import org.apache.commons.math3.distribution.NormalDistribution
import org.jblas.DoubleMatrix

object LogisticRegressionGenerator {

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: LogisticRegressionGenerator " +
        "<master> <output_dir> <num_examples> <num_features> <num_partitions>")
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 2
    val parts: Int = if (args.length > 4) args(4).toInt else 2
    val eps = 3

    val sc = new SparkContext(sparkMaster, "LogisticRegressionGenerator")

    val data: RDD[(Double, Array[Double])] = sc.parallelize(0 until nexamples, parts).map { idx =>
      val rnd = new NormalDistribution(0, 1)
      rnd.reseedRandomGenerator(42 + idx)

      val y = if (idx % 2 == 0) 0 else 1
      val x = Array.fill[Double](nfeatures) {
        rnd.sample() + (y * eps)
      }
      (y, x)
    }

    MLUtils.saveData(data, outputPath)
    sc.stop()
  }
}

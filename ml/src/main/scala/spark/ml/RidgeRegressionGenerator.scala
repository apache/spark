package spark.ml

import spark._
import spark.SparkContext._

import org.apache.commons.math3.distribution.NormalDistribution
import org.jblas.DoubleMatrix

object RidgeRegressionGenerator {

  // Helper methods to load and save data used for RidgeRegression
  // Data format:
  // <l>, <f1> <f2> ...
  // where <f1>, <f2> are feature values in Double and 
  //       <l> is the corresponding label as Double
  def loadData(sc: SparkContext, dir: String) = {
    val data = sc.textFile(dir).map{ line => 
      val parts = line.split(",")
      val label = parts(0).toDouble
      val features = parts(1).trim().split(" ").map(_.toDouble)
      (label, features)
    }
    data
  }

  def saveData(data: RDD[(Double, Array[Double])], dir: String) {
    val dataStr = data.map(x => x._1 + "," + x._2.mkString(" "))
    dataStr.saveAsTextFile(dir)
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: RidgeRegressionGenerator <master> <output_dir>")
      System.exit(1)
    }
    org.jblas.util.Random.seed(42)
    val sc = new SparkContext(args(0), "RidgeRegressionGenerator")

    val nexamples = 1000
    val nfeatures = 100
    val eps = 10
    val parts = 2

    // Random values distributed uniformly in [-0.5, 0.5]
    val w = DoubleMatrix.rand(nfeatures, 1).subi(0.5)
    w.put(0, 0, 10)
    w.put(1, 0, 10)

    val data = sc.parallelize(0 until parts, parts).flatMap { p =>
      org.jblas.util.Random.seed(42 + p)
      val examplesInPartition = nexamples / parts

      val X = DoubleMatrix.rand(examplesInPartition, nfeatures)
      val y = X.mmul(w)

      val rnd = new NormalDistribution(0, eps)
      rnd.reseedRandomGenerator(42 + p)

      val normalValues = (0 until examplesInPartition).map(_ => rnd.sample())
      val yObs = new DoubleMatrix(examplesInPartition, 1, normalValues:_*).addi(y)
      
      (0 until examplesInPartition).map(i => 
        (yObs.get(i, 0), X.getRow(i).toArray)
      )
    }

    saveData(data, args(1))
    System.exit(0)
  }
}

package org.apache.spark.mllib.stat

import org.apache.spark.mllib.linalg.{Vectors, Matrix, Vector}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.rdd.RDD

import org.scalatest.FunSuite

class StatisticsSuite extends FunSuite with LocalSparkContext {

  //def corr(X: RDD[Vector]): Matrix = Correlations.corr(X)

  //def corr(X: RDD[Vector], method: String): Matrix = Correlations.corr(X, method)

  //def corr(x: RDD[Double], y: RDD[Double], method: String): Double = Correlations.corr(x, y, method)

  //def corr(x: RDD[Double], y: RDD[Double]): Double = Correlations.corr(x, y)

  val m = 4
  val n = 3
  val data = Seq(
    Vectors.dense(0.0, 1.0, 2.0),
    Vectors.dense(3.0, 4.0, 5.0),
    Vectors.dense(6.0, 7.0, 8.0),
    Vectors.dense(9.0, 0.0, 1.0)
  )
  //val X = sc.parallelize(data)


  val pearson = 0.6546537
  val spearman = 0.5

  test("corr(x, y) default and pearson") {
    val x = sc.parallelize(Array(1.0, 0.0, -2.0))
    val y = sc.parallelize(Array(4.0, 5.0, 3.0))
    val default = Statistics.corr(x, y)
    val p1 = Statistics.corr(x, y, "pearson")
    val p2 = Statistics.corr(x, y, "P")
    println("default = " + default + " p1 = " + p1 + " p2 = " + p2)
  }

  test("corr(x, y) spearman") {

  }

  test("corr(X) default and pearson") {

  }

  test("corr(X) spearman") {

  }
}

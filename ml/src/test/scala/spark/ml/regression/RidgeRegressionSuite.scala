package spark.ml.regression

import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import spark.SparkContext
import spark.SparkContext._


class RidgeRegressionSuite extends FunSuite with BeforeAndAfterAll {
  val sc = new SparkContext("local", "test")

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // Test if we can correctly learn Y = 3 + X1 + X2 when
  // X1 and X2 are collinear.
  test("multi-collinear variables") {
    val rnd = new Random(43)
    val x1 = Array.fill[Double](20)(rnd.nextGaussian())

    // Pick a mean close to mean of x1
    val rnd1 = new Random(42) //new NormalDistribution(0.1, 0.01)
    val x2 = Array.fill[Double](20)(0.1 + rnd1.nextGaussian() * 0.01)

    val xMat = (0 until 20).map(i => Array(x1(i), x2(i))).toArray

    val y = xMat.map(i => 3 + i(0) + i(1))
    val testData = (0 until 20).map(i => (y(i), xMat(i))).toArray

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val ridgeReg = new RidgeRegression().setLowLambda(0)
                                        .setHighLambda(10)

    val model = ridgeReg.train(testRDD)

    assert(model.intercept >= 2.9 && model.intercept <= 3.1)
    assert(model.weights.length === 2)
    assert(model.weights.get(0) >= 0.9 && model.weights.get(0) <= 1.1)
    assert(model.weights.get(1) >= 0.9 && model.weights.get(1) <= 1.1)
  }
}

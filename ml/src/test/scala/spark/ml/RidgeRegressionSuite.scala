package spark.ml

import spark.SparkContext
import spark.SparkContext._

import org.apache.commons.math3.distribution.NormalDistribution
import org.scalatest.FunSuite

class RidgeRegressionSuite extends FunSuite {

  // Test if we can correctly learn Y = 3 + X1 + X2 when
  // X1 and X2 are collinear.
  test("multi-collinear variables") {
    val rnd = new NormalDistribution(0, 1)
    rnd.reseedRandomGenerator(43)
    val sc = new SparkContext("local", "test")
    val x1 = Array.fill[Double](20)(rnd.sample())  

    // Pick a mean close to mean of x1
    val rnd1 = new NormalDistribution(0.1, 0.01)
    rnd1.reseedRandomGenerator(42)
    val x2 = Array.fill[Double](20)(rnd1.sample())

    val xMat = (0 until 20).map(i => Array(x1(i), x2(i))).toArray

    val y = xMat.map(i => 3 + i(0) + i(1))
    val testData = (0 until 20).map(i => (y(i), xMat(i))).toArray

    val testRDD = sc.parallelize(testData, 2)
    val ridgeReg = new RidgeRegression().setLowLambda(0)
                                        .setHighLambda(10)

    val model = ridgeReg.train(testRDD)

    assert(model.intercept >= 2.9 && model.intercept <= 3.1)
    assert(model.weights.length === 2)
    assert(model.weights.get(0) >= 0.9 && model.weights.get(0) <= 1.1)
    assert(model.weights.get(1) >= 0.9 && model.weights.get(1) <= 1.1)

    sc.stop()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }
}

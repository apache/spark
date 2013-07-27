package spark.mllib.regression

import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import spark.SparkContext
import spark.SparkContext._


class LassoSuite extends FunSuite with BeforeAndAfterAll {
  val sc = new SparkContext("local", "test")

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("Lasso_LocalRandomSGD") {
    val nPoints = 10000
    val rnd = new Random(42)

    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())
    val x2 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val A = 2.0
    val B = -1.5
    val C = 1.0e-2

    val y = (0 until nPoints).map { i =>
      A + B * x1(i) + C * x2(i) + 0.1*rnd.nextGaussian()
    }

    val testData = (0 until nPoints).map(i => (y(i).toDouble, Array(x1(i),x2(i)))).toArray

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val ls = new Lasso_LocalRandomSGD().setStepSize(1.0)
                        .setRegParam(0.01)
                                     .setNumIterations(20)

    val model = ls.train(testRDD)

    val weight0 = model.weights.get(0)
    val weight1 = model.weights.get(1)
    assert(weight0 >= -1.60 && weight0 <= -1.40, weight0 + " not in [-1.6, -1.4]")
    assert(weight1 >= -1.0e-3 && weight1 <= 1.0e-3, weight1 + " not in [-0.001, 0.001]")
    assert(model.intercept >= 1.9 && model.intercept <= 2.1, model.intercept + " not in [1.9, 2.1]")
  }
}

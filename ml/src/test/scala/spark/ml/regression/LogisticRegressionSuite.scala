package spark.ml.regression

import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import spark.SparkContext
import spark.SparkContext._


class LogisticRegressionSuite extends FunSuite with BeforeAndAfterAll {
  val sc = new SparkContext("local", "test")

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("logistic regression") {
    val nPoints = 10000
    val rnd = new Random(42)

    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val A = 2.0
    val B = -1.5

    // NOTE: if U is uniform[0, 1] then ln(u) - ln(1-u) is Logistic(0,1)
    val unifRand = new scala.util.Random(45)
    val rLogis = (0 until nPoints).map { i =>
      val u = unifRand.nextDouble()
      math.log(u) - math.log(1.0-u)
    }

    // y <- A + B*x + rlogis(100)
    // y <- as.numeric(y > 0)
    val y = (0 until nPoints).map { i =>
      val yVal = A + B * x1(i) + rLogis(i)
      if (yVal > 0) 1.0 else 0.0
    }

    val testData = (0 until nPoints).map(i => (y(i).toDouble, Array(x1(i)))).toArray

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val lr = new LogisticRegression().setStepSize(10.0)
                                     .setNumIterations(20)

    val model = lr.train(testRDD)

    val weight0 = model.weights.get(0)
    assert(weight0 >= -1.60 && weight0 <= -1.40, weight0 + " not in [-1.6, -1.4]")
    assert(model.intercept >= 1.9 && model.intercept <= 2.1, model.intercept + " not in [1.9, 2.1]")
  }
}

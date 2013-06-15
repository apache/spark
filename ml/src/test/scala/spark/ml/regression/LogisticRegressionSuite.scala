package spark.ml.regression

import spark.SparkContext
import spark.SparkContext._
import spark.Logging

import org.apache.commons.math3.distribution.NormalDistribution
import org.scalatest.FunSuite

class LogisticRegressionSuite extends FunSuite with Logging {

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("logistic regression") {
    val nPoints = 10000
    val rnd = new NormalDistribution(0, 1)
    rnd.reseedRandomGenerator(42)

    val sc = new SparkContext("local", "test")
    val x1 = Array.fill[Double](nPoints)(rnd.sample())

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
    val lr = new LogisticRegression().setStepSize(1.0)
                                     .setNumIterations(1000)

    val model = lr.train(testRDD)

    assert(model.weights.get(0) >= -1.60 && model.weights.get(0) <= -1.40)
    assert(model.intercept >= 1.9 && model.intercept <= 2.1)

    sc.stop()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }
}

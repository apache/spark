package spark.ml.recommendation

import scala.util.Random

import org.scalatest.FunSuite

import spark.SparkContext
import spark.SparkContext._
import spark.Logging

import org.jblas._


class ALSSuite extends FunSuite with Logging {

  test("rank-1 matrices") {
    testALS(10, 20, 1, 5, 0.5, 0.1)
  }

  test("rank-2 matrices") {
    testALS(20, 30, 2, 10, 0.7, 0.3)
  }

  /**
   * Test if we can correctly factorize R = U * P where U and P are of known rank.
   *
   * @param users          number of users
   * @param products       number of products
   * @param features       number of features (rank of problem)
   * @param iterations     number of iterations to run
   * @param samplingRate   what fraction of the user-product pairs are known
   * @param matchThreshold max difference allowed to consider a predicted rating correct
   */
  def testALS(users: Int, products: Int, features: Int, iterations: Int,
    samplingRate: Double, matchThreshold: Double)
  {
    val rand = new Random(42)

    // Create a random matrix with uniform values from -1 to 1
    def randomMatrix(m: Int, n: Int) =
      new DoubleMatrix(m, n, Array.fill(m * n)(rand.nextDouble() * 2 - 1): _*)

    val userMatrix = randomMatrix(users, features)
    val productMatrix = randomMatrix(features, products)
    val trueRatings = userMatrix.mmul(productMatrix)

    val sampledRatings = {
      for (u <- 0 until users; p <- 0 until products if rand.nextDouble() < samplingRate)
        yield (u, p, trueRatings.get(u, p))
    }

    val sc = new SparkContext("local", "test")

    val model = ALS.train(sc.parallelize(sampledRatings), features, iterations)

    val predictedU = new DoubleMatrix(users, features)
    for ((u, vec) <- model.userFeatures.collect(); i <- 0 until features) {
      predictedU.put(u, i, vec(i))
    }
    val predictedP = new DoubleMatrix(products, features)
    for ((p, vec) <- model.productFeatures.collect(); i <- 0 until features) {
      predictedP.put(p, i, vec(i))
    }
    val predictedRatings = predictedU.mmul(predictedP.transpose)

    for (u <- 0 until users; p <- 0 until products) {
      val prediction = predictedRatings.get(u, p)
      val correct = trueRatings.get(u, p)
      if (math.abs(prediction - correct) > matchThreshold) {
        fail("Model failed to predict (%d, %d): %f vs %f\ncorr: %s\npred: %s\nU: %s\n P: %s".format(
          u, p, correct, prediction, trueRatings, predictedRatings, predictedU, predictedP))
      }
    }

    sc.stop()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }
}


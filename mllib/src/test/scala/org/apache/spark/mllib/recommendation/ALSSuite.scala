/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.recommendation

import scala.collection.JavaConversions._
import scala.util.Random

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.jblas._

object ALSSuite {

  def generateRatingsAsJavaList(
      users: Int,
      products: Int,
      features: Int,
      samplingRate: Double): (java.util.List[Rating], DoubleMatrix) = {
    val (sampledRatings, trueRatings) = generateRatings(users, products, features, samplingRate)
    (seqAsJavaList(sampledRatings), trueRatings)
  }

  def generateRatings(
      users: Int,
      products: Int,
      features: Int,
      samplingRate: Double): (Seq[Rating], DoubleMatrix) = {
    val rand = new Random(42)

    // Create a random matrix with uniform values from -1 to 1
    def randomMatrix(m: Int, n: Int) =
      new DoubleMatrix(m, n, Array.fill(m * n)(rand.nextDouble() * 2 - 1): _*)

    val userMatrix = randomMatrix(users, features)
    val productMatrix = randomMatrix(features, products)
    val trueRatings = userMatrix.mmul(productMatrix)

    val sampledRatings = {
      for (u <- 0 until users; p <- 0 until products if rand.nextDouble() < samplingRate)
        yield Rating(u, p, trueRatings.get(u, p))
    }

    (sampledRatings, trueRatings)
  }

}


class ALSSuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("rank-1 matrices") {
    testALS(10, 20, 1, 15, 0.7, 0.3)
  }

  test("rank-2 matrices") {
    testALS(20, 30, 2, 15, 0.7, 0.3)
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
    val (sampledRatings, trueRatings) = ALSSuite.generateRatings(users, products,
      features, samplingRate)
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
  }
}


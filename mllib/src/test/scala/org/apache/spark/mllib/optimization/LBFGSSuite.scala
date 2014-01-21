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

package org.apache.spark.mllib.optimization

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

class LBFGSSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  @transient private var sc: SparkContext = _
  var dataRDD:RDD[(Double, Array[Double])] = _

  val nPoints = 10000
  val A = 2.0
  val B = -1.5

  val initialB = -1.0
  val initialWeights = Array(initialB)

  val gradient = new LogisticGradient()
  val numCorrections = 10
  val lineSearchTolerance = 0.9
  val convTolerance = 1e-12
  val maxNumIterations = 10
  val miniBatchFrac = 1.0

  val simpleUpdater = new SimpleUpdater()
  val squaredL2Updater = new SquaredL2Updater()

  // Add a extra variable consisting of all 1.0's for the intercept.
  val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
  val data = testData.map { case LabeledPoint(label, features) =>
    label -> Array(1.0, features: _*)
  }

  override def beforeAll() {
    sc = new SparkContext("local", "test")
    dataRDD = sc.parallelize(data, 2).cache()
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  def compareDouble(x: Double, y: Double, tol: Double = 1E-3): Boolean = {
    math.abs(x - y) / math.abs(y + 1e-15) < tol
  }

  test("Assert LBFGS loss is decreasing and matches the result of Gradient Descent.") {
    val updater = new SimpleUpdater()
    val regParam = 0

    val initialWeightsWithIntercept = Array(1.0, initialWeights: _*)

    val (_, loss) = LBFGS.runMiniBatchLBFGS(
      dataRDD,
      gradient,
      updater,
      numCorrections,
      lineSearchTolerance,
      convTolerance,
      maxNumIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(loss.last - loss.head < 0, "loss isn't decreasing.")

    val lossDiff = loss.init.zip(loss.tail).map {
      case (lhs, rhs) => lhs - rhs
    }
    assert(lossDiff.count(_ > 0).toDouble / lossDiff.size > 0.8)

    val stepSize = 1.0
    // Well, GD converges slower, so it requires more iterations!
    val numGDIterations = 50
    val (_, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(Math.abs((lossGD.last - loss.last) / loss.last) < 0.05,
      "LBFGS should match GD result within 5% error.")
  }

  test("Assert that LBFGS and Gradient Descent with L2 regularization get the same result.") {
    val regParam = 0.2

    // Prepare non-zero weights to compare the loss in first iteration.
    val initialWeightsWithIntercept = Array(0.3, 0.12)

    val (weightLBFGS, lossLBFGS) = LBFGS.runMiniBatchLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      lineSearchTolerance,
      convTolerance,
      maxNumIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    // With regularization, GD converges faster now!
    val numGDIterations = 20
    val stepSize = 1.0
    val (weightGD, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(compareDouble(lossGD(0), lossLBFGS(0)),
      "The first losses of LBFGS and GD should be the same.")

    assert(compareDouble(lossGD.last, lossLBFGS.last, 0.05),
      "The last losses of LBFGS and GD should be within 5% difference.")

    assert(
      compareDouble(weightLBFGS(0), weightGD(0), 0.05) &&
      compareDouble(weightLBFGS(1), weightGD(1), 0.05),
      "The weight differences between LBFGS and GD should be within 5% difference.")
  }
}

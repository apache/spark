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

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LocalSparkContext

class LBFGSSuite extends FunSuite with LocalSparkContext with Matchers {

  val nPoints = 10000
  val A = 2.0
  val B = -1.5

  val initialB = -1.0
  val initialWeights = Array(initialB)

  val gradient = new LogisticGradient()
  val numCorrections = 10
  val miniBatchFrac = 1.0

  val simpleUpdater = new SimpleUpdater()
  val squaredL2Updater = new SquaredL2Updater()

  // Add an extra variable consisting of all 1.0's for the intercept.
  val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
  val data = testData.map { case LabeledPoint(label, features) =>
    label -> Vectors.dense(1.0 +: features.toArray)
  }

  lazy val dataRDD = sc.parallelize(data, 2).cache()

  def compareDouble(x: Double, y: Double, tol: Double = 1E-3): Boolean = {
    math.abs(x - y) / (math.abs(y) + 1e-15) < tol
  }

  test("LBFGS loss should be decreasing and match the result of Gradient Descent.") {
    val regParam = 0

    val initialWeightsWithIntercept = Vectors.dense(1.0 +: initialWeights.toArray)
    val convergenceTol = 1e-12
    val maxNumIterations = 10

    val (_, loss) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      simpleUpdater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)

    // Since the cost function is convex, the loss is guaranteed to be monotonically decreasing
    // with L-BFGS optimizer.
    // (SGD doesn't guarantee this, and the loss will be fluctuating in the optimization process.)
    assert((loss, loss.tail).zipped.forall(_ > _), "loss should be monotonically decreasing.")

    val stepSize = 1.0
    // Well, GD converges slower, so it requires more iterations!
    val numGDIterations = 50
    val (_, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      simpleUpdater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    // GD converges a way slower than L-BFGS. To achieve 1% difference,
    // it requires 90 iterations in GD. No matter how hard we increase
    // the number of iterations in GD here, the lossGD will be always
    // larger than lossLBFGS. This is based on observation, no theoretically guaranteed
    assert(Math.abs((lossGD.last - loss.last) / loss.last) < 0.02,
      "LBFGS should match GD result within 2% difference.")
  }

  test("LBFGS and Gradient Descent with L2 regularization should get the same result.") {
    val regParam = 0.2

    // Prepare another non-zero weights to compare the loss in the first iteration.
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val convergenceTol = 1e-12
    val maxNumIterations = 10

    val (weightLBFGS, lossLBFGS) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)

    val numGDIterations = 50
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

    // The 2% difference here is based on observation, but is not theoretically guaranteed.
    assert(compareDouble(lossGD.last, lossLBFGS.last, 0.02),
      "The last losses of LBFGS and GD should be within 2% difference.")

    assert(compareDouble(weightLBFGS(0), weightGD(0), 0.02) &&
      compareDouble(weightLBFGS(1), weightGD(1), 0.02),
      "The weight differences between LBFGS and GD should be within 2%.")
  }

  test("The convergence criteria should work as we expect.") {
    val regParam = 0.0

    /**
     * For the first run, we set the convergenceTol to 0.0, so that the algorithm will
     * run up to the maxNumIterations which is 8 here.
     */
    val initialWeightsWithIntercept = Vectors.dense(0.0, 0.0)
    val maxNumIterations = 8
    var convergenceTol = 0.0

    val (_, lossLBFGS1) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)

    // Note that the first loss is computed with initial weights,
    // so the total numbers of loss will be numbers of iterations + 1
    assert(lossLBFGS1.length == 9)

    convergenceTol = 0.1
    val (_, lossLBFGS2) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)

    // Based on observation, lossLBFGS2 runs 3 iterations, no theoretically guaranteed.
    assert(lossLBFGS2.length == 4)
    assert((lossLBFGS2(2) - lossLBFGS2(3)) / lossLBFGS2(2) < convergenceTol)

    convergenceTol = 0.01
    val (_, lossLBFGS3) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)

    // With smaller convergenceTol, it takes more steps.
    assert(lossLBFGS3.length > lossLBFGS2.length)

    // Based on observation, lossLBFGS2 runs 5 iterations, no theoretically guaranteed.
    assert(lossLBFGS3.length == 6)
    assert((lossLBFGS3(4) - lossLBFGS3(5)) / lossLBFGS3(4) < convergenceTol)
  }
}

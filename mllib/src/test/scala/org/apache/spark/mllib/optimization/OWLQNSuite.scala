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
import org.scalatest.matchers.ShouldMatchers

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LocalSparkContext

class OWLQNSuite extends FunSuite with LocalSparkContext with ShouldMatchers {

  val nPoints = 10000
  val A = 2.0
  val B = -1.5

  val initialB = -1.0
  val initialWeights = Array(initialB)

  val gradient = new LogisticGradient()
  val numCorrections = 10
  val miniBatchFrac = 1.0

  val l1Updater = new L1Updater()
  val squaredL2Updater = new SquaredL2Updater()

  val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
  val data = testData.map { case LabeledPoint(label, features) =>
    label -> Vectors.dense(1.0, features.toArray: _*)
  }

  lazy val dataRDD = sc.parallelize(data, 2).cache()

  def compareDouble(x: Double, y: Double, tol: Double = 1E-3): Boolean = {
    math.abs(x - y) / (math.abs(y) + 1e-15) < tol
  }

  test("OWLQN loss should be decreasing and match the result of Gradient Descent.") {
    val regParam = 0.3
    val alpha = 1.0

    val initialWeightsWithIntercept = Vectors.dense(1.0, initialWeights: _*)
    val convergenceTol = 1e-12
    val maxNumIterations = 10

    val (weights1, loss) = OWLQN.runOWLQN(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      alpha,
      initialWeightsWithIntercept)

    // Since the cost function is convex, the loss is guaranteed to be monotonically decreasing
    // with OWLQN optimizer.
    assert((loss, loss.tail).zipped.forall(_ > _), "loss should be monotonically decreasing.")

    val stepSize = 1.0

    // Well, GD converges slower, so it requires more iterations!
    val numGDIterations = 100
    val (weights2, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      l1Updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    // GD converges a way slower than OWLQN. To achieve 1% difference,
    // it requires 90 iterations in GD. No matter how hard we increase
    // the number of iterations in GD here, the lossGD will be always
    // larger than lossLBFGS. This is based on observation, no theoretically guaranteed
    assert(Math.abs((lossGD.last - loss.last) / loss.last) < 0.02,
      "OWLQN should match GD result within 2% difference.")
  }

  test("OWLQN with L2 regularization should get the same result as LBFGS with L2 regularization.") {
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

    val (weightOWLQN, lossOWLQN) = OWLQN.runOWLQN(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      0.0,
      initialWeightsWithIntercept)

    assert(compareDouble(lossOWLQN(0), lossLBFGS(0)),
      "The first losses of LBFGS and OWLQN should be the same.")

    // OWLQN and LBFGS employ different line search, so the results might be slightly different.
    assert(compareDouble(lossOWLQN.last, lossLBFGS.last, 0.02),
      "The last losses of LBFGS and OWLQN should be within 2% difference.")

    assert(compareDouble(weightLBFGS(0), weightOWLQN(0), 0.02) &&
      compareDouble(weightLBFGS(1), weightOWLQN(1), 0.02),
      "The weight differences between LBFGS and OWLQN should be within 2%.")
  }

  test("The convergence criteria should work as expected.") {
    val regParam = 0.01
    val alpha = 0.5

    /**
     * For the first run, we set the convergenceTol to 0.0, so that the algorithm will
     * run up to the maxNumIterations which is 8 here.
     */
    val initialWeightsWithIntercept = Vectors.dense(0.0, 0.0)
    val maxNumIterations = 8
    var convergenceTol = 0.0

    val (weights1, lossOWLQN1) = OWLQN.runOWLQN(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      alpha,
      initialWeightsWithIntercept)

    // Note that the first loss is computed with initial weights,
    // so the total numbers of loss will be numbers of iterations + 1
    assert(lossOWLQN1.length == 9)

    convergenceTol = 0.1
    val (_, lossOWLQN2) = OWLQN.runOWLQN(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      alpha,
      initialWeightsWithIntercept)

    // Based on observation, lossLBFGS2 runs 3 iterations, no theoretically guaranteed.
    assert(lossOWLQN2.length == 4)
    assert((lossOWLQN2(2) - lossOWLQN2(3)) / lossOWLQN2(2) < convergenceTol)

    convergenceTol = 0.01
    val (_, lossOWLQN3) = OWLQN.runOWLQN(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      alpha,
      initialWeightsWithIntercept)

    // With smaller convergenceTol, it takes more steps.
    assert(lossOWLQN3.length > lossOWLQN2.length)

    // Based on observation, lossLBFGS2 runs 6 iterations, no theoretically guaranteed.
    assert(lossOWLQN3.length == 7)
    assert((lossOWLQN3(4) - lossOWLQN3(5)) / lossOWLQN3(4) < convergenceTol)
  }
}

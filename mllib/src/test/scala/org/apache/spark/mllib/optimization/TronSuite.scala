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

import scala.util.Random

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._

class TronSSuite extends FunSuite with MLlibTestSparkContext with Matchers {

  val nPoints = 10000
  val A = 2.0
  val B = -1.5

  val initialB = -1.0
  val initialWeights = Array(initialB)

  val gradient = new LogisticGradient()
  val hessian = new LogisticHessian()
  val numCorrections = 10

  val squaredL2Updater = new SquaredL2Updater()

  // Add an extra variable consisting of all 1.0's for the intercept.
  val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
  val data = testData.map { case LabeledPoint(label, features) =>
    label -> Vectors.dense(1.0 +: features.toArray)
  }

  lazy val dataRDD = sc.parallelize(data, 2).cache()

  test("Tron loss should be decreasing and match the result of L-BFGS.") {
    val regParam = 1.0

    val initialWeightsWithIntercept = Vectors.dense(1.0 +: initialWeights.toArray)
    val convergenceTol = 1e-5
    val numIterations = 10
    val (_, loss) = TRON.runTRON(
      dataRDD,
      gradient,
      hessian,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    val (_, lossLBFGS) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    // Since the cost function is convex, the loss is guaranteed to be monotonically decreasing
    // with Tron optimizer.
    assert((loss, loss.tail).zipped.forall(_ > _), "loss should be monotonically decreasing.")

    assert(Math.abs((lossLBFGS.last - loss.last) / loss.last) < 0.02,
      "LBFGS should match TRON result within 2% difference.")
    // Note that in TRON, the objective function is 1/2 w^T w + regparam \sum loss,
    // while in LBFGS, the function is regparam/2 w^T w + 1/nPoints \sum loss
  }

  test("TRON and LBFGS with L2 regularization should get the same result.") {
    val regParam = 3.0

    // Prepare another non-zero weights to compare the loss in the first iteration.
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val convergenceTol = 1e-5
    val numIterations = 10

    val (weightTRON, lossTRON) = TRON.runTRON(
      dataRDD,
      gradient,
      hessian,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    val (weightLBFGS, lossLBFGS) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    assert(lossTRON(0) ~= lossLBFGS(0) absTol 1E-5,
      "The first losses of LBFGS and TRON should be the same.")

    // The 2% difference here is based on observation, but is not theoretically guaranteed.
    assert(lossLBFGS.last ~= lossTRON.last relTol 0.02,
      "The last losses of LBFGS and TRON should be within 2% difference.")

    assert(
      (weightLBFGS(0) ~= weightTRON(0) relTol 0.02) && (weightLBFGS(1) ~= weightTRON(1) relTol 0.02),
      "The weight differences between LBFGS and TRON should be within 2%.")
  }

  test("The convergence criteria should work as we expect.") {
    val regParam = 0.0

    /**
     * For the first run, we set the convergenceTol to 0.0, so that the algorithm will
     * run up to the numIterations which is 8 here.
     */
    val initialWeightsWithIntercept = Vectors.dense(0.0, 0.0)
    val numIterations = 8
    var convergenceTol = 0.0

    val (_, lossTRON1) = TRON.runTRON(
      dataRDD,
      gradient,
      hessian,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)


    // Note that the first loss is computed with initial weights,
    // so the total numbers of loss will be numbers of iterations + 1
    assert(lossTRON1.length == 9)

    convergenceTol = 0.1
    val (_, lossTRON2) = TRON.runTRON(
      dataRDD,
      gradient,
      hessian,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    convergenceTol = 0.01
    val (_, lossTRON3) = TRON.runTRON(
      dataRDD,
      gradient,
      hessian,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    // With smaller convergenceTol, it takes more steps.
    assert(lossTRON3.length >= lossTRON2.length)
  }

  test("Optimize via class TRON.") {
    val regParam = 2.0

    // Prepare another non-zero weights to compare the loss in the first iteration.
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val convergenceTol = 1e-5
    val numIterations = 10

    val lbfgsOptimizer = new LBFGS(gradient, squaredL2Updater)
      .setNumCorrections(numCorrections)
      .setConvergenceTol(convergenceTol)
      .setNumIterations(numIterations)
      .setRegParam(regParam)

    val weightLBFGS = lbfgsOptimizer.optimize(dataRDD, initialWeightsWithIntercept)

    val tronOptimizer = new TRON(gradient, hessian)
      .setConvergenceTol(convergenceTol)
      .setNumIterations(numIterations)
      .setRegParam(regParam)

    val weightTRON = tronOptimizer.optimize(dataRDD, initialWeightsWithIntercept)

    // for class TRON and the optimize method, we only look at the weights
    assert(
      (weightLBFGS(0) ~= weightTRON(0) relTol 0.02) && (weightLBFGS(1) ~= weightTRON(1) relTol 0.02),
      "The weight differences between LBFGS and TRON should be within 2%.")
  }
}

class TRONClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small") {
    val m = 10
    val n = 50000
    val examples = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(n)(random.nextDouble))))
    }.cache()
    val tron = new TRON(new LogisticGradient, new LogisticHessian)
      .setConvergenceTol(1e-12)
      .setNumIterations(1)
      .setRegParam(1.0)
    val random = new Random(0)
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val weights = tron.optimize(examples, Vectors.dense(Array.fill(n)(random.nextDouble)))
  }
}


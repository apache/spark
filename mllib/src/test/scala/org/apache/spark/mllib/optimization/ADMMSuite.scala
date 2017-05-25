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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.scalatest.Matchers
import org.apache.spark.mllib.util.TestingUtils._

import scala.util.Random

class ADMMSuite extends SparkFunSuite with MLlibTestSparkContext with Matchers {

  val nPoints = 10000
  val A = 2.0
  val B = -1.5

  val initialB = -1.0
  val initialWeights = Array(initialB)

  val gradient = new LogisticGradient()
  val l1Updater = new L1Updater()
  val squaredL2Updater = new SquaredL2Updater()

  val miniBatchFrac = 1.0

  val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
  val data = testData.map { case LabeledPoint(label, features) =>
    label -> Vectors.dense(1.0 +: features.toArray)
  }

  lazy val dataRDD = sc.parallelize(data, 2).cache()
  test("ADMM loss should be decreasing and match the result of Gradient Descent.") {
    val initialWeightsWithIntercept = Vectors.dense(initialWeights.toArray :+ 1.0)
    val numSubModels = 2
    val regParam = 0.001
    val rho = 0.01
    val maxNumIterations = 3

    val (_, loss) = ADMM.runADMM(
      dataRDD,
      initialWeightsWithIntercept,
      numSubModels,
      gradient,
      l1Updater,
      regParam,
      rho,
      maxNumIterations)

    assert((loss, loss.tail).zipped.forall(_ > _), "loss should be monotonically decreasing.")

    val stepSize = 8.0
    val numGDIterations = 50
    val (_, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      l1Updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(lossGD.last ~= loss.last relTol 0.02,
      "ADMM should match GD result within 2% difference.")
  }

  test("ADMM and Gradient Descent with L2 regularization should get the same result.") {
    val regParam = 0.001

    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val convergenceTol = 1e-4
    val numIterations = 3
    val numSubModels = 2
    val rho = 0.01

    val (weightADMM, lossADMM) = ADMM.runADMM(
      dataRDD,
      initialWeightsWithIntercept,
      numSubModels,
      gradient,
      squaredL2Updater,
      regParam,
      rho,
      numIterations)

    val numGDIterations = 50
    val stepSize = 8.0
    val (weightGD, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept,
      convergenceTol)


    assert(lossGD(0) ~= lossADMM(0) absTol 1e-5,
      "the first losses of ADMM and GD should be the same.")

    assert(lossGD.last ~= lossADMM.last relTol 0.02,
      "the last losses of ADMM and GD should be within 2% difference.")

    assert(
      (weightADMM(0) ~= weightGD(0) relTol 0.02) && (weightADMM(1) ~= weightGD(1) relTol 0.02),
      "The Weight differences between ADMM and GD should be within 2%")
  }

  test("The convergence criteria should be work as we expect.") {
    val regParam = 0.001
    /**
     * For the first run, we set the convergenceTol to 0.0, so that the algorithm will
     * run up to the numIterations which is 8 here.
     */
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val numIteration = 10
    val numSubModels = 2
    val rho = 0.01
    var primalConvergenceTol = 0.0
    var dualConvergenceTol = 0.0
    val (_, lossADMM1) = ADMM.runADMM(
      dataRDD,
      initialWeightsWithIntercept,
      numSubModels,
      gradient,
      l1Updater,
      regParam,
      rho,
      numIteration,
      primalConvergenceTol,
      dualConvergenceTol)

    assert(lossADMM1.length == 11)

    primalConvergenceTol = 0.1
    dualConvergenceTol = 0.1
    val (_, lossADMM2) = ADMM.runADMM(
      dataRDD,
      initialWeightsWithIntercept,
      numSubModels,
      gradient,
      l1Updater,
      regParam,
      rho,
      numIteration,
      primalConvergenceTol,
      dualConvergenceTol)

    // Based on observation, lossADMM2 runs 2 iterations, no theoretically guaranteed.
    assert(lossADMM2.length == 3)
    assert((lossADMM2(1) - lossADMM2(2)) / lossADMM2(1) < primalConvergenceTol)

    primalConvergenceTol = 0.03
    dualConvergenceTol = 0.03
    val (_, lossADMM3) = ADMM.runADMM(
      dataRDD,
      initialWeightsWithIntercept,
      numSubModels,
      gradient,
      l1Updater,
      regParam,
      rho,
      numIteration,
      primalConvergenceTol,
      dualConvergenceTol)

    // With smaller convergenceTo, it takes more steps.
    assert(lossADMM3.length > lossADMM2.length)

    // Based on observation, lossADMM3 runs 6 iterations, no theoretically guaranteed.
    assert(lossADMM3.length == 7)
    assert((lossADMM3(5) - lossADMM3(6)) / lossADMM3(5) < primalConvergenceTol)
  }

  test("Optimize via class ADMM.") {
    val regParam = 0.001
    val numSubModels = 2
    val numIterations = 5
    val rho = 0.01
    val convergenceTol = 1e-12
    // Prepare another non-zero weights to compare the loss in the first iteration.
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val admmOptimizer = new ADMM(gradient, l1Updater)
      .setNumSubModels(numSubModels)
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setRho(rho)

    val weightADMM = admmOptimizer.optimize(dataRDD, initialWeightsWithIntercept)

    val numGDIterations = 50
    val stepSize = 8
    val (weightGD, _) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept,
      convergenceTol)
    // for class ADMM and the optimize method, we only look at the weights.
    assert(
      (weightADMM(0) ~= weightGD(0) relTol 0.02) &&  (weightADMM(1) ~= weightGD(1) relTol 0.02),
      "The weight differences between ADMM and GD should be within 2%.")
  }
}

class ADMMClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small") {
    val m = 4
    val n = 20000
    val examples = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    examples.count()

    val admm = new ADMM(new LogisticGradient, new L1Updater)
      .setNumSubModels(2)
      .setRegParam(0.001)
      .setRho(0.01)

    val random = new Random(0)
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val weights = admm.optimize(examples, Vectors.dense(Array.fill(n)(random.nextDouble)))
  }
}

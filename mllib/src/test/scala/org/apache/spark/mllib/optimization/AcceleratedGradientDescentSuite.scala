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

import breeze.linalg.{DenseVector => BDV, norm}
import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._

class AcceleratedGradientDescentSuite extends FunSuite with MLlibTestSparkContext with Matchers {

  val nPoints = 10000
  val A = 2.0
  val B = -1.5

  val initialB = -1.0
  val initialWeights = Array(initialB)

  val gradient = new LogisticGradient()
  val miniBatchFrac = 1.0

  val simpleUpdater = new SimpleUpdater()
  val squaredL2Updater = new SquaredL2Updater()

  // Add an extra variable consisting of all 1.0's for the intercept.
  val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
  val data = testData.map { case LabeledPoint(label, features) =>
    label -> Vectors.dense(1.0 +: features.toArray)
  }

  lazy val dataRDD = sc.parallelize(data, 2).cache()

  test("The optimal loss returned by Accelerated Gradient Descent should be similar to that from" +
    " Gradient Descent.") {
    val regParam = 0

    val initialWeightsWithIntercept = Vectors.dense(1.0 +: initialWeights.toArray)
    val stepSize = 1.0
    val convergenceTol = 1e-12
    val numIterations = 10

    val (_, lossAGD) = AcceleratedGradientDescent.run(
      dataRDD,
      gradient,
      simpleUpdater,
      stepSize,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    // GD converges more slowly, requiring more iterations.
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

    assert(lossAGD.last ~= lossGD.last relTol 0.02,
      "The optimal losses of AGD and GD should match within 2%.")
      // The 2% difference is based on observation and is not theoretically guaranteed.
  }

  test("The L2-regularized optimal loss returned by Accelerated Gradient Descent should be" +
    " similar to that from Gradient Descent.") {
    val regParam = 0.2

    // Provide non-zero weights to compare the loss values for these weights on the first iteration.
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val stepSize = 1.0
    val convergenceTol = 1e-12
    val numIterations = 10

    val (weightsAGD, lossAGD) = AcceleratedGradientDescent.run(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    val numGDIterations = 50
    val (weightsGD, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(lossAGD.last ~= lossGD.last relTol 0.02,
      "The optimal losses of AGD and GD should match within 2%.")
      // The 2% difference is based on observation and is not theoretically guaranteed.

    assert(
      (weightsAGD(0) ~= weightsGD(0) relTol 0.02) && (weightsAGD(1) ~= weightsGD(1) relTol 0.02),
      "The optimal weights returned by AGD and GD should match within 2%.")
      // The 2% difference is based on observation and is not theoretically guaranteed.
  }

  test("The convergenceTol parameter should behave as expected.") {
    val regParam = 0.0

    val initialWeightsWithIntercept = Vectors.dense(0.0, 0.0)
    val stepSize = 1.0

    // Optimize with a high maximum number of iterations and a loose convergenceTol. The
    // optimization will complete upon reaching convergenceTol, without reaching the iteration
    // limit.
    var numIterations = 1000
    var convergenceTol = 0.1
    val (weights1, loss1) = AcceleratedGradientDescent.run(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    // Optimize using one fewer step than the above run, and a strict convergenceTol. The returned
    // weights come from one iteration prior to the iteration producing weights1 above.
    numIterations = loss1.length - 1
    convergenceTol = 0.0
    val (weights2, loss2) = AcceleratedGradientDescent.run(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    assert(loss2.length == numIterations,
      "AGD should run for the specified number of iterations.")

    val w1 = BDV[Double](weights1.toArray)
    val w2 = BDV[Double](weights2.toArray)
    assert(norm(w1 - w2) / norm(w1) < 0.1,
      "The weights of AGD's last two steps should meet the convergence tolerance.")

    numIterations = 100
    convergenceTol = 0.01
    val (weights3, loss3) = AcceleratedGradientDescent.run(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      convergenceTol,
      numIterations,
      regParam,
      initialWeightsWithIntercept)

    assert(loss3.length > loss1.length,
      "AGD runs for more iterations with a tighter convergence tolerance.")
  }

  test("Optimize by calling the AcceleratedGradientDescent class directly.") {
    val regParam = 0.2

    val initialWeightsWithIntercept = Vectors.dense(1.0 +: initialWeights.toArray)
    val stepSize = 1.0
    val convergenceTol = 1e-12
    val numIterations = 10

    val agdOptimizer = new AcceleratedGradientDescent(gradient, squaredL2Updater)
      .setStepSize(stepSize)
      .setConvergenceTol(convergenceTol)
      .setNumIterations(numIterations)
      .setRegParam(regParam)

    val weightsAGD = agdOptimizer.optimize(dataRDD, initialWeightsWithIntercept)

    val numGDIterations = 50
    val (weightsGD, _) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,
      numGDIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(
      (weightsAGD(0) ~= weightsGD(0) relTol 0.02) && (weightsAGD(1) ~= weightsGD(1) relTol 0.02),
      "The optimal weights returned by AGD and GD should match within 2%.")
      // The 2% difference is based on observation and is not theoretically guaranteed.
  }
}

class AcceleratedGradientDescentClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small") {
    val m = 10
    val n = 200000
    val examples = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(n)(random.nextDouble))))
    }.cache()
    val agd = new AcceleratedGradientDescent(new LogisticGradient, new SquaredL2Updater)
      .setStepSize(1)
      .setConvergenceTol(1e-12)
      .setNumIterations(1)
      .setRegParam(1.0)
    val random = new Random(0)
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val weights = agd.optimize(examples, Vectors.dense(Array.fill(n)(random.nextDouble)))
  }
}

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

import scala.collection.JavaConverters._
import scala.util.Random

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext, MLUtils}
import org.apache.spark.util.TestingUtils._

object GradientDescentSuite {

  def generateLogisticInputAsList(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): java.util.List[LabeledPoint] = {
    generateGDInput(offset, scale, nPoints, seed).asJava
  }

  // Generate input of the form Y = logistic(offset + scale * X)
  def generateGDInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val unifRand = new Random(45)
    val rLogis = (0 until nPoints).map { i =>
      val u = unifRand.nextDouble()
      math.log(u) - math.log(1.0-u)
    }

    val y: Seq[Int] = (0 until nPoints).map { i =>
      val yVal = offset + scale * x1(i) + rLogis(i)
      if (yVal > 0) 1 else 0
    }

    (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(x1(i))))
  }
}

class GradientDescentSuite extends SparkFunSuite with MLlibTestSparkContext with Matchers {

  test("Assert the loss is decreasing.") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val initialB = -1.0
    val initialWeights = Array(initialB)

    val gradient = new LogisticGradient()
    val updater = new SimpleUpdater()
    val stepSize = 1.0
    val numIterations = 10
    val regParam = 0
    val miniBatchFrac = 1.0

    // Add a extra variable consisting of all 1.0's for the intercept.
    val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
    val data = testData.map { case LabeledPoint(label, features) =>
      label -> MLUtils.appendBias(features)
    }

    val dataRDD = sc.parallelize(data, 2).cache()
    val initialWeightsWithIntercept = Vectors.dense(initialWeights.toArray :+ 1.0)

    val (_, loss) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(loss.last - loss.head < 0, "loss isn't decreasing.")

    val lossDiff = loss.init.zip(loss.tail).map { case (lhs, rhs) => lhs - rhs }
    assert(lossDiff.count(_ > 0).toDouble / lossDiff.size > 0.8)
  }

  test("Test the loss and gradient of first iteration with regularization.") {

    val gradient = new LogisticGradient()
    val updater = new SquaredL2Updater()

    // Add a extra variable consisting of all 1.0's for the intercept.
    val testData = GradientDescentSuite.generateGDInput(2.0, -1.5, 10000, 42)
    val data = testData.map { case LabeledPoint(label, features) =>
      label -> Vectors.dense(1.0 +: features.toArray)
    }

    val dataRDD = sc.parallelize(data, 2).cache()

    // Prepare non-zero weights
    val initialWeightsWithIntercept = Vectors.dense(1.0, 0.5)

    val regParam0 = 0
    val (newWeights0, loss0) = GradientDescent.runMiniBatchSGD(
      dataRDD, gradient, updater, 1, 1, regParam0, 1.0, initialWeightsWithIntercept)

    val regParam1 = 1
    val (newWeights1, loss1) = GradientDescent.runMiniBatchSGD(
      dataRDD, gradient, updater, 1, 1, regParam1, 1.0, initialWeightsWithIntercept)

    assert(
      loss1(0) ~= (loss0(0) + (math.pow(initialWeightsWithIntercept(0), 2) +
        math.pow(initialWeightsWithIntercept(1), 2)) / 2) absTol 1E-5,
      """For non-zero weights, the regVal should be 0.5 * sum(w_i ^ 2).""")

    assert(
      (newWeights1(0) ~= (newWeights0(0) - initialWeightsWithIntercept(0)) absTol 1E-5) &&
      (newWeights1(1) ~= (newWeights0(1) - initialWeightsWithIntercept(1)) absTol 1E-5),
      "The different between newWeights with/without regularization " +
        "should be initialWeightsWithIntercept.")
  }

  test("iteration should end with convergence tolerance") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val initialB = -1.0
    val initialWeights = Array(initialB)

    val gradient = new LogisticGradient()
    val updater = new SimpleUpdater()
    val stepSize = 1.0
    val numIterations = 10
    val regParam = 0
    val miniBatchFrac = 1.0
    val convergenceTolerance = 5.0e-1

    // Add a extra variable consisting of all 1.0's for the intercept.
    val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
    val data = testData.map { case LabeledPoint(label, features) =>
      label -> MLUtils.appendBias(features)
    }

    val dataRDD = sc.parallelize(data, 2).cache()
    val initialWeightsWithIntercept = Vectors.dense(initialWeights.toArray :+ 1.0)

    val (_, loss) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept,
      convergenceTolerance)

    assert(loss.length < numIterations, "convergenceTolerance failed to stop optimization early")
  }
}

class GradientDescentClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val (weights, loss) = GradientDescent.runMiniBatchSGD(
      points,
      new LogisticGradient,
      new SquaredL2Updater,
      0.1,
      2,
      1.0,
      1.0,
      Vectors.dense(new Array[Double](n)))
  }
}

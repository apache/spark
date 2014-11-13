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

package org.apache.spark.mllib.classification

import scala.util.Random
import scala.collection.JavaConversions._

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._

object LogisticRegressionSuite {

  def generateLogisticInputAsList(
    offset: Double,
    scale: Double,
    nPoints: Int,
    seed: Int): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateLogisticInput(offset, scale, nPoints, seed))
  }

  // Generate input of the form Y = logistic(offset + scale*X)
  def generateLogisticInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val y = (0 until nPoints).map { i =>
      val p = 1.0 / (1.0 + math.exp(-(offset + scale * x1(i))))
      if (rnd.nextDouble() < p) 1.0 else 0.0
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(Array(x1(i)))))
    testData
  }
}

class LogisticRegressionSuite extends FunSuite with MLlibTestSparkContext with Matchers {
  def validatePrediction(
      predictions: Seq[Double],
      input: Seq[LabeledPoint],
      expectedAcc: Double = 0.83) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    // At least 83% of the predictions should be on.
    ((input.length - numOffPredictions).toDouble / input.length) should be > expectedAcc
  }

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("logistic regression with SGD") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val lr = new LogisticRegressionWithSGD().setIntercept(true)
    lr.optimizer.setStepSize(10.0).setNumIterations(20)

    val model = lr.run(testRDD)

    // Test the weights
    assert(model.weights(0) ~== -1.52 relTol 0.01)
    assert(model.intercept ~== 2.00 relTol 0.01)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn A, B where Y = logistic(A + B*X)
  test("logistic regression with LBFGS") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)

    val model = lr.run(testRDD)

    // Test the weights
    assert(model.weights(0) ~== -1.52 relTol 0.01)
    assert(model.intercept ~== 2.00 relTol 0.01)
    assert(model.weights(0) ~== model.weights(0) relTol 0.01)
    assert(model.intercept ~== model.intercept relTol 0.01)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("logistic regression with initial weights with SGD") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialB = -1.0
    val initialWeights = Vectors.dense(initialB)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    // Use half as many iterations as the previous test.
    val lr = new LogisticRegressionWithSGD().setIntercept(true)
    lr.optimizer.setStepSize(10.0).setNumIterations(10)

    val model = lr.run(testRDD, initialWeights)

    // Test the weights
    assert(model.weights(0) ~== -1.50 relTol 0.01)
    assert(model.intercept ~== 1.97 relTol 0.01)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("logistic regression with initial weights and non-default regularization parameter") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialB = -1.0
    val initialWeights = Vectors.dense(initialB)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    // Use half as many iterations as the previous test.
    val lr = new LogisticRegressionWithSGD().setIntercept(true)
    lr.optimizer.
      setStepSize(10.0).
      setNumIterations(10).
      setRegParam(1.0)

    val model = lr.run(testRDD, initialWeights)

    // Test the weights
    assert(model.weights(0) ~== -430000.0 relTol 20000.0)
    assert(model.intercept ~== 370000.0 relTol 20000.0)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData, 0.8)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData, 0.8)
  }

  test("logistic regression with initial weights with LBFGS") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialB = -1.0
    val initialWeights = Vectors.dense(initialB)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    // Use half as many iterations as the previous test.
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)

    val model = lr.run(testRDD, initialWeights)

    // Test the weights
    assert(model.weights(0) ~== -1.50 relTol 0.02)
    assert(model.intercept ~== 1.97 relTol 0.02)

    val validationData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("numerical stability of scaling features using logistic regression with LBFGS") {
    /**
     * If we rescale the features, the condition number will be changed so the convergence rate
     * and the solution will not equal to the original solution multiple by the scaling factor
     * which it should be.
     *
     * However, since in the LogisticRegressionWithLBFGS, we standardize the training dataset first,
     * no matter how we multiple a scaling factor into the dataset, the convergence rate should be
     * the same, and the solution should equal to the original solution multiple by the scaling
     * factor.
     */

    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val testData = LogisticRegressionSuite.generateLogisticInput(A, B, nPoints, 42)

    val initialWeights = Vectors.dense(0.0)

    val testRDD1 = sc.parallelize(testData, 2)

    val testRDD2 = sc.parallelize(
      testData.map(x => LabeledPoint(x.label, Vectors.fromBreeze(x.features.toBreeze * 1.0E3))), 2)

    val testRDD3 = sc.parallelize(
      testData.map(x => LabeledPoint(x.label, Vectors.fromBreeze(x.features.toBreeze * 1.0E6))), 2)

    testRDD1.cache()
    testRDD2.cache()
    testRDD3.cache()

    val lrA = new LogisticRegressionWithLBFGS().setIntercept(true)
    val lrB = new LogisticRegressionWithLBFGS().setIntercept(true).setFeatureScaling(false)

    val modelA1 = lrA.run(testRDD1, initialWeights)
    val modelA2 = lrA.run(testRDD2, initialWeights)
    val modelA3 = lrA.run(testRDD3, initialWeights)

    val modelB1 = lrB.run(testRDD1, initialWeights)
    val modelB2 = lrB.run(testRDD2, initialWeights)
    val modelB3 = lrB.run(testRDD3, initialWeights)

    // For model trained with feature standardization, the weights should
    // be the same in the scaled space. Note that the weights here are already
    // in the original space, we transform back to scaled space to compare.
    assert(modelA1.weights(0) ~== modelA2.weights(0) * 1.0E3 absTol 0.01)
    assert(modelA1.weights(0) ~== modelA3.weights(0) * 1.0E6 absTol 0.01)

    // Training data with different scales without feature standardization
    // will not yield the same result in the scaled space due to poor
    // convergence rate.
    assert(modelB1.weights(0) !~== modelB2.weights(0) * 1.0E3 absTol 0.1)
    assert(modelB1.weights(0) !~== modelB3.weights(0) * 1.0E6 absTol 0.1)
  }

}

class LogisticRegressionClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction using SGD optimizer") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = LogisticRegressionWithSGD.train(points, 2)

    val predictions = model.predict(points.map(_.features))

    // Materialize the RDDs
    predictions.count()
  }

  test("task size should be small in both training and prediction using LBFGS optimizer") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true)
    lr.optimizer.setNumIterations(2)
    val model = lr.run(points)

    val predictions = model.predict(points.map(_.features))

    // Materialize the RDDs
    predictions.count()
  }

}

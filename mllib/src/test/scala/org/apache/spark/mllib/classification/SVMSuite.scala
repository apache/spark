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

import scala.collection.JavaConversions._
import scala.util.Random

import org.jblas.DoubleMatrix
import org.scalatest.FunSuite

import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.util.Utils

object SVMSuite {

  def generateSVMInputAsList(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateSVMInput(intercept, weights, nPoints, seed))
  }

  // Generate noisy input of the form Y = signum(x.dot(weights) + intercept + noise)
  def generateSVMInput(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val weightsMat = new DoubleMatrix(1, weights.length, weights:_*)
    val x = Array.fill[Array[Double]](nPoints)(
        Array.fill[Double](weights.length)(rnd.nextDouble() * 2.0 - 1.0))
    val y = x.map { xi =>
      val yD = new DoubleMatrix(1, xi.length, xi: _*).dot(weightsMat) +
        intercept + 0.01 * rnd.nextGaussian()
      if (yD < 0) 0.0 else 1.0
    }
    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }

  /** Binary labels, 3 features */
  private val binaryModel = new SVMModel(weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5)

}

class SVMSuite extends FunSuite with MLlibTestSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }

  test("SVM with threshold") {
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val svm = new SVMWithSGD().setIntercept(true)
    svm.optimizer.setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.run(testRDD)

    val validationData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 17)
    val validationRDD  = sc.parallelize(validationData, 2)

    // Test prediction on RDD.

    var predictions = model.predict(validationRDD.map(_.features)).collect()
    assert(predictions.count(_ == 0.0) != predictions.length)

    // High threshold makes all the predictions 0.0
    model.setThreshold(10000.0)
    predictions = model.predict(validationRDD.map(_.features)).collect()
    assert(predictions.count(_ == 0.0) == predictions.length)

    // Low threshold makes all the predictions 1.0
    model.setThreshold(-10000.0)
    predictions = model.predict(validationRDD.map(_.features)).collect()
    assert(predictions.count(_ == 1.0) == predictions.length)
  }

  test("SVM using local random SGD") {
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B,C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val svm = new SVMWithSGD().setIntercept(true)
    svm.optimizer.setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.run(testRDD)

    val validationData = SVMSuite.generateSVMInput(A, Array[Double](B,C), nPoints, 17)
    val validationRDD  = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("SVM local random SGD with initial weights") {
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B,C), nPoints, 42)

    val initialB = -1.0
    val initialC = -1.0
    val initialWeights = Vectors.dense(initialB, initialC)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val svm = new SVMWithSGD().setIntercept(true)
    svm.optimizer.setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.run(testRDD, initialWeights)

    val validationData = SVMSuite.generateSVMInput(A, Array[Double](B,C), nPoints, 17)
    val validationRDD  = sc.parallelize(validationData,2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("SVM with invalid labels") {
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B,C), nPoints, 42)
    val testRDD = sc.parallelize(testData, 2)

    val testRDDInvalid = testRDD.map { lp =>
      if (lp.label == 0.0) {
        LabeledPoint(-1.0, lp.features)
      } else {
        lp
      }
    }

    intercept[SparkException] {
      SVMWithSGD.train(testRDDInvalid, 100)
    }

    // Turning off data validation should not throw an exception
    new SVMWithSGD().setValidateData(false).run(testRDDInvalid)
  }

  test("model save/load") {
    // NOTE: This will need to be generalized once there are multiple model format versions.
    val model = SVMSuite.binaryModel

    model.clearThreshold()
    assert(model.getThreshold.isEmpty)

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    try {
      model.save(sc, path)
      val sameModel = SVMModel.load(sc, path)
      assert(model.weights == sameModel.weights)
      assert(model.intercept == sameModel.intercept)
      assert(sameModel.getThreshold.isEmpty)
    } finally {
      Utils.deleteRecursively(tempDir)
    }

    // Save model with threshold.
    try {
      model.setThreshold(0.7)
      model.save(sc, path)
      val sameModel2 = SVMModel.load(sc, path)
      assert(model.getThreshold.get == sameModel2.getThreshold.get)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class SVMClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = SVMWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}

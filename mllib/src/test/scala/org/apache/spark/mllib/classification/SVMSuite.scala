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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.jblas.DoubleMatrix

import org.apache.spark.{SparkException, SparkContext}
import org.apache.spark.mllib.regression._

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
    y.zip(x).map(p => LabeledPoint(p._1, p._2))
  }

}

class SVMSuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      prediction != expected.label
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
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

    val svm = new SVMWithSGD()
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
    val initialWeights = Array(initialB,initialC)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val svm = new SVMWithSGD()
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
      val model = SVMWithSGD.train(testRDDInvalid, 100)
    }

    // Turning off data validation should not throw an exception
    val noValidationModel = new SVMWithSGD().setValidateData(false).run(testRDDInvalid)
  }
}

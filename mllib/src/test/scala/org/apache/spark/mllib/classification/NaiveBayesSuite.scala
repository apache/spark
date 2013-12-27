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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext

object NaiveBayesSuite {

  private def calcLabel(p: Double, weightPerLabel: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until weightPerLabel.length) {
      sum += weightPerLabel(j)
      if (p < sum) return j
    }
    -1
  }

  // Generate input of the form Y = (weightMatrix*x).argmax()
  def generateNaiveBayesInput(
      weightPerLabel: Array[Double],          // 1XC
      weightsMatrix: Array[Array[Double]],    // CXD
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val D = weightsMatrix(0).length
    val rnd = new Random(seed)

    val _weightPerLabel = weightPerLabel.map(math.pow(math.E, _))
    val _weightMatrix = weightsMatrix.map(row => row.map(math.pow(math.E, _)))

    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _weightPerLabel)
      val xi = Array.tabulate[Double](D) { j =>
        if (rnd.nextDouble() < _weightMatrix(y)(j)) 1 else 0
      }

      LabeledPoint(y, xi)
    }
  }
}

class NaiveBayesSuite extends FunSuite with BeforeAndAfterAll {
  @transient private var sc: SparkContext = _

  override def beforeAll() {
    sc = new SparkContext("local", "test")
  }

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOfPredictions = predictions.zip(input).count {
      case (prediction, expected) =>
        prediction != expected.label
    }
    // At least 80% of the predictions should be on.
    assert(numOfPredictions < input.length / 5)
  }

  test("Naive Bayes") {
    val nPoints = 10000

    val weightPerLabel = Array(math.log(0.5), math.log(0.3), math.log(0.2))
    val weightsMatrix = Array(
      Array(math.log(0.91), math.log(0.03), math.log(0.03), math.log(0.03)), // label 0
      Array(math.log(0.03), math.log(0.91), math.log(0.03), math.log(0.03)), // label 1
      Array(math.log(0.03), math.log(0.03), math.log(0.91), math.log(0.03))  // label 2
    )

    val testData = NaiveBayesSuite.generateNaiveBayesInput(weightPerLabel, weightsMatrix, nPoints, 42)
    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val model = NaiveBayes.train(3, 4, testRDD)

    val validationData = NaiveBayesSuite.generateNaiveBayesInput(weightPerLabel, weightsMatrix, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }
}

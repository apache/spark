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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{LocalClusterSparkContext, LocalSparkContext}

object NaiveBayesSuite {

  private def calcLabel(p: Double, pi: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until pi.length) {
      sum += pi(j)
      if (p < sum) return j
    }
    -1
  }

  // Generate input of the form Y = (theta * x).argmax()
  def generateNaiveBayesInput(
      pi: Array[Double],            // 1XC
      theta: Array[Array[Double]],  // CXD
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val D = theta(0).length
    val rnd = new Random(seed)

    val _pi = pi.map(math.pow(math.E, _))
    val _theta = theta.map(row => row.map(math.pow(math.E, _)))

    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _pi)
      val xi = Array.tabulate[Double](D) { j =>
        if (rnd.nextDouble() < _theta(y)(j)) 1 else 0
      }

      LabeledPoint(y, Vectors.dense(xi))
    }
  }
}

class NaiveBayesSuite extends FunSuite with LocalSparkContext {

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

    val pi = Array(0.5, 0.3, 0.2).map(math.log)
    val theta = Array(
      Array(0.91, 0.03, 0.03, 0.03), // label 0
      Array(0.03, 0.91, 0.03, 0.03), // label 1
      Array(0.03, 0.03, 0.91, 0.03)  // label 2
    ).map(_.map(math.log))

    val testData = NaiveBayesSuite.generateNaiveBayesInput(pi, theta, nPoints, 42)
    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val model = NaiveBayes.train(testRDD)

    val validationData = NaiveBayesSuite.generateNaiveBayesInput(pi, theta, nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }
}

class NaiveBayesClusterSuite extends FunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 10
    val n = 200000
    val examples = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map { i =>
        LabeledPoint(random.nextInt(2), Vectors.dense(Array.fill(n)(random.nextDouble())))
      }
    }
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = NaiveBayes.train(examples)
    val predictions = model.predict(examples.map(_.features))
  }
}

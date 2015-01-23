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

package org.apache.spark.mllib.regression

import java.io.Serializable
import java.util.Arrays.binarySearch

import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}
import org.apache.spark.rdd.RDD

/**
 * Regression model for Isotonic regression
 *
 * @param features Array of features.
 * @param labels Array of labels associated to the features at the same index.
 */
class IsotonicRegressionModel (
    features: Array[Double],
    val labels: Array[Double])
  extends Serializable {

  /**
   * Predict labels for provided features
   * Using a piecewise constant function
   *
   * @param testData features to be labeled
   * @return predicted labels
   */
  def predict(testData: RDD[Double]): RDD[Double] =
    testData.map(predict)

  /**
   * Predict labels for provided features
   * Using a piecewise constant function
   *
   * @param testData features to be labeled
   * @return predicted labels
   */
  def predict(testData: JavaRDD[java.lang.Double]): JavaDoubleRDD =
    JavaDoubleRDD.fromRDD(predict(testData.rdd.asInstanceOf[RDD[Double]]))

  /**
   * Predict a single label
   * Using a piecewise constant function
   *
   * @param testData feature to be labeled
   * @return predicted label
   */
  def predict(testData: Double): Double = {
    val result = binarySearch(features, testData)

    val index =
      if (result == -1) {
        0
      } else if (result < 0) {
        -result - 2
      } else {
        result
      }

    labels(index)
  }
}

/**
 * Isotonic regression
 * Currently implemented using oarallel pool adjacent violators algorithm for monotone regression
 */
class IsotonicRegression
  extends Serializable {

  /**
   * Run algorithm to obtain isotonic regression model
   *
   * @param input (label, feature, weight)
   * @param isotonic isotonic (increasing) or antitonic (decreasing) sequence
   * @return isotonic regression model
   */
  def run(
      input: RDD[(Double, Double, Double)],
      isotonic: Boolean = true): IsotonicRegressionModel = {
    createModel(
      parallelPoolAdjacentViolators(input, isotonic),
      isotonic)
  }

  /**
   * Creates isotonic regression model with given parameters
   *
   * @param predictions labels estimated using isotonic regression algorithm.
   *                    Used for predictions on new data points.
   * @param isotonic isotonic (increasing) or antitonic (decreasing) sequence
   * @return isotonic regression model
   */
  protected def createModel(
      predictions: Array[(Double, Double, Double)],
      isotonic: Boolean): IsotonicRegressionModel = {

    val labels = predictions.map(_._1)
    val features = predictions.map(_._2)

    new IsotonicRegressionModel(features, labels)
  }

  /**
   * Performs a pool adjacent violators algorithm (PAVA)
   * Uses approach with single processing of data where violators in previously processed
   * data created by pooling are fixed immediatelly.
   * Uses optimization of discovering monotonicity violating sequences
   * Method in situ mutates input array
   *
   * @param in input data
   * @param isotonic asc or desc
   * @return result
   */
  private def poolAdjacentViolators(
      in: Array[(Double, Double, Double)],
      isotonic: Boolean): Array[(Double, Double, Double)] = {

    // Pools sub array within given bounds assigning weighted average value to all elements
    def pool(in: Array[(Double, Double, Double)], start: Int, end: Int): Unit = {
      val poolSubArray = in.slice(start, end + 1)

      val weightedSum = poolSubArray.map(lp => lp._1 * lp._3).sum
      val weight = poolSubArray.map(_._3).sum

      var i = start
      while (i <= end) {
        in(i) = (weightedSum / weight, in(i)._2, in(i)._3)
        i = i + 1
      }
    }

    val monotonicityConstraintHolds: (Double, Double) => Boolean =
      (x, y) => if (isotonic) x <= y else x >= y

    var i = 0
    while (i < in.length) {
      var j = i

      // Find monotonicity violating sequence, if any
      while (j < in.length - 1 && !monotonicityConstraintHolds(in(j)._1, in(j + 1)._1)) {
        j = j + 1
      }

      // If monotonicity was not violated, move to next data point
      if (i == j) {
        i = i + 1
      } else {
        // Otherwise pool the violating sequence
        // And check if pooling caused monotonicity violation in previously processed points
        while (i >= 0 && !monotonicityConstraintHolds(in(i)._1, in(i + 1)._1)) {
          pool(in, i, j)
          i = i - 1
        }

        i = j
      }
    }

    in
  }

  /**
   * Performs parallel pool adjacent violators algorithm
   * Calls Pool adjacent violators on each partition and then again on the result
   *
   * @param testData input
   * @param isotonic isotonic (increasing) or antitonic (decreasing) sequence
   * @return result
   */
  private def parallelPoolAdjacentViolators(
      testData: RDD[(Double, Double, Double)],
      isotonic: Boolean): Array[(Double, Double, Double)] = {

    val parallelStepResult = testData
      .sortBy(_._2)
      .mapPartitions(it => poolAdjacentViolators(it.toArray, isotonic).toIterator)

    poolAdjacentViolators(parallelStepResult.collect(), isotonic)
  }
}

/**
 * Top-level methods for monotone regression (either isotonic or antitonic).
 */
object IsotonicRegression {

  /**
   * Train a monotone regression model given an RDD of (label, feature, weight).
   * Label is the dependent y value
   * Weight of the data point is the number of measurements. Default is 1
   *
   * @param input RDD of (label, feature, weight).
   *              Each point describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   *              and weight as number of measurements
   * @param isotonic isotonic (increasing) or antitonic (decreasing) sequence
   */
  def train(
      input: RDD[(Double, Double, Double)],
      isotonic: Boolean = true): IsotonicRegressionModel = {
    new IsotonicRegression().run(input, isotonic)
  }

  /**
   * Train a monotone regression model given an RDD of (label, feature, weight).
   * Label is the dependent y value
   * Weight of the data point is the number of measurements. Default is 1
   *
   * @param input RDD of (label, feature, weight).
   * @param isotonic isotonic (increasing) or antitonic (decreasing) sequence
   * @return
   */
  def train(
      input: JavaRDD[(java.lang.Double, java.lang.Double, java.lang.Double)],
      isotonic: Boolean): IsotonicRegressionModel = {
    new IsotonicRegression()
      .run(
        input.rdd.asInstanceOf[RDD[(Double, Double, Double)]],
        isotonic)
  }
}

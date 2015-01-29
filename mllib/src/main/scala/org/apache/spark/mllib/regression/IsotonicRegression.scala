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
 * Regression model for isotonic regression.
 *
 * @param boundaries Array of boundaries for which predictions are known.
 * @param predictions Array of predictions associated to the boundaries at the same index.
 */
class IsotonicRegressionModel (
    boundaries: Array[Double],
    val predictions: Array[Double])
  extends Serializable {

  private def isSorted(xs: Array[Double]): Boolean = {
    var i = 1
    while (i < xs.length) {
      if (xs(i) < xs(i - 1)) false
      i += 1
    }
    true
  }

  assert(isSorted(boundaries))
  assert(boundaries.length == predictions.length)

  /**
   * Predict labels for provided features.
   * Using a piecewise linear function.
   *
   * @param testData Features to be labeled.
   * @return Predicted labels.
   */
  def predict(testData: RDD[Double]): RDD[Double] =
    testData.map(predict)

  /**
   * Predict labels for provided features.
   * Using a piecewise linear function.
   *
   * @param testData Features to be labeled.
   * @return Predicted labels.
   */
  def predict(testData: JavaDoubleRDD): JavaDoubleRDD =
    JavaDoubleRDD.fromRDD(predict(testData.rdd.asInstanceOf[RDD[Double]]))

  /**
   * Predict a single label.
   * Using a piecewise linear function.
   *
   * @param testData Feature to be labeled.
   * @return Predicted label.
   *         If testData exactly matches a boundary then associated prediction is directly returned
   *         If testData is lower or higher than all boundaries
   *           then first or last prediction is returned respectively
   *         If testData falls between two values in boundary then predictions is treated as piecewise
   *           linear function and interpolated value is returned
   */
  def predict(testData: Double): Double = {

    def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x: Double): Double = {
      y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    }

    val insertIndex = binarySearch(boundaries, testData)

    val normalisedInsertIndex = -insertIndex - 1

    //Find if the index was lower than all values,
    //higher than all values, inbetween two values or exact match.
    if (insertIndex == -1) {
      predictions.head
    } else if (normalisedInsertIndex == boundaries.length){
      predictions.last
    } else if (insertIndex < 0) {
      linearInterpolation(
        boundaries(normalisedInsertIndex - 1),
        predictions(normalisedInsertIndex - 1),
        boundaries(normalisedInsertIndex),
        predictions(normalisedInsertIndex),
        testData)
    } else {
      predictions(insertIndex)
    }
  }
}

/**
 * Isotonic regression.
 * Currently implemented using parallelized pool adjacent violators algorithm.
 * Currently only univariate (single feature) algorithm supported.
 *
 * Sequential PAV implementation based on:
 * Tibshirani, Ryan J., Holger Hoefling, and Robert Tibshirani.
 *   "Nearly-isotonic regression." Technometrics 53.1 (2011): 54-61.
 *
 * Sequential PAV parallelized as per:
 * Kearsley, Anthony J., Richard A. Tapia, and Michael W. Trosset.
 *   "An approach to parallelizing isotonic regression."
 *   Applied Mathematics and Parallel Computing. Physica-Verlag HD, 1996. 141-147.
 */
class IsotonicRegression extends Serializable {

  /**
   * Run pool adjacent violators algorithm to obtain isotonic regression model.
   *
   * @param input RDD of tuples (label, feature, weight) where label is dependent variable
   *              for which we calculate isotonic regression, feature is independent variable
   *              and weight represents number of measures with default 1.
   *
   * @param isotonic Isotonic (increasing) or antitonic (decreasing) sequence.
   * @return Isotonic regression model.
   */
  def run(
      input: RDD[(Double, Double, Double)],
      isotonic: Boolean): IsotonicRegressionModel =
    createModel(parallelPoolAdjacentViolators(input, isotonic), isotonic)

/**
   * Run pool adjacent violators algorithm to obtain isotonic regression model.
   *
   * @param input JavaRDD of tuples (label, feature, weight) where label is dependent variable
   *              for which we calculate isotonic regression, feature is independent variable
   *              and weight represents number of measures with default 1.
   *
   * @param isotonic Isotonic (increasing) or antitonic (decreasing) sequence.
   * @return Isotonic regression model.
   */
  def run(
       input: JavaRDD[(java.lang.Double, java.lang.Double, java.lang.Double)],
       isotonic: Boolean): IsotonicRegressionModel =
    run(input.rdd.asInstanceOf[RDD[(Double, Double, Double)]], isotonic)

  /**
   * Creates isotonic regression model with given parameters.
   *
   * @param predictions Predictions calculated using pool adjacent violators algorithm.
   *                    Used for predictions on new data points.
   * @param isotonic Isotonic (increasing) or antitonic (decreasing) sequence.
   * @return Isotonic regression model.
   */
  protected def createModel(
      predictions: Array[(Double, Double, Double)],
      isotonic: Boolean): IsotonicRegressionModel = {

    val labels = predictions.map(_._1)
    val features = predictions.map(_._2)

    new IsotonicRegressionModel(features, labels)
  }

  /**
   * Performs a pool adjacent violators algorithm (PAV).
   * Uses approach with single processing of data where violators
   * in previously processed data created by pooling are fixed immediatelly.
   * Uses optimization of discovering monotonicity violating sequences (blocks).
   *
   * @param input Input data of tuples (label, feature, weight).
   * @param isotonic Isotonic (increasing) or antitonic (decreasing) sequence.
   * @return Result tuples (label, feature, weight) where labels were updated
   *         to form a monotone sequence as per isotonic regression definition.
   */
  private def poolAdjacentViolators(
      input: Array[(Double, Double, Double)],
      isotonic: Boolean): Array[(Double, Double, Double)] = {

    // Pools sub array within given bounds assigning weighted average value to all elements.
    def pool(input: Array[(Double, Double, Double)], start: Int, end: Int): Unit = {
      val poolSubArray = input.slice(start, end + 1)

      val weightedSum = poolSubArray.map(lp => lp._1 * lp._3).sum
      val weight = poolSubArray.map(_._3).sum

      var i = start
      while (i <= end) {
        input(i) = (weightedSum / weight, input(i)._2, input(i)._3)
        i = i + 1
      }
    }

    val monotonicityConstraintHolds: (Double, Double) => Boolean =
      (x, y) => if (isotonic) x <= y else x >= y

    var i = 0
    while (i < input.length) {
      var j = i

      // Find monotonicity violating sequence, if any.
      while (j < input.length - 1 && !monotonicityConstraintHolds(input(j)._1, input(j + 1)._1)) {
        j = j + 1
      }

      // If monotonicity was not violated, move to next data point.
      if (i == j) {
        i = i + 1
      } else {
        // Otherwise pool the violating sequence
        // and check if pooling caused monotonicity violation in previously processed points.
        while (i >= 0 && !monotonicityConstraintHolds(input(i)._1, input(i + 1)._1)) {
          pool(input, i, j)
          i = i - 1
        }

        i = j
      }
    }

    input
  }

  /**
   * Performs parallel pool adjacent violators algorithm.
   * Performs Pool adjacent violators algorithm on each partition and then again on the result.
   *
   * @param testData Input data of tuples (label, feature, weight).
   * @param isotonic Isotonic (increasing) or antitonic (decreasing) sequence.
   * @return Result tuples (label, feature, weight) where labels were updated
   *         to form a monotone sequence as per isotonic regression definition.
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
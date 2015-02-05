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
import java.lang.{Double => JDouble}
import java.util.Arrays.binarySearch

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaDoubleRDD, JavaRDD}
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 *
 * Regression model for isotonic regression.
 *
 * @param boundaries Array of boundaries for which predictions are known.
 *                   Boundaries must be sorted in increasing order.
 * @param predictions Array of predictions associated to the boundaries at the same index.
 *                    Results of isotonic regression and therefore monotone.
 * @param isotonic indicates whether this is isotonic or antitonic.
 */
@Experimental
class IsotonicRegressionModel (
    val boundaries: Array[Double],
    val predictions: Array[Double],
    val isotonic: Boolean) extends Serializable {

  private val predictionOrd = if (isotonic) Ordering[Double] else Ordering[Double].reverse

  require(boundaries.length == predictions.length)
  assertOrdered(boundaries)
  assertOrdered(predictions)(predictionOrd)

  /** Asserts the input array is monotone with the given ordering. */
  private def assertOrdered(xs: Array[Double])(implicit ord: Ordering[Double]): Unit = {
    var i = 1
    while (i < xs.length) {
      require(ord.compare(xs(i - 1), xs(i)) <= 0,
        s"Elements (${xs(i - 1)}, ${xs(i)}) are not ordered.")
      i += 1
    }
  }

  /**
   * Predict labels for provided features.
   * Using a piecewise linear function.
   *
   * @param testData Features to be labeled.
   * @return Predicted labels.
   */
  def predict(testData: RDD[Double]): RDD[Double] = {
    testData.map(predict)
  }

  /**
   * Predict labels for provided features.
   * Using a piecewise linear function.
   *
   * @param testData Features to be labeled.
   * @return Predicted labels.
   */
  def predict(testData: JavaDoubleRDD): JavaDoubleRDD = {
    JavaDoubleRDD.fromRDD(predict(testData.rdd.retag.asInstanceOf[RDD[Double]]))
  }

  /**
   * Predict a single label.
   * Using a piecewise linear function.
   *
   * @param testData Feature to be labeled.
   * @return Predicted label.
   *         1) If testData exactly matches a boundary then associated prediction is returned.
   *           In case there are multiple predictions with the same boundary then one of them
   *           is returned. Which one is undefined (same as java.util.Arrays.binarySearch).
   *         2) If testData is lower or higher than all boundaries then first or last prediction
   *           is returned respectively. In case there are multiple predictions with the same
   *           boundary then the lowest or highest is returned respectively.
   *         3) If testData falls between two values in boundary array then prediction is treated
   *           as piecewise linear function and interpolated value is returned. In case there are
   *           multiple values with the same boundary then the same rules as in 2) are used.
   */
  def predict(testData: Double): Double = {

    def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x: Double): Double = {
      y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    }

    val foundIndex = binarySearch(boundaries, testData)
    val insertIndex = -foundIndex - 1

    // Find if the index was lower than all values,
    // higher than all values, in between two values or exact match.
    if (insertIndex == 0) {
      predictions.head
    } else if (insertIndex == boundaries.length){
      predictions.last
    } else if (foundIndex < 0) {
      linearInterpolation(
        boundaries(insertIndex - 1),
        predictions(insertIndex - 1),
        boundaries(insertIndex),
        predictions(insertIndex),
        testData)
    } else {
      predictions(foundIndex)
    }
  }
}

/**
 * :: Experimental ::
 *
 * Isotonic regression.
 * Currently implemented using parallelized pool adjacent violators algorithm.
 * Only univariate (single feature) algorithm supported.
 *
 * Sequential PAV implementation based on:
 * Tibshirani, Ryan J., Holger Hoefling, and Robert Tibshirani.
 *   "Nearly-isotonic regression." Technometrics 53.1 (2011): 54-61.
 *   Available from [[http://www.stat.cmu.edu/~ryantibs/papers/neariso.pdf]]
 *
 * Sequential PAV parallelization based on:
 * Kearsley, Anthony J., Richard A. Tapia, and Michael W. Trosset.
 *   "An approach to parallelizing isotonic regression."
 *   Applied Mathematics and Parallel Computing. Physica-Verlag HD, 1996. 141-147.
 *   Available from [[http://softlib.rice.edu/pub/CRPC-TRs/reports/CRPC-TR96640.pdf]]
 *
 * @see [[http://en.wikipedia.org/wiki/Isotonic_regression Isotonic regression (Wikipedia)]]
 */
@Experimental
class IsotonicRegression private (private var isotonic: Boolean) extends Serializable {

  /**
   * Constructs IsotonicRegression instance with default parameter isotonic = true.
   *
   * @return New instance of IsotonicRegression.
   */
  def this() = this(true)

  /**
   * Sets the isotonic parameter.
   *
   * @param isotonic Isotonic (increasing) or antitonic (decreasing) sequence.
   * @return This instance of IsotonicRegression.
   */
  def setIsotonic(isotonic: Boolean): this.type = {
    this.isotonic = isotonic
    this
  }

  /**
   * Run IsotonicRegression algorithm to obtain isotonic regression model.
   *
   * @param input RDD of tuples (label, feature, weight) where label is dependent variable
   *              for which we calculate isotonic regression, feature is independent variable
   *              and weight represents number of measures with default 1.
   *              If multiple labels share the same feature value then they are ordered before
   *              the algorithm is executed.
   * @return Isotonic regression model.
   */
  def run(input: RDD[(Double, Double, Double)]): IsotonicRegressionModel = {
    val preprocessedInput = if (isotonic) {
      input
    } else {
      input.map(x => (-x._1, x._2, x._3))
    }

    val pooled = parallelPoolAdjacentViolators(preprocessedInput)

    val predictions = if (isotonic) pooled.map(_._1) else pooled.map(-_._1)
    val boundaries = pooled.map(_._2)

    new IsotonicRegressionModel(boundaries, predictions, isotonic)
  }

  /**
   * Run pool adjacent violators algorithm to obtain isotonic regression model.
   *
   * @param input JavaRDD of tuples (label, feature, weight) where label is dependent variable
   *              for which we calculate isotonic regression, feature is independent variable
   *              and weight represents number of measures with default 1.
   *              If multiple labels share the same feature value then they are ordered before
   *              the algorithm is executed.
   * @return Isotonic regression model.
   */
  def run(input: JavaRDD[(JDouble, JDouble, JDouble)]): IsotonicRegressionModel = {
    run(input.rdd.retag.asInstanceOf[RDD[(Double, Double, Double)]])
  }

  /**
   * Performs a pool adjacent violators algorithm (PAV).
   * Uses approach with single processing of data where violators
   * in previously processed data created by pooling are fixed immediately.
   * Uses optimization of discovering monotonicity violating sequences (blocks).
   *
   * @param input Input data of tuples (label, feature, weight).
   * @return Result tuples (label, feature, weight) where labels were updated
   *         to form a monotone sequence as per isotonic regression definition.
   */
  private def poolAdjacentViolators(
      input: Array[(Double, Double, Double)]): Array[(Double, Double, Double)] = {

    if (input.isEmpty) {
      return Array.empty
    }

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

    var i = 0
    while (i < input.length) {
      var j = i

      // Find monotonicity violating sequence, if any.
      while (j < input.length - 1 && input(j)._1 > input(j + 1)._1) {
        j = j + 1
      }

      // If monotonicity was not violated, move to next data point.
      if (i == j) {
        i = i + 1
      } else {
        // Otherwise pool the violating sequence
        // and check if pooling caused monotonicity violation in previously processed points.
        while (i >= 0 && input(i)._1 > input(i + 1)._1) {
          pool(input, i, j)
          i = i - 1
        }

        i = j
      }
    }

    // For points having the same prediction, we only keep two boundary points.
    val compressed = ArrayBuffer.empty[(Double, Double, Double)]

    var (curLabel, curFeature, curWeight) = input.head
    var rightBound = curFeature
    def merge(): Unit = {
      compressed += ((curLabel, curFeature, curWeight))
      if (rightBound > curFeature) {
        compressed += ((curLabel, rightBound, 0.0))
      }
    }
    i = 1
    while (i < input.length) {
      val (label, feature, weight) = input(i)
      if (label == curLabel) {
        curWeight += weight
        rightBound = feature
      } else {
        merge()
        curLabel = label
        curFeature = feature
        curWeight = weight
        rightBound = curFeature
      }
      i += 1
    }
    merge()

    compressed.toArray
  }

  /**
   * Performs parallel pool adjacent violators algorithm.
   * Performs Pool adjacent violators algorithm on each partition and then again on the result.
   *
   * @param input Input data of tuples (label, feature, weight).
   * @return Result tuples (label, feature, weight) where labels were updated
   *         to form a monotone sequence as per isotonic regression definition.
   */
  private def parallelPoolAdjacentViolators(
      input: RDD[(Double, Double, Double)]): Array[(Double, Double, Double)] = {
    val parallelStepResult = input
      .sortBy(x => (x._2, x._1))
      .glom()
      .flatMap(poolAdjacentViolators)
      .collect()
      .sortBy(x => (x._2, x._1)) // Sort again because collect() doesn't promise ordering.
    poolAdjacentViolators(parallelStepResult)
  }
}

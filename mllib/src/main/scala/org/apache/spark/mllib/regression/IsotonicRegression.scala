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

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.MonotonicityConstraint.MonotonicityConstraint._
import org.apache.spark.rdd.RDD

/**
 * Monotonicity constrains for monotone regression
 * Isotonic (increasing)
 * Antitonic (decreasing)
 */
object MonotonicityConstraint {

  object MonotonicityConstraint {

    sealed trait MonotonicityConstraint {
      private[regression] def holds(
        current: WeightedLabeledPoint,
        next: WeightedLabeledPoint): Boolean
    }

    /**
     * Isotonic monotonicity constraint. Increasing sequence
     */
    case object Isotonic extends MonotonicityConstraint {
      override def holds(current: WeightedLabeledPoint, next: WeightedLabeledPoint): Boolean = {
        current.label <= next.label
      }
    }

    /**
     * Antitonic monotonicity constrain. Decreasing sequence
     */
    case object Antitonic extends MonotonicityConstraint {
      override def holds(current: WeightedLabeledPoint, next: WeightedLabeledPoint): Boolean = {
        current.label >= next.label
      }
    }
  }

  val Isotonic = MonotonicityConstraint.Isotonic
  val Antitonic = MonotonicityConstraint.Antitonic
}

/**
 * Regression model for Isotonic regression
 *
 * @param predictions Weights computed for every feature.
 * @param monotonicityConstraint specifies if the sequence is increasing or decreasing
 */
class IsotonicRegressionModel(
    val predictions: Seq[WeightedLabeledPoint],
    val monotonicityConstraint: MonotonicityConstraint)
  extends RegressionModel {

  override def predict(testData: RDD[Vector]): RDD[Double] =
    testData.map(predict)

  override def predict(testData: Vector): Double = {
    // Take the highest of data points smaller than our feature or data point with lowest feature
    (predictions.head +:
      predictions.filter(y => y.features.toArray.head <= testData.toArray.head)).last.label
  }
}

/**
 * Base representing algorithm for isotonic regression
 */
trait IsotonicRegressionAlgorithm
  extends Serializable {

  /**
   * Creates isotonic regression model with given parameters
   *
   * @param predictions labels estimated using isotonic regression algorithm.
   *                    Used for predictions on new data points.
   * @param monotonicityConstraint isotonic or antitonic
   * @return isotonic regression model
   */
  protected def createModel(
      predictions: Seq[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel

  /**
   * Run algorithm to obtain isotonic regression model
   *
   * @param input data
   * @param monotonicityConstraint ascending or descenting
   * @return isotonic regression model
   */
  def run(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel
}

/**
 * Parallel pool adjacent violators algorithm for monotone regression
 */
class PoolAdjacentViolators private [mllib]
  extends IsotonicRegressionAlgorithm {

  override def run(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel = {
    createModel(
      parallelPoolAdjacentViolators(input, monotonicityConstraint),
      monotonicityConstraint)
  }

  override protected def createModel(
      predictions: Seq[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel = {
    new IsotonicRegressionModel(predictions, monotonicityConstraint)
  }

  /**
   * Performs a pool adjacent violators algorithm (PAVA)
   * Uses approach with single processing of data where violators in previously processed
   * data created by pooling are fixed immediatelly.
   * Uses optimization of discovering monotonicity violating sequences
   * Method in situ mutates input array
   *
   * @param in input data
   * @param monotonicityConstraint asc or desc
   * @return result
   */
  private def poolAdjacentViolators(
      in: Array[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): Array[WeightedLabeledPoint] = {

    // Pools sub array within given bounds assigning weighted average value to all elements
    def pool(in: Array[WeightedLabeledPoint], start: Int, end: Int): Unit = {
      val poolSubArray = in.slice(start, end + 1)

      val weightedSum = poolSubArray.map(lp => lp.label * lp.weight).sum
      val weight = poolSubArray.map(_.weight).sum

      for(i <- start to end) {
        in(i) = WeightedLabeledPoint(weightedSum / weight, in(i).features, in(i).weight)
      }
    }

    var i = 0

    while(i < in.length) {
      var j = i

      // Find monotonicity violating sequence, if any
      while(j < in.length - 1 && !monotonicityConstraint.holds(in(j), in(j + 1))) {
        j = j + 1
      }

      // If monotonicity was not violated, move to next data point
      if(i == j) {
        i = i + 1
      } else {
        // Otherwise pool the violating sequence
        // And check if pooling caused monotonicity violation in previously processed points
        while (i >= 0 && !monotonicityConstraint.holds(in(i), in(i + 1))) {
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
   * @param monotonicityConstraint asc or desc
   * @return result
   */
  private def parallelPoolAdjacentViolators(
      testData: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): Seq[WeightedLabeledPoint] = {

    poolAdjacentViolators(
      testData
        .sortBy(_.features.toArray.head)
        .cache()
        .mapPartitions(it => poolAdjacentViolators(it.toArray, monotonicityConstraint).toIterator)
        .collect(), monotonicityConstraint)
  }
}

/**
 * Top-level methods for monotone regression (either isotonic or antitonic).
 */
object IsotonicRegression {

  /**
   * Train a monotone regression model given an RDD of (label, features, weight).
   * Currently only one dimensional algorithm is supported (features.length is one)
   * Label is the dependent y value
   * Weight of the data point is the number of measurements. Default is 1
   *
   * @param input RDD of (label, array of features, weight).
   *              Each point describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   *              and weight as number of measurements
   * @param monotonicityConstraint Isotonic (increasing) or Antitonic (decreasing) sequence
   */
  def train(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint = Isotonic): IsotonicRegressionModel = {
    new PoolAdjacentViolators().run(input, monotonicityConstraint)
  }
}

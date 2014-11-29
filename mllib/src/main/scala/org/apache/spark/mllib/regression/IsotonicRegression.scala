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

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.MonotonicityConstraint.Enum.MonotonicityConstraint
import org.apache.spark.rdd.RDD

object MonotonicityConstraint {

  object Enum {

    sealed trait MonotonicityConstraint {
      private[regression] def holds(current: WeightedLabeledPoint, next: WeightedLabeledPoint): Boolean
    }

    case object Isotonic extends MonotonicityConstraint {
      override def holds(current: WeightedLabeledPoint, next: WeightedLabeledPoint): Boolean = {
        current.label <= next.label
      }
    }

    case object Antitonic extends MonotonicityConstraint {
      override def holds(current: WeightedLabeledPoint, next: WeightedLabeledPoint): Boolean = {
        current.label >= next.label
      }
    }
  }

  val Isotonic = Enum.Isotonic
  val Antitonic = Enum.Antitonic
}

/**
 * Regression model for Isotonic regression
 *
 * @param predictions Weights computed for every feature.
 */
class IsotonicRegressionModel(
    val predictions: Seq[WeightedLabeledPoint],
    val monotonicityConstraint: MonotonicityConstraint)
  extends RegressionModel {

  override def predict(testData: RDD[Vector]): RDD[Double] =
    testData.map(predict)

  //take the highest of elements smaller than our feature or weight with lowest feature
  override def predict(testData: Vector): Double =
    (predictions.head +:
      predictions.filter(y => y.features.toArray.head <= testData.toArray.head)).last.label
}

/**
 * Base representing algorithm for isotonic regression
 */
trait IsotonicRegressionAlgorithm
  extends Serializable {

  protected def createModel(
      weights: Seq[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel

  /**
   * Run algorithm to obtain isotonic regression model
   * @param input data
   * @param monotonicityConstraint ascending or descenting
   * @return model
   */
  def run(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel

  /**
   * Run algorithm to obtain isotonic regression model
   * @param input data
   * @param monotonicityConstraint asc or desc
   * @param weights weights
   * @return
   */
  def run(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint,
      weights: Vector): IsotonicRegressionModel
}

class PoolAdjacentViolators extends IsotonicRegressionAlgorithm {

  override def run(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel = {
    createModel(
      parallelPoolAdjacentViolators(input, monotonicityConstraint, Vectors.dense(Array(0d))),
      monotonicityConstraint)
  }

  override def run(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint,
      weights: Vector): IsotonicRegressionModel = {
    createModel(
      parallelPoolAdjacentViolators(input, monotonicityConstraint, weights),
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

    //Pools sub array within given bounds assigning weighted average value to all elements
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

      //find monotonicity violating sequence, if any
      while(j < in.length - 1 && !monotonicityConstraint.holds(in(j), in(j + 1))) {
        j = j + 1
      }

      //if monotonicity was not violated, move to next data point
      if(i == j) {
        i = i + 1
      } else {
        //otherwise pool the violating sequence
        //and check if pooling caused monotonicity violation in previously processed points
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
   * Calls PAVA on each partition and then again on the result
   *
   * @param testData input
   * @param monotonicityConstraint asc or desc
   * @return result
   */
  private def parallelPoolAdjacentViolators(
      testData: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint,
      weights: Vector): Seq[WeightedLabeledPoint] = {

    poolAdjacentViolators(
      testData
        .sortBy(_.features.toArray.head)
        .cache()
        .mapPartitions(it => poolAdjacentViolators(it.toArray, monotonicityConstraint).toIterator)
        .collect(), monotonicityConstraint)
  }
}

/**
 * Top-level methods for calling IsotonicRegression.
 */
object IsotonicRegression {

  /**
   * Train a Linear Regression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient. The weights used
   * in gradient descent are initialized using the initial weights provided.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   * @param weights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint,
      weights: Vector): IsotonicRegressionModel = {
    new PoolAdjacentViolators().run(input, monotonicityConstraint, weights)
  }

  /**
   * Train a LinearRegression model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate a stochastic gradient.
   *
   * @param input RDD of (label, array of features) pairs. Each pair describes a row of the data
   *              matrix A as well as the corresponding right hand side label y
   */
  def train(
      input: RDD[WeightedLabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel = {
    new PoolAdjacentViolators().run(input, monotonicityConstraint)
  }
}

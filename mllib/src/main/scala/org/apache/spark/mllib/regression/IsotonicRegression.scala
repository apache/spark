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
import org.apache.spark.rdd.RDD

sealed trait MonotonicityConstraint {
  def holds(current: LabeledPoint, next: LabeledPoint): Boolean
}

case object Isotonic extends MonotonicityConstraint {
  override def holds(current: LabeledPoint, next: LabeledPoint): Boolean = {
    current.label <= next.label
  }
}
case object Antitonic extends MonotonicityConstraint {
  override def holds(current: LabeledPoint, next: LabeledPoint): Boolean = {
    current.label >= next.label
  }
}

/**
 * Regression model for Isotonic regression
 *
 * @param predictions Weights computed for every feature.
 */
class IsotonicRegressionModel(
    val predictions: Seq[LabeledPoint],
    val monotonicityConstraint: MonotonicityConstraint)
  extends RegressionModel {
  override def predict(testData: RDD[Vector]): RDD[Double] =
    testData.map(predict)

  //take the highest of elements smaller than our feature or weight with lowest feature
  override def predict(testData: Vector): Double =
    (predictions.head +: predictions.filter(y => y.features.toArray.head <= testData.toArray.head)).last.label
}

trait IsotonicRegressionAlgorithm
  extends Serializable {

  protected def createModel(weights: Seq[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel
  def run(input: RDD[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel
  def run(input: RDD[LabeledPoint], initialWeights: Vector, monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel
}

class PoolAdjacentViolators extends IsotonicRegressionAlgorithm {

  override def run(input: RDD[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel =
    createModel(parallelPoolAdjacentViolators(input, monotonicityConstraint), monotonicityConstraint)

  override def run(input: RDD[LabeledPoint], initialWeights: Vector, monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel = ???

  override protected def createModel(weights: Seq[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel =
    new IsotonicRegressionModel(weights, monotonicityConstraint)

  //Performs a pool adjacent violators algorithm (PAVA)
  //Uses approach with single processing of data where violators in previously processed
  //data created by pooling are fixed immediatelly.
  //Uses optimization of discovering monotonicity violating sequences
  //Method in situ mutates input array
  private def poolAdjacentViolators(in: Array[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): Array[LabeledPoint] = {

    //Pools sub array within given bounds assigning weighted average value to all elements
    def pool(in: Array[LabeledPoint], start: Int, end: Int): Unit = {
      val poolSubArray = in.slice(start, end + 1)

      val weightedSum = poolSubArray.map(_.label).sum
      val weight = poolSubArray.length

      for(i <- start to end) {
        in(i) = LabeledPoint(weightedSum / weight, in(i).features)
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

  private def parallelPoolAdjacentViolators(testData: RDD[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): Seq[LabeledPoint] = {
    poolAdjacentViolators(
      testData
        .sortBy(_.features.toArray.head)
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
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   */
  def train(
      input: RDD[LabeledPoint],
      initialWeights: Vector,
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel = {
    new PoolAdjacentViolators().run(input, initialWeights, monotonicityConstraint)
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
      input: RDD[LabeledPoint],
      monotonicityConstraint: MonotonicityConstraint): IsotonicRegressionModel = {
    new PoolAdjacentViolators().run(input, monotonicityConstraint)
  }
}

/*def functionalOption(in: Array[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): Array[LabeledPoint] = {
     def pool2(in: Array[LabeledPoint]): Array[LabeledPoint] =
       in.map(p => LabeledPoint(in.map(_.label).sum / in.length, p.features))

     def iterate(checked: Array[LabeledPoint], remaining: Array[LabeledPoint], monotonicityConstraint: MonotonicityConstraint): Array[LabeledPoint] = {
       if(remaining.size < 2) {
         checked ++ remaining
       } else {
         val newRemaining = if(remaining.size == 2) Array[LabeledPoint]() else remaining.slice(2, remaining.length)

         if(!monotonicityConstraint.holds(remaining.head, remaining.tail.head)) {
           iterate(checked ++ pool2(remaining.slice(0, 2)), newRemaining, monotonicityConstraint)
         } else {
           iterate(checked ++ remaining.slice(0, 2), newRemaining, monotonicityConstraint)
         }
       }
     }

     iterate(Array(), in, monotonicityConstraint)
   }


   functionalOption(in, monotonicityConstraint)*/

/*def option1(in: Array[LabeledPoint], monotonicityConstraint: MonotonicityConstraint) = {
      def findMonotonicityViolators(in: Array[LabeledPoint], start: Int, monotonicityConstraint: MonotonicityConstraint): Unit = {
        var j = start

        while (j >= 1 && !monotonicityConstraint.holds(in(j - 1), in(j))) {
          pool(in, j - 1, start + 1)
          j = j - 1
        }
      }

      for (i <- 0 to in.length - 1) {
        findMonotonicityViolators(in, i, monotonicityConstraint)
      }

      in
    }*/

/*
def pool(in: Array[LabeledPoint], start: Int, end: Int): Unit = {
val subArraySum = in.slice(start, end).map(_.label).sum
val subArrayLength = math.abs(start - end)

for(i <- start to end - 1) {
in(i) = LabeledPoint(subArraySum / subArrayLength, in(i).features)
}
}*/



/*
OPTION 2
def pool(in: Array[LabeledPoint], range: Range): Unit = {
      val subArray = in.slice(range.start, range.end + 1)

      val subArraySum = subArray.map(_.label).sum
      val subArrayLength = subArray.length

      for(i <- range.start to range.end) {
        in(i) = LabeledPoint(subArraySum / subArrayLength, in(i).features)
      }
    }

    def poolExtendedViolators(in: Array[LabeledPoint], range: Range, monotonicityConstraint: MonotonicityConstraint): Unit = {
      var extendedRange = Range(range.start, range.end)

      while (extendedRange.start >= 0 && !monotonicityConstraint.holds(in(extendedRange.start), in(extendedRange.start + 1))) {
        pool(in, Range(extendedRange.start, extendedRange.end))
        extendedRange = Range(extendedRange.start - 1, extendedRange.end)
      }
    }

    def findViolatingSequence(in: Array[LabeledPoint], start: Int, monotonicityConstraint: MonotonicityConstraint): Option[Range] = {
      var j = start

      while(j < in.length - 1 && !monotonicityConstraint.holds(in(start), in(j + 1))) {
        j = j + 1
      }

      if(j == start) {
        None
      } else {
        Some(Range(start, j))
      }
    }

    var i = 0;

    while(i < in.length) {
      findViolatingSequence(in, i, monotonicityConstraint).fold[Unit]({
        i = i + 1
      })(r => {
        poolExtendedViolators(in, r, monotonicityConstraint)
        i = r.end
      })
    }

    in
 */
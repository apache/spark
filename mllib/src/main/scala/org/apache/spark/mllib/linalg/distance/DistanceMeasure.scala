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

package org.apache.spark.mllib.linalg.distance

import breeze.linalg.{DenseVector => DBV, Vector => BV, sum}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

import scala.language.implicitConversions

/**
 * :: Experimental ::
 * This trait is used for objects which can determine a distance between two points
 *
 * Classes which inherits from this class are required to satisfy the follow condition:
 * 1. d(x, y) >= 0 (non-negative)
 * 2. d(x, y) = 0 if and only if x = y (identity of indiscernibles)
 * 3. d(x, y) = d(y, x) (symmetry)
 * However, classes which inherits aren't require to satisfy triangle inequality
 */
@Experimental
trait DistanceMeasure extends Function2[BV[Double], BV[Double], Double] with Serializable {

  // each measure/metric defines for itself:
  override def apply(v1: BV[Double], v2: BV[Double]): Double

  // a catch-all overloading of "()" for spark vectors
  // can also be overridden on a per-class basis, if it is advantageous
  def apply(v1: Vector, v2: Vector): Double = this(v1.toBreeze, v2.toBreeze)
}


object DistanceMeasure {

  /**
   * Implicit method for DistanceMeasure
   *
   * @param f calculating distance function (Vector, Vector) => Double
   * @return DistanceMeasure
   */
  implicit def functionToDistanceMeasure(f: (Vector, Vector) => Double): DistanceMeasure = new
      DistanceMeasure {
    override def apply(v1: Vector, v2: Vector): Double = f(v1, v2)

    override def apply(v1: BV[Double], v2: BV[Double]): Double = {
      throw new NotImplementedError(s"This DistanceMeasure is made by a lambda function")
    }
  }
}

/**
 * :: Experimental ::
 * Squared euclidean distance implementation
 */
@Experimental
class SquaredEuclideanDistanceMeasure extends DistanceMeasure {

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val d = v1 - v2
    d dot d
  }
}

/**
 * :: Experimental ::
 * Cosine distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Cosine_similarity
 */
@Experimental
class CosineDistanceMeasure extends DistanceMeasure {

  /**
   * Calculates the cosine distance between 2 points
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return a scalar doubles of the distance
   */
  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val dotProduct = v1 dot v2
    var denominator = v1.norm(2) * v2.norm(2)

    // correct for floating-point rounding errors
    if (denominator < dotProduct) {
      denominator = dotProduct
    }

    // correct for zero-vector corner case
    if (denominator == 0 && dotProduct == 0) {
      return 0.0
    }
    1.0 - (dotProduct / denominator)
  }
}

/**
 * :: Experimental ::
 * Tanimoto distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Jaccard_index
 */
@Experimental
class TanimotoDistanceMeasure extends DistanceMeasure {

  /**
   * Calculates the tanimoto distance between 2 points
   *
   * The coefficient (a measure of similarity) is: Td(a, b) = a.b / (|a|^2 + |b|^2 - a.b)
   * The distance d(a,b) = 1 - T(a,b)
   *
   * @param v1 a Vector defining a multidimensional point in some feature space
   * @param v2 a Vector defining a multidimensional point in some feature space
   * @return 0 for perfect match, > 0 for greater distance
   */
  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val calcSquaredSum = (bv: BV[Double]) => sum(bv.map(x => x * x))
    val dotProduct = v1 dot v2
    var denominator = calcSquaredSum(v1) + calcSquaredSum(v2) - dotProduct

    // correct for floating-point round-off: distance >= 0
    if(denominator < dotProduct) {
      denominator = dotProduct
    }

    // denominator == 0 only when dot(a,a) == dot(b,b) == dot(a,b) == 0
    val distance = if(denominator > 0) {
      1 - dotProduct / denominator
    }
    else {
      0.0
    }
    distance
  }
}

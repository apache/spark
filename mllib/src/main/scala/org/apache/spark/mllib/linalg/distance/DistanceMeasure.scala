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

import breeze.linalg.{sum, DenseVector => DBV, Vector => BV}
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.language.implicitConversions

/**
 * :: Experimental ::
 * This abstract class is used for objects which can determine a distance between two points
 *
 * Classes which inherits from this class are required to satisfy the follow condition:
 * 1. d(x, y) >= 0 (non-negative)
 * 2. d(x, y) = 0 if and only if x = y (identity of indiscernibles)
 * 3. d(x, y) = d(y, x) (symmetry)
 * However, classes which inherits aren't require to satisfy triangle inequality
 */
@Experimental
abstract class DistanceMeasure extends Function2[BV[Double], BV[Double], Double] with Serializable


@Experimental
object DistanceMeasure {

  /**
   * Implicit method for DistanceMeasure
   *
   * @param f calculating distance function (Vector, Vector) => Double
   * @return DistanceMeasure
   */
  implicit def convert_V_V_DM(f: (Vector, Vector) => Double): DistanceMeasure =
    new DistanceMeasure {
    override def apply(bv1: BV[Double], bv2: BV[Double]): Double =
      f(Vectors.fromBreeze(bv1), Vectors.fromBreeze(bv2))
  }

  /**
   * Implicit method for DistanceMeasure
   *
   * @param f calculating distance function (BV[Double], BV[Double]) => Double (BV: Breeze Vector)
   * @return DistanceMeasure
   */
  implicit def convert_BV_BV_DM(f: (BV[Double], BV[Double]) => Double): DistanceMeasure =
    new DistanceMeasure {
      override def apply(bv1: BV[Double], bv2: BV[Double]): Double = f(bv1, bv2)
    }
}

/**
 * :: Experimental ::
 * Squared euclidean distance implementation
 */
@Experimental
@DeveloperApi
sealed private[mllib]
class SquaredEuclideanDistanceMeasure extends DistanceMeasure {

  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val d = v1 - v2
    d dot d
  }
}

@Experimental
object SquaredEuclideanDistanceMeasure {
  def apply(v1: Vector, v2: Vector): Double =
    new SquaredEuclideanDistanceMeasure().apply(v1.toBreeze, v2.toBreeze)
}

/**
 * :: Experimental ::
 * Cosine distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Cosine_similarity
 */
@Experimental
@DeveloperApi
sealed private[mllib]
class CosineDistanceMeasure extends DistanceMeasure {

  /**
   * Calculates the cosine distance between 2 points
   *
   * @param v1 a vector defining a multidimensional point in some feature space
   * @param v2 a vector defining a multidimensional point in some feature space
   * @return a scalar doubles of the distance
   */
  override def apply(v1: BV[Double], v2: BV[Double]): Double = {
    val dotProduct = v1 dot v2
    var denominator = v1.norm(2) * v2.norm(2)

    // correct for floating-point rounding errors
    if(denominator < dotProduct) {
      denominator = dotProduct
    }

    // correct for zero-vector corner case
    if(denominator == 0 && dotProduct == 0) {
      return 0.0
    }
    1.0 - (dotProduct / denominator)
  }
}

@Experimental
object CosineDistanceMeasure {
  def apply(v1: Vector, v2: Vector): Double =
    new CosineDistanceMeasure().apply(v1.toBreeze, v2.toBreeze)
}

/**
 * :: Experimental ::
 * Tanimoto distance implementation
 *
 * @see http://en.wikipedia.org/wiki/Jaccard_index
 */
@Experimental
@DeveloperApi
final private[mllib]
class TanimotoDistanceMeasure extends DistanceMeasure {

  /**
   * Calculates the tanimoto distance between 2 points
   *
   * The coefficient (a measure of similarity) is: Td(a, b) = a.b / (|a|^2 + |b|^2 - a.b)
   * The distance d(a,b) = 1 - T(a,b)
   *
   * @param v1 a vector defining a multidimensional point in some feature space
   * @param v2 a vector defining a multidimensional point in some feature space
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

@Experimental
object TanimotoDistanceMeasure {
  def apply(v1: Vector, v2: Vector): Double =
    new TanimotoDistanceMeasure().apply(v1.toBreeze, v2.toBreeze)
}

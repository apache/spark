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

import breeze.linalg.{DenseVector => DBV, Vector => BV}
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector

/**
 * :: Experimental ::
 * Distance function type enumeration
 */
@Experimental
object DistanceType extends Enumeration {
  val euclidean = Value("euclidean")
  val manhattan = Value("manhattan")
  val chebyshev = Value("chebyshev")
  val minkowski = Value("minkowski")
  val cosine = Value("cosine")
  val tanimoto = Value("tanimoto")
}

/**
 * :: Experimental ::
 * DistanceMeasure/DistanceMetric Factory
 *
 * NOTE: If you want to generate a weighted distance metric/measure, use WeightedDistanceMeasure
 */
@Experimental
object DistanceMeasureFactory {

  /**
   * Generates a distance metric/measure class with String
   *
   * @param distanceType want to generate distance metric/measure type
   * @return a sub-DistanceMeasure instance
   */
  def apply(distanceType: String): DistanceMeasure = {
    apply(DistanceType.withName(distanceType))
  }

  /**
   * Generates a distance metric/measure class with a Enumeration value
   *
   * @param distanceType want to generate distance metric/measure type
   * @return a sub-DistanceMeasure instance
   */
  def apply(distanceType: DistanceType.Value): DistanceMeasure = {
    distanceType match {
      case DistanceType.euclidean => new EuclideanDistanceMetric
      case DistanceType.manhattan => new ManhattanDistanceMetric
      case DistanceType.chebyshev => new ChebyshevDistanceMetric
      case DistanceType.cosine => new CosineDistanceMeasure
      case DistanceType.tanimoto => new TanimotoDistanceMeasure
      case _ => throw new NoSuchElementException(s"Can not use ${distanceType}")
    }
  }
}

/**
 * :: Experimental ::
 * WeightedDistanceMeasure/WeightedDistanceMetric Factory
 */
@Experimental
object WeightedDistanceFactory {

  /**
   * Genrates a weighted distance metric/measure with String and weight vector
   *
   * @param distanceType want to generate distance function type
   * @param weights weight vector
   * @return a class to calculate weighted distance
   */
  def apply(distanceType: String, weights: Vector): DistanceMeasure = {
    apply(DistanceType.withName(distanceType), weights.toBreeze)
  }

  def apply(distanceType: String, weights: BV[Double]): DistanceMeasure = {
    apply(DistanceType.withName(distanceType), weights)
  }

  /**
   * Genrates a weighted distance metric/measure with a Enumeration value and weight vector
   *
   * @param distanceType want to generate distance function type
   * @param weights weight vector
   * @return a class to calculate weighted distance
   */
  def apply(distanceType: DistanceType.Value, weights: Vector): DistanceMeasure = {
    apply(distanceType, weights.toBreeze)
  }

  def apply(distanceType: DistanceType.Value, weights: BV[Double]): DistanceMeasure = {
    distanceType match {
      case DistanceType.euclidean => new WeightedEuclideanDistanceMetric(weights)
      case DistanceType.manhattan => new WeightedManhattanDistanceMetric(weights)
      case DistanceType.chebyshev => new WeightedChebyshevDistanceMetric(weights)
      case DistanceType.cosine => new WeightedCosineDistanceMeasure(weights)
      case _ => throw new NoSuchElementException(s"Sorry, not support ${distanceType}")
    }
  }
}

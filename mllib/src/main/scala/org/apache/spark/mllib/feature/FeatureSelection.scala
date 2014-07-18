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

package org.apache.spark.mllib.feature

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Generic trait for feature selection
 */
@Experimental
trait FeatureSelection[T] extends java.io.Serializable {

  /**
   * Returns data that will be process by feature selection
   */
  def data: RDD[T]

  /**
   * Returns the set of feature indexes given the feature selection criteria
   */
  def select: Set[Int]
}

/**
 * :: Experimental ::
 * Generic trait for feature selection and filter
 */
@Experimental
sealed trait FeatureFilter[T] extends FeatureSelection[T] {

  /**
   * Returns a dataset with features filtered
   */
  def filter: RDD[T]
}

/**
 * :: Experimental ::
 * Feature selection and filter trait that processes LabeledPoints
 */
@Experimental
trait LabeledPointFeatureFilter extends FeatureFilter[LabeledPoint] {

  /**
   * Returns a dataset with features filtered
   */
  def filter: RDD[LabeledPoint] = {
    val indices = select
    data.map { lp => new LabeledPoint(lp.label, Compress(lp.features, indices)) }
  }
}

/**
 * :: Experimental ::
 * Filters features in a given vector
 */
@Experimental
object Compress {

  /**
   * Returns a vector with features filtered
   * @param features vector
   * @param indexes indexes of features to filter
   */
  def apply(features: Vector, indexes: Set[Int]): Vector = {
    val (values, _) =
      features.toArray.zipWithIndex.filter { case (value, index) =>
        indexes.contains(index)}.unzip
    /**  probably make a sparse vector if it was initially sparse */
    Vectors.dense(values.toArray)
  }
}

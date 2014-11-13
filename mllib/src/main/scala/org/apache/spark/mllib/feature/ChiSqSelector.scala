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
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Chi Squared selector model.
 *
 * @param indices list of indices to select (filter)
 */
@Experimental
class ChiSqSelectorModel(indices: IndexedSeq[Int]) extends VectorTransformer {
  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  override def transform(vector: linalg.Vector): linalg.Vector = {
    Compress(vector, indices)
  }
}

/**
 * :: Experimental ::
 * Creates a ChiSquared feature selector.
 */
@Experimental
object ChiSqSelector {

  /**
   * Returns a ChiSquared feature selector.
   *
   * @param data data used to compute the Chi Squared statistic.
   * @param numTopFeatures number of features that selector will select
   *                       (ordered by statistic value descending)
   */
  def fit(data: RDD[LabeledPoint], numTopFeatures: Int): ChiSqSelectorModel = {
    val (_, indices) = Statistics.chiSqTest(data).zipWithIndex.sortBy{ case(res, index) =>
      -res.statistic}.take(numTopFeatures).unzip
    new ChiSqSelectorModel(indices)
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
  def apply(features: Vector, indexes: IndexedSeq[Int]): Vector = {
    val (values, _) =
      features.toArray.zipWithIndex.filter { case (value, index) =>
        indexes.contains(index)}.unzip
    /**  probably make a sparse vector if it was initially sparse */
    Vectors.dense(values.toArray)
  }
}


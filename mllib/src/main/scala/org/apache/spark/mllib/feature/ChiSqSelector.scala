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
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors, Vector}
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
class ChiSqSelectorModel(indices: Array[Int]) extends VectorTransformer {
  /**
   * Applies transformation on a vector.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  override def transform(vector: Vector): Vector = {
    Compress(vector, indices)
  }
}

/**
 * :: Experimental ::
 * Creates a ChiSquared feature selector.
 * @param numTopFeatures number of features that selector will select
 *                       (ordered by statistic value descending)
 */
@Experimental
class ChiSqSelector (val numTopFeatures: Int) {

  /**
   * Returns a ChiSquared feature selector.
   *
   * @param data data used to compute the Chi Squared statistic.
   */
  def fit(data: RDD[LabeledPoint]): ChiSqSelectorModel = {
    val indices = Statistics.chiSqTest(data)
      .zipWithIndex.sortBy { case(res, _) => -res.statistic }
      .take(numTopFeatures)
      .map{ case(_, indices) => indices }
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
   * Returns a vector with features filtered.
   * Preserves the order of filtered features the same as their indices are stored.
   * @param features vector
   * @param filterIndices indices of features to filter
   */
  def apply(features: Vector, filterIndices: Array[Int]): Vector = {
    features match {
      case SparseVector(size, indices, values) =>
        val filterMap = filterIndices.zipWithIndex.toMap
        val newSize = filterIndices.length
        var k = 0
        var intersectionSize = 0
        while (k < indices.length) {
          if( filterMap.contains(indices(k))) {
            intersectionSize += 1
          }
          k += 1
        }
        val newIndices = new Array[Int](intersectionSize)
        val newValues = new Array[Double](intersectionSize)
        k = 0
        var m = 0
        while (k < indices.length) {
          if( filterMap.contains(indices(k))) {
            newIndices(m) = filterMap(indices(k))
            newValues(m) = values(k)
            m += 1
          }
          k += 1
        }
        /** Sparse representation might be ineffective if newIndices is small */
        Vectors.sparse(newSize, newIndices, newValues)
      case DenseVector(values) =>
        val values = features.toArray
        Vectors.dense(filterIndices.map(i => values(i)))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}


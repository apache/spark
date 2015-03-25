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

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg._

/**
 * Feature utils object for selector methods.
 *  
 */

@Experimental
object FeatureUtils {
  /**
   * Returns a vector with features filtered.
   * Preserves the order of filtered features the same as their indices are stored.
   * Might be moved to Vector as .slice
   * @param features vector
   * @param filterIndices indices of features to filter, must be ordered asc
   */
  private[feature] def compress(features: Vector, filterIndices: Array[Int]): Vector = {
    features match {
      case v: SparseVector =>
        val newSize = filterIndices.length
        val newValues = new ArrayBuilder.ofDouble
        val newIndices = new ArrayBuilder.ofInt
        var i = 0
        var j = 0
        var indicesIdx = 0
        var filterIndicesIdx = 0
        while (i < v.indices.length && j < filterIndices.length) {
          indicesIdx = v.indices(i)
          filterIndicesIdx = filterIndices(j)
          if (indicesIdx == filterIndicesIdx) {
            newIndices += j
            newValues += v.values(i)
            j += 1
            i += 1
          } else {
            if (indicesIdx > filterIndicesIdx) {
              j += 1
            } else {
              i += 1
            }
          }
        }
        // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
        Vectors.sparse(newSize, newIndices.result(), newValues.result())
      case v: DenseVector =>
        Vectors.dense(filterIndices.map(i => v.values(i)))
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}

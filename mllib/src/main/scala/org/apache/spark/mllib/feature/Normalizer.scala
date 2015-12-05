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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

/**
 * :: Experimental ::
 * Normalizes samples individually to unit L^p^ norm
 *
 * For any 1 &lt;= p &lt; Double.PositiveInfinity, normalizes samples using
 * sum(abs(vector).^p^)^(1/p)^ as norm.
 *
 * For p = Double.PositiveInfinity, max(abs(vector)) will be used as norm for normalization.
 *
 * @param p Normalization in L^p^ space, p = 2 by default.
 */
@Since("1.1.0")
@Experimental
class Normalizer @Since("1.1.0") (p: Double) extends VectorTransformer {

  @Since("1.1.0")
  def this() = this(2)

  require(p >= 1.0)

  /**
   * Applies unit length normalization on a vector.
   *
   * @param vector vector to be normalized.
   * @return normalized vector. If the norm of the input is zero, it will return the input vector.
   */
  @Since("1.1.0")
  override def transform(vector: Vector): Vector = {
    val norm = Vectors.norm(vector, p)

    if (norm != 0.0) {
      // For dense vector, we've to allocate new memory for new output vector.
      // However, for sparse vector, the `index` array will not be changed,
      // so we can re-use it to save memory.
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.size
          var i = 0
          while (i < size) {
            values(i) /= norm
            i += 1
          }
          Vectors.dense(values)
        case SparseVector(size, ids, vs) =>
          val values = vs.clone()
          val nnz = values.size
          var i = 0
          while (i < nnz) {
            values(i) /= norm
            i += 1
          }
          Vectors.sparse(size, ids, values)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else {
      // Since the norm is zero, return the input vector object itself.
      // Note that it's safe since we always assume that the data in RDD
      // should be immutable.
      vector
    }
  }

}

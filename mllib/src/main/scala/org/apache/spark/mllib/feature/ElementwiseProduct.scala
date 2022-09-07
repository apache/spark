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

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg._

/**
 * Outputs the Hadamard product (i.e., the element-wise product) of each input vector with a
 * provided "weight" vector. In other words, it scales each column of the dataset by a scalar
 * multiplier.
 * @param scalingVec The values used to scale the reference vector's individual components.
 */
@Since("1.4.0")
class ElementwiseProduct @Since("1.4.0") (
    @Since("1.4.0") val scalingVec: Vector) extends VectorTransformer {

  /**
   * Does the hadamard product transformation.
   *
   * @param vector vector to be transformed.
   * @return transformed vector.
   */
  @Since("1.4.0")
  override def transform(vector: Vector): Vector = {
    require(vector.size == scalingVec.size,
      s"vector sizes do not match: Expected ${scalingVec.size} but found ${vector.size}")
    vector match {
      case DenseVector(values) =>
        val newValues = transformDense(values)
        Vectors.dense(newValues)
      case SparseVector(size, indices, values) =>
        val (newIndices, newValues) = transformSparse(indices, values)
        Vectors.sparse(size, newIndices, newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }

  private[spark] def transformDense(values: Array[Double]): Array[Double] = {
    val newValues = values.clone()
    val dim = scalingVec.size
    var i = 0
    while (i < dim) {
      newValues(i) *= scalingVec(i)
      i += 1
    }
    newValues
  }

  private[spark] def transformSparse(
      indices: Array[Int],
      values: Array[Double]): (Array[Int], Array[Double]) = {
    val newValues = values.clone()
    val dim = newValues.length
    var i = 0
    while (i < dim) {
      newValues(i) *= scalingVec(indices(i))
      i += 1
    }
    (indices, newValues)
  }
}

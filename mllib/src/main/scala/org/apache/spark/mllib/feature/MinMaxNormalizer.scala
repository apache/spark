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

import org.apache.spark.Logging
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Performs a linear transformation on the original data values using column summary
 * statistics, to scale each feature to a given range, which is commonly known as min-max
 * normalization or Rescaling.
 *
 *  Normalized(x) = (x - min) / (max - min) * scale + newBase
 *
 * With default parameters (newBase = 0 and scale = 1), MinMaxScaler is handy to normalize input
 * to range [0, 1].
 * @param newBase minimum value after transformation, shared by all features
 * @param scale parameter for vector scaling, shared by all features
 */

@Experimental
class MinMaxScaler(newBase: Double, scale: Double) extends Logging {

  def this() = this(newBase = 0.0, scale = 1.0)
  /**
   * Computes the min and max and stores as a model to be used for later scaling.
   *
   * @param data The data used to compute the min and max to build the transformation model.
   * @return a MinMaxScalerModel
   */
  def fit(data: RDD[Vector]): MinMaxScalerModel = {

    val summary = data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))

    new MinMaxScalerModel(summary.min, summary.max, newBase, scale)
  }
}

/**
 * :: Experimental ::
 * Represents a MinMaxScaler model that can transform vectors.
 *
 * @param min column min values
 * @param max column max values
 * @param newBase new minimum value after transformation, shared by all features
 * @param scale controls the range after transformation, shared by all features
 */
@Experimental
class MinMaxScalerModel (
    val min: Vector,
    val max: Vector,
    var newBase: Double,
    var scale: Double) extends VectorTransformer {

  def this(min: Vector, max: Vector) {
    this(min, max, newBase = 0, scale = 1.0)
    require(min.size == max.size,
        "min and max vectors must have equal size if both are provided")
  }

  @DeveloperApi
  def setNewBase(newBase: Double): this.type = {
    this.newBase = newBase
    this
  }

  @DeveloperApi
  def setScale(scale: Double): this.type = {
    this.scale = scale
    this
  }

  /**
   * Applies MinMax normalization transformation on a vector.
   *
   * @param vector Vector to be Rescaled (normalized).
   * @return Rescaled vector. If min == max for a feature, it will return 0.5 * scale + base
   */
  override def transform(vector: Vector): Vector = {

    vector match {
      case DenseVector(vs) =>
        val values = vs.clone()
        val size = values.size
        var i = 0
        while(i < size) {
          val range = max(i) - min(i)
          val raw = if(range != 0) (values(i) - min(i)) / range else 0.5
          values(i) =  raw * scale + newBase
          i += 1
        }
        Vectors.dense(values)
      case SparseVector(size, indices, vs) =>
        // For sparse vector, the `index` array inside sparse vector object will not be changed,
        // so we can re-use it to save memory.
        val values = vs.clone()
        val nnz = values.size
        var i = 0
        while (i < nnz) {
          val index = indices(i)
          val range = max(index) - min(index)
          val raw = if(range != 0) (values(i) - min(index)) / range else 0.5
          values(i) = raw  * scale + newBase
          i += 1
        }
        Vectors.sparse(size, indices, values)
      case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
    }
  }
}


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
 * Rescale original data values to a new range [lowerBound, upperBound] linearly using column
 * summary statistics (minimum and maximum), which is also known as min-max normalization or
 * Rescaling. The rescaled value for feature E is calculated as,
 *
 * Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (upperBound - lowerBound) + lowerBound
 *
 * @param lowerBound minimum value after transformation, shared by all features
 * @param upperBound maximum value after transformation, shared by all features
 */

@Experimental
class MinMaxScaler(lowerBound: Double, upperBound: Double) extends Logging {

  def this() = this(lowerBound = 0.0, upperBound = 1.0)
  /**
   * Computes the min and max and stores as a model to be used for later scaling.
   *
   * @param data The data used to collect the min and max for each feature.
   * @return a MinMaxScalerModel containing statistics and can be used to perform rescaling
   */
  def fit(data: RDD[Vector]): MinMaxScalerModel = {

    val summary = data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))

    new MinMaxScalerModel(summary.min, summary.max, lowerBound, upperBound)
  }
}

/**
 * :: Experimental ::
 * Represents a MinMaxScaler model that can rescale vectors to a new range [lowerBound, upperBound]
 *
 * @param min column min values
 * @param max column max values
 * @param lowerBound new minimum value after transformation, shared by all features
 * @param upperBound new maximum value after transformation, shared by all features
 */
@Experimental
class MinMaxScalerModel (
    val min: Vector,
    val max: Vector,
    var lowerBound: Double,
    var upperBound: Double) extends VectorTransformer {

  def this(min: Vector, max: Vector) {
    this(min, max, lowerBound = 0, upperBound = 1.0)
    require(min.size == max.size,
        "min and max vectors must have equal size if both are provided")
  }

  @DeveloperApi
  def setLowerBound(lowerBound: Double): this.type = {
    this.lowerBound = lowerBound
    this
  }

  @DeveloperApi
  def setUpperBound(upperBound: Double): this.type = {
    this.upperBound = upperBound
    this
  }

  /**
   * Applies MinMax normalization transformation on a vector.
   *
   * @param vector Vector to be Rescaled (normalized).
   * @return Rescaled vector. If min == max for a feature, it will return 0.5 * (lowerBound +
   *         upperBound)
   */
  override def transform(vector: Vector): Vector = {

    val scale = upperBound - lowerBound
    vector match {
      case DenseVector(vs) =>
        val values = vs.clone()
        val size = values.size
        var i = 0
        while(i < size) {
          val range = max(i) - min(i)
          val raw = if(range != 0) (values(i) - min(i)) / range else 0.5
          values(i) =  raw * scale + lowerBound
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
          values(i) = raw  * scale + lowerBound
          i += 1
        }
        Vectors.sparse(size, indices, values)
      case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
    }
  }
}


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

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD

/**
 * Standardizes features by removing the mean and scaling to unit std using column summary
 * statistics on the samples in the training set.
 *
 * The "unit std" is computed using the corrected sample standard deviation
 * (https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation),
 * which is computed as the square root of the unbiased sample variance.
 *
 * @param withMean False by default. Centers the data with mean before scaling. It will build a
 *                 dense output, so take care when applying to sparse input.
 * @param withStd True by default. Scales the data to unit standard deviation.
 */
@Since("1.1.0")
class StandardScaler @Since("1.1.0") (withMean: Boolean, withStd: Boolean) extends Logging {

  @Since("1.1.0")
  def this() = this(false, true)

  if (!(withMean || withStd)) {
    logWarning("Both withMean and withStd are false. The model does nothing.")
  }

  /**
   * Computes the mean and variance and stores as a model to be used for later scaling.
   *
   * @param data The data used to compute the mean and variance to build the transformation model.
   * @return a StandardScalarModel
   */
  @Since("1.1.0")
  def fit(data: RDD[Vector]): StandardScalerModel = {
    // TODO: skip computation if both withMean and withStd are false
    val summary = data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
    new StandardScalerModel(
      Vectors.dense(summary.variance.toArray.map(v => math.sqrt(v))),
      summary.mean,
      withStd,
      withMean)
  }
}

/**
 * Represents a StandardScaler model that can transform vectors.
 *
 * @param std column standard deviation values
 * @param mean column mean values
 * @param withStd whether to scale the data to have unit standard deviation
 * @param withMean whether to center the data before scaling
 */
@Since("1.1.0")
class StandardScalerModel @Since("1.3.0") (
    @Since("1.3.0") val std: Vector,
    @Since("1.1.0") val mean: Vector,
    @Since("1.3.0") var withStd: Boolean,
    @Since("1.3.0") var withMean: Boolean) extends VectorTransformer {

  /**
   */
  @Since("1.3.0")
  def this(std: Vector, mean: Vector) {
    this(std, mean, withStd = std != null, withMean = mean != null)
    require(this.withStd || this.withMean,
      "at least one of std or mean vectors must be provided")
    if (this.withStd && this.withMean) {
      require(mean.size == std.size,
        "mean and std vectors must have equal size if both are provided")
    }
  }

  @Since("1.3.0")
  def this(std: Vector) = this(std, null)

  /**
   * :: DeveloperApi ::
   */
  @Since("1.3.0")
  @DeveloperApi
  def setWithMean(withMean: Boolean): this.type = {
    require(!(withMean && this.mean == null), "cannot set withMean to true while mean is null")
    this.withMean = withMean
    this
  }

  /**
   * :: DeveloperApi ::
   */
  @Since("1.3.0")
  @DeveloperApi
  def setWithStd(withStd: Boolean): this.type = {
    require(!(withStd && this.std == null),
      "cannot set withStd to true while std is null")
    this.withStd = withStd
    this
  }

  // Since `shift` will be only used in `withMean` branch, we have it as
  // `lazy val` so it will be evaluated in that branch. Note that we don't
  // want to create this array multiple times in `transform` function.
  private lazy val shift: Array[Double] = mean.toArray

  /**
   * Applies standardization transformation on a vector.
   *
   * @param vector Vector to be standardized.
   * @return Standardized vector. If the std of a column is zero, it will return default `0.0`
   *         for the column with zero std.
   */
  @Since("1.1.0")
  override def transform(vector: Vector): Vector = {
    require(mean.size == vector.size)
    if (withMean) {
      // By default, Scala generates Java methods for member variables. So every time when
      // the member variables are accessed, `invokespecial` will be called which is expensive.
      // This can be avoid by having a local reference of `shift`.
      val localShift = shift
      // Must have a copy of the values since it will be modified in place
      val values = vector match {
        // specially handle DenseVector because its toArray does not clone already
        case d: DenseVector => d.values.clone()
        case v: Vector => v.toArray
      }
      val size = values.length
      if (withStd) {
        var i = 0
        while (i < size) {
          values(i) = if (std(i) != 0.0) (values(i) - localShift(i)) * (1.0 / std(i)) else 0.0
          i += 1
        }
      } else {
        var i = 0
        while (i < size) {
          values(i) -= localShift(i)
          i += 1
        }
      }
      Vectors.dense(values)
    } else if (withStd) {
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.length
          var i = 0
          while(i < size) {
            values(i) *= (if (std(i) != 0.0) 1.0 / std(i) else 0.0)
            i += 1
          }
          Vectors.dense(values)
        case SparseVector(size, indices, vs) =>
          // For sparse vector, the `index` array inside sparse vector object will not be changed,
          // so we can re-use it to save memory.
          val values = vs.clone()
          val nnz = values.length
          var i = 0
          while (i < nnz) {
            values(i) *= (if (std(indices(i)) != 0.0) 1.0 / std(indices(i)) else 0.0)
            i += 1
          }
          Vectors.sparse(size, indices, values)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else {
      // Note that it's safe since we always assume that the data in RDD should be immutable.
      vector
    }
  }
}

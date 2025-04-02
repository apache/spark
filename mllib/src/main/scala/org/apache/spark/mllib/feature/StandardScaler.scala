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
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.{StandardScalerModel => NewStandardScalerModel}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
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
    val summary = Statistics.colStats(data.map((_, 1.0)), Seq("mean", "std"))

    new StandardScalerModel(
      Vectors.fromML(summary.std),
      Vectors.fromML(summary.mean),
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
  def this(std: Vector, mean: Vector) = {
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

  @Since("1.3.0")
  def setWithMean(withMean: Boolean): this.type = {
    require(!(withMean && this.mean == null), "cannot set withMean to true while mean is null")
    this.withMean = withMean
    this
  }

  @Since("1.3.0")
  def setWithStd(withStd: Boolean): this.type = {
    require(!(withStd && this.std == null),
      "cannot set withStd to true while std is null")
    this.withStd = withStd
    this
  }

  // Since `shift` will be only used in `withMean` branch, we have it as
  // `lazy val` so it will be evaluated in that branch. Note that we don't
  // want to create this array multiple times in `transform` function.
  private lazy val shift = mean.toArray
  private lazy val scale = std.toArray.map { v => if (v == 0) 0.0 else 1.0 / v }

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

    (withMean, withStd) match {
      case (true, true) =>
        // By default, Scala generates Java methods for member variables. So every time when
        // the member variables are accessed, `invokespecial` will be called which is expensive.
        // This can be avoid by having a local reference of `shift`.
        val localShift = shift
        val localScale = scale
        val values = vector match {
          // specially handle DenseVector because its toArray does not clone already
          case d: DenseVector => d.values.clone()
          case v: Vector => v.toArray
        }
        val newValues = NewStandardScalerModel
          .transformWithBoth(localShift, localScale, values)
        Vectors.dense(newValues)

      case (true, false) =>
        val localShift = shift
        val values = vector match {
          case d: DenseVector => d.values.clone()
          case v: Vector => v.toArray
        }
        val newValues = NewStandardScalerModel
          .transformWithShift(localShift, values)
        Vectors.dense(newValues)

      case (false, true) =>
        val localScale = scale
        vector match {
          case DenseVector(values) =>
            val newValues = NewStandardScalerModel
              .transformDenseWithScale(localScale, values.clone())
            Vectors.dense(newValues)
          case SparseVector(size, indices, values) =>
            // For sparse vector, the `index` array inside sparse vector object will not be changed,
            // so we can re-use it to save memory.
            val newValues = NewStandardScalerModel
              .transformSparseWithScale(localScale, indices, values.clone())
            Vectors.sparse(size, indices, newValues)
          case v =>
            throw new IllegalArgumentException(s"Unknown vector type ${v.getClass}.")
        }

      case _ => vector
    }
  }
}

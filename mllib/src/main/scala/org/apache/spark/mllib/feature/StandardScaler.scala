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
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 *
 * @param withMean False by default. Centers the data with mean before scaling. It will build a
 *                 dense output, so this does not work on sparse input and will raise an exception.
 * @param withStd True by default. Scales the data to unit standard deviation.
 */
@Experimental
class StandardScaler(withMean: Boolean, withStd: Boolean) extends Logging {

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
  def fit(data: RDD[Vector]): StandardScalerModel = {
    // TODO: skip computation if both withMean and withStd are false
    val summary = data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))
    new StandardScalerModel(withMean, withStd, summary.mean, summary.variance)
  }
}

/**
 * :: Experimental ::
 * Represents a StandardScaler model that can transform vectors.
 *
 * @param withMean whether to center the data before scaling
 * @param withStd whether to scale the data to have unit standard deviation
 * @param mean column mean values
 * @param variance column variance values
 */
@Experimental
class StandardScalerModel private[mllib] (
    val withMean: Boolean,
    val withStd: Boolean,
    val mean: Vector,
    val variance: Vector) extends VectorTransformer {

  require(mean.size == variance.size)

  private lazy val factor: Array[Double] = {
    val f = Array.ofDim[Double](variance.size)
    var i = 0
    while (i < f.size) {
      f(i) = if (variance(i) != 0.0) 1.0 / math.sqrt(variance(i)) else 0.0
      i += 1
    }
    f
  }

  // Since `shift` will be only used in `withMean` branch, we have it as
  // `lazy val` so it will be evaluated in that branch. Note that we don't
  // want to create this array multiple times in `transform` function.
  private lazy val shift: Array[Double] = mean.toArray

  /**
   * Applies standardization transformation on a vector.
   *
   * @param vector Vector to be standardized.
   * @return Standardized vector. If the variance of a column is zero, it will return default `0.0`
   *         for the column with zero variance.
   */
  override def transform(vector: Vector): Vector = {
    require(mean.size == vector.size)
    if (withMean) {
      // By default, Scala generates Java methods for member variables. So every time when
      // the member variables are accessed, `invokespecial` will be called which is expensive.
      // This can be avoid by having a local reference of `shift`.
      val localShift = shift
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.size
          if (withStd) {
            // Having a local reference of `factor` to avoid overhead as the comment before.
            val localFactor = factor
            var i = 0
            while (i < size) {
              values(i) = (values(i) - localShift(i)) * localFactor(i)
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
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else if (withStd) {
      // Having a local reference of `factor` to avoid overhead as the comment before.
      val localFactor = factor
      vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.size
          var i = 0
          while(i < size) {
            values(i) *= localFactor(i)
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
            values(i) *= localFactor(indices(i))
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

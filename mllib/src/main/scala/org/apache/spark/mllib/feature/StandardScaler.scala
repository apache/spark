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

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 *
 * @param withMean False by default. Centers the data with mean before scaling. It will build a
 *                 dense output, so this does not work on sparse input and will raise an exception.
 * @param withStd True by default. Scales the data to unit standard deviation.
 */
@DeveloperApi
class StandardScaler(withMean: Boolean, withStd: Boolean) extends VectorTransformer {

  def this() = this(false, true)

  require(withMean || withStd, s"withMean and withStd both equal to false. Doing nothing.")

  private var mean: BV[Double] = _
  private var factor: BV[Double] = _

  /**
   * Computes the mean and variance and stores as a model to be used for later scaling.
   *
   * @param data The data used to compute the mean and variance to build the transformation model.
   * @return This StandardScalar object.
   */
  def fit(data: RDD[Vector]): this.type = {
    val summary = data.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))

    mean = summary.mean.toBreeze
    factor = summary.variance.toBreeze
    require(mean.length == factor.length)

    var i = 0
    while (i < factor.length) {
      factor(i) = if (factor(i) != 0.0) 1.0 / math.sqrt(factor(i)) else 0.0
      i += 1
    }

    this
  }

  /**
   * Applies standardization transformation on a vector.
   *
   * @param vector Vector to be standardized.
   * @return Standardized vector. If the variance of a column is zero, it will return default `0.0`
   *         for the column with zero variance.
   */
  override def transform(vector: Vector): Vector = {
    if (mean == null || factor == null) {
      throw new IllegalStateException(
        "Haven't learned column summary statistics yet. Call fit first.")
    }

    require(vector.size == mean.length)

    if (withMean) {
      vector.toBreeze match {
        case dv: BDV[Double] =>
          val output = vector.toBreeze.copy
          var i = 0
          while (i < output.length) {
            output(i) = (output(i) - mean(i)) * (if (withStd) factor(i) else 1.0)
            i += 1
          }
          Vectors.fromBreeze(output)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else if (withStd) {
      vector.toBreeze match {
        case dv: BDV[Double] => Vectors.fromBreeze(dv :* factor)
        case sv: BSV[Double] =>
          // For sparse vector, the `index` array inside sparse vector object will not be changed,
          // so we can re-use it to save memory.
          val output = new BSV[Double](sv.index, sv.data.clone(), sv.length)
          var i = 0
          while (i < output.data.length) {
            output.data(i) *= factor(output.index(i))
            i += 1
          }
          Vectors.fromBreeze(output)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
    } else {
      // Note that it's safe since we always assume that the data in RDD should be immutable.
      vector
    }
  }

}

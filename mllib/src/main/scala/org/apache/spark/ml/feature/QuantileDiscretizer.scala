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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * Params for [[QuantileDiscretizer]].
 */
private[feature] trait QuantileDiscretizerBase extends Params
  with HasInputCol with HasOutputCol {

  /**
   * Number of buckets (quantiles, or categories) into which data points are grouped. Must
   * be >= 2.
   * default: 2
   * @group param
   */
  val numBuckets = new IntParam(this, "numBuckets", "Maximum number of buckets (quantiles, or " +
    "categories) into which data points are grouped. Must be >= 2.",
    ParamValidators.gtEq(2))
  setDefault(numBuckets -> 2)

  /** @group getParam */
  def getNumBuckets: Int = getOrDefault(numBuckets)

  /**
   * Relative error (see documentation for
   * [[org.apache.spark.sql.DataFrameStatFunctions.approxQuantile approxQuantile]] for description)
   * Must be in the range [0, 1].
   * default: 0.001
   * @group param
   */
  val relativeError = new DoubleParam(this, "relativeError", "The relative target precision " +
    "for the approximate quantile algorithm used to generate buckets. " +
    "Must be in the range [0, 1].", ParamValidators.inRange(0.0, 1.0))
  setDefault(relativeError -> 0.001)

  /** @group getParam */
  def getRelativeError: Double = getOrDefault(relativeError)
}

/**
 * `QuantileDiscretizer` takes a column with continuous features and outputs a column with binned
 * categorical features. The number of bins can be set using the `numBuckets` parameter.
 * The bin ranges are chosen using an approximate algorithm (see the documentation for
 * [[org.apache.spark.sql.DataFrameStatFunctions.approxQuantile approxQuantile]]
 * for a detailed description). The precision of the approximation can be controlled with the
 * `relativeError` parameter. The lower and upper bin bounds will be `-Infinity` and `+Infinity`,
 * covering all real values.
 */
@Since("1.6.0")
final class QuantileDiscretizer @Since("1.6.0") (@Since("1.6.0") override val uid: String)
  extends Estimator[Bucketizer] with QuantileDiscretizerBase with DefaultParamsWritable {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("quantileDiscretizer"))

  /** @group setParam */
  @Since("2.0.0")
  def setRelativeError(value: Double): this.type = set(relativeError, value)

  /** @group setParam */
  @Since("1.6.0")
  def setNumBuckets(value: Int): this.type = set(numBuckets, value)

  /** @group setParam */
  @Since("1.6.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    val inputFields = schema.fields
    require(inputFields.forall(_.name != $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): Bucketizer = {
    val splits = dataset.stat.approxQuantile($(inputCol),
      (0.0 to 1.0 by 1.0/$(numBuckets)).toArray, $(relativeError))
    splits(0) = Double.NegativeInfinity
    splits(splits.length - 1) = Double.PositiveInfinity

    val bucketizer = new Bucketizer(uid).setSplits(splits)
    copyValues(bucketizer.setParent(this))
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): QuantileDiscretizer = defaultCopy(extra)
}

@Since("1.6.0")
object QuantileDiscretizer extends DefaultParamsReadable[QuantileDiscretizer] with Logging {

  @Since("1.6.0")
  override def load(path: String): QuantileDiscretizer = super.load(path)
}

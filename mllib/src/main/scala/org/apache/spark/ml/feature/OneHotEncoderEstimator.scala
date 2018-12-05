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
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

/**
 * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
 * at most a single one-value per row that indicates the input category index.
 * For example with 5 categories, an input value of 2.0 would map to an output vector of
 * `[0.0, 0.0, 1.0, 0.0]`.
 * The last category is not included by default (configurable via `dropLast`),
 * because it makes the vector entries sum up to one, and hence linearly dependent.
 * So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.
 *
 * @note This is different from scikit-learn's OneHotEncoder, which keeps all categories.
 * The output vectors are sparse.
 *
 * When `handleInvalid` is configured to 'keep', an extra "category" indicating invalid values is
 * added as last category. So when `dropLast` is true, invalid values are encoded as all-zeros
 * vector.
 *
 * @note When encoding multi-column by using `inputCols` and `outputCols` params, input/output cols
 * come in pairs, specified by the order in the arrays, and each pair is treated independently.
 *
 * @note `OneHotEncoderEstimator` is renamed to `OneHotEncoder` in 3.0.0. This
 * `OneHotEncoderEstimator` is kept as an alias and will be removed in further version.
 *
 * @see `StringIndexer` for converting categorical values into category indices
 */
@Since("2.3.0")
class OneHotEncoderEstimator @Since("2.3.0") (@Since("2.3.0") override val uid: String)
    extends Estimator[OneHotEncoderModel] with OneHotEncoderBase with DefaultParamsWritable {

  private val oneHotEncoder = new OneHotEncoder(uid)

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("oneHotEncoder"))

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  @Since("2.3.0")
  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  /** @group setParam */
  @Since("2.3.0")
  def setDropLast(value: Boolean): this.type = set(dropLast, value)

  /** @group setParam */
  @Since("2.3.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType =
    copyValues(oneHotEncoder).transformSchema(schema)

  @Since("2.3.0")
  override def fit(dataset: Dataset[_]): OneHotEncoderModel = copyValues(oneHotEncoder).fit(dataset)

  @Since("2.3.0")
  override def copy(extra: ParamMap): OneHotEncoderEstimator = defaultCopy(extra)
}

@Since("2.3.0")
object OneHotEncoderEstimator extends DefaultParamsReadable[OneHotEncoderEstimator] {
  @Since("2.3.0")
  override def load(path: String): OneHotEncoderEstimator = super.load(path)
}

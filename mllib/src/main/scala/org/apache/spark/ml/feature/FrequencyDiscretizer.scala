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

import org.apache.spark.internal.Logging
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

/*
  * Params for [[FrequencyDiscretizer]].
  */

private[feature] trait FrequencyDiscretizerBase extends Params
  with HasInputCol with HasOutputCol {
  /**
    * Number of buckets into which data points are grouped.
    * Must be greater than or equal to 2.
    * default: 2
    * @group param
    */
  val numBuckets = new IntParam(this, "numBuckets", "Number of buckets into " +
    "which data points are grouped. Must be >= 2",
    ParamValidators.gtEq(2))
  setDefault(numBuckets -> 2)

  /** @group getParam */
  def getNumBuckets: Int = getOrDefault(numBuckets)

}

/*
  * FrequencyDiscretizer takes a column with continuous features and
  * outputs acolumn with binned categorical features. The number of bins can be set using the 'numBuckets' parameter.
  * It is possible that the number of buckets used will be smaller than this value, for example, if there
  * are too few distinct values of the input to create enough distinct quantiles.
  *
  * Algorithm: sort all the values at first, and the value sorted index and the numBuckets decides the locations
  */

final class FrequencyDiscretizer (override val uid: String)
  extends Estimator[Bucketizer] with FrequencyDiscretizerBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("frequencyDiscretizer"))

  /** @group setParam */
  def setNumBuckets(value: Int): this.type = set(numBuckets, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkNumericType(schema, $(inputCol))
    val inputFields = schema.fields
    require(inputFields.forall(_.name != $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  override def fit(dataset: Dataset[_]): Bucketizer = {
    transformSchema(dataset.schema, logging = true)
    val sortData = dataset.select($(inputCol)).sort(asc($(inputCol)))
    val arrays = sortData.rdd.map(r => r.getDouble(0)).collect()
    val k = arrays.size / $(numBuckets)
    val ranges = 0 to k * ($(numBuckets) - 1) by k
    var splits = ranges.map{r => arrays(r)}.toArray
    splits(0) = Double.NegativeInfinity
    splits :+= Double.PositiveInfinity
    val bucketizer = new Bucketizer(uid)
      .setSplits(splits)
    copyValues(bucketizer.setParent(this))
  }

  override def copy(extra: ParamMap): QuantileDiscretizer = defaultCopy(extra)
}

object FrequencyDiscretizer extends DefaultParamsReadable[FrequencyDiscretizer] with Logging {

  override def load(path: String): FrequencyDiscretizer = super.load(path)
}
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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{NominalAttribute, BinaryAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * :: AlphaComponent ::
 * Binarize a column of continuous features given a threshold.
 */
@AlphaComponent
final class Bucketizer extends Transformer with HasInputCol with HasOutputCol {

  /**
   * Param for threshold used to binarize continuous features.
   * The features greater than the threshold, will be binarized to 1.0.
   * The features equal to or less than the threshold, will be binarized to 0.0.
   * @group param
   */
  val buckets: Param[Array[Double]] = new Param[Array[Double]](this, "buckets", "")

  /** @group getParam */
  def getBuckets: Array[Double] = $(buckets)

  /** @group setParam */
  def setBuckets(value: Array[Double]): this.type = set(buckets, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val bucketizer = udf { feature: Double => binarySearchForBins($(buckets), feature) }
    val outputColName = $(outputCol)
    val metadata = NominalAttribute.defaultAttr
      .withName(outputColName).withValues($(buckets).map(_.toString)).toMetadata()
    dataset.select(col("*"), bucketizer(dataset($(inputCol))).as(outputColName, metadata))
  }

  /**
   * Binary searching in several bins to place each data point.
   */
  private def binarySearchForBins(splits: Array[Double], feature: Double): Double = {
    val wrappedSplits = Array(Double.MinValue) ++ splits ++ Array(Double.MaxValue)
    var left = 0
    var right = wrappedSplits.length - 2
    while (left <= right) {
      val mid = left + (right - left) / 2
      val split = wrappedSplits(mid)
      if ((feature > split) && (feature <= wrappedSplits(mid + 1))) {
        return mid
      } else if (feature <= split) {
        right = mid - 1
      } else {
        left = mid + 1
      }
    }
    -1
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)

    val inputFields = schema.fields
    val outputColName = $(outputCol)

    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")

    val attr = NominalAttribute.defaultAttr.withName(outputColName)
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }
}

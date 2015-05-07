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
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * :: AlphaComponent ::
 * `Bucketizer` maps a column of continuous features to a column of feature buckets.
 */
@AlphaComponent
final class Bucketizer extends Transformer with HasInputCol with HasOutputCol {

  /**
   * The given buckets should match 1) its size is larger than zero; 2) it is ordered in a non-DESC
   * way.
   */
  private def checkBuckets(buckets: Array[Double]): Boolean = {
    if (buckets.size == 0) false
    else if (buckets.size == 1) true
    else {
      buckets.foldLeft((true, Double.MinValue)) { case ((validator, prevValue), currValue) =>
        if (validator & prevValue <= currValue) {
          (true, currValue)
        } else {
          (false, currValue)
        }
      }._1
    }
  }

  /**
   * Parameter for mapping continuous features into buckets.
   * @group param
   */
  val buckets: Param[Array[Double]] = new Param[Array[Double]](this, "buckets",
    "Split points for mapping continuous features into buckets.", checkBuckets)

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
    val bucketizer = udf { feature: Double => binarySearchForBuckets($(buckets), feature) }
    val outputColName = $(outputCol)
    val metadata = NominalAttribute.defaultAttr
      .withName(outputColName).withValues($(buckets).map(_.toString)).toMetadata()
    dataset.select(col("*"), bucketizer(dataset($(inputCol))).as(outputColName, metadata))
  }

  /**
   * Binary searching in several buckets to place each data point.
   */
  private def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
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

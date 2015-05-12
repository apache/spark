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
import org.apache.spark.ml.Model
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * :: AlphaComponent ::
 * `Bucketizer` maps a column of continuous features to a column of feature buckets.
 */
@AlphaComponent
final class Bucketizer(override val uid: String)
  extends Model[Bucketizer] with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("bucketizer"))

  /**
   * Parameter for mapping continuous features into buckets. With n splits, there are n+1 buckets.
   * A bucket defined by splits x,y holds values in the range [x,y). Splits should be strictly
   * increasing. Values at -inf, inf must be explicitly provided to cover all Double values;
   * otherwise, values outside the splits specified will be treated as errors.
   * @group param
   */
  val splits: Param[Array[Double]] = new Param[Array[Double]](this, "splits",
    "Split points for mapping continuous features into buckets. With n splits, there are n+1 " +
      "buckets. A bucket defined by splits x,y holds values in the range [x,y). The splits " +
      "should be strictly increasing. Values at -inf, inf must be explicitly provided to cover" +
      " all Double values; otherwise, values outside the splits specified will be treated as" +
      " errors.",
    Bucketizer.checkSplits _)

  /** @group getParam */
  def getSplits: Array[Double] = $(splits)

  /** @group setParam */
  def setSplits(value: Array[Double]): this.type = set(splits, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val bucketizer = udf { feature: Double =>
      Bucketizer.binarySearchForBuckets($(splits), feature)
    }
    val newCol = bucketizer(dataset($(inputCol)))
    val newField = prepOutputField(dataset.schema)
    dataset.withColumn($(outputCol), newCol.as($(outputCol), newField.metadata))
  }

  private def prepOutputField(schema: StructType): StructField = {
    val buckets = $(splits).sliding(2).map(bucket => bucket.mkString(", ")).toArray
    val attr = new NominalAttribute(name = Some($(outputCol)), isOrdinal = Some(true),
      values = Some(buckets))
    attr.toStructField()
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    SchemaUtils.appendColumn(schema, prepOutputField(schema))
  }
}

private[feature] object Bucketizer {
  /** We require splits to be of length >= 3 and to be in strictly increasing order. */
  def checkSplits(splits: Array[Double]): Boolean = {
    if (splits.length < 3) {
      false
    } else {
      var i = 0
      while (i < splits.length - 1) {
        if (splits(i) >= splits(i + 1)) return false
        i += 1
      }
      true
    }
  }

  /**
   * Binary searching in several buckets to place each data point.
   * @throws RuntimeException if a feature is < splits.head or >= splits.last
   */
  def binarySearchForBuckets(
      splits: Array[Double],
      feature: Double): Double = {
    // Check bounds.  We make an exception for +inf so that it can exist in some bin.
    if ((feature < splits.head) || (feature >= splits.last && feature != Double.PositiveInfinity)) {
      throw new RuntimeException(s"Feature value $feature out of Bucketizer bounds" +
        s" [${splits.head}, ${splits.last}).  Check your features, or loosen " +
        s"the lower/upper bound constraints.")
    }
    var left = 0
    var right = splits.length - 2
    while (left < right) {
      val mid = (left + right) / 2
      val split = splits(mid + 1)
      if (feature < split) {
        right = mid
      } else {
        left = mid + 1
      }
    }
    left
  }
}

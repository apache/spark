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
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * :: AlphaComponent ::
 * `Bucketizer` maps a column of continuous features to a column of feature buckets.
 */
@AlphaComponent
private[ml] final class Bucketizer(override val parent: Estimator[Bucketizer])
  extends Model[Bucketizer] with HasInputCol with HasOutputCol {

  def this() = this(null)

  /**
   * Parameter for mapping continuous features into buckets. With n splits, there are n+1 buckets.
   * A bucket defined by splits x,y holds values in the range (x,y].
   * @group param
   */
  val splits: Param[Array[Double]] = new Param[Array[Double]](this, "splits",
    "Split points for mapping continuous features into buckets. With n splits, there are n+1" +
      " buckets. A bucket defined by splits x,y holds values in the range (x,y].",
    Bucketizer.checkSplits)

  /** @group getParam */
  def getSplits: Array[Double] = $(splits)

  /** @group setParam */
  def setSplits(value: Array[Double]): this.type = set(splits, value)

  /** @group Param */
  val lowerInclusive: BooleanParam = new BooleanParam(this, "lowerInclusive",
    "An indicator of the inclusiveness of negative infinite.")
  setDefault(lowerInclusive -> true)

  /** @group getParam */
  def getLowerInclusive: Boolean = $(lowerInclusive)

  /** @group setParam */
  def setLowerInclusive(value: Boolean): this.type = set(lowerInclusive, value)

  /** @group Param */
  val upperInclusive: BooleanParam = new BooleanParam(this, "upperInclusive",
    "An indicator of the inclusiveness of positive infinite.")
  setDefault(upperInclusive -> true)

  /** @group getParam */
  def getUpperInclusive: Boolean = $(upperInclusive)

  /** @group setParam */
  def setUpperInclusive(value: Boolean): this.type = set(upperInclusive, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val wrappedSplits = Array(Double.MinValue) ++ $(splits) ++ Array(Double.MaxValue)
    val bucketizer = udf { feature: Double =>
      Bucketizer.binarySearchForBuckets(wrappedSplits, feature) }
    val newCol = bucketizer(dataset($(inputCol)))
    val newField = prepOutputField(dataset.schema)
    dataset.withColumn($(outputCol), newCol.as($(outputCol), newField.metadata))
  }

  private def prepOutputField(schema: StructType): StructField = {
    val attr = new NominalAttribute(
      name = Some($(outputCol)),
      isOrdinal = Some(true),
      numValues = Some($(splits).size),
      values = Some($(splits).map(_.toString)))

    attr.toStructField()
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    require(schema.fields.forall(_.name != $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    StructType(schema.fields :+ prepOutputField(schema))
  }
}

object Bucketizer {
  /**
   * The given splits should match 1) its size is larger than zero; 2) it is ordered in a strictly
   * increasing way.
   */
  private def checkSplits(splits: Array[Double]): Boolean = {
    if (splits.size == 0) false
    else if (splits.size == 1) true
    else {
      splits.foldLeft((true, Double.MinValue)) { case ((validator, prevValue), currValue) =>
        if (validator && prevValue < currValue) {
          (true, currValue)
        } else {
          (false, currValue)
        }
      }._1
    }
  }

  /**
   * Binary searching in several buckets to place each data point.
   */
  private[feature] def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
    var left = 0
    var right = splits.length - 2
    while (left <= right) {
      val mid = left + (right - left) / 2
      val split = splits(mid)
      if ((feature > split) && (feature <= splits(mid + 1))) {
        return mid
      } else if (feature <= split) {
        right = mid - 1
      } else {
        left = mid + 1
      }
    }
    throw new Exception("Failed to find a bucket.")
  }
}

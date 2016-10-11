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

import java.{util => ju}

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Model
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * `Bucketizer` maps a column of continuous features to a column of feature buckets.
 */
@Since("1.4.0")
final class Bucketizer @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Model[Bucketizer] with HasInputCol with HasOutputCol with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("bucketizer"))

  /**
   * Parameter for mapping continuous features into buckets. With n+1 splits, there are n buckets.
   * A bucket defined by splits x,y holds values in the range [x,y) except the last bucket, which
   * also includes y. Splits should be of length >= 3 and strictly increasing.
   * Values at -inf, inf must be explicitly provided to cover all Double values;
   * otherwise, values outside the splits specified will be treated as errors.
   * @group param
   */
  @Since("1.4.0")
  val splits: DoubleArrayParam = new DoubleArrayParam(this, "splits",
    "Split points for mapping continuous features into buckets. With n+1 splits, there are n " +
      "buckets. A bucket defined by splits x,y holds values in the range [x,y) except the last " +
      "bucket, which also includes y. The splits should be of length >= 3 and strictly " +
      "increasing. Values at -inf, inf must be explicitly provided to cover all Double values; " +
      "otherwise, values outside the splits specified will be treated as errors.",
    Bucketizer.checkSplits)

  /** @group getParam */
  @Since("1.4.0")
  def getSplits: Array[Double] = $(splits)

  /** @group setParam */
  @Since("1.4.0")
  def setSplits(value: Array[Double]): this.type = set(splits, value)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    val bucketizer = udf { feature: Double =>
      Bucketizer.binarySearchForBuckets($(splits), feature)
    }
    val newCol = bucketizer(dataset($(inputCol)))
    val newField = prepOutputField(dataset.schema)
    dataset.withColumn($(outputCol), newCol, newField.metadata)
  }

  private def prepOutputField(schema: StructType): StructField = {
    val buckets = $(splits).sliding(2).map(bucket => bucket.mkString(", ")).toArray
    val attr = new NominalAttribute(name = Some($(outputCol)), isOrdinal = Some(true),
      values = Some(buckets))
    attr.toStructField()
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    SchemaUtils.appendColumn(schema, prepOutputField(schema))
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Bucketizer = {
    defaultCopy[Bucketizer](extra).setParent(parent)
  }
}

@Since("1.6.0")
object Bucketizer extends DefaultParamsReadable[Bucketizer] {

  /**
   * We require splits to be of length >= 3 and to be in strictly increasing order.
   * No NaN split should be accepted.
   */
  private[feature] def checkSplits(splits: Array[Double]): Boolean = {
    if (splits.length < 3) {
      false
    } else {
      var i = 0
      val n = splits.length - 1
      while (i < n) {
        if (splits(i) >= splits(i + 1) || splits(i).isNaN) return false
        i += 1
      }
      !splits(n).isNaN
    }
  }

  /**
   * Binary searching in several buckets to place each data point.
   * @throws SparkException if a feature is < splits.head or > splits.last
   */
  private[feature] def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
    if (feature.isNaN) {
      splits.length - 1
    } else if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new SparkException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }

  @Since("1.6.0")
  override def load(path: String): Bucketizer = super.load(path)
}

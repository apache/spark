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

import org.json4s.JsonDSL._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Model
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol, HasInputCols, HasOutputCol, HasOutputCols}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * `Bucketizer` maps a column of continuous features to a column of feature buckets.
 *
 * Since 2.3.0,
 * `Bucketizer` can map multiple columns at once by setting the `inputCols` parameter. Note that
 * when both the `inputCol` and `inputCols` parameters are set, an Exception will be thrown. The
 * `splits` parameter is only used for single column usage, and `splitsArray` is for multiple
 * columns.
 */
@Since("1.4.0")
final class Bucketizer @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Model[Bucketizer] with HasHandleInvalid with HasInputCol with HasOutputCol
    with HasInputCols with HasOutputCols with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("bucketizer"))

  /**
   * Parameter for mapping continuous features into buckets. With n+1 splits, there are n buckets.
   * A bucket defined by splits x,y holds values in the range [x,y) except the last bucket, which
   * also includes y. Splits should be of length greater than or equal to 3 and strictly increasing.
   * Values at -inf, inf must be explicitly provided to cover all Double values;
   * otherwise, values outside the splits specified will be treated as errors.
   *
   * See also [[handleInvalid]], which can optionally create an additional bucket for NaN values.
   *
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

  /**
   * Param for how to handle invalid entries containing NaN values. Values outside the splits
   * will always be treated as errors. Options are 'skip' (filter out rows with
   * invalid values), 'error' (throw an error), or 'keep' (keep invalid values in a special
   * additional bucket). Note that in the multiple column case, the invalid handling is applied
   * to all columns. That said for 'error' it will throw an error if any invalids are found in
   * any column, for 'skip' it will skip rows with any invalids in any columns, etc.
   * Default: "error"
   * @group param
   */
  @Since("2.1.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "how to handle invalid entries containing NaN values. Values outside the splits will always " +
    "be treated as errorsOptions are skip (filter out rows with invalid values), " +
    "error (throw an error), or keep (keep invalid values in a special additional bucket).",
    ParamValidators.inArray(Bucketizer.supportedHandleInvalids))

  /** @group setParam */
  @Since("2.1.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, Bucketizer.ERROR_INVALID)

  /**
   * Parameter for specifying multiple splits parameters. Each element in this array can be used to
   * map continuous features into buckets.
   *
   * @group param
   */
  @Since("2.3.0")
  val splitsArray: DoubleArrayArrayParam = new DoubleArrayArrayParam(this, "splitsArray",
    "The array of split points for mapping continuous features into buckets for multiple " +
      "columns. For each input column, with n+1 splits, there are n buckets. A bucket defined by " +
      "splits x,y holds values in the range [x,y) except the last bucket, which also includes y. " +
      "The splits should be of length >= 3 and strictly increasing. Values at -inf, inf must be " +
      "explicitly provided to cover all Double values; otherwise, values outside the splits " +
      "specified will be treated as errors.",
    Bucketizer.checkSplitsArray)

  /** @group getParam */
  @Since("2.3.0")
  def getSplitsArray: Array[Array[Double]] = $(splitsArray)

  /** @group setParam */
  @Since("2.3.0")
  def setSplitsArray(value: Array[Array[Double]]): this.type = set(splitsArray, value)

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("2.3.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformedSchema = transformSchema(dataset.schema)

    val (inputColumns, outputColumns) = if (isSet(inputCols)) {
      ($(inputCols).toSeq, $(outputCols).toSeq)
    } else {
      (Seq($(inputCol)), Seq($(outputCol)))
    }

    val (filteredDataset, keepInvalid) = {
      if (getHandleInvalid == Bucketizer.SKIP_INVALID) {
        // "skip" NaN option is set, will filter out NaN values in the dataset
        (dataset.na.drop(inputColumns).toDF(), false)
      } else {
        (dataset.toDF(), getHandleInvalid == Bucketizer.KEEP_INVALID)
      }
    }

    val seqOfSplits = if (isSet(inputCols)) {
      $(splitsArray).toSeq
    } else {
      Seq($(splits))
    }

    val bucketizers: Seq[UserDefinedFunction] = seqOfSplits.zipWithIndex.map { case (splits, idx) =>
      udf { (feature: Double) =>
        Bucketizer.binarySearchForBuckets(splits, feature, keepInvalid)
      }.withName(s"bucketizer_$idx")
    }


    val newCols = inputColumns.zipWithIndex.map { case (inputCol, idx) =>
      bucketizers(idx)(filteredDataset(inputCol).cast(DoubleType))
    }
    val metadata = outputColumns.map { col =>
      transformedSchema(col).metadata
    }
    filteredDataset.withColumns(outputColumns, newCols, metadata)
  }

  private def prepOutputField(splits: Array[Double], outputCol: String): StructField = {
    val buckets = splits.sliding(2).map(bucket => bucket.mkString(", ")).toArray
    val attr = new NominalAttribute(name = Some(outputCol), isOrdinal = Some(true),
      values = Some(buckets))
    attr.toStructField()
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol, splits),
      Seq(outputCols, splitsArray))

    if (isSet(inputCols)) {
      require(getInputCols.length == getOutputCols.length &&
        getInputCols.length == getSplitsArray.length, s"Bucketizer $this has mismatched Params " +
        s"for multi-column transform.  Params (inputCols, outputCols, splitsArray) should have " +
        s"equal lengths, but they have different lengths: " +
        s"(${getInputCols.length}, ${getOutputCols.length}, ${getSplitsArray.length}).")

      var transformedSchema = schema
      $(inputCols).zip($(outputCols)).zipWithIndex.foreach { case ((inputCol, outputCol), idx) =>
        SchemaUtils.checkNumericType(transformedSchema, inputCol)
        transformedSchema = SchemaUtils.appendColumn(transformedSchema,
          prepOutputField($(splitsArray)(idx), outputCol))
      }
      transformedSchema
    } else {
      SchemaUtils.checkNumericType(schema, $(inputCol))
      SchemaUtils.appendColumn(schema, prepOutputField($(splits), $(outputCol)))
    }
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Bucketizer = {
    defaultCopy[Bucketizer](extra).setParent(parent)
  }
}

@Since("1.6.0")
object Bucketizer extends DefaultParamsReadable[Bucketizer] {

  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)

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
   * Check each splits in the splits array.
   */
  private[feature] def checkSplitsArray(splitsArray: Array[Array[Double]]): Boolean = {
    splitsArray.forall(checkSplits(_))
  }

  /**
   * Binary searching in several buckets to place each data point.
   * @param splits array of split points
   * @param feature data point
   * @param keepInvalid NaN flag.
   *                    Set "true" to make an extra bucket for NaN values;
   *                    Set "false" to report an error for NaN values
   * @return bucket for each data point
   * @throws SparkException if a feature is < splits.head or > splits.last
   */

  private[feature] def binarySearchForBuckets(
      splits: Array[Double],
      feature: Double,
      keepInvalid: Boolean): Double = {
    if (feature.isNaN) {
      if (keepInvalid) {
        splits.length - 1
      } else {
        throw new SparkException("Bucketizer encountered NaN value. To handle or skip NaNs," +
          " try setting Bucketizer.handleInvalid.")
      }
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

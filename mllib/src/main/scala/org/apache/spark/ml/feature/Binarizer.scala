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

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.{SparkException, SparkIllegalArgumentException}
import org.apache.spark.annotation.Since
import org.apache.spark.internal.{LogKeys, MDC}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Binarize a column of continuous features given a threshold.
 *
 * Since 3.0.0,
 * `Binarize` can map multiple columns at once by setting the `inputCols` parameter. Note that
 * when both the `inputCol` and `inputCols` parameters are set, an Exception will be thrown. The
 * `threshold` parameter is used for single column usage, and `thresholds` is for multiple
 * columns.
 */
@Since("1.4.0")
final class Binarizer @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Transformer with HasThreshold with HasThresholds with HasInputCol with HasOutputCol
    with HasInputCols with HasOutputCols with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("binarizer"))

  /**
   * Param for threshold used to binarize continuous features.
   * The features greater than the threshold, will be binarized to 1.0.
   * The features equal to or less than the threshold, will be binarized to 0.0.
   * Default: 0.0
   * @group param
   */
  @Since("1.4.0")
  override val threshold: DoubleParam =
    new DoubleParam(this, "threshold", "threshold used to binarize continuous features")

  /** @group setParam */
  @Since("1.4.0")
  def setThreshold(value: Double): this.type = set(threshold, value)

  setDefault(threshold -> 0.0)

  /**
   * Array of threshold used to binarize continuous features.
   * This is for multiple columns input. If transforming multiple columns and thresholds is
   * not set, but threshold is set, then threshold will be applied across all columns.
   *
   * @group param
   */
  @Since("3.0.0")
  override val thresholds: DoubleArrayParam = new DoubleArrayParam(this, "thresholds", "Array of " +
    "threshold used to binarize continuous features. This is for multiple columns input. " +
    "If transforming multiple columns and thresholds is not set, but threshold is set, " +
    "then threshold will be applied across all columns.")

  /** @group setParam */
  @Since("3.0.0")
  def setThresholds(value: Array[Double]): this.type = set(thresholds, value)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("3.0.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val (inputColNames, outputColNames, tds) =
      if (isSet(inputCols)) {
        if (isSet(thresholds)) {
          ($(inputCols).toImmutableArraySeq,
            $(outputCols).toImmutableArraySeq, $(thresholds).toImmutableArraySeq)
        } else {
          ($(inputCols).toImmutableArraySeq,
            $(outputCols).toImmutableArraySeq, Seq.fill($(inputCols).length)($(threshold)))
        }
      } else {
        (Seq($(inputCol)), Seq($(outputCol)), Seq($(threshold)))
      }

    val mappedOutputCols = inputColNames.zip(tds).map { case (colName, td) =>
      SchemaUtils.getSchemaField(dataset.schema, colName).dataType match {
        case DoubleType =>
          when(!col(colName).isNaN && col(colName) > td, lit(1.0))
            .otherwise(lit(0.0))

        case _: VectorUDT if td >= 0 =>
          udf { vector: Vector =>
            val indices = ArrayBuilder.make[Int]
            val values = ArrayBuilder.make[Double]
            vector.foreachNonZero { (index, value) =>
              if (value > td) {
                indices += index
                values += 1.0
              }
            }

            val idxArray = indices.result()
            val valArray = values.result()
            Vectors.sparse(vector.size, idxArray, valArray)
              .compressedWithNNZ(idxArray.length)
          }.apply(col(colName))

        case _: VectorUDT if td < 0 =>
          logWarning(log"Binarization operations on sparse dataset with negative threshold " +
            log"${MDC(LogKeys.THRESHOLD, td)} will build a dense output, so take care when " +
            log"applying to sparse input.")
          udf { vector: Vector =>
            val values = Array.fill(vector.size)(1.0)
            var nnz = vector.size
            vector.foreachNonZero { (index, value) =>
              if (value <= td) {
                values(index) = 0.0
                nnz -= 1
              }
            }

            Vectors.dense(values).compressedWithNNZ(nnz)
          }.apply(col(colName))
      }
    }

    val outputMetadata = outputColNames.map(outputSchema(_).metadata)
    dataset.withColumns(outputColNames, mappedOutputCols, outputMetadata)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol),
      Seq(outputCols))

    if (isSet(inputCol)) {
      require(!isSet(thresholds),
        s"thresholds can't be set for single-column Binarizer.")
    }

    if (isSet(inputCols)) {
      require(getInputCols.length == getOutputCols.length,
        s"Binarizer $this has mismatched Params " +
          s"for multi-column transform. Params (inputCols, outputCols) should have " +
          s"equal lengths, but they have different lengths: " +
          s"(${getInputCols.length}, ${getOutputCols.length}).")
      if (isSet(thresholds)) {
        require(getInputCols.length == getThresholds.length,
          s"Binarizer $this has mismatched Params " +
            s"for multi-column transform. Params (inputCols, outputCols, thresholds) " +
            s"should have equal lengths, but they have different lengths: " +
            s"(${getInputCols.length}, ${getOutputCols.length}, ${getThresholds.length}).")
        require(!isSet(threshold),
          s"exactly one of threshold, thresholds Params to be set, but both are set." )
      }
    }

    val (inputColNames, outputColNames) = if (isSet(inputCols)) {
      ($(inputCols).toImmutableArraySeq, $(outputCols).toImmutableArraySeq)
    } else {
      (Seq($(inputCol)), Seq($(outputCol)))
    }

    var outputFields = schema.fields
    inputColNames.zip(outputColNames).foreach { case (inputColName, outputColName) =>
      require(!schema.fieldNames.contains(outputColName),
        s"Output column $outputColName already exists.")

      val inputType = try {
        SchemaUtils.getSchemaFieldType(schema, inputColName)
      } catch {
        case e: SparkIllegalArgumentException if e.getCondition == "FIELD_NOT_FOUND" =>
          throw new SparkException(s"Input column $inputColName does not exist.")
        case e: Exception =>
          throw e
      }

      val outputField = inputType match {
        case DoubleType =>
          BinaryAttribute.defaultAttr.withName(outputColName).toStructField()
        case _: VectorUDT =>
          val size = AttributeGroup.fromStructField(schema(inputColName)).size
          if (size < 0) {
            StructField(outputColName, new VectorUDT)
          } else {
            new AttributeGroup(outputColName, numAttributes = size).toStructField()
          }
        case _ =>
          throw new IllegalArgumentException(s"Data type $inputType is not supported.")
      }
      outputFields :+= outputField
    }
    StructType(outputFields)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): Binarizer = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"Binarizer: uid=$uid" +
      get(inputCols).map(c => s", numInputCols=${c.length}").getOrElse("") +
      get(outputCols).map(c => s", numOutputCols=${c.length}").getOrElse("")
  }
}

@Since("1.6.0")
object Binarizer extends DefaultParamsReadable[Binarizer] {

  @Since("1.6.0")
  override def load(path: String): Binarizer = super.load(path)
}

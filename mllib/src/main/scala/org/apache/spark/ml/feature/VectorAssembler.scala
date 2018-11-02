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

import java.util.NoSuchElementException

import scala.collection.mutable
import scala.language.existentials

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute, UnresolvedAttribute}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * A feature transformer that merges multiple columns into a vector column.
 *
 * This requires one pass over the entire dataset. In case we need to infer column lengths from the
 * data we require an additional call to the 'first' Dataset method, see 'handleInvalid' parameter.
 */
@Since("1.4.0")
class VectorAssembler @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Transformer with HasInputCols with HasOutputCol with HasHandleInvalid
    with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("vecAssembler"))

  /** @group setParam */
  @Since("1.4.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("2.4.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /**
   * Param for how to handle invalid data (NULL values). Options are 'skip' (filter out rows with
   * invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN in the
   * output). Column lengths are taken from the size of ML Attribute Group, which can be set using
   * `VectorSizeHint` in a pipeline before `VectorAssembler`. Column lengths can also be inferred
   * from first rows of the data since it is safe to do so but only in case of 'error' or 'skip'.
   * Default: "error"
   * @group param
   */
  @Since("2.4.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    """Param for how to handle invalid data (NULL and NaN values). Options are 'skip' (filter out
      |rows with invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN
      |in the output). Column lengths are taken from the size of ML Attribute Group, which can be
      |set using `VectorSizeHint` in a pipeline before `VectorAssembler`. Column lengths can also
      |be inferred from first rows of the data since it is safe to do so but only in case of 'error'
      |or 'skip'.""".stripMargin.replaceAll("\n", " "),
    ParamValidators.inArray(VectorAssembler.supportedHandleInvalids))

  setDefault(handleInvalid, VectorAssembler.ERROR_INVALID)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    // Schema transformation.
    val schema = dataset.schema

    val vectorCols = $(inputCols).filter { c =>
      schema(c).dataType match {
        case _: VectorUDT => true
        case _ => false
      }
    }
    val vectorColsLengths = VectorAssembler.getLengths(dataset, vectorCols, $(handleInvalid))

    val featureAttributesMap = $(inputCols).map { c =>
      val field = schema(c)
      field.dataType match {
        case DoubleType =>
          val attribute = Attribute.fromStructField(field)
          attribute match {
            case UnresolvedAttribute =>
              Seq(NumericAttribute.defaultAttr.withName(c))
            case _ =>
              Seq(attribute.withName(c))
          }
        case _: NumericType | BooleanType =>
          // If the input column type is a compatible scalar type, assume numeric.
          Seq(NumericAttribute.defaultAttr.withName(c))
        case _: VectorUDT =>
          val attributeGroup = AttributeGroup.fromStructField(field)
          if (attributeGroup.attributes.isDefined) {
            attributeGroup.attributes.get.zipWithIndex.toSeq.map { case (attr, i) =>
              if (attr.name.isDefined) {
                // TODO: Define a rigorous naming scheme.
                attr.withName(c + "_" + attr.name.get)
              } else {
                attr.withName(c + "_" + i)
              }
            }
          } else {
            // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
            // from metadata, check the first row.
            (0 until vectorColsLengths(c)).map { i =>
              NumericAttribute.defaultAttr.withName(c + "_" + i)
            }
          }
        case otherType =>
          throw new SparkException(s"VectorAssembler does not support the $otherType type")
      }
    }
    val featureAttributes = featureAttributesMap.flatten[Attribute].toArray
    val lengths = featureAttributesMap.map(a => a.length).toArray
    val metadata = new AttributeGroup($(outputCol), featureAttributes).toMetadata()
    val (filteredDataset, keepInvalid) = $(handleInvalid) match {
      case VectorAssembler.SKIP_INVALID => (dataset.na.drop($(inputCols)), false)
      case VectorAssembler.KEEP_INVALID => (dataset, true)
      case VectorAssembler.ERROR_INVALID => (dataset, false)
    }
    // Data transformation.
    val assembleFunc = udf { r: Row =>
      VectorAssembler.assemble(lengths, keepInvalid)(r.toSeq: _*)
    }.asNondeterministic()
    val args = $(inputCols).map { c =>
      schema(c).dataType match {
        case DoubleType => dataset(c)
        case _: VectorUDT => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType).as(s"${c}_double_$uid")
      }
    }

    filteredDataset.select(col("*"), assembleFunc(struct(args: _*)).as($(outputCol), metadata))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColName = $(outputCol)
    val incorrectColumns = inputColNames.flatMap { name =>
      schema(name).dataType match {
        case _: NumericType | BooleanType => None
        case t if t.isInstanceOf[VectorUDT] => None
        case other => Some(s"Data type ${other.catalogString} of column $name is not supported.")
      }
    }
    if (incorrectColumns.nonEmpty) {
      throw new IllegalArgumentException(incorrectColumns.mkString("\n"))
    }
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, new VectorUDT, true))
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): VectorAssembler = defaultCopy(extra)
}

@Since("1.6.0")
object VectorAssembler extends DefaultParamsReadable[VectorAssembler] {

  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)

  /**
   * Infers lengths of vector columns from the first row of the dataset
   * @param dataset the dataset
   * @param columns name of vector columns whose lengths need to be inferred
   * @return map of column names to lengths
   */
  private[feature] def getVectorLengthsFromFirstRow(
      dataset: Dataset[_],
      columns: Seq[String]): Map[String, Int] = {
    try {
      val first_row = dataset.toDF().select(columns.map(col): _*).first()
      columns.zip(first_row.toSeq).map {
        case (c, x) => c -> x.asInstanceOf[Vector].size
      }.toMap
    } catch {
      case e: NullPointerException => throw new NullPointerException(
        s"""Encountered null value while inferring lengths from the first row. Consider using
           |VectorSizeHint to add metadata for columns: ${columns.mkString("[", ", ", "]")}. """
          .stripMargin.replaceAll("\n", " ") + e.toString)
      case e: NoSuchElementException => throw new NoSuchElementException(
        s"""Encountered empty dataframe while inferring lengths from the first row. Consider using
           |VectorSizeHint to add metadata for columns: ${columns.mkString("[", ", ", "]")}. """
          .stripMargin.replaceAll("\n", " ") + e.toString)
    }
  }

  private[feature] def getLengths(
      dataset: Dataset[_],
      columns: Seq[String],
      handleInvalid: String): Map[String, Int] = {
    val groupSizes = columns.map { c =>
      c -> AttributeGroup.fromStructField(dataset.schema(c)).size
    }.toMap
    val missingColumns = groupSizes.filter(_._2 == -1).keys.toSeq
    val firstSizes = (missingColumns.nonEmpty, handleInvalid) match {
      case (true, VectorAssembler.ERROR_INVALID) =>
        getVectorLengthsFromFirstRow(dataset, missingColumns)
      case (true, VectorAssembler.SKIP_INVALID) =>
        getVectorLengthsFromFirstRow(dataset.na.drop(missingColumns), missingColumns)
      case (true, VectorAssembler.KEEP_INVALID) => throw new RuntimeException(
        s"""Can not infer column lengths with handleInvalid = "keep". Consider using VectorSizeHint
           |to add metadata for columns: ${columns.mkString("[", ", ", "]")}."""
          .stripMargin.replaceAll("\n", " "))
      case (_, _) => Map.empty
    }
    groupSizes ++ firstSizes
  }


  @Since("1.6.0")
  override def load(path: String): VectorAssembler = super.load(path)

  /**
   * Returns a function that has the required information to assemble each row.
   * @param lengths an array of lengths of input columns, whose size should be equal to the number
   *                of cells in the row (vv)
   * @param keepInvalid indicate whether to throw an error or not on seeing a null in the rows
   * @return  a udf that can be applied on each row
   */
  private[feature] def assemble(lengths: Array[Int], keepInvalid: Boolean)(vv: Any*): Vector = {
    val indices = mutable.ArrayBuilder.make[Int]
    val values = mutable.ArrayBuilder.make[Double]
    var featureIndex = 0

    var inputColumnIndex = 0
    vv.foreach {
      case v: Double =>
        if (v.isNaN && !keepInvalid) {
          throw new SparkException(
            s"""Encountered NaN while assembling a row with handleInvalid = "error". Consider
               |removing NaNs from dataset or using handleInvalid = "keep" or "skip"."""
              .stripMargin)
        } else if (v != 0.0) {
          indices += featureIndex
          values += v
        }
        inputColumnIndex += 1
        featureIndex += 1
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += featureIndex + i
            values += v
          }
        }
        inputColumnIndex += 1
        featureIndex += vec.size
      case null =>
        if (keepInvalid) {
          val length: Int = lengths(inputColumnIndex)
          Array.range(0, length).foreach { i =>
            indices += featureIndex + i
            values += Double.NaN
          }
          inputColumnIndex += 1
          featureIndex += length
        } else {
          throw new SparkException(
            s"""Encountered null while assembling a row with handleInvalid = "keep". Consider
               |removing nulls from dataset or using handleInvalid = "keep" or "skip"."""
              .stripMargin)
        }
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }
    Vectors.sparse(featureIndex, indices.result(), values.result()).compressed
  }
}

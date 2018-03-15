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

import scala.collection.mutable.ArrayBuilder
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
  @Since("1.6.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  /**
   * Param for how to handle invalid data (NULL values). Options are 'skip' (filter out rows with
   * invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN in the
   * output).
   * Default: "error"
   * @group param
   */
  @Since("1.6.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "Hhow to handle invalid data (NULL values). Options are 'skip' (filter out rows with " +
      "invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN " +
      "in the * output).", ParamValidators.inArray(StringIndexer.supportedHandleInvalids))

  setDefault(handleInvalid, StringIndexer.ERROR_INVALID)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    // Schema transformation.
    val schema = dataset.schema
    val colsMissingNumAttrs = $(inputCols).filter { c =>
      val field = schema(c)
      field.dataType match {
        case _: VectorUDT => AttributeGroup.fromStructField(field).numAttributes.isEmpty
        case _ => false
      }
    }
    print("colsMissingNumAttrs: " + colsMissingNumAttrs.mkString(",") + "\n")
    if (dataset.isStreaming && colsMissingNumAttrs.nonEmpty) {
      throw new RuntimeException(
        s"""
           |VectorAssembler cannot dynamically determine the size of vectors for streaming data.
           |Consider applying VectorSizeHint to ${colsMissingNumAttrs.mkString("[", ", ", "]")}
           |so that this transformer can be used to transform streaming inputs.
           """.stripMargin)
    }
    val missingVectorSizes = colsMissingNumAttrs.map { c =>
        $(handleInvalid) match {
          case StringIndexer.ERROR_INVALID => c -> 0
          case StringIndexer.SKIP_INVALID => c -> 0
          case StringIndexer.KEEP_INVALID =>
            try {
              c -> dataset.select(c).na.drop().first.getAs[Vector](0).size
            } catch {
              case _: NoSuchElementException =>
                throw new RuntimeException(
                  s"""
                     |VectorAssembler cannot determine the size of empty vectors. Consider applying
                     |VectorSizeHint to ${colsMissingNumAttrs.mkString("[", ", ", "]")} so that this
                     |transformer can be used to transform empty columns.
                   """.stripMargin)
            }
        }
    }.toMap
    val lengths = $(inputCols).map { c =>
      val field = schema(c)
      field.dataType match {
        case _: NumericType | BooleanType => c -> 1  // DoubleType is also NumericType
        case _: VectorUDT =>
          c -> AttributeGroup.fromStructField(field).numAttributes.getOrElse(missingVectorSizes(c))
      }
    }.toMap
    val attrs = $(inputCols).flatMap { c =>
      val field = schema(c)
      field.dataType match {
        case DoubleType =>
          val attr = Attribute.fromStructField(field)
          // If the input column doesn't have ML attribute, assume numeric.
          if (attr == UnresolvedAttribute) {
            Some(NumericAttribute.defaultAttr.withName(c))
          } else {
            Some(attr.withName(c))
          }
        case _: NumericType | BooleanType =>
          // If the input column type is a compatible scalar type, assume numeric.
          Some(NumericAttribute.defaultAttr.withName(c))
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          if (group.attributes.isDefined) {
            // If attributes are defined, copy them with updated names.
            group.attributes.get.zipWithIndex.map { case (attr, i) =>
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
            val numAttrs = lengths(c)
            // group.numAttributes.getOrElse(missingVectorSizes(c))
            Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName(c + "_" + i))
          }
        case otherType =>
          throw new SparkException(s"VectorAssembler does not support the $otherType type")
      }
    }
    val metadata = new AttributeGroup($(outputCol), attrs).toMetadata()
    val (filteredDataset, keepInvalid) = $(handleInvalid) match {
      case StringIndexer.SKIP_INVALID => (dataset.na.drop("any", $(inputCols)), false)
      case StringIndexer.KEEP_INVALID => (dataset, true)
      case StringIndexer.ERROR_INVALID => (dataset, false)
    }
    // Data transformation.
    val assembleFunc = udf { r: Row =>
      VectorAssembler.assemble( $(inputCols).map(c => lengths(c)), keepInvalid)(r.toSeq: _*)
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
        case other => Some(s"Data type $other of column $name is not supported.")
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

  @Since("1.6.0")
  override def load(path: String): VectorAssembler = super.load(path)

  private[feature] def assemble(lengths: Array[Int], keepInvalid: Boolean)(vv: Any*): Vector = {
    val indices = ArrayBuilder.make[Int]
    val values = ArrayBuilder.make[Double]
    var featureIndex = 0

    var inputColumnIndex = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
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
        keepInvalid match {
          case false => throw new SparkException("Values to assemble cannot be null.")
          case true =>
            val length: Int = lengths(inputColumnIndex)
            Array.range(0, length).foreach { case (i) =>
              indices += featureIndex + i
              values += Double.NaN
            }
            inputColumnIndex += 1
            featureIndex += length
        }
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }
    Vectors.sparse(featureIndex, indices.result(), values.result()).compressed
  }
}

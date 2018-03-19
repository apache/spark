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
  @Since("2.4.0")
  override val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data (NULL values). Options are 'skip' (filter out rows with " +
      "invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN " +
      "in the output).", ParamValidators.inArray(VectorAssembler.supportedHandleInvalids))

  setDefault(handleInvalid, VectorAssembler.ERROR_INVALID)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    // Schema transformation.
    val schema = dataset.schema

    val vectorCols = $(inputCols).toSeq.filter { c =>
      schema(c).dataType match {
        case _: VectorUDT => true
        case _ => false
      }
    }
    val vectorColsLengths = VectorAssembler.getLengths(dataset, vectorCols, $(handleInvalid))

    val featureAttributesMap = $(inputCols).toSeq.map { c =>
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
        case _ : NumericType | BooleanType =>
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
    val featureAttributes = featureAttributesMap.flatten[Attribute]
    val lengths = featureAttributesMap.map(a => a.length)
    val metadata = new AttributeGroup($(outputCol), featureAttributes.toArray).toMetadata()
    val (filteredDataset, keepInvalid) = $(handleInvalid) match {
      case StringIndexer.SKIP_INVALID => (dataset.na.drop($(inputCols)), false)
      case StringIndexer.KEEP_INVALID => (dataset, true)
      case StringIndexer.ERROR_INVALID => (dataset, false)
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


  private[feature] def getLengthsFromFirst(dataset: Dataset[_],
                                           columns: Seq[String]): Map[String, Int] = {
    try {
      val first_row = dataset.toDF.select(columns.map(col): _*).first
      columns.zip(first_row.toSeq).map {
        case (c, x) => c -> x.asInstanceOf[Vector].size
      }.toMap
    } catch {
      case e: NullPointerException => throw new NullPointerException(
        "Saw null value on the first row: " + e.toString)
      case e: NoSuchElementException => throw new NoSuchElementException(
        "Cannot infer vector size from all empty DataFrame" + e.toString)
    }
  }

  private[feature] def getLengths(dataset: Dataset[_], columns: Seq[String],
                                  handleInvalid: String) = {
    val group_sizes = columns.map { c =>
      c -> AttributeGroup.fromStructField(dataset.schema(c)).size
    }.toMap
    val missing_columns: Seq[String] = group_sizes.filter(_._2 == -1).keys.toSeq
    val first_sizes: Map[String, Int] = (missing_columns.nonEmpty, handleInvalid) match {
      case (true, VectorAssembler.ERROR_INVALID) =>
        getLengthsFromFirst(dataset, missing_columns)
      case (true, VectorAssembler.SKIP_INVALID) =>
        getLengthsFromFirst(dataset.na.drop, missing_columns)
      case (true, VectorAssembler.KEEP_INVALID) => throw new RuntimeException(
        "Consider using VectorSizeHint for columns: " + missing_columns.mkString("[", ",", "]"))
      case (_, _) => Map.empty
    }
    group_sizes ++ first_sizes
  }


  @Since("1.6.0")
  override def load(path: String): VectorAssembler = super.load(path)

  private[feature] def assemble(lengths: Seq[Int], keepInvalid: Boolean)(vv: Any*): Vector = {
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

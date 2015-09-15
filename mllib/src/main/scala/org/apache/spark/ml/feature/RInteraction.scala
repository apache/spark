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

import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}

import org.apache.spark.SparkException
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * :: Experimental ::
 * Implements the transforms required for R-style feature interactions. In summary, once fitted to
 * a dataset, this transformer jointly one-hot encodes all factor input columns, then scales
 * the encoded vector by all numeric input columns. If only numeric columns are specified, the
 * output column will be a one-length vector containing their product. During one-hot encoding,
 * the last category will be preserved unless the interaction is trivial.
 *
 * See https://stat.ethz.ch/R-manual/R-devel/library/base/html/formula.html for more
 * information about factor interactions in R formulae.
 */
@Experimental
class RInteraction(override val uid: String) extends Estimator[PipelineModel]
  with HasInputCols with HasOutputCol {

  def this() = this(Identifiable.randomUID("interaction"))

  /** @group setParam */
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): PipelineModel = {
    checkParams()
    val encoderStages = ArrayBuffer[PipelineStage]()
    val tempColumns = ArrayBuffer[String]()
    val (factorCols, nonFactorCols) = $(inputCols)
      .partition(input => dataset.schema(input).dataType == StringType)

    val encodedFactors: Option[String] =
      if (factorCols.length > 0) {
        val indexedCols = factorCols.map { input =>
          val output = input + "_idx_" + uid
          encoderStages += new StringIndexer()
            .setInputCol(input)
            .setOutputCol(output)
          tempColumns += output
          output
        }
        val combinedIndex = "combined_idx_" + uid
        tempColumns += combinedIndex
        val encodedCol = if (nonFactorCols.length > 0) {
          "factors_" + uid
        } else {
          $(outputCol)
        }
        encoderStages += new IndexCombiner(indexedCols, factorCols, combinedIndex)
        encoderStages += {
          val encoder = new OneHotEncoder()
            .setInputCol(combinedIndex)
            .setOutputCol(encodedCol)
          if ($(inputCols).length > 1) {
            encoder.setDropLast(false)  // R includes all columns for interactions.
          }
          encoder
        }
        Some(encodedCol)
      } else {
        None
      }

    if (nonFactorCols.length > 0) {
      encoderStages += new NumericInteraction(nonFactorCols, encodedFactors, $(outputCol))
      if (encodedFactors.isDefined) {
        tempColumns += encodedFactors.get
      }
    }

    encoderStages += new ColumnPruner(tempColumns.toSet)
    new Pipeline(uid)
      .setStages(encoderStages.toArray)
      .fit(dataset)
      .setParent(this)
  }

  // optimistic schema; does not contain any ML attributes
  override def transformSchema(schema: StructType): StructType = {
    checkParams()
    if ($(inputCols).exists(col => schema(col).dataType == StringType)) {
      StructType(schema.fields :+ StructField($(outputCol), new VectorUDT, true))
    } else {
      StructType(schema.fields :+ StructField($(outputCol), DoubleType, true))
    }
  }

  override def copy(extra: ParamMap): RInteraction = defaultCopy(extra)

  private def checkParams(): Unit = {
    require(get(inputCols).isDefined, "Input cols must be defined first.")
    require(get(outputCol).isDefined, "Output col must be defined first.")
    require($(inputCols).length > 0, "Input cols must have non-zero length.")
    require($(inputCols).distinct.length == $(inputCols).length, "Input cols must be distinct.")
  }
}

class Interaction(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol {

  def this() = this(Identifiable.randomUID("interaction"))

  /** @group setParam */
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    checkParams()
    if ($(inputCols).exists(col => schema(col).dataType == StringType)) {
      StructType(schema.fields :+ StructField($(outputCol), new VectorUDT, true))
    } else {
      StructType(schema.fields :+ StructField($(outputCol), DoubleType, true))
    }
  }

  override def transform(dataset: DataFrame): DataFrame = {
    checkParams()

    val numValues: Array[Array[Int]] = $(inputCols).map { col =>
      val field = dataset.schema(col)
      field.dataType match {
        case _: NumericType | BooleanType => Array[Int]()
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          assert(group.attributes.isDefined, "TODO")
          val cardinalities = group.attributes.get.map {
            case nominal: NominalAttribute => nominal.getNumValues.get
            case _ => 0
          }
          cardinalities.toArray
      }
    }
    assert(numValues.length == $(inputCols).length)

    def getIterator(fieldIndex: Int, v: Any) = v match {
      case d: Double =>
        Vectors.dense(d)
      case vec: Vector =>
        var indices = ArrayBuilder.make[Int]
        var values = ArrayBuilder.make[Double]
        var cur = 0
        vec.foreachActive { case (i, v) =>
          val numColumns = numValues(fieldIndex)(i)
          if (numColumns > 0) {
            // One-hot encode the field. TODO(ekl) support drop-last?
            indices += cur + v.toInt
            values += 1.0
            cur += numColumns
          } else {
            // Copy the field verbatim.
            indices += cur
            values += v
            cur += 1
          }
        }
        Vectors.sparse(cur, indices.result(), values.result())
      case null =>
        throw new SparkException("Values to interact cannot be null.")
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }

    def interact(vv: Any*): Vector = {
      var indices = ArrayBuilder.make[Int]
      var values = ArrayBuilder.make[Double]
      var size = 1
      indices += 0
      values += 1.0
      var fieldIndex = 0
      while (fieldIndex < vv.length) {
        val prevIndices = indices.result()
        val prevValues = values.result()
        val prevSize = size
        val currentVector = getIterator(fieldIndex, vv(fieldIndex))
        indices = ArrayBuilder.make[Int]
        values = ArrayBuilder.make[Double]
        size *= currentVector.size
        currentVector.foreachActive { (i, a) =>
          var j = 0
          while (j < prevIndices.length) {
            indices += prevIndices(j) + i * prevSize
            values += prevValues(j) * a
            j += 1
          }
        }
        fieldIndex += 1
      }
      Vectors.sparse(size, indices.result(), values.result()).compressed
    }

    val interactFunc = udf { r: Row =>
      interact(r.toSeq: _*)
    }

    val args = $(inputCols).map { c =>
      dataset.schema(c).dataType match {
        case DoubleType => dataset(c)
        case _: VectorUDT => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType)
      }
    }

    dataset.select(
      col("*"),
      interactFunc(struct(args: _*)).as($(outputCol)))
  }

  override def copy(extra: ParamMap): Interaction = defaultCopy(extra)

  private def checkParams(): Unit = {
    require(get(inputCols).isDefined, "Input cols must be defined first.")
    require(get(outputCol).isDefined, "Output col must be defined first.")
    require($(inputCols).length > 0, "Input cols must have non-zero length.")
    require($(inputCols).distinct.length == $(inputCols).length, "Input cols must be distinct.")
  }
}

/**
 * Computes the joint index of multiple string-indexed columns such that the combined index
 * covers the cartesian product of column values.
 */
private class IndexCombiner(
    inputCols: Array[String], attrNames: Array[String], outputCol: String)
  extends Transformer {

  override val uid = Identifiable.randomUID("indexCombiner")

  override def transform(dataset: DataFrame): DataFrame = {
    val inputMetadata = inputCols.map(col =>
      Attribute.fromStructField(dataset.schema(col)).asInstanceOf[NominalAttribute])
    val cardinalities = inputMetadata.map(_.values.get.length)
    val combiner = udf { values: Seq[Double] =>
      var offset = 1
      var res = 0.0
      var i = 0
      while (i < values.length) {
        res += values(i) * offset
        offset *= cardinalities(i)
        i += 1
      }
      res
    }
    val metadata = NominalAttribute.defaultAttr
      .withName(outputCol)
      .withValues(generateAttrNames(inputMetadata, attrNames))
      .toMetadata()
    dataset.select(
      col("*"),
      combiner(array(inputCols.map(dataset(_)): _*)).as(outputCol, metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol, DoubleType, true))
  }

  override def copy(extra: ParamMap): IndexCombiner = defaultCopy(extra)

  private def generateAttrNames(
      attrs: Array[NominalAttribute], names: Array[String]): Array[String] = {
    val colName = names.head
    val attrNames = attrs.head.values.get.map(colName + "_" + _)
    if (attrs.length <= 1) {
      attrNames
    } else {
      generateAttrNames(attrs.tail, names.tail).flatMap { rest =>
        attrNames.map(n => n + ":" + rest)
      }
    }
  }
}

/**
 * Scales the input vector column by the product of the input numeric columns. If no vector column
 * is specified, the output is just the product of the numeric columns.
 */
private class NumericInteraction(
    inputCols: Array[String], vectorCol: Option[String], outputCol: String)
  extends Transformer {

  override val uid = Identifiable.randomUID("numericInteraction")

  override def transform(dataset: DataFrame): DataFrame = {
    if (vectorCol.isDefined) {
      val scale = udf { (vec: Vector, scalars: Seq[Double]) =>
        var x = 1.0
        var i = 0
        while (i < scalars.length) {
          x *= scalars(i)
          i += 1
        }
        val indices = ArrayBuilder.make[Int]
        val values = ArrayBuilder.make[Double]
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += i
            values += v * x
          }
        }
        Vectors.sparse(vec.size, indices.result(), values.result()).compressed
      }
      val group = AttributeGroup.fromStructField(dataset.schema(vectorCol.get))
      val attrs = group.attributes.get.map { attr =>
        attr.withName(attr.name.get + ":" + inputCols.mkString(":"))
      }
      val metadata = new AttributeGroup(outputCol, attrs).toMetadata()
      dataset.select(
        col("*"),
        scale(
          col(vectorCol.get),
          array(inputCols.map(dataset(_).cast(DoubleType)): _*)).as(outputCol, metadata))
    } else {
      val multiply = udf { values: Seq[Double] =>
        var x = 1.0
        var i = 0
        while (i < values.length) {
          x *= values(i)
          i += 1
        }
        x
      }
      val metadata = NumericAttribute.defaultAttr
        .withName(inputCols.mkString(":"))
        .toMetadata()
      dataset.select(
        col("*"),
        multiply(array(inputCols.map(dataset(_).cast(DoubleType)): _*)).as(outputCol, metadata))
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    if (vectorCol.isDefined) {
      StructType(schema.fields :+ StructField(outputCol, new VectorUDT, true))
    } else {
      StructType(schema.fields :+ StructField(outputCol, DoubleType, true))
    }
  }

  override def copy(extra: ParamMap): NumericInteraction = defaultCopy(extra)
}

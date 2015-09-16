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
 * Implements the transforms required for R-style feature interactions. This transformer takes in
 * Double and Vector columns and outputs a flattened vector of interactions. To handle interaction,
 * we first one-hot encode nominal columns. Then, a vector of all their cross-products is
 * produced.
 *
 * For example, given the inputs Double(2), Vector(3, 4), the result would be Vector(6, 8) if
 * all columns were numeric. If the first was nominal with three different values, the result
 * would then be Vector(0, 0, 3, 0, 0, 4).
 *
 * See https://stat.ethz.ch/R-manual/R-devel/library/base/html/formula.html for more
 * information about interactions in R formulae.
 */
@Experimental
class Interaction(override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol {

  def this() = this(Identifiable.randomUID("interaction"))

  /** @group setParam */
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  // optimistic schema; does not contain any ML attributes
  override def transformSchema(schema: StructType): StructType = {
    checkParams()
    StructType(schema.fields :+ StructField($(outputCol), new VectorUDT, true))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    checkParams()
    val fieldIterators = getIterators(dataset)

    def interactFunc = udf { row: Row =>
      var indices = ArrayBuilder.make[Int]
      var values = ArrayBuilder.make[Double]
      var size = 1
      indices += 0
      values += 1.0
      var fieldIndex = row.length - 1
      while (fieldIndex >= 0) {
        val prevIndices = indices.result()
        val prevValues = values.result()
        val prevSize = size
        val currentIterator = fieldIterators(fieldIndex)
        indices = ArrayBuilder.make[Int]
        values = ArrayBuilder.make[Double]
        size *= currentIterator.size
        currentIterator.foreachActive(row(fieldIndex), (i, a) => {
          var j = 0
          while (j < prevIndices.length) {
            indices += prevIndices(j) + i * prevSize
            values += prevValues(j) * a
            j += 1
          }
        })
        fieldIndex -= 1
      }
      Vectors.sparse(size, indices.result(), values.result()).compressed
    }

    val args = $(inputCols).map { c =>
      dataset.schema(c).dataType match {
        case DoubleType => dataset(c)
        case _: VectorUDT => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType)
      }
    }
    val attrs = generateAttrs($(inputCols).map(col => dataset.schema(col)))
    dataset.select(
      col("*"),
      interactFunc(struct(args: _*)).as($(outputCol), attrs.toMetadata()))
  }

  private def getIterators(dataset: DataFrame): Array[EncodingIterator] = {
    def getCardinality(attr: Attribute): Int = {
      attr match {
        case nominal: NominalAttribute =>
          nominal.getNumValues.getOrElse(
            throw new SparkException("Nominal fields must have attr numValues defined."))
        case _ =>
          0  // treated as numeric
      }
    }
    $(inputCols).map { col =>
      val field = dataset.schema(col)
      val cardinalities = field.dataType match {
        case _: NumericType | BooleanType =>
          Array(getCardinality(Attribute.fromStructField(field)))
        case _: VectorUDT =>
          val attrs = AttributeGroup.fromStructField(field).attributes.getOrElse(
            throw new SparkException("Vector attributes must be defined for interaction."))
          attrs.map(getCardinality).toArray
      }
      new EncodingIterator(cardinalities)
    }.toArray
  }

  private def generateAttrs(schema: Seq[StructField]): AttributeGroup = {
    var attrs: Seq[Attribute] = Nil
    schema.reverse.map { field =>
      val attrIterator = field.dataType match {
        case _: NumericType | BooleanType =>
          val attr = Attribute.fromStructField(field)
          getAttributesAfterEncoding(None, Seq(attr))
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(field)
          getAttributesAfterEncoding(Some(group.name), group.attributes.get)
      }
      if (attrs.isEmpty) {
        attrs = attrIterator
      } else {
        attrs = attrIterator.flatMap { head =>
          attrs.map { tail =>
            NumericAttribute.defaultAttr.withName(head.name.get + ":" + tail.name.get)
          }
        }
      }
    }
    new AttributeGroup($(outputCol), attrs.toArray)
  }

  private def getAttributesAfterEncoding(
      groupName: Option[String], attrs: Seq[Attribute]): Seq[Attribute] = {
    def format(i: Int, attrName: Option[String], value: Option[String]): String = {
      val parts = Seq(groupName, Some(attrName.getOrElse(i.toString)), value)
      parts.flatten.mkString("_")
    }
    attrs.zipWithIndex.flatMap {
      case (nominal: NominalAttribute, i) =>
        if (nominal.values.isDefined) {
          nominal.values.get.map(
            v => BinaryAttribute.defaultAttr.withName(format(i, nominal.name, Some(v))))
        } else {
          Array.tabulate(nominal.getNumValues.get)(
            j => BinaryAttribute.defaultAttr.withName(format(i, nominal.name, Some(j.toString))))
        }
      case (a: Attribute, i) =>
        Seq(NumericAttribute.defaultAttr.withName(format(i, a.name, None)))
    }
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
 * An iterator over VectorUDT or Double that one-hot encodes nominal fields in the output.
 *
 * @param cardinalities An array defining the cardinality of each vector sub-field, or a single
 *                      value if the field is numeric. Fields with zero cardinality will not be
 *                      one-hot encoded (output verbatim).
 */
// TODO(ekl) support drop-last option like OneHotEncoder does
private[ml] class EncodingIterator(cardinalities: Array[Int]) {
  /** The size of the output vector. */
  val size = cardinalities.map(i => if (i > 0) i else 1).sum

  /**
   * @param value The row to iterate over, either a Double or Vector.
   * @param f The callback to invoke on each non-zero (index, value) output pair.
   */
  def foreachActive(value: Any, f: (Int, Double) => Unit) = value match {
    case d: Double =>
      assert(cardinalities.length == 1)
      val numOutputCols = cardinalities(0)
      if (numOutputCols > 0) {
        assert(
          d >= 0.0 && d == d.toInt && d < numOutputCols,
          s"Values from column must be indices, but got $d.")
        f(d.toInt, 1.0)
      } else {
        f(0, d)
      }
    case vec: Vector =>
      assert(cardinalities.length == vec.size,
        s"Vector column size was ${vec.size}, expected ${cardinalities.length}")
      val dense = vec.toDense
      var i = 0
      var cur = 0
      while (i < dense.size) {
        val numOutputCols = cardinalities(i)
        if (numOutputCols > 0) {
          val x = dense.values(i)
          assert(
            x >= 0.0 && x == x.toInt && x < numOutputCols,
            s"Values from column must be indices, but got $x.")
          f(cur + x.toInt, 1.0)
          cur += numOutputCols
        } else {
          f(cur, dense.values(i))
          cur += 1
        }
        i += 1
      }
    case null =>
      throw new SparkException("Values to interact cannot be null.")
    case o =>
      throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
  }
}

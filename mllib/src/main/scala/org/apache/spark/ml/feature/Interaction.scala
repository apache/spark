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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@Experimental
class Interaction(override val uid: String) extends Estimator[PipelineModel]
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
        encoderStages += new OneHotEncoder()
          .setInputCol(combinedIndex)
          .setOutputCol(encodedCol)
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

  override def copy(extra: ParamMap): Interaction = defaultCopy(extra)

  private def checkParams(): Unit = {
    require(isDefined(inputCols), "Input cols must be defined first.")
    require(isDefined(outputCol), "Output col must be defined first.")
    require($(inputCols).length > 0, "Input cols must have non-zero length.")
    require($(inputCols).distinct.length == $(inputCols).length, "Input cols must be distinct.")
  }
}

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

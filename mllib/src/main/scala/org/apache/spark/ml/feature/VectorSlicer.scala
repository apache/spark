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
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * :: AlphaComponent ::
 * Given either indices or names, it takes a vector column and output a vector column with the
 * specified subset of features. Note that the vector column should contain ML [[Attribute]].
 */
@AlphaComponent
final class VectorSlicer extends Transformer with HasInputCol with HasOutputCol {

  /** Type of feature selector, either an array of int (indices), or an array of string (names). */
  private type FeatureSelector = Either[Array[Int], Array[String]]

  /**
   * Param for selecting features from a single vector.
   * @group Param
   */
  val selectedFeatures = new Param[FeatureSelector](this, "selectedFeatures", "")
  setDefault(selectedFeatures -> Left(Array.empty[Int]))

  /** @group getParam */
  def getSelectedFeatures: FeatureSelector = getOrDefault(selectedFeatures)

  /** @group setParam */
  private def setSelectedFeatures(value: FeatureSelector): this.type = set(selectedFeatures, value)

  /** @group setParam */
  def setSelectedFeatures(value: Array[String]): this.type = setSelectedFeatures(Right(value))

  /** @group setParam */
  def setSelectedFeatures(first: String, others: String*): this.type =
    setSelectedFeatures((Seq(first) ++ others).toArray)

  /** @group setParam */
  def setSelectedFeatures(value: Array[Int]): this.type = setSelectedFeatures(Left(value))

  /** @group setParam */
  def setSelectedFeatures(first: Int, others: Int*): this.type =
    setSelectedFeatures((Seq(first) ++ others).toArray)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Slice a dense vector with an array of indices.
   */
  private def selectColumns(indices: Array[Int], features: DenseVector): Vector = {
    Vectors.dense(indices.map(features.apply))
  }

  /**
   * Slice a sparse vector with a set of indices.
   */
  private def selectColumns(indices: Set[Int], features: SparseVector): Vector = {
    Vectors.sparse(
      indices.size, features.indices.zip(features.values).filter(x => indices.contains(x._1)))
  }

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val selectedIndices = map(selectedFeatures) match {
      case Left(indices) => indices
      case Right(names) =>
        val inputAttr = AttributeGroup.fromStructField(dataset.schema(map(inputCol)))
        names.map(name => inputAttr.getAttr(name).index.get)
    }
    val selectedIndexSet = selectedIndices.toSet
    val slicer = udf { vec: Vector =>
      vec match {
        case features: DenseVector => selectColumns(selectedIndices, features)
        case features: SparseVector => selectColumns(selectedIndexSet, features)
      }
    }
    dataset.withColumn(map(outputCol), slicer(dataset(map(inputCol))))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    SchemaUtils.checkColumnType(schema, map(inputCol), new VectorUDT)
    val inputAttr = AttributeGroup.fromStructField(schema(map(inputCol)))
    val selectedAttrs = map(selectedFeatures) match {
      case Left(indices) =>
        assert(indices.max < inputAttr.size, "Selected feature index exceeds length of vector.")
        indices.map(index => inputAttr.getAttr(index))
      case Right(names) =>
        names.foreach { name =>
          assert(inputAttr.hasAttr(name), "Selected feature does not belong in the vector.")
        }
        names.map(name => inputAttr.getAttr(name))
    }
    if (schema.fieldNames.contains(map(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${map(outputCol)} already exists.")
    }
    val outputAttr = new AttributeGroup(map(outputCol), selectedAttrs)
    val outputFields = schema.fields :+ outputAttr.toStructField()
    StructType(outputFields)
  }
}

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
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{Metadata, StructField, StructType, DataType}
import org.apache.spark.sql.functions._

/**
 * :: AlphaComponent ::
 * Normalize a vector to have unit norm using the given p-norm.
 */
@AlphaComponent
class VectorSlicer extends UnaryTransformer[Vector, Vector, VectorSlicer] {

  private type FeatureSelector = Either[Array[Int], Array[String]]

  val selectedFeatures = new Param[FeatureSelector](this, "selectedFeatures", "")
  setDefault(selectedFeatures -> Left(Array.empty[Int]))

  private val selectedFeaturesIndices = new Param[Array[Int]](this, "selectedFeaturesIndices", "")
  private val selectedFeaturesIndicesSet = new Param[Set[Int]](this, "selectedFeaturesIndicesSet", "")

  def getSelectedFeatures: FeatureSelector = getOrDefault(selectedFeatures)

  private def setSelectedFeatures(value: FeatureSelector): this.type = set(selectedFeatures, value)

  def setSelectedFeatures(value: Array[String]) = setSelectedFeatures(Right(value))

  def setSelectedFeatures(value: String*) = setSelectedFeatures(value.toArray)

  def setSelectedFeatures(value: Array[Int]) = setSelectedFeatures(Left(value))

  def setSelectedFeatures(value: Int*) = setSelectedFeatures(value.toArray)

  private def selectColumns(indices: Array[Int], features: DenseVector): Vector = {
    Vectors.dense(indices.map(features.apply))
  }

  private def selectColumns(indices: Set[Int], features: SparseVector): Vector = {
    Vectors.sparse(
      indices.size, features.indices.zip(features.values).filter(x => indices.contains(x._1)))
  }

  override protected def outputDataType: DataType = new VectorUDT()

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val metadata = extractMetadata(dataset.schema, paramMap)
    val map = extractParamMap(paramMap)
    val slicer = udf { features: Vector => this.createTransformFunc(map)(features) }
    dataset.select(col("*"), slicer(dataset(map(inputCol))).as(map(outputCol), metadata))
  }

  private def extractMetadata(schema: StructType, paramMap: ParamMap): Metadata = {
    ???
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val inputType = schema(map(inputCol)).dataType
    validateInputType(inputType)
    val attr = AttributeGroup.fromStructField(schema(map(inputCol)))
    val names = map(selectedFeatures) match {
      case Left(indices) =>
        assert(indices.max < attr.size, "Selected feature index exceeds length of vector.")
        paramMap.put(selectedFeaturesIndices, indices)
        paramMap.put(selectedFeaturesIndicesSet, indices.toSet)
        indices.map(index => attr.getAttr(index).name)
      case Right(names) =>
        names.foreach { name =>
          assert(attr.hasAttr(name), "Selected feature does not belong in the vector.")
        }
        val indices = names.map(name => attr.indexOf(name))
        paramMap.put(selectedFeaturesIndices, indices)
        paramMap.put(selectedFeaturesIndicesSet, indices.toSet)
        names
    }
    // name will be used to extract attrs, index will be use to index values.
    if (schema.fieldNames.contains(map(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${map(outputCol)} already exists.")
    }

    val outputFields = schema.fields :+
      StructField(map(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  override protected def createTransformFunc(paramMap: ParamMap): (Vector) => Vector = {
    case vec: DenseVector => selectColumns(paramMap(selectedFeaturesIndices), vec)
    case vec: SparseVector => selectColumns(paramMap(selectedFeaturesIndicesSet), vec)
  }
}

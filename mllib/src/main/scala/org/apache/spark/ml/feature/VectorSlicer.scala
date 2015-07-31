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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{IntArrayParam, ParamMap, StringArrayParam}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * :: Experimental ::
 * Given either indices or names, it takes a vector column and output a vector column with the
 * specified subset of features. Note that the vector column should contain ML [[Attribute]].
 */
@Experimental
final class VectorSlicer(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("vectorSlicer"))

  /**
   * An array of indices to select features from a vector column.
   * @group param
   */
  val selectedIndices = new IntArrayParam(this, "selectedIndices",
    "An array of indices to select features from a vector column",
    (x: Array[Int]) => if (x.isEmpty) true else x.min >= 0)

  setDefault(selectedIndices -> Array.empty[Int])

  /** @group getParam */
  def getSelectedIndices: Array[Int] = getOrDefault(selectedIndices)

  /** @group setParam */
  def setSelectedIndices(value: Array[Int]): this.type = set(selectedIndices, value)

  /**
   * An array of feature names to select features from a vector column.
   * @group param
   */
  val selectedNames = new StringArrayParam(this, "selectedNames",
    "An array of feature names to select features from a vector column")

  setDefault(selectedNames -> Array.empty[String])

  /** @group getParam */
  def getSelectedNames: Array[String] = getOrDefault(selectedNames)

  /** @group setParam */
  def setSelectedNames(value: Array[String]) = set(selectedNames, value)

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

  private def getFeatureIndicesFromNames(inputAttr: AttributeGroup): Array[Int] = {
    $(selectedNames).map(name => inputAttr.getAttr(name).index.get)
  }

  private def merge(xs: List[Int], ys: List[Int]): List[Int] = {
    (xs, ys) match {
      case (Nil, ys) => ys
      case (xs, Nil) => xs
      case (x :: xs1, y :: ys1) =>
        if (x < y) x :: merge(xs1, ys)
        else y :: merge(xs, ys1)
    }
  }

  /**
   * Union feature indices from user specified indices and indices that transformed from user
   * specified feature names. After the union process, indices are sorted ASC and any potential
   * duplicates are removed.
   */
  private def unionFeatureIndices(first: Array[Int], second: Array[Int]): Array[Int] = {
    merge(first.sorted.toList, second.sorted.toList).distinct.toArray
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)

    val indices = $(selectedIndices)
    val slicer = udf { vec: Vector =>
      vec match {
        case features: DenseVector => selectColumns(indices, features)
        case features: SparseVector => selectColumns(indices.toSet, features)
      }
    }
    dataset.withColumn($(outputCol), slicer(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    val inputAttr = AttributeGroup.fromStructField(schema($(inputCol)))
    $(selectedNames).foreach { name =>
      assert(inputAttr.hasAttr(name), s"Selected feature $name does not belong in the vector.")
    }
   assert($(selectedIndices).max < inputAttr.size, s"Selected index out of bound.")

    val featureIndicesFromNames = getFeatureIndicesFromNames(inputAttr)
    val unionIndices = unionFeatureIndices($(selectedIndices), featureIndicesFromNames)
    set(selectedIndices, unionIndices)

    val selectedAttrs = unionIndices.map(index => inputAttr.getAttr(index))

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputAttr = new AttributeGroup($(outputCol), selectedAttrs)
    val outputFields = schema.fields :+ outputAttr.toStructField()
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): VectorSlicer = defaultCopy(extra)
}

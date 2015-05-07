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

import org.apache.spark.SparkException
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute}
import org.apache.spark.mllib.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * A one-hot encoder that maps a column of label indices to a column of binary vectors, with
 * at most a single one-value. By default, the binary vector has an element for each category, so
 * with 5 categories, an input value of 2.0 would map to an output vector of
 * (0.0, 0.0, 1.0, 0.0, 0.0). If includeFirst is set to false, the first category is omitted, so the
 * output vector for the previous example would be (0.0, 1.0, 0.0, 0.0) and an input value
 * of 0.0 would map to a vector of all zeros. Including the first category makes the vector columns
 * linearly dependent because they sum up to one.
 */
@AlphaComponent
class OneHotEncoder(override val uid: String)
  extends UnaryTransformer[Double, Vector, OneHotEncoder] with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("oneHot"))

  /**
   * Whether to include a component in the encoded vectors for the first category, defaults to true.
   * @group param
   */
  final val includeFirst: BooleanParam =
    new BooleanParam(this, "includeFirst", "include first category")
  setDefault(includeFirst -> true)

  private var categories: Array[String] = _

  /** @group setParam */
  def setIncludeFirst(value: Boolean): this.type = set(includeFirst, value)

  /** @group setParam */
  override def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  override def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")

    val inputColAttr = Attribute.fromStructField(schema($(inputCol)))
    categories = inputColAttr match {
      case nominal: NominalAttribute =>
        nominal.values.getOrElse((0 until nominal.numValues.get).map(_.toString).toArray)
      case binary: BinaryAttribute => binary.values.getOrElse(Array("0", "1"))
      case _ =>
        throw new SparkException(s"OneHotEncoder input column ${$(inputCol)} is not nominal")
    }

    val attrValues = (if ($(includeFirst)) categories else categories.drop(1)).toArray
    val attr = NominalAttribute.defaultAttr.withName(outputColName).withValues(attrValues)
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  protected override def createTransformFunc(): (Double) => Vector = {
    val first = $(includeFirst)
    val vecLen = if (first) categories.length else categories.length - 1
    val oneValue = Array(1.0)
    val emptyValues = Array[Double]()
    val emptyIndices = Array[Int]()
    label: Double => {
      val values = if (first || label != 0.0) oneValue else emptyValues
      val indices = if (first) {
        Array(label.toInt)
      } else if (label != 0.0) {
        Array(label.toInt - 1)
      } else {
        emptyIndices
      }
      Vectors.sparse(vecLen, indices, values)
    }
  }

  /**
   * Returns the data type of the output column.
   */
  protected def outputDataType: DataType = new VectorUDT
}

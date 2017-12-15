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
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * A feature transformer that transform a vector to sigle fields.
 */
@Since("2.1.0")
class VectorDisassembler @Since("1.4.0")(@Since("1.4.0") override val uid: String)
  extends Transformer with HasInputCol with DefaultParamsWritable {

  @Since("2.1.0")
  def this() = this(Identifiable.randomUID("vecDisassembler"))

  /** @group setParam */
  @Since("2.1.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /**
   * Transforms the input dataset.
   */
  @Since("2.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = dataset.schema
    lazy val first = dataset.toDF().first()

    val field = schema($(inputCol))
    val index = schema.fieldIndex($(inputCol))
    val group =
      field.dataType match {
        case _: VectorUDT => AttributeGroup.fromStructField(field)
        case _ => throw new SparkException(s"VectorDisassembler only support vector inputCol")
      }

    val attrs: Array[Attribute] =
    // add structure fields to metadata
      if (group.attributes.isDefined) {
        // If attributes are defined, copy them with updated names.
        group.attributes.get.zipWithIndex.map { case (attr, i) =>
          if (attr.name.isEmpty) attr.withName($(inputCol) + "_" + i)
          else attr
        }
      } else {
        // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
        // from metadata, check the first row.
        val numAttrs = group.numAttributes.getOrElse(first.getAs[Vector](index).size)
        Array.tabulate(numAttrs)(i => NumericAttribute.defaultAttr.withName($(inputCol) + "_" + i))
      }

    val fieldCols = attrs.zipWithIndex.map(x => {

      val assembleFunc = udf {
        (vector: Vector) =>
          vector(x._2)
      }
      assembleFunc(dataset($(inputCol)).cast(new VectorUDT)).as(x._1.name.get, x._1.toMetadata())
    }
    )

    dataset.select(col("*") +: fieldCols: _*)
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): VectorDisassembler = defaultCopy(extra)

  @Since("2.1.0")
  override def transformSchema(schema: StructType): StructType = {
    val field = schema($(inputCol))
    val group =
      field.dataType match {
        case _: VectorUDT => AttributeGroup.fromStructField(field)
        case _ => throw new SparkException(s"VectorDisassembler only support vector inputCol")
      }

    val attrs: Array[Attribute] =
    // add structure fields to metadata
      if (group.attributes.isDefined) {
        // If attributes are defined, copy them with updated names.
        group.attributes.get.zipWithIndex.map { case (attr, i) =>
          if (attr.name.isEmpty) attr.withName($(inputCol) + "_" + i)
          else attr
        }
      } else {
        // Otherwise, treat all attributes as numeric. If we cannot get the number of attributes
        // from metadata, check the first row.
        Array(NumericAttribute.defaultAttr.withName($(inputCol) + "_" + 0))
      }
    StructType((schema.fields ++ attrs.map(x => x.toStructField())).toSeq)
  }
}

@Since("2.1.0")
object VectorDisassembler extends DefaultParamsReadable[VectorDisassembler] {

  @Since("2.1.0")
  override def load(path: String): VectorDisassembler = super.load(path)
}

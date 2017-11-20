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
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType

/**
 * A feature transformer that adds vector size information to a vector column.
 */
@Experimental
@Since("2.3.0")
class VectorSizeHint @Since("2.3.0") (@Since("2.3.0") override val uid: String)
  extends Transformer with HasInputCol with HasHandleInvalid with DefaultParamsWritable {

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("vectSizeHint"))

  @Since("2.3.0")
  val size = new IntParam(this, "size", "Size of vectors in column.", {s: Int => s >= 0})

  @Since("2.3.0")
  def getSize: Int = getOrDefault(size)

  /** @group setParam */
  @Since("2.3.0")
  def setSize(value: Int): this.type = set(size, value)

  /** @group setParam */
  @Since("2.3.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  @Since("2.3.0")
  override val handleInvalid: Param[String] = new Param[String](
    this,
    "handleInvalid",
    "How to handle invalid vectors in inputCol, (invalid vectors include nulls and vectors with " +
      "the wrong size. The options are `skip` (filter out rows with invalid vectors), `error` " +
      "(throw an error) and `optimistic` (don't check the vector size).",
    ParamValidators.inArray(VectorSizeHint.supportedHandleInvalids))

  /** @group setParam */
  @Since("2.3.0")
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, VectorSizeHint.ERROR_INVALID)

  @Since("2.3.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val localInputCol = getInputCol
    val localSize = getSize
    val localHandleInvalid = getHandleInvalid

    val group = AttributeGroup.fromStructField(dataset.schema(localInputCol))
    if (localHandleInvalid == VectorSizeHint.OPTIMISTIC_INVALID && group.size == localSize) {
      dataset.toDF
    } else {
      val newGroup = if (group.size == localSize) {
        // Pass along any existing metadata about vector.
        group
      } else {
        new AttributeGroup(localInputCol, localSize)
      }

      val newCol: Column = localHandleInvalid match {
        case VectorSizeHint.OPTIMISTIC_INVALID => col(localInputCol)
        case VectorSizeHint.ERROR_INVALID =>
          val checkVectorSizeUDF = udf { vector: Vector =>
            if (vector == null) {
              throw new SparkException(s"Got null vector in VectorSizeHint, set `handleInvalid` " +
                s"to 'skip' to filter invalid rows.")
            }
            if (vector.size != localSize) {
              throw new SparkException(s"VectorSizeHint Expecting a vector of size $localSize but" +
                s" got ${vector.size}")
            }
            vector
          }.asNondeterministic
          checkVectorSizeUDF(col(localInputCol))
        case VectorSizeHint.SKIP_INVALID =>
          val checkVectorSizeUDF = udf { vector: Vector =>
            if (vector != null && vector.size == localSize) {
              vector
            } else {
              null
            }
          }
          checkVectorSizeUDF(col(localInputCol))
      }

      val res = dataset.withColumn(localInputCol, newCol.as(localInputCol, newGroup.toMetadata))
      if (localHandleInvalid == VectorSizeHint.SKIP_INVALID) {
        res.na.drop(Array(localInputCol))
      } else {
        res
      }
    }
  }

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputColType = schema(getInputCol).dataType
    require(
      inputColType.isInstanceOf[VectorUDT],
      s"Input column, $getInputCol must be of Vector type, got $inputColType"
    )
    schema
  }

  @Since("2.3.0")
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

@Experimental
@Since("2.3.0")
object VectorSizeHint extends DefaultParamsReadable[VectorSizeHint] {

  private[feature] val OPTIMISTIC_INVALID = "optimistic"
  private[feature] val ERROR_INVALID = "error"
  private[feature] val SKIP_INVALID = "skip"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(OPTIMISTIC_INVALID, ERROR_INVALID, SKIP_INVALID)

  @Since("2.3.0")
  override def load(path: String): VectorSizeHint = super.load(path)
}

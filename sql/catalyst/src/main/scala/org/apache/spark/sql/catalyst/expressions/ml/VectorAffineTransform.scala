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

package org.apache.spark.sql.catalyst.expressions.ml

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, GenericInternalRow, TernaryExpression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
 * Applies an element-wise affine transformation to SQL struct representations of MLlib vectors:
 * `vector(i) * scale(i) + shift(i)`. This expression is dedicated only for Spark ML and should be
 * used together with `unwrap_udt` and `wrap_udt`.
 */
case class VectorAffineTransform(
    vector: Expression,
    scale: Expression,
    shift: Expression)
  extends TernaryExpression with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def first: Expression = vector
  override def second: Expression = scale
  override def third: Expression = shift

  override def prettyName: String = "ml_vector_affine_transform"

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(3)(VectorAffineTransform.vectorSqlType)

  override def dataType: DataType = VectorAffineTransform.vectorSqlType

  override protected def nullSafeEval(
      vectorInput: Any,
      scaleInput: Any,
      shiftInput: Any): Any = {
    VectorAffineTransform.transform(
      vectorInput.asInstanceOf[InternalRow],
      scaleInput.asInstanceOf[InternalRow],
      shiftInput.asInstanceOf[InternalRow])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val cls = VectorAffineTransform.getClass.getName
    defineCodeGen(ctx, ev, (vectorInput, scaleInput, shiftInput) =>
      s"$cls.MODULE$$.transform($vectorInput, $scaleInput, $shiftInput)")
  }

  override protected def withNewChildrenInternal(
      newVector: Expression,
      newScale: Expression,
      newShift: Expression): VectorAffineTransform = {
    copy(vector = newVector, scale = newScale, shift = newShift)
  }
}

object VectorAffineTransform {
  private val SparseVectorType: Byte = 0
  private val DenseVectorType: Byte = 1

  private[ml] val vectorSqlType = StructType(Array(
    StructField("type", ByteType, nullable = false),
    StructField("size", IntegerType, nullable = true),
    StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
    StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true)))

  private def vectorSize(vector: InternalRow, vectorType: Byte, values: ArrayData): Int = {
    vectorType match {
      case SparseVectorType => vector.getInt(1)
      case DenseVectorType => values.numElements()
      case _ => throw new IllegalArgumentException(s"Unknown vector type $vectorType.")
    }
  }

  private def isZeroVector(vectorType: Byte, values: ArrayData): Boolean = {
    var index = 0
    while (index < values.numElements()) {
      if (values.getDouble(index) != 0.0) return false
      index += 1
    }
    vectorType match {
      case SparseVectorType | DenseVectorType => true
      case _ => throw new IllegalArgumentException(s"Unknown vector type $vectorType.")
    }
  }

  private def sparseResult(
      vector: InternalRow,
      scale: InternalRow,
      size: Int,
      vectorValues: ArrayData,
      scaleType: Byte,
      scaleValues: ArrayData): InternalRow = {
    val vectorIndices = vector.getArray(2)
    val scaleIndices = if (scaleType == SparseVectorType) scale.getArray(2) else null
    val resultValues = new Array[Double](vectorValues.numElements())
    var vectorIndex = 0
    var scaleIndex = 0
    while (vectorIndex < resultValues.length) {
      val featureIndex = vectorIndices.getInt(vectorIndex)
      val scaleValue = if (scaleType == DenseVectorType) {
        scaleValues.getDouble(featureIndex)
      } else {
        while (scaleIndex < scaleValues.numElements() &&
            scaleIndices.getInt(scaleIndex) < featureIndex) {
          scaleIndex += 1
        }
        if (scaleIndex < scaleValues.numElements() &&
            scaleIndices.getInt(scaleIndex) == featureIndex) {
          scaleValues.getDouble(scaleIndex)
        } else {
          0.0
        }
      }
      resultValues(vectorIndex) = vectorValues.getDouble(vectorIndex) * scaleValue
      vectorIndex += 1
    }
    new GenericInternalRow(Array[Any](
      SparseVectorType,
      size,
      vectorIndices,
      UnsafeArrayData.fromPrimitiveArray(resultValues)))
  }

  private def denseResult(
      vector: InternalRow,
      scale: InternalRow,
      shift: InternalRow,
      size: Int,
      vectorType: Byte,
      scaleType: Byte,
      shiftType: Byte,
      vectorValues: ArrayData,
      scaleValues: ArrayData,
      shiftValues: ArrayData): InternalRow = {
    val vectorIndices = if (vectorType == SparseVectorType) vector.getArray(2) else null
    val scaleIndices = if (scaleType == SparseVectorType) scale.getArray(2) else null
    val shiftIndices = if (shiftType == SparseVectorType) shift.getArray(2) else null
    val resultValues = new Array[Double](size)
    var vectorIndex = 0
    var scaleIndex = 0
    var shiftIndex = 0
    var featureIndex = 0
    while (featureIndex < size) {
      val vectorIsActive = vectorType == DenseVectorType ||
        (vectorIndex < vectorValues.numElements() &&
          vectorIndices.getInt(vectorIndex) == featureIndex)
      val vectorValue = if (vectorType == DenseVectorType) {
        vectorValues.getDouble(featureIndex)
      } else if (vectorIsActive) {
        val value = vectorValues.getDouble(vectorIndex)
        vectorIndex += 1
        value
      } else {
        0.0
      }

      val scaleValue = if (scaleType == DenseVectorType) {
        scaleValues.getDouble(featureIndex)
      } else if (scaleIndex < scaleValues.numElements() &&
          scaleIndices.getInt(scaleIndex) == featureIndex) {
        val value = scaleValues.getDouble(scaleIndex)
        scaleIndex += 1
        value
      } else {
        0.0
      }

      val shiftValue = if (shiftType == DenseVectorType) {
        shiftValues.getDouble(featureIndex)
      } else if (shiftIndex < shiftValues.numElements() &&
          shiftIndices.getInt(shiftIndex) == featureIndex) {
        val value = shiftValues.getDouble(shiftIndex)
        shiftIndex += 1
        value
      } else {
        0.0
      }

      resultValues(featureIndex) =
        (if (vectorIsActive) vectorValue * scaleValue else 0.0) + shiftValue
      featureIndex += 1
    }
    new GenericInternalRow(Array[Any](
      DenseVectorType,
      null,
      null,
      UnsafeArrayData.fromPrimitiveArray(resultValues)))
  }

  private[ml] def transform(
      vector: InternalRow,
      scale: InternalRow,
      shift: InternalRow): InternalRow = {
    val vectorType = vector.getByte(0)
    val scaleType = scale.getByte(0)
    val shiftType = shift.getByte(0)
    val vectorValues = vector.getArray(3)
    val scaleValues = scale.getArray(3)
    val shiftValues = shift.getArray(3)
    val size = vectorSize(vector, vectorType, vectorValues)
    val scaleSize = vectorSize(scale, scaleType, scaleValues)
    val shiftSize = vectorSize(shift, shiftType, shiftValues)
    require(size == scaleSize && size == shiftSize,
      "VectorAffineTransform was given vectors with non-matching sizes:" +
        s" vector.size = $size, scale.size = $scaleSize, shift.size = $shiftSize")

    if (vectorType == SparseVectorType && isZeroVector(shiftType, shiftValues)) {
      sparseResult(vector, scale, size, vectorValues, scaleType, scaleValues)
    } else {
      denseResult(
        vector,
        scale,
        shift,
        size,
        vectorType,
        scaleType,
        shiftType,
        vectorValues,
        scaleValues,
        shiftValues)
    }
  }
}

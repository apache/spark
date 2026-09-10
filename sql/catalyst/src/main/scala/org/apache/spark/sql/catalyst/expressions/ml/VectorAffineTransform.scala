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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
 * Applies an element-wise affine transformation to SQL struct representations of MLlib vectors:
 * `vector(i) * scale(i) + shift(i)`. This expression is dedicated only for Spark ML and should be
 * used together with `unwrap_udt` and `wrap_udt`. A null scale is treated as an identity scale, and
 * a null shift is treated as a zero shift. If both are null, the input vector is returned
 * unchanged.
 */
case class VectorAffineTransform(
    vector: Expression,
    scale: Expression,
    shift: Expression)
  extends TernaryExpression with ExpectsInputTypes {

  override def first: Expression = vector
  override def second: Expression = scale
  override def third: Expression = shift

  override def prettyName: String = "ml_vector_affine_transform"

  override def inputTypes: Seq[AbstractDataType] = Seq(
    VectorAffineTransform.vectorSqlType,
    VectorAffineTransform.NonNullableDoubleArrayType,
    VectorAffineTransform.NonNullableDoubleArrayType)

  override def dataType: DataType = VectorAffineTransform.vectorSqlType

  override def nullable: Boolean = vector.nullable

  override def eval(input: InternalRow): Any = {
    val vectorInput = vector.eval(input)
    if (vectorInput == null) {
      null
    } else {
      VectorAffineTransform.transform(
        vectorInput.asInstanceOf[InternalRow],
        scale.eval(input).asInstanceOf[ArrayData],
        shift.eval(input).asInstanceOf[ArrayData])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val vectorJavaType = CodeGenerator.javaType(dataType)
    val arrayJavaType = CodeGenerator.javaType(VectorAffineTransform.doubleArraySqlType)
    val vectorGen = vector.genCode(ctx)
    val scaleInput = ctx.freshName("scaleInput")
    val shiftInput = ctx.freshName("shiftInput")
    val scaleGen = scale.genCode(ctx)
    val shiftGen = shift.genCode(ctx)
    val vectorType = ctx.freshName("vectorType")
    val vectorValues = ctx.freshName("vectorValues")
    val size = ctx.freshName("size")
    val scaleSize = ctx.freshName("scaleSize")
    val shiftSize = ctx.freshName("shiftSize")
    val vectorIndices = ctx.freshName("vectorIndices")
    val resultValues = ctx.freshName("resultValues")
    val featureIndex = ctx.freshName("featureIndex")
    val vectorIndex = ctx.freshName("vectorIndex")

    ev.copy(code = code"""
      ${vectorGen.code}
      boolean ${ev.isNull} = ${vectorGen.isNull};
      $vectorJavaType ${ev.value} = null;
      if (!${ev.isNull}) {
        ${scaleGen.code}
        ${shiftGen.code}
        $arrayJavaType $scaleInput = ${scaleGen.isNull} ? null : ${scaleGen.value};
        $arrayJavaType $shiftInput = ${shiftGen.isNull} ? null : ${shiftGen.value};
        if ($scaleInput == null && $shiftInput == null) {
          ${ev.value} = ${vectorGen.value};
        } else {
          final byte $vectorType = ${vectorGen.value}.getByte(0);
          final ArrayData $vectorValues = ${vectorGen.value}.getArray(3);
          int $size = -1;
          if ($vectorType == ${VectorAffineTransform.SparseVectorType}) {
            $size = ${vectorGen.value}.getInt(1);
          } else if ($vectorType == ${VectorAffineTransform.DenseVectorType}) {
            $size = $vectorValues.numElements();
          } else {
            throw new IllegalArgumentException("Unknown vector type " + $vectorType + ".");
          }

          final int $scaleSize = $scaleInput == null ? $size : $scaleInput.numElements();
          final int $shiftSize = $shiftInput == null ? $size : $shiftInput.numElements();
          if ($size != $scaleSize || $size != $shiftSize) {
            throw new IllegalArgumentException(
              "requirement failed: VectorAffineTransform was given inputs with " +
              "non-matching sizes: vector.size = " + $size + ", scale.size = " +
              $scaleSize + ", shift.size = " + $shiftSize);
          }

          if ($vectorType == ${VectorAffineTransform.SparseVectorType} &&
              $shiftInput == null) {
            final ArrayData $vectorIndices = ${vectorGen.value}.getArray(2);
            final double[] $resultValues = new double[$vectorValues.numElements()];
            for (int $vectorIndex = 0;
                 $vectorIndex < $resultValues.length;
                 $vectorIndex++) {
              final int $featureIndex = $vectorIndices.getInt($vectorIndex);
              $resultValues[$vectorIndex] = $vectorValues.getDouble($vectorIndex) *
                $scaleInput.getDouble($featureIndex);
            }
            ${ev.value} = new GenericInternalRow(new Object[] {
              (byte) ${VectorAffineTransform.SparseVectorType},
              $size,
              $vectorIndices,
              UnsafeArrayData.fromPrimitiveArray($resultValues)
            });
          } else {
            final double[] $resultValues = new double[$size];
            if ($vectorType == ${VectorAffineTransform.DenseVectorType}) {
              if ($scaleInput == null) {
                for (int $featureIndex = 0; $featureIndex < $size; $featureIndex++) {
                  $resultValues[$featureIndex] = $vectorValues.getDouble($featureIndex) +
                    $shiftInput.getDouble($featureIndex);
                }
              } else if ($shiftInput == null) {
                for (int $featureIndex = 0; $featureIndex < $size; $featureIndex++) {
                  $resultValues[$featureIndex] = $vectorValues.getDouble($featureIndex) *
                    $scaleInput.getDouble($featureIndex);
                }
              } else {
                for (int $featureIndex = 0; $featureIndex < $size; $featureIndex++) {
                  $resultValues[$featureIndex] = $vectorValues.getDouble($featureIndex) *
                    $scaleInput.getDouble($featureIndex) +
                    $shiftInput.getDouble($featureIndex);
                }
              }
            } else {
              for (int $featureIndex = 0; $featureIndex < $size; $featureIndex++) {
                $resultValues[$featureIndex] = 0.0D + $shiftInput.getDouble($featureIndex);
              }

              final ArrayData $vectorIndices = ${vectorGen.value}.getArray(2);
              if ($scaleInput == null) {
                for (int $vectorIndex = 0;
                     $vectorIndex < $vectorValues.numElements();
                     $vectorIndex++) {
                  final int $featureIndex = $vectorIndices.getInt($vectorIndex);
                  $resultValues[$featureIndex] += $vectorValues.getDouble($vectorIndex);
                }
              } else {
                for (int $vectorIndex = 0;
                     $vectorIndex < $vectorValues.numElements();
                     $vectorIndex++) {
                  final int $featureIndex = $vectorIndices.getInt($vectorIndex);
                  $resultValues[$featureIndex] += $vectorValues.getDouble($vectorIndex) *
                    $scaleInput.getDouble($featureIndex);
                }
              }
            }
            ${ev.value} = new GenericInternalRow(new Object[] {
              (byte) ${VectorAffineTransform.DenseVectorType},
              null,
              null,
              UnsafeArrayData.fromPrimitiveArray($resultValues)
            });
          }
        }
      }
    """)
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

  private[ml] val doubleArraySqlType = ArrayType(DoubleType, containsNull = false)

  private object NonNullableDoubleArrayType extends AbstractDataType {
    override private[sql] def defaultConcreteType: DataType = doubleArraySqlType

    override private[sql] def acceptsType(other: DataType): Boolean = other == doubleArraySqlType

    override private[spark] def simpleString: String = doubleArraySqlType.simpleString
  }

  private def vectorSize(vector: InternalRow, vectorType: Byte, values: ArrayData): Int = {
    vectorType match {
      case SparseVectorType => vector.getInt(1)
      case DenseVectorType => values.numElements()
      case _ => throw new IllegalArgumentException(s"Unknown vector type $vectorType.")
    }
  }

  private def sparseResult(
      vector: InternalRow,
      size: Int,
      vectorValues: ArrayData,
      scale: ArrayData): InternalRow = {
    val vectorIndices = vector.getArray(2)
    val resultValues = new Array[Double](vectorValues.numElements())
    var vectorIndex = 0
    while (vectorIndex < resultValues.length) {
      val featureIndex = vectorIndices.getInt(vectorIndex)
      resultValues(vectorIndex) =
        vectorValues.getDouble(vectorIndex) * scale.getDouble(featureIndex)
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
      size: Int,
      vectorType: Byte,
      vectorValues: ArrayData,
      scale: ArrayData,
      shift: ArrayData): InternalRow = {
    val resultValues = new Array[Double](size)
    if (vectorType == DenseVectorType) {
      var featureIndex = 0
      if (scale == null) {
        while (featureIndex < size) {
          resultValues(featureIndex) =
            vectorValues.getDouble(featureIndex) + shift.getDouble(featureIndex)
          featureIndex += 1
        }
      } else if (shift == null) {
        while (featureIndex < size) {
          resultValues(featureIndex) =
            vectorValues.getDouble(featureIndex) * scale.getDouble(featureIndex)
          featureIndex += 1
        }
      } else {
        while (featureIndex < size) {
          resultValues(featureIndex) = vectorValues.getDouble(featureIndex) *
            scale.getDouble(featureIndex) + shift.getDouble(featureIndex)
          featureIndex += 1
        }
      }
    } else {
      var featureIndex = 0
      while (featureIndex < size) {
        resultValues(featureIndex) = 0.0 + shift.getDouble(featureIndex)
        featureIndex += 1
      }

      val vectorIndices = vector.getArray(2)
      var vectorIndex = 0
      if (scale == null) {
        while (vectorIndex < vectorValues.numElements()) {
          val featureIndex = vectorIndices.getInt(vectorIndex)
          resultValues(featureIndex) += vectorValues.getDouble(vectorIndex)
          vectorIndex += 1
        }
      } else {
        while (vectorIndex < vectorValues.numElements()) {
          val featureIndex = vectorIndices.getInt(vectorIndex)
          resultValues(featureIndex) +=
            vectorValues.getDouble(vectorIndex) * scale.getDouble(featureIndex)
          vectorIndex += 1
        }
      }
    }
    new GenericInternalRow(Array[Any](
      DenseVectorType,
      null,
      null,
      UnsafeArrayData.fromPrimitiveArray(resultValues)))
  }

  private[ml] def transform(
      vector: InternalRow,
      scale: ArrayData,
      shift: ArrayData): InternalRow = {
    if (scale == null && shift == null) return vector
    val vectorType = vector.getByte(0)
    val vectorValues = vector.getArray(3)
    val size = vectorSize(vector, vectorType, vectorValues)
    val scaleSize = if (scale == null) size else scale.numElements()
    val shiftSize = if (shift == null) size else shift.numElements()
    require(size == scaleSize && size == shiftSize,
      "VectorAffineTransform was given inputs with non-matching sizes:" +
        s" vector.size = $size, scale.size = $scaleSize, shift.size = $shiftSize")

    if (vectorType == SparseVectorType && shift == null) {
      sparseResult(vector, size, vectorValues, scale)
    } else {
      denseResult(vector, size, vectorType, vectorValues, scale, shift)
    }
  }
}

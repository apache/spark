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
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
 * Computes the dot product of two SQL struct representations of MLlib vectors. This expression is
 * dedicated only for Spark ML and should be used together with `unwrap_udt`.
 */
case class VectorDotProduct(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "ml_vector_dot_product"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(VectorDotProduct.vectorSqlType, VectorDotProduct.vectorSqlType)

  override def dataType: DataType = DoubleType

  override def nullable: Boolean = left.nullable || right.nullable

  override protected def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    VectorDotProduct.dot(
      leftInput.asInstanceOf[InternalRow],
      rightInput.asInstanceOf[InternalRow])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (leftInput, rightInput) => {
      val leftType = ctx.freshName("leftType")
      val leftValues = ctx.freshName("leftValues")
      val leftSize = ctx.freshName("leftSize")
      val rightType = ctx.freshName("rightType")
      val rightValues = ctx.freshName("rightValues")
      val rightSize = ctx.freshName("rightSize")
      val index = ctx.freshName("index")
      val size = ctx.freshName("size")
      val sparseIndices = ctx.freshName("sparseIndices")
      val leftIndices = ctx.freshName("leftIndices")
      val rightIndices = ctx.freshName("rightIndices")
      val leftIndex = ctx.freshName("leftIndex")
      val rightIndex = ctx.freshName("rightIndex")
      val leftNumActives = ctx.freshName("leftNumActives")
      val rightNumActives = ctx.freshName("rightNumActives")
      val activeIndex = ctx.freshName("activeIndex")

      s"""
         |final byte $leftType = $leftInput.getByte(0);
         |final ArrayData $leftValues = $leftInput.getArray(3);
         |int $leftSize = -1;
         |if ($leftType == ${VectorDotProduct.SparseVectorType}) {
         |  $leftSize = $leftInput.getInt(1);
         |} else if ($leftType == ${VectorDotProduct.DenseVectorType}) {
         |  $leftSize = $leftValues.numElements();
         |} else {
         |  throw new IllegalArgumentException("Unknown vector type " + $leftType + ".");
         |}
         |
         |final byte $rightType = $rightInput.getByte(0);
         |final ArrayData $rightValues = $rightInput.getArray(3);
         |int $rightSize = -1;
         |if ($rightType == ${VectorDotProduct.SparseVectorType}) {
         |  $rightSize = $rightInput.getInt(1);
         |} else if ($rightType == ${VectorDotProduct.DenseVectorType}) {
         |  $rightSize = $rightValues.numElements();
         |} else {
         |  throw new IllegalArgumentException("Unknown vector type " + $rightType + ".");
         |}
         |
         |if ($leftSize != $rightSize) {
         |  throw new IllegalArgumentException(
         |    "requirement failed: VectorDotProduct was given vectors with non-matching sizes:" +
         |    " left.size = " + $leftSize + ", right.size = " + $rightSize);
         |}
         |
         |${ev.value} = 0.0D;
         |if ($leftType == ${VectorDotProduct.DenseVectorType} &&
         |    $rightType == ${VectorDotProduct.DenseVectorType}) {
         |  for (int $index = 0; $index < $leftSize; $index++) {
         |    ${ev.value} += $leftValues.getDouble($index) * $rightValues.getDouble($index);
         |  }
         |} else if ($leftType == ${VectorDotProduct.SparseVectorType} &&
         |    $rightType == ${VectorDotProduct.DenseVectorType}) {
         |  final ArrayData $sparseIndices = $leftInput.getArray(2);
         |  final int $size = $sparseIndices.numElements();
         |  for (int $index = 0; $index < $size; $index++) {
         |    ${ev.value} += $leftValues.getDouble($index) *
         |      $rightValues.getDouble($sparseIndices.getInt($index));
         |  }
         |} else if ($leftType == ${VectorDotProduct.DenseVectorType} &&
         |    $rightType == ${VectorDotProduct.SparseVectorType}) {
         |  final ArrayData $sparseIndices = $rightInput.getArray(2);
         |  final int $size = $sparseIndices.numElements();
         |  for (int $index = 0; $index < $size; $index++) {
         |    ${ev.value} += $rightValues.getDouble($index) *
         |      $leftValues.getDouble($sparseIndices.getInt($index));
         |  }
         |} else {
         |  final ArrayData $leftIndices = $leftInput.getArray(2);
         |  final ArrayData $rightIndices = $rightInput.getArray(2);
         |  final int $leftNumActives = $leftIndices.numElements();
         |  final int $rightNumActives = $rightIndices.numElements();
         |  int $leftIndex = 0;
         |  int $rightIndex = 0;
         |  while ($leftIndex < $leftNumActives && $rightIndex < $rightNumActives) {
         |    final int $activeIndex = $leftIndices.getInt($leftIndex);
         |    while ($rightIndex < $rightNumActives &&
         |        $rightIndices.getInt($rightIndex) < $activeIndex) {
         |      $rightIndex++;
         |    }
         |    if ($rightIndex < $rightNumActives &&
         |        $rightIndices.getInt($rightIndex) == $activeIndex) {
         |      ${ev.value} += $leftValues.getDouble($leftIndex) *
         |        $rightValues.getDouble($rightIndex);
         |      $rightIndex++;
         |    }
         |    $leftIndex++;
         |  }
         |}
         |""".stripMargin
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): VectorDotProduct = copy(left = newLeft, right = newRight)
}

object VectorDotProduct {
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

  private def dot(left: InternalRow, right: InternalRow): Double = {
    val leftType = left.getByte(0)
    val leftValues = left.getArray(3)
    val leftSize = vectorSize(left, leftType, leftValues)
    val rightType = right.getByte(0)
    val rightValues = right.getArray(3)
    val rightSize = vectorSize(right, rightType, rightValues)
    require(leftSize == rightSize,
      "VectorDotProduct was given vectors with non-matching sizes:" +
        s" left.size = $leftSize, right.size = $rightSize")
    (leftType, rightType) match {
      case (DenseVectorType, DenseVectorType) =>
        dotDenseDense(leftValues, rightValues)
      case (SparseVectorType, DenseVectorType) =>
        dotSparseDense(left.getArray(2), leftValues, rightValues)
      case (DenseVectorType, SparseVectorType) =>
        dotSparseDense(right.getArray(2), rightValues, leftValues)
      case (SparseVectorType, SparseVectorType) =>
        dotSparseSparse(left.getArray(2), leftValues, right.getArray(2), rightValues)
    }
  }

  private def dotDenseDense(left: ArrayData, right: ArrayData): Double = {
    var sum = 0.0
    var index = 0
    val size = left.numElements()
    while (index < size) {
      sum += left.getDouble(index) * right.getDouble(index)
      index += 1
    }
    sum
  }

  private def dotSparseDense(
      sparseIndices: ArrayData,
      sparseValues: ArrayData,
      denseValues: ArrayData): Double = {
    var sum = 0.0
    var index = 0
    val size = sparseIndices.numElements()
    while (index < size) {
      sum += sparseValues.getDouble(index) * denseValues.getDouble(sparseIndices.getInt(index))
      index += 1
    }
    sum
  }

  private def dotSparseSparse(
      leftIndices: ArrayData,
      leftValues: ArrayData,
      rightIndices: ArrayData,
      rightValues: ArrayData): Double = {
    var sum = 0.0
    var leftIndex = 0
    var rightIndex = 0
    val leftSize = leftIndices.numElements()
    val rightSize = rightIndices.numElements()
    while (leftIndex < leftSize && rightIndex < rightSize) {
      val index = leftIndices.getInt(leftIndex)
      while (rightIndex < rightSize && rightIndices.getInt(rightIndex) < index) {
        rightIndex += 1
      }
      if (rightIndex < rightSize && rightIndices.getInt(rightIndex) == index) {
        sum += leftValues.getDouble(leftIndex) * rightValues.getDouble(rightIndex)
        rightIndex += 1
      }
      leftIndex += 1
    }
    sum
  }
}

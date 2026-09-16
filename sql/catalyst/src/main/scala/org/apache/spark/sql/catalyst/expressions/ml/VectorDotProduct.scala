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
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, EmptyRow, ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
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
    MLExpressionUtils.dotProduct(
      leftInput.asInstanceOf[InternalRow],
      rightInput.asInstanceOf[InternalRow])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    foldableVector(right)
      .map(cached => doGenCodeWithCachedVector(ctx, ev, left, cached))
      .orElse(foldableVector(left)
        .map(cached => doGenCodeWithCachedVector(ctx, ev, right, cached)))
      .getOrElse {
        val utils = classOf[MLExpressionUtils].getName
        nullSafeCodeGen(ctx, ev, (leftInput, rightInput) => {
          s"${ev.value} = $utils.dotProduct($leftInput, $rightInput);"
        })
      }
  }

  private def foldableVector(expression: Expression): Option[InternalRow] = {
    if (expression.foldable) {
      Option(expression.eval(EmptyRow).asInstanceOf[InternalRow])
    } else {
      None
    }
  }

  private def doGenCodeWithCachedVector(
      ctx: CodegenContext,
      ev: ExprCode,
      vector: Expression,
      cachedVector: InternalRow): ExprCode = {
    val values = cachedVector.getArray(3)
    val cachedVectorValues = ctx.addReferenceObj(
      "cachedVectorValues", values.toDoubleArray(), "double[]")
    val (cachedVectorSize, cachedVectorIndices) = cachedVector.getByte(0) match {
      case VectorDotProduct.SparseVectorType =>
        (cachedVector.getInt(1), ctx.addReferenceObj(
          "cachedVectorIndices", cachedVector.getArray(2).toIntArray(), "int[]"))
      case VectorDotProduct.DenseVectorType =>
        (values.numElements(), "null")
      case vectorType =>
        throw new IllegalArgumentException(s"Unknown vector type $vectorType.")
    }
    val vectorGen = vector.genCode(ctx)
    val utils = classOf[MLExpressionUtils].getName

    ev.copy(code = code"""
      ${vectorGen.code}
      boolean ${ev.isNull} = ${vectorGen.isNull};
      double ${ev.value} = 0.0D;
      if (!${ev.isNull}) {
        ${ev.value} = $utils.dotProduct(
          ${vectorGen.value}, $cachedVectorSize, $cachedVectorIndices, $cachedVectorValues);
      }
    """)
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
}

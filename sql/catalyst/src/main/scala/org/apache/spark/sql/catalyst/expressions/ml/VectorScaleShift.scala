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
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, Literal, TernaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
 * Applies element-wise scaling and shifting to SQL struct representations of MLlib vectors:
 * `vector(i) * scale(i) + shift(i)`. This expression is dedicated only for Spark ML and should be
 * used together with `unwrap_udt` and `wrap_udt`. A null scale is treated as an identity scale, and
 * a null shift is treated as a zero shift. If both are null, the input vector is returned
 * unchanged.
 */
case class VectorScaleShift(
    vector: Expression,
    scale: Expression,
    shift: Expression)
  extends TernaryExpression with ExpectsInputTypes {

  override def first: Expression = vector
  override def second: Expression = scale
  override def third: Expression = shift

  override def prettyName: String = "ml_vector_scale_shift"

  override def inputTypes: Seq[AbstractDataType] = Seq(
    VectorScaleShift.vectorSqlType,
    VectorScaleShift.NonNullableDoubleArrayType,
    VectorScaleShift.NonNullableDoubleArrayType)

  override def dataType: DataType = VectorScaleShift.vectorSqlType

  override def nullable: Boolean = vector.nullable

  override def eval(input: InternalRow): Any = {
    val vectorInput = vector.eval(input)
    if (vectorInput == null) {
      null
    } else {
      MLExpressionUtils.scaleShift(
        vectorInput.asInstanceOf[InternalRow],
        scale.eval(input).asInstanceOf[ArrayData],
        shift.eval(input).asInstanceOf[ArrayData])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val utils = classOf[MLExpressionUtils].getName
    val vectorJavaType = CodeGenerator.javaType(dataType)
    val arrayJavaType = CodeGenerator.javaType(VectorScaleShift.doubleArraySqlType)
    val vectorGen = vector.genCode(ctx)
    val scaleInput = ctx.freshName("scaleInput")
    val shiftInput = ctx.freshName("shiftInput")
    val (scaleCode, cachedScale) = scale match {
      case Literal(value: ArrayData, _) =>
        (code"$arrayJavaType $scaleInput = null;",
          ctx.addReferenceObj("cachedScale", value.toDoubleArray(), "double[]"))
      case _ =>
        val scaleGen = scale.genCode(ctx)
        (code"""
          ${scaleGen.code}
          $arrayJavaType $scaleInput = ${scaleGen.isNull} ? null : ${scaleGen.value};
        """, "null")
    }
    val (shiftCode, cachedShift) = shift match {
      case Literal(value: ArrayData, _) =>
        (code"$arrayJavaType $shiftInput = null;",
          ctx.addReferenceObj("cachedShift", value.toDoubleArray(), "double[]"))
      case _ =>
        val shiftGen = shift.genCode(ctx)
        (code"""
          ${shiftGen.code}
          $arrayJavaType $shiftInput = ${shiftGen.isNull} ? null : ${shiftGen.value};
        """, "null")
    }

    ev.copy(code = code"""
      ${vectorGen.code}
      boolean ${ev.isNull} = ${vectorGen.isNull};
      $vectorJavaType ${ev.value} = null;
      if (!${ev.isNull}) {
        $scaleCode
        $shiftCode
        ${ev.value} = $utils.scaleShift(
          ${vectorGen.value}, $scaleInput, $shiftInput, $cachedScale, $cachedShift);
      }
    """)
  }

  override protected def withNewChildrenInternal(
      newVector: Expression,
      newScale: Expression,
      newShift: Expression): VectorScaleShift = {
    copy(vector = newVector, scale = newScale, shift = newShift)
  }
}

object VectorScaleShift {
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
}

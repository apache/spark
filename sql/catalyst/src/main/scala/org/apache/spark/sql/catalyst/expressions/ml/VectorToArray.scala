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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo, Expression, GetStructField, If, Literal, RuntimeReplaceable, StringLiteral, UnwrapUDT, With}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Converts an MLlib vector to a dense array of doubles or floats. The registered internal function
 * takes a vector UDT column and a "float64" or "float32" literal. The primary constructor also
 * accepts the unwrapped vector SQL struct and takes the output element type directly.
 */
case class VectorToArray(child: Expression, elementType: DataType)
  extends RuntimeReplaceable with UnaryLike[Expression] {

  require(elementType == DoubleType || elementType == FloatType,
    "VectorToArray requires a double or float element type.")

  def this(input: Expression, dtype: Expression) = {
    this(input, VectorToArray.elementType(dtype))
  }

  override def prettyName: String = "vector_to_array"

  override def dataType: DataType = ArrayType(elementType, containsNull = false)

  override def nullable: Boolean = true

  override def sql: String = {
    val dtype = if (elementType == FloatType) "float32" else "float64"
    s"$prettyName(${child.sql}, '${dtype}')"
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val inputType = child.dataType match {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }
    if (inputType == VectorScaleShift.vectorSqlType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(VectorScaleShift.vectorSqlType),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)))
    }
  }

  override lazy val replacement: Expression = {
    def convert(input: Expression): Expression = {
      val vector = input.dataType match {
        case _: UserDefinedType[_] => UnwrapUDT(input)
        case _ => input
      }
      If(EqualTo(GetStructField(vector, 0), Literal(1.toByte)),
        Cast(GetStructField(vector, 3), dataType),
        SparseVectorToArray(vector, elementType))
    }
    // Bind beneath an explicit UnwrapUDT too, so a plain UDT column remains cheap to inline.
    val input = child match {
      case UnwrapUDT(vector) => vector
      case other => other
    }
    if (SQLConf.get.getConf(SQLConf.ALWAYS_INLINE_COMMON_EXPR)) {
      convert(input)
    } else {
      With(input) { case Seq(ref) => convert(ref) }
    }
  }

  override protected def withNewChildInternal(newChild: Expression): VectorToArray = {
    copy(child = newChild)
  }
}

object VectorToArray {
  private def elementType(dtype: Expression): DataType = dtype match {
    case StringLiteral("float64") => DoubleType
    case StringLiteral("float32") => FloatType
    case invalid @ StringLiteral(_) =>
      throw QueryCompilationErrors.invalidParameter("DTYPE", "vector_to_array", "dtype", invalid)
    case invalid =>
      throw QueryCompilationErrors.invalidStringParameter("vector_to_array", "dtype", invalid)
  }
}

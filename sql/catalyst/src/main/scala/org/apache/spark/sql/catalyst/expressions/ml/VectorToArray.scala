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
import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo, Expression, GetStructField, If, Literal, RuntimeReplaceable, StringLiteral, UnwrapUDT}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

/**
 * Converts the SQL struct representation of an MLlib vector to a dense array of doubles or floats.
 * This expression is dedicated only for Spark ML and should be used with UnwrapUDT.
 */
case class VectorToArray(child: Expression, elementType: DataType)
  extends RuntimeReplaceable with UnaryLike[Expression] {

  require(elementType == DoubleType || elementType == FloatType,
    "VectorToArray requires a double or float element type.")

  def this(input: Expression, dtype: Expression) = {
    this(UnwrapUDT(input), VectorToArray.elementType(dtype))
  }

  override def prettyName: String = "vector_to_array"

  override def dataType: DataType = ArrayType(elementType, containsNull = false)

  override def nullable: Boolean = true

  override def checkInputDataTypes(): TypeCheckResult = {
    SparseVectorToArray(child, elementType).checkInputDataTypes()
  }

  override lazy val replacement: Expression = {
    If(EqualTo(GetStructField(child, 0), Literal(1.toByte)),
      Cast(GetStructField(child, 3), dataType),
      SparseVectorToArray(child, elementType))
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

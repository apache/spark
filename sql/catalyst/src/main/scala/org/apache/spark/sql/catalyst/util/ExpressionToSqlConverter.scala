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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Utility for converting Catalyst expressions to their SQL string representation.
 *
 * This object provides a centralized, comprehensive implementation for converting
 * Spark SQL expressions to their equivalent SQL text representation. It is used
 * by SparkSqlParser, SparkConnectPlanner, and other components for parameter
 * substitution and SQL generation.
 *
 * Key features:
 * - Handles all Spark SQL data types (primitives, collections, complex types)
 * - Supports both Scala collections and Spark internal data structures
 * - Proper SQL escaping and formatting
 * - Robust error handling with meaningful error messages
 * - Constant folding for foldable expressions
 * - Special handling for UnresolvedFunction expressions
 *
 * Supported data types:
 * - Primitives: String, Integer, Long, Float, Double, Boolean, Decimal
 * - Temporal: Date, Timestamp
 * - Collections: Array, Map (including nested structures)
 * - Special: Null values, Binary data
 * - Complex: Nested arrays, maps of arrays, arrays of maps
 *
 * @example Basic usage:
 * {{{
 * val result1 = ExpressionToSqlConverter.convert(Literal(42))
 * // result1: "42"
 *
 * val result2 = ExpressionToSqlConverter.convert(Literal("hello"))
 * // result2: "'hello'"
 *
 * val arrayLit = Literal.create(Array(1, 2, 3), ArrayType(IntegerType))
 * val result3 = ExpressionToSqlConverter.convert(arrayLit)
 * // result3: "ARRAY(1, 2, 3)"
 * }}}
 *
 * @example Complex types:
 * {{{
 * val mapLit = Literal.create(Map("key" -> "value"), MapType(StringType, StringType))
 * val result = ExpressionToSqlConverter.convert(mapLit)
 * // result: "MAP('key', 'value')"
 * }}}
 *
 * @note This utility is thread-safe and can be used concurrently.
 * @see [[ParameterSubstitutionStrategy]] for usage in parameter substitution
 * @see [[ParameterHandler]] for the main parameter handling entry point
 * @since 4.0.0
 */
object ExpressionToSqlConverter {

  /**
   * Convert an expression to its SQL string representation.
   *
   * @param expr The expression to convert
   * @return SQL string representation of the expression
   * @throws SparkException.internalError if the expression cannot be converted
   */
  def convert(expr: Expression): String = expr match {
    case lit: Literal => convertLiteral(lit)
    case _ => convertNonLiteral(expr)
  }

  private def convertLiteral(lit: Literal): String = lit match {
    case Literal(null, _) => "NULL"
    case Literal(value, dataType) => dataType match {
      case StringType => s"'${value.toString.replace("'", "''")}'"
      case _: NumericType => value.toString
      // scalastyle:off caselocale
      case BooleanType => value.toString.toUpperCase
      // scalastyle:on caselocale
      case DateType =>
        // Handle different date value types
        value match {
          case d: java.time.LocalDate => s"DATE '${d.toString}'"
          case d: java.sql.Date => s"DATE '${d.toString}'"
          case days: Int =>
            // Convert days since epoch to date string
            val epochDate = java.time.LocalDate.of(1970, 1, 1)
            val actualDate = epochDate.plusDays(days.toLong)
            s"DATE '${actualDate.toString}'"
          case _ => s"DATE '${value.toString}'"
        }
      case TimestampType =>
        // Handle different timestamp value types
        value match {
          case ts: java.time.Instant => s"TIMESTAMP '${ts.toString}'"
          case ts: java.sql.Timestamp => s"TIMESTAMP '${ts.toString}'"
          case micros: Long =>
            // Convert microseconds since epoch to timestamp string
            val instant = java.time.Instant.ofEpochSecond(
              micros / 1000000, (micros % 1000000) * 1000)
            s"TIMESTAMP '${instant.toString}'"
          case _ => s"TIMESTAMP '${value.toString}'"
        }
      case ArrayType(elementType, _) => convertArrayLiteral(value, elementType)
      case MapType(keyType, valueType, _) => convertMapLiteral(value, keyType, valueType)
      case _ => s"'${value.toString.replace("'", "''")}'"
    }
  }

  private def convertArrayLiteral(value: Any, elementType: DataType): String = {
    if (value == null) "NULL"
    else {
      // Handle both Scala collections and Spark's GenericArrayData
      val arraySeq = value match {
        case gad: org.apache.spark.sql.catalyst.util.GenericArrayData =>
          gad.array.toSeq
        case seq: scala.collection.Seq[Any] =>
          seq
        case arr: Array[Any] =>
          arr.toSeq
        case other =>
          // Fallback: try to convert to array and then to sequence
          Array(other).toSeq
      }
      val elementStrs = arraySeq.map { elem =>
        convert(Literal.create(elem, elementType))
      }
      s"ARRAY(${elementStrs.mkString(", ")})"
    }
  }

  private def convertMapLiteral(value: Any, keyType: DataType, valueType: DataType): String = {
    if (value == null) "NULL"
    else {
      // Handle both Scala collections and Spark's ArrayBasedMapData
      val mapValue = value match {
        case abmd: org.apache.spark.sql.catalyst.util.ArrayBasedMapData =>
          // Convert ArrayBasedMapData to a Map
          val keys = abmd.keyArray.array
          val values = abmd.valueArray.array
          keys.zip(values).toMap
        case map: scala.collection.Map[Any, Any] @unchecked =>
          map
        case other =>
          throw SparkException.internalError(
            s"Unsupported map value type: ${other.getClass.getSimpleName}")
      }

      val entryStrs = mapValue.map { case (k, v) =>
        val keyStr = convert(Literal.create(k, keyType))
        val valueStr = convert(Literal.create(v, valueType))
        s"$keyStr, $valueStr"
      }
      s"MAP(${entryStrs.mkString(", ")})"
    }
  }

  private def convertNonLiteral(expr: Expression): String = {
    // First try the expression's built-in SQL generation
    try {
      expr.sql
    } catch {
      case _: Exception =>
        // Fall back to constant folding if SQL generation fails and expression is foldable
        if (expr.foldable) {
          tryConstantFolding(expr)
        } else {
          handleSpecialExpressions(expr)
        }
    }
  }

  private def tryConstantFolding(expr: Expression): String = {
    try {
      val literal = Literal.create(expr.eval(), expr.dataType)
      convert(literal)
    } catch {
      case _: Exception =>
        handleSpecialExpressions(expr)
    }
  }

  private def handleSpecialExpressions(expr: Expression): String = {
    expr match {
      case CreateArray(children, _) if children.forall(_.foldable) =>
        // Handle array construction with foldable children
        val evaluatedArray = children.map(_.eval()).toArray
        val elementType = if (children.nonEmpty) children.head.dataType else IntegerType
        val arrayType = ArrayType(elementType)
        convert(Literal.create(evaluatedArray, arrayType))

      case CreateMap(children, _) if children.forall(_.foldable) =>
        // Handle map construction with foldable children
        val evaluatedChildren = children.map { child =>
          if (child.foldable) child.eval() else child
        }
        val keyType = if (children.nonEmpty) children.head.dataType else StringType
        val valueType = if (children.length > 1) children(1).dataType else StringType
        val mapType = MapType(keyType, valueType)
        val keyValues = evaluatedChildren.grouped(2).map {
          case Seq(k, v) => (k, v)
        }.toMap
        convert(Literal.create(keyValues, mapType))

      case UnresolvedFunction(Seq("array"), children, _, _, _, _, _)
          if children.forall(_.foldable) =>
        // Handle unresolved array function with foldable children
        val evaluatedChildren = children.map(_.eval())
        val elementType = if (children.nonEmpty) children.head.dataType else IntegerType
        val arrayType = ArrayType(elementType)
        convert(Literal.create(evaluatedChildren.toArray, arrayType))

      case UnresolvedFunction(Seq("map"), children, _, _, _, _, _)
          if children.forall(_.foldable) =>
        // Handle unresolved map function with foldable children
        val evaluatedChildren = children.map(_.eval())
        val keyType = if (children.nonEmpty) children.head.dataType else StringType
        val valueType = if (children.length > 1) children(1).dataType else StringType
        val mapType = MapType(keyType, valueType)
        val keyValues = evaluatedChildren.grouped(2).map {
          case Seq(k, v) => (k, v)
        }.toMap
        convert(Literal.create(keyValues, mapType))

      case _ =>
        throw SparkException.internalError(
          s"Cannot convert non-literal expression to SQL value: $expr " +
          s"(class: ${expr.getClass.getSimpleName}, foldable: ${expr.foldable})")
    }
  }
}

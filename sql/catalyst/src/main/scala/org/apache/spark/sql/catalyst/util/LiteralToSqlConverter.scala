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
import org.apache.spark.sql.catalyst.{InternalRow}
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

/**
 * Utility for converting Catalyst literal expressions to their SQL string representation.
 *
 * This object provides a specialized implementation for converting Spark SQL literal
 * expressions to their equivalent SQL text representation. It is used by the parameter
 * substitution system for EXECUTE IMMEDIATE and other parameterized queries.
 *
 * Key features:
 * - Handles all Spark SQL data types for literal values
 * - Supports both Scala collections and Spark internal data structures
 * - Proper SQL escaping and formatting
 * - Optimized for literal expressions only
 *
 * Supported data types:
 * - Primitives: String, Integer, Long, Float, Double, Boolean, Decimal
 * - Temporal: Date, Timestamp, TimestampNTZ, Interval
 * - Collections: Array, Map (including nested structures)
 * - Special: Null values, Binary data
 * - Complex: Nested arrays, maps of arrays, arrays of maps
 *
 * @example Basic usage:
 * {{{
 * val result1 = LiteralToSqlConverter.convert(Literal(42))
 * // result1: "42"
 *
 * val result2 = LiteralToSqlConverter.convert(Literal("hello"))
 * // result2: "'hello'"
 *
 * val arrayLit = Literal.create(Array(1, 2, 3), ArrayType(IntegerType))
 * val result3 = LiteralToSqlConverter.convert(arrayLit)
 * // result3: "ARRAY(1, 2, 3)"
 * }}}
 *
 * @example Complex types:
 * {{{
 * val mapLit = Literal.create(Map("key" -> "value"), MapType(StringType, StringType))
 * val result = LiteralToSqlConverter.convert(mapLit)
 * // result: "MAP('key', 'value')"
 * }}}
 *
 * @note This utility is thread-safe and can be used concurrently.
 * @note Only supports Literal expressions - all parameter values must be pre-evaluated.
 * @see [[ParameterHandler]] for the main parameter handling entry point
 * @since 4.0.0
 */
object LiteralToSqlConverter {

  /**
   * Convert an expression to its SQL string representation.
   *
   * This method handles both simple literals and complex expressions that result from
   * parameter evaluation. For complex types like arrays and maps, the expressions are
   * evaluated to internal data structures that need to be converted back to SQL constructors.
   *
   * @param expr The expression to convert (typically a Literal, but may be other expressions
   *             for complex types)
   * @return SQL string representation of the expression value
   */
  def convert(expr: Expression): String = expr match {
    case lit: Literal => convertLiteral(lit)

    // Special handling for UnresolvedFunction expressions that don't naturally evaluate
    // Only handle functions that are whitelisted in legacy mode but don't eval() naturally
    case UnresolvedFunction(name, children, _, _, _, _, _) =>
      val functionName = name.mkString(".")
      functionName.toLowerCase(java.util.Locale.ROOT) match {
        case "array" | "map" | "struct" | "map_from_arrays" | "map_from_entries" =>
          // Convert whitelisted functions to SQL function call syntax
          val childrenSql = children.map(convert).mkString(", ")
          s"${functionName.toUpperCase(java.util.Locale.ROOT)}($childrenSql)"
        case _ =>
          // Non-whitelisted function - not supported in parameter substitution
          throw QueryCompilationErrors.unsupportedParameterExpression(expr)
      }

    case _ =>
      // For non-literal expressions, they should be resolved before reaching this converter
      // If we get an unresolved expression, it indicates a problem in the calling code
      if (!expr.resolved) {
        throw SparkException.internalError(
          s"LiteralToSqlConverter received unresolved expression: " +
          s"${expr.getClass.getSimpleName}. All expressions should be resolved before " +
          s"parameter conversion.")
      }
      if (expr.foldable) {
        val value = expr.eval()
        val dataType = expr.dataType
        convertLiteral(Literal.create(value, dataType))
      } else {
        throw SparkException.internalError(
          s"LiteralToSqlConverter cannot convert non-foldable expression: " +
          s"${expr.getClass.getSimpleName}. All parameter values should be evaluable to " +
          s"literals before conversion.")
      }
  }

  private def convertLiteral(lit: Literal): String = {
    // For simple cases, delegate to the existing Literal.sql method
    // which already has the correct logic for most data types
    try {
      lit.sql
    } catch {
      case _: Exception =>
        // Fallback to manual conversion for cases where Literal.sql doesn't work
        // (e.g., complex types that need constructor syntax)
        lit match {
          case Literal(null, _) => "NULL"
          case Literal(value, dataType) => dataType match {
            case ArrayType(elementType, _) => convertArrayLiteral(value, elementType)
            case MapType(keyType, valueType, _) => convertMapLiteral(value, keyType, valueType)
            case _: StructType =>
              // Struct literals (row values) - convert to ROW constructor
              value match {
                case row: InternalRow =>
                  val structType = dataType.asInstanceOf[StructType]
                  val fieldValues = (0 until row.numFields).map { i =>
                    if (row.isNullAt(i)) {
                      "NULL"
                    } else {
                      val fieldType = structType.fields(i).dataType
                      val fieldValue = row.get(i, fieldType)
                      val fieldLiteral = Literal.create(fieldValue, fieldType)
                      convert(fieldLiteral)
                    }
                  }
                  s"ROW(${fieldValues.mkString(", ")})"
                case _ => s"ROW(${value.toString})"
              }
            case _ =>
              // For any other unsupported type, fall back to string representation
              s"'${value.toString.replace("'", "''")}'"
          }
        }
    }
  }

  private def convertArrayLiteral(value: Any, elementType: DataType): String = {
    if (value == null) "NULL"
    else {
      // Handle both Scala collections and Spark's GenericArrayData
      val arraySeq = value match {
        case gad: GenericArrayData =>
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
        case abmd: ArrayBasedMapData =>
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

}

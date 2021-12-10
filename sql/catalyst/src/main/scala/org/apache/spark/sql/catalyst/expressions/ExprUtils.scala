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

package org.apache.spark.sql.catalyst.expressions

import java.text.{DecimalFormat, DecimalFormatSymbols, ParsePosition}
import java.util.Locale

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, CharVarcharUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.types.{DataType, MapType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

object ExprUtils {

  def evalTypeExpr(exp: Expression): DataType = {
    if (exp.foldable) {
      exp.eval() match {
        case s: UTF8String if s != null =>
          val dataType = DataType.fromDDL(s.toString)
          CharVarcharUtils.failIfHasCharVarchar(dataType)
        case _ => throw QueryCompilationErrors.invalidSchemaStringError(exp)

      }
    } else {
      throw QueryCompilationErrors.schemaNotFoldableError(exp)
    }
  }

  def evalSchemaExpr(exp: Expression): StructType = {
    val dataType = evalTypeExpr(exp)
    if (!dataType.isInstanceOf[StructType]) {
      throw QueryCompilationErrors.schemaIsNotStructTypeError(dataType)
    }
    dataType.asInstanceOf[StructType]
  }

  def convertToMapData(exp: Expression): Map[String, String] = exp match {
    case m: CreateMap
      if m.dataType.acceptsType(MapType(StringType, StringType, valueContainsNull = false)) =>
      val arrayMap = m.eval().asInstanceOf[ArrayBasedMapData]
      ArrayBasedMapData.toScalaMap(arrayMap).map { case (key, value) =>
        key.toString -> value.toString
      }
    case m: CreateMap =>
      throw QueryCompilationErrors.keyValueInMapNotStringError(m)
    case _ =>
      throw QueryCompilationErrors.nonMapFunctionNotAllowedError
  }

  /**
   * A convenient function for schema validation in datasources supporting
   * `columnNameOfCorruptRecord` as an option.
   */
  def verifyColumnNameOfCorruptRecord(
      schema: StructType,
      columnNameOfCorruptRecord: String): Unit = {
    schema.getFieldIndex(columnNameOfCorruptRecord).foreach { corruptFieldIndex =>
      val f = schema(corruptFieldIndex)
      if (f.dataType != StringType || !f.nullable) {
        throw QueryCompilationErrors.invalidFieldTypeForCorruptRecordError
      }
    }
  }

  def getDecimalParser(locale: Locale): String => java.math.BigDecimal = {
    if (locale == Locale.US) { // Special handling the default locale for backward compatibility
      (s: String) => new java.math.BigDecimal(s.replaceAll(",", ""))
    } else {
      val decimalFormat = new DecimalFormat("", new DecimalFormatSymbols(locale))
      decimalFormat.setParseBigDecimal(true)
      (s: String) => {
        val pos = new ParsePosition(0)
        val result = decimalFormat.parse(s, pos).asInstanceOf[java.math.BigDecimal]
        if (pos.getIndex() != s.length() || pos.getErrorIndex() != -1) {
          throw QueryExecutionErrors.cannotParseDecimalError
        } else {
          result
        }
      }
    }
  }

  /**
   * Check if the schema is valid for Json
   * @param schema
   * @return
   *  None if the schema is valid
   *  Some(msg) with the error message if the schema is not valid
   */
  def checkJsonSchema(schema: DataType): Option[Throwable] =
    if (schema.existsRecursively {
      case MapType(keyType, _, _) if keyType != StringType => true
      case _ => false
    }) {
      Some(QueryCompilationErrors.invalidJsonSchema(schema))
    } else {
      None
    }
}

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
package org.apache.spark.sql.errors

import org.apache.spark.{QueryContext, SparkArithmeticException, SparkException, SparkIllegalArgumentException, SparkNumberFormatException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.catalyst.util.QuotingUtils.toSQLSchema
import org.apache.spark.sql.types.{DataType, Decimal, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Object for grouping error messages from (most) exceptions thrown during query execution.
 * This does not include exceptions thrown during the eager execution of commands, which are
 * grouped into [[CompilationErrors]].
 */
private[sql] object DataTypeErrors extends DataTypeErrorsBase {
  def unsupportedOperationExceptionError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_2225")
  }

  def decimalPrecisionExceedsMaxPrecisionError(
      precision: Int, maxPrecision: Int): SparkArithmeticException = {
    new SparkArithmeticException(
      errorClass = "DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION",
      messageParameters = Map(
        "precision" -> precision.toString,
        "maxPrecision" -> maxPrecision.toString
      ),
      context = Array.empty,
      summary = "")
  }

  def unsupportedRoundingMode(roundMode: BigDecimal.RoundingMode.Value): SparkException = {
    SparkException.internalError(s"Not supported rounding mode: ${roundMode.toString}.")
  }

  def outOfDecimalTypeRangeError(str: UTF8String): SparkArithmeticException = {
    new SparkArithmeticException(
      errorClass = "NUMERIC_OUT_OF_SUPPORTED_RANGE",
      messageParameters = Map(
        "value" -> str.toString),
      context = Array.empty,
      summary = "")
  }

  def unsupportedJavaTypeError(clazz: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2121",
      messageParameters = Map("clazz" -> clazz.toString()))
  }

  def nullLiteralsCannotBeCastedError(name: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2226",
      messageParameters = Map(
        "name" -> name))
  }

  def notUserDefinedTypeError(name: String, userClass: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2227",
      messageParameters = Map(
        "name" -> name,
        "userClass" -> userClass),
      cause = null)
  }

  def cannotLoadUserDefinedTypeError(name: String, userClass: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2228",
      messageParameters = Map(
        "name" -> name,
        "userClass" -> userClass),
      cause = null)
  }

  def unsupportedArrayTypeError(clazz: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2120",
      messageParameters = Map("clazz" -> clazz.toString()))
  }

  def schemaFailToParseError(schema: String, e: Throwable): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SCHEMA.PARSE_ERROR",
      messageParameters = Map(
        "inputSchema" -> toSQLSchema(schema),
        "reason" -> e.getMessage
      ),
      cause = Some(e))
  }

  def invalidDayTimeIntervalType(startFieldName: String, endFieldName: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1224",
      messageParameters = Map(
        "startFieldName" -> startFieldName,
        "endFieldName" -> endFieldName))
  }

  def invalidDayTimeField(field: Byte, supportedIds: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1223",
      messageParameters = Map(
        "field" -> field.toString,
        "supportedIds" -> supportedIds.mkString(", ")))
  }

  def invalidYearMonthField(field: Byte, supportedIds: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1225",
      messageParameters = Map(
        "field" -> field.toString,
        "supportedIds" -> supportedIds.mkString(", ")))
  }

  def decimalCannotGreaterThanPrecisionError(scale: Int, precision: Int): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1228",
      messageParameters = Map(
        "scale" -> scale.toString,
        "precision" -> precision.toString))
  }

  def negativeScaleNotAllowedError(scale: Int): Throwable = {
    val sqlConf = QuotingUtils.toSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal")
    SparkException.internalError(s"Negative scale is not allowed: ${scale.toString}." +
      s" Set the config ${sqlConf}" +
      " to \"true\" to allow it.")
  }

  def attributeNameSyntaxError(name: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1049",
      messageParameters = Map("name" -> name))
  }

  def cannotMergeIncompatibleDataTypesError(left: DataType, right: DataType): Throwable = {
    new SparkException(
      errorClass = "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE",
      messageParameters = Map(
        "left" -> toSQLType(left),
        "right" -> toSQLType(right)),
      cause = null)
  }

  def cannotMergeDecimalTypesWithIncompatibleScaleError(
      leftScale: Int, rightScale: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2124",
      messageParameters = Map(
        "leftScale" -> leftScale.toString(),
        "rightScale" -> rightScale.toString()),
      cause = null)
  }

  def dataTypeUnsupportedError(dataType: String, failure: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> (dataType + failure)))
  }

  def invalidFieldName(fieldName: Seq[String], path: Seq[String], context: Origin): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_FIELD_NAME",
      messageParameters = Map(
        "fieldName" -> toSQLId(fieldName),
        "path" -> toSQLId(path)),
      origin = context)
  }

  def unscaledValueTooLargeForPrecisionError(
      value: Decimal,
      decimalPrecision: Int,
      decimalScale: Int,
      context: QueryContext = null): ArithmeticException = {
    numericValueOutOfRange(value, decimalPrecision, decimalScale, context)
  }

  def cannotChangeDecimalPrecisionError(
      value: Decimal,
      decimalPrecision: Int,
      decimalScale: Int,
      context: QueryContext = null): ArithmeticException = {
    numericValueOutOfRange(value, decimalPrecision, decimalScale, context)
  }

  private def numericValueOutOfRange(
      value: Decimal,
      decimalPrecision: Int,
      decimalScale: Int,
      context: QueryContext): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION",
      messageParameters = Map(
        "value" -> value.toPlainString,
        "precision" -> decimalPrecision.toString,
        "scale" -> decimalScale.toString,
        "config" -> toSQLConf("spark.sql.ansi.enabled")),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidInputInCastToNumberError(
      to: DataType,
      s: UTF8String,
      context: QueryContext): SparkNumberFormatException = {
    val convertedValueStr = "'" + s.toString.replace("\\", "\\\\").replace("'", "\\'") + "'"
    new SparkNumberFormatException(
      errorClass = "CAST_INVALID_INPUT",
      messageParameters = Map(
        "expression" -> convertedValueStr,
        "sourceType" -> toSQLType(StringType),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf("spark.sql.ansi.enabled")),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def ambiguousColumnOrFieldError(
      name: Seq[String], numMatches: Int, context: Origin): Throwable = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_COLUMN_OR_FIELD",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "n" -> numMatches.toString),
      origin = context)
  }

  def castingCauseOverflowError(t: String, from: DataType, to: DataType): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "CAST_OVERFLOW",
      messageParameters = Map(
        "value" -> t,
        "sourceType" -> toSQLType(from),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf("spark.sql.ansi.enabled")),
      context = Array.empty,
      summary = "")
  }

  def failedParsingStructTypeError(raw: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "FAILED_PARSE_STRUCT_TYPE",
      messageParameters = Map("raw" -> s"'$raw'"))
  }

  def fieldIndexOnRowWithoutSchemaError(fieldName: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_CALL.FIELD_INDEX",
      messageParameters = Map(
        "methodName" -> "fieldIndex",
        "className" -> "Row",
        "fieldName" -> toSQLId(fieldName))
    )
  }

  def valueIsNullError(index: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2232",
      messageParameters = Map(
        "index" -> index.toString),
      cause = null)
  }

  def charOrVarcharTypeAsStringUnsupportedError(): Throwable = {
    new AnalysisException(
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING",
      messageParameters = Map.empty)
  }

  def userSpecifiedSchemaUnsupportedError(operation: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1189",
      messageParameters = Map("operation" -> operation))
  }
}

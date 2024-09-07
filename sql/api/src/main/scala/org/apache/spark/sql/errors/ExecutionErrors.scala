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

import java.time.LocalDate
import java.time.temporal.ChronoField

import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.{QueryContext, SparkArithmeticException, SparkBuildInfo, SparkDateTimeException, SparkException, SparkRuntimeException, SparkUnsupportedOperationException, SparkUpgradeException}
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

private[sql] trait ExecutionErrors extends DataTypeErrorsBase {
  def fieldDiffersFromDerivedLocalDateError(
      field: ChronoField,
      actual: Int,
      expected: Int,
      candidate: LocalDate): SparkDateTimeException = {
    new SparkDateTimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2129",
      messageParameters = Map(
        "field" -> field.toString,
        "actual" -> actual.toString,
        "expected" -> expected.toString,
        "candidate" -> candidate.toString),
      context = Array.empty,
      summary = "")
  }

  def failToParseDateTimeInNewParserError(s: String, e: Throwable): SparkUpgradeException = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER",
      messageParameters = Map(
        "datetime" -> toSQLValue(s),
        "config" -> toSQLConf(SqlApiConf.LEGACY_TIME_PARSER_POLICY_KEY)),
      e)
  }

  def stateStoreHandleNotInitialized(): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "STATE_STORE_HANDLE_NOT_INITIALIZED",
      messageParameters = Map.empty)
  }

  def failToRecognizePatternAfterUpgradeError(
      pattern: String,
      e: Throwable): SparkUpgradeException = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION",
      messageParameters = Map(
        "pattern" -> toSQLValue(pattern),
        "config" -> toSQLConf(SqlApiConf.LEGACY_TIME_PARSER_POLICY_KEY),
        "docroot" -> SparkBuildInfo.spark_doc_root),
      e)
  }

  def failToRecognizePatternError(pattern: String, e: Throwable): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2130",
      messageParameters =
        Map("pattern" -> toSQLValue(pattern), "docroot" -> SparkBuildInfo.spark_doc_root),
      cause = e)
  }

  def unreachableError(err: String = ""): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2028",
      messageParameters = Map("err" -> err))
  }

  def invalidInputInCastToDatetimeError(
      value: UTF8String,
      to: DataType,
      context: QueryContext): SparkDateTimeException = {
    invalidInputInCastToDatetimeErrorInternal(toSQLValue(value), StringType, to, context)
  }

  def invalidInputInCastToDatetimeError(
      value: Double,
      to: DataType,
      context: QueryContext): SparkDateTimeException = {
    invalidInputInCastToDatetimeErrorInternal(toSQLValue(value), DoubleType, to, context)
  }

  protected def invalidInputInCastToDatetimeErrorInternal(
      sqlValue: String,
      from: DataType,
      to: DataType,
      context: QueryContext): SparkDateTimeException = {
    new SparkDateTimeException(
      errorClass = "CAST_INVALID_INPUT",
      messageParameters = Map(
        "expression" -> sqlValue,
        "sourceType" -> toSQLType(from),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf(SqlApiConf.ANSI_ENABLED_KEY)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      context: QueryContext = null): ArithmeticException = {
    val alternative = if (hint.nonEmpty) {
      s" Use '$hint' to tolerate overflow and return NULL instead."
    } else ""
    new SparkArithmeticException(
      errorClass = "ARITHMETIC_OVERFLOW",
      messageParameters = Map(
        "message" -> message,
        "alternative" -> alternative,
        "config" -> toSQLConf(SqlApiConf.ANSI_ENABLED_KEY)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def cannotParseStringAsDataTypeError(
      pattern: String,
      value: String,
      dataType: DataType): Throwable = {
    SparkException.internalError(
      s"Cannot parse field value ${toSQLValue(value)} for pattern ${toSQLValue(pattern)} " +
        s"as the target spark data type ${toSQLType(dataType)}.")
  }

  def unsupportedArrowTypeError(typeName: ArrowType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_ARROWTYPE",
      messageParameters = Map("typeName" -> typeName.toString))
  }

  def duplicatedFieldNameInArrowStructError(
      fieldNames: Seq[String]): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
      messageParameters = Map("fieldNames" -> fieldNames.mkString("[", ", ", "]")))
  }

  def unsupportedDataTypeError(typeName: DataType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> toSQLType(typeName)))
  }

  def userDefinedTypeNotAnnotatedAndRegisteredError(udt: UserDefinedType[_]): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2155",
      messageParameters = Map("userClass" -> udt.userClass.getName),
      cause = null)
  }

  def cannotFindEncoderForTypeError(typeName: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "ENCODER_NOT_FOUND",
      messageParameters = Map("typeName" -> typeName, "docroot" -> SparkBuildInfo.spark_doc_root))
  }

  def cannotHaveCircularReferencesInBeanClassError(
      clazz: Class[_]): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2138",
      messageParameters = Map("clazz" -> clazz.toString))
  }

  def cannotFindConstructorForTypeError(tpe: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2144",
      messageParameters = Map("tpe" -> tpe))
  }

  def cannotHaveCircularReferencesInClassError(t: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2139",
      messageParameters = Map("t" -> t))
  }

  def cannotUseInvalidJavaIdentifierAsFieldNameError(
      fieldName: String,
      walkedTypePath: WalkedTypePath): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2140",
      messageParameters =
        Map("fieldName" -> fieldName, "walkedTypePath" -> walkedTypePath.toString))
  }

  def primaryConstructorNotFoundError(cls: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Map(
        "message" -> s"Couldn't find a primary constructor on ${cls.toString}."))
  }

  def cannotGetOuterPointerForInnerClassError(innerCls: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2154",
      messageParameters = Map("innerCls" -> innerCls.getName))
  }

  def cannotUseKryoSerialization(): SparkRuntimeException = {
    new SparkRuntimeException(errorClass = "CANNOT_USE_KRYO", messageParameters = Map.empty)
  }

  def notPublicClassError(name: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2229",
      messageParameters = Map("name" -> name))
  }

  def primitiveTypesNotSupportedError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(errorClass = "_LEGACY_ERROR_TEMP_2230")
  }
}

private[sql] object ExecutionErrors extends ExecutionErrors

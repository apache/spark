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

import java.io.{FileNotFoundException, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.{URISyntaxException, URL}
import java.sql.{SQLException, SQLFeatureNotSupportedException}
import java.time.{DateTimeException, LocalDate}
import java.time.temporal.ChronoField
import java.util.ConcurrentModificationException
import java.util.concurrent.TimeoutException

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.codehaus.commons.compiler.{CompileException, InternalCompilerException}

import org.apache.spark._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.sql.catalyst.{TableIdentifier, WalkedTypePath}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.analysis.UnresolvedGenerator
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ValueInterval
import org.apache.spark.sql.catalyst.trees.{SQLQueryContext, TreeNode}
import org.apache.spark.sql.catalyst.util.{sideBySide, BadRecordException, DateTimeUtils, FailFastMode}
import org.apache.spark.sql.connector.catalog.{CatalogNotFoundException, Identifier, Table, TableProvider}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.GLOBAL_TEMP_DATABASE
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.CircularBuffer

/**
 * Object for grouping error messages from (most) exceptions thrown during query execution.
 * This does not include exceptions thrown during the eager execution of commands, which are
 * grouped into [[QueryCompilationErrors]].
 */
private[sql] object QueryExecutionErrors extends QueryErrorsBase {

  def cannotEvaluateExpressionError(expression: Expression): Throwable = {
    SparkException.internalError(s"Cannot evaluate expression: $expression")
  }

  def cannotGenerateCodeForExpressionError(expression: Expression): Throwable = {
    SparkException.internalError(s"Cannot generate code for expression: $expression")
  }

  def cannotTerminateGeneratorError(generator: UnresolvedGenerator): Throwable = {
    SparkException.internalError(s"Cannot terminate expression: $generator")
  }

  def castingCauseOverflowError(t: Any, from: DataType, to: DataType): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "CAST_OVERFLOW",
      messageParameters = Map(
        "value" -> toSQLValue(t, from),
        "sourceType" -> toSQLType(from),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = Array.empty,
      summary = "")
  }

  def castingCauseOverflowErrorInTableInsert(
      from: DataType,
      to: DataType,
      columnName: String): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "CAST_OVERFLOW_IN_TABLE_INSERT",
      messageParameters = Map(
        "sourceType" -> toSQLType(from),
        "targetType" -> toSQLType(to),
        "columnName" -> toSQLId(columnName)),
      context = Array.empty,
      summary = ""
    )
  }

  def cannotChangeDecimalPrecisionError(
      value: Decimal,
      decimalPrecision: Int,
      decimalScale: Int,
      context: SQLQueryContext = null): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "NUMERIC_VALUE_OUT_OF_RANGE",
      messageParameters = Map(
        "value" -> value.toPlainString,
        "precision" -> decimalPrecision.toString,
        "scale" -> decimalScale.toString,
        "config" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidInputInCastToDatetimeError(
      value: Any,
      from: DataType,
      to: DataType,
      context: SQLQueryContext): Throwable = {
    new SparkDateTimeException(
      errorClass = "CAST_INVALID_INPUT",
      messageParameters = Map(
        "expression" -> toSQLValue(value, from),
        "sourceType" -> toSQLType(from),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidInputSyntaxForBooleanError(
      s: UTF8String,
      context: SQLQueryContext): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "CAST_INVALID_INPUT",
      messageParameters = Map(
        "expression" -> toSQLValue(s, StringType),
        "sourceType" -> toSQLType(StringType),
        "targetType" -> toSQLType(BooleanType),
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidInputInCastToNumberError(
      to: DataType,
      s: UTF8String,
      context: SQLQueryContext): SparkNumberFormatException = {
    new SparkNumberFormatException(
      errorClass = "CAST_INVALID_INPUT",
      messageParameters = Map(
        "expression" -> toSQLValue(s, StringType),
        "sourceType" -> toSQLType(StringType),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidInputInConversionError(
      to: DataType,
      s: UTF8String,
      fmt: UTF8String,
      hint: String): SparkIllegalArgumentException = {
      new SparkIllegalArgumentException(
        errorClass = "CONVERSION_INVALID_INPUT",
        messageParameters = Map(
          "str" -> toSQLValue(s, StringType),
          "fmt" -> toSQLValue(fmt, StringType),
          "targetType" -> toSQLType(to),
          "suggestion" -> toSQLId(hint)))
  }

  def cannotCastFromNullTypeError(to: DataType): Throwable = {
    new SparkException(
      errorClass = "CANNOT_CAST_DATATYPE",
      messageParameters = Map(
        "sourceType" -> NullType.typeName,
        "targetType" -> to.typeName),
      cause = null)
  }

  def cannotCastError(from: DataType, to: DataType): Throwable = {
    new SparkException(
      errorClass = "CANNOT_CAST_DATATYPE",
      messageParameters = Map(
        "sourceType" -> from.typeName,
        "targetType" -> to.typeName),
      cause = null)
  }

  def cannotParseDecimalError(): Throwable = {
    new SparkRuntimeException(
      errorClass = "CANNOT_PARSE_DECIMAL",
      messageParameters = Map.empty)
  }

  def dataTypeUnsupportedError(dataType: String, failure: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> (dataType + failure)))
  }

  def failedExecuteUserDefinedFunctionError(funcCls: String, inputTypes: String,
      outputType: String, e: Throwable): Throwable = {
    new SparkException(
      errorClass = "FAILED_EXECUTE_UDF",
      messageParameters = Map(
        "functionName" -> funcCls,
        "signature" -> inputTypes,
        "result" -> outputType),
      cause = e)
  }

  def divideByZeroError(context: SQLQueryContext): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "DIVIDE_BY_ZERO",
      messageParameters = Map("config" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def intervalDividedByZeroError(context: SQLQueryContext): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "INTERVAL_DIVIDED_BY_ZERO",
      messageParameters = Map.empty,
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidArrayIndexError(
      index: Int,
      numElements: Int,
      context: SQLQueryContext): ArrayIndexOutOfBoundsException = {
    new SparkArrayIndexOutOfBoundsException(
      errorClass = "INVALID_ARRAY_INDEX",
      messageParameters = Map(
        "indexValue" -> toSQLValue(index, IntegerType),
        "arraySize" -> toSQLValue(numElements, IntegerType),
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidElementAtIndexError(
      index: Int,
      numElements: Int,
      context: SQLQueryContext): ArrayIndexOutOfBoundsException = {
    new SparkArrayIndexOutOfBoundsException(
      errorClass = "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
      messageParameters = Map(
        "indexValue" -> toSQLValue(index, IntegerType),
        "arraySize" -> toSQLValue(numElements, IntegerType),
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def invalidFractionOfSecondError(): DateTimeException = {
    new SparkDateTimeException(
      errorClass = "INVALID_FRACTION_OF_SECOND",
      messageParameters = Map(
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)
      ),
      context = Array.empty,
      summary = "")
  }

  def ansiDateTimeParseError(e: Exception): SparkDateTimeException = {
    new SparkDateTimeException(
      errorClass = "CANNOT_PARSE_TIMESTAMP",
      messageParameters = Map(
        "message" -> e.getMessage,
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = Array.empty,
      summary = "")
  }

  def ansiDateTimeError(e: Exception): SparkDateTimeException = {
    new SparkDateTimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2000",
      messageParameters = Map(
        "message" -> e.getMessage,
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = Array.empty,
      summary = "")
  }

  def ansiIllegalArgumentError(message: String): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2000",
      messageParameters = Map(
        "message" -> message,
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)))
  }

  def ansiIllegalArgumentError(e: Exception): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2002",
      messageParameters = Map("message" -> e.getMessage))
  }

  def overflowInSumOfDecimalError(context: SQLQueryContext): ArithmeticException = {
    arithmeticOverflowError("Overflow in sum of decimals", context = context)
  }

  def overflowInIntegralDivideError(context: SQLQueryContext): ArithmeticException = {
    arithmeticOverflowError("Overflow in integral divide", "try_divide", context)
  }

  def mapSizeExceedArraySizeWhenZipMapError(size: Int): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2003",
      messageParameters = Map(
        "size" -> size.toString(),
        "maxRoundedArrayLength" -> ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH.toString()))
  }

  def literalTypeUnsupportedError(v: Any): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "UNSUPPORTED_FEATURE.LITERAL_TYPE",
      messageParameters = Map(
        "value" -> v.toString,
        "type" ->  v.getClass.toString))
  }

  def pivotColumnUnsupportedError(v: Any, dataType: DataType): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "UNSUPPORTED_FEATURE.PIVOT_TYPE",
      messageParameters = Map(
        "value" -> v.toString,
        "type" ->  toSQLType(dataType)))
  }

  def noDefaultForDataTypeError(dataType: DataType): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2004",
      messageParameters = Map("dataType" -> dataType.toString()))
  }

  def orderedOperationUnsupportedByDataTypeError(
      dataType: DataType): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2005",
      messageParameters = Map("dataType" -> dataType.toString()))
  }

  def regexGroupIndexLessThanZeroError(): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2006",
      messageParameters = Map.empty)
  }

  def regexGroupIndexExceedGroupCountError(
      groupCount: Int, groupIndex: Int): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2007",
      messageParameters = Map(
        "groupCount" -> groupCount.toString(),
        "groupIndex" -> groupIndex.toString()))
  }

  def invalidUrlError(url: UTF8String, e: URISyntaxException): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2008",
      messageParameters = Map(
        "url" -> url.toString,
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)))
  }

  def illegalUrlError(url: UTF8String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "CANNOT_DECODE_URL",
      messageParameters = Map("url" -> url.toString)
    )
  }

  def dataTypeOperationUnsupportedError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2009",
      messageParameters = Map.empty)
  }

  def mergeUnsupportedByWindowFunctionError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2010",
      messageParameters = Map.empty)
  }

  def dataTypeUnexpectedError(dataType: DataType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2011",
      messageParameters = Map("dataType" -> dataType.catalogString))
  }

  def typeUnsupportedError(dataType: DataType): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2012",
      messageParameters = Map("dataType" -> dataType.toString()))
  }

  def negativeValueUnexpectedError(
      frequencyExpression : Expression): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2013",
      messageParameters = Map("frequencyExpression" -> frequencyExpression.sql))
  }

  def addNewFunctionMismatchedWithFunctionError(funcName: String): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2014",
      messageParameters = Map("funcName" -> funcName))
  }

  def cannotGenerateCodeForIncomparableTypeError(
      codeType: String, dataType: DataType): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2015",
      messageParameters = Map(
        "codeType" -> codeType,
        "dataType" -> dataType.catalogString))
  }

  def cannotInterpolateClassIntoCodeBlockError(arg: Any): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2016",
      messageParameters = Map("arg" -> arg.getClass.getName))
  }

  def customCollectionClsNotResolvedError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2017",
      messageParameters = Map.empty)
  }

  def classUnsupportedByMapObjectsError(cls: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2018",
      messageParameters = Map("cls" -> cls.getName))
  }

  def nullAsMapKeyNotAllowedError(): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2019",
      messageParameters = Map.empty)
  }

  def methodNotDeclaredError(name: String): Throwable = {
    SparkException.internalError(
      s"""A method named "$name" is not declared in any enclosing class nor any supertype""")
  }

  def constructorNotFoundError(cls: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2020",
      messageParameters = Map("cls" -> cls.toString()))
  }

  def primaryConstructorNotFoundError(cls: Class[_]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2021",
      messageParameters = Map("cls" -> cls.toString()))
  }

  def unsupportedNaturalJoinTypeError(joinType: JoinType): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2022",
      messageParameters = Map("joinType" -> joinType.toString()))
  }

  def notExpectedUnresolvedEncoderError(attr: AttributeReference): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2023",
      messageParameters = Map("attr" -> attr.toString()))
  }

  def unsupportedEncoderError(): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2024",
      messageParameters = Map.empty)
  }

  def notOverrideExpectedMethodsError(
      className: String, m1: String, m2: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2025",
      messageParameters = Map("className" -> className, "m1" -> m1, "m2" -> m2))
  }

  def failToConvertValueToJsonError(
      value: AnyRef, cls: Class[_], dataType: DataType): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2026",
      messageParameters = Map(
        "value" -> value.toString(),
        "cls" -> cls.toString(),
        "dataType" -> dataType.toString()))
  }

  def unexpectedOperatorInCorrelatedSubquery(
      op: LogicalPlan, pos: String = ""): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2027",
      messageParameters = Map("op" -> op.toString(), "pos" -> pos))
  }

  def unreachableError(err: String = ""): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2028",
      messageParameters = Map("err" -> err))
  }

  def unsupportedRoundingMode(roundMode: BigDecimal.RoundingMode.Value): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2029",
      messageParameters = Map("roundMode" -> roundMode.toString()))
  }

  def resolveCannotHandleNestedSchema(plan: LogicalPlan): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2030",
      messageParameters = Map("plan" -> plan.toString()))
  }

  def inputExternalRowCannotBeNullError(): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2031",
      messageParameters = Map.empty)
  }

  def fieldCannotBeNullMsg(index: Int, fieldName: String): String = {
    s"The ${index}th field '$fieldName' of input row cannot be null."
  }

  def fieldCannotBeNullError(index: Int, fieldName: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2032",
      messageParameters = Map("fieldCannotBeNullMsg" -> fieldCannotBeNullMsg(index, fieldName)))
  }

  def unableToCreateDatabaseAsFailedToCreateDirectoryError(
      dbDefinition: CatalogDatabase, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2033",
      messageParameters = Map(
        "name" -> dbDefinition.name,
        "locationUri" -> dbDefinition.locationUri.toString()),
      cause = e)
  }

  def unableToDropDatabaseAsFailedToDeleteDirectoryError(
      dbDefinition: CatalogDatabase, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2034",
      messageParameters = Map(
        "name" -> dbDefinition.name,
        "locationUri" -> dbDefinition.locationUri.toString()),
      cause = e)
  }

  def unableToCreateTableAsFailedToCreateDirectoryError(
      table: String, defaultTableLocation: Path, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2035",
      messageParameters = Map(
        "table" -> table,
        "defaultTableLocation" -> defaultTableLocation.toString()),
      cause = e)
  }

  def unableToDeletePartitionPathError(partitionPath: Path, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2036",
      messageParameters = Map("partitionPath" -> partitionPath.toString()),
      cause = e)
  }

  def unableToDropTableAsFailedToDeleteDirectoryError(
      table: String, dir: Path, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2037",
      messageParameters = Map("table" -> table, "dir" -> dir.toString()),
      cause = e)
  }

  def unableToRenameTableAsFailedToRenameDirectoryError(
      oldName: String, newName: String, oldDir: Path, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2038",
      messageParameters = Map(
        "oldName" -> oldName,
        "newName" -> newName,
        "oldDir" -> oldDir.toString()),
      cause = e)
  }

  def unableToCreatePartitionPathError(partitionPath: Path, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2039",
      messageParameters = Map("partitionPath" -> partitionPath.toString()),
      cause = e)
  }

  def unableToRenamePartitionPathError(oldPartPath: Path, e: IOException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2040",
      messageParameters = Map("oldPartPath" -> oldPartPath.toString()),
      cause = e)
  }

  def methodNotImplementedError(methodName: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2041",
      messageParameters = Map("methodName" -> methodName))
  }

  def arithmeticOverflowError(e: ArithmeticException): SparkArithmeticException = {
    new SparkArithmeticException(
      errorClass = "_LEGACY_ERROR_TEMP_2042",
      messageParameters = Map(
        "message" -> e.getMessage,
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = Array.empty,
      summary = "")
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      context: SQLQueryContext = null): ArithmeticException = {
    val alternative = if (hint.nonEmpty) {
      s" Use '$hint' to tolerate overflow and return NULL instead."
    } else ""
    new SparkArithmeticException(
      errorClass = "ARITHMETIC_OVERFLOW",
      messageParameters = Map(
        "message" -> message,
        "alternative" -> alternative,
        "config" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def unaryMinusCauseOverflowError(originValue: Int): SparkArithmeticException = {
    new SparkArithmeticException(
      errorClass = "_LEGACY_ERROR_TEMP_2043",
      messageParameters = Map("sqlValue" -> toSQLValue(originValue, IntegerType)),
      context = Array.empty,
      summary = "")
  }

  def binaryArithmeticCauseOverflowError(
      eval1: Short, symbol: String, eval2: Short): SparkArithmeticException = {
    new SparkArithmeticException(
      errorClass = "_LEGACY_ERROR_TEMP_2044",
      messageParameters = Map(
        "sqlValue1" -> toSQLValue(eval1, ShortType),
        "symbol" -> symbol,
        "sqlValue2" -> toSQLValue(eval2, ShortType)),
      context = Array.empty,
      summary = "")
  }

  def intervalArithmeticOverflowError(
      message: String,
      hint: String = "",
      context: SQLQueryContext): ArithmeticException = {
    val alternative = if (hint.nonEmpty) {
      s" Use '$hint' to tolerate overflow and return NULL instead."
    } else ""
    new SparkArithmeticException(
      errorClass = "INTERVAL_ARITHMETIC_OVERFLOW",
      messageParameters = Map(
        "message" -> message,
        "alternative" -> alternative),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def failedToCompileMsg(e: Exception): String = {
    s"failed to compile: $e"
  }

  def internalCompilerError(e: InternalCompilerException): Throwable = {
    new InternalCompilerException(failedToCompileMsg(e), e)
  }

  def compilerError(e: CompileException): Throwable = {
    new CompileException(failedToCompileMsg(e), e.getLocation)
  }

  def unsupportedTableChangeError(e: IllegalArgumentException): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2045",
      messageParameters = Map("message" -> e.getMessage),
      cause = e)
  }

  def notADatasourceRDDPartitionError(split: Partition): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2046",
      messageParameters = Map("split" -> split.toString()),
      cause = null)
  }

  def dataPathNotSpecifiedError(): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2047",
      messageParameters = Map.empty)
  }

  def createStreamingSourceNotSpecifySchemaError(): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2048",
      messageParameters = Map.empty)
  }

  def inferDateWithLegacyTimeParserError(): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "CANNOT_INFER_DATE",
      messageParameters = Map.empty)
  }

  def streamedOperatorUnsupportedByDataSourceError(
      className: String, operator: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2049",
      messageParameters = Map("className" -> className, "operator" -> operator))
  }

  def multiplePathsSpecifiedError(allPaths: Seq[String]): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2050",
      messageParameters = Map("paths" -> allPaths.mkString(", ")))
  }

  def failedToFindDataSourceError(
      provider: String, error: Throwable): SparkClassNotFoundException = {
    new SparkClassNotFoundException(
      errorClass = "_LEGACY_ERROR_TEMP_2051",
      messageParameters = Map("provider" -> provider),
      cause = error)
  }

  def removedClassInSpark2Error(className: String, e: Throwable): SparkClassNotFoundException = {
    new SparkClassNotFoundException(
      errorClass = "_LEGACY_ERROR_TEMP_2052",
      messageParameters = Map("className" -> className),
      cause = e)
  }

  def incompatibleDataSourceRegisterError(e: Throwable): Throwable = {
    new SparkClassNotFoundException(
      errorClass = "INCOMPATIBLE_DATASOURCE_REGISTER",
      messageParameters = Map("message" -> e.getMessage),
      cause = e)
  }

  def sparkUpgradeInReadingDatesError(
      format: String, config: String, option: String): SparkUpgradeException = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME",
      messageParameters = Map(
        "format" -> format,
        "config" -> toSQLConf(config),
        "option" -> toDSOption(option)),
      cause = null
    )
  }

  def sparkUpgradeInWritingDatesError(format: String, config: String): SparkUpgradeException = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.WRITE_ANCIENT_DATETIME",
      messageParameters = Map(
        "format" -> format,
        "config" -> toSQLConf(config)),
      cause = null
    )
  }

  def buildReaderUnsupportedForFileFormatError(
      format: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2053",
      messageParameters = Map("format" -> format))
  }

  def taskFailedWhileWritingRowsError(cause: Throwable): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2054",
      messageParameters = Map.empty,
      cause = cause)
  }

  def readCurrentFileNotFoundError(e: FileNotFoundException): SparkFileNotFoundException = {
    new SparkFileNotFoundException(
      errorClass = "_LEGACY_ERROR_TEMP_2055",
      messageParameters = Map("message" -> e.getMessage))
  }

  def saveModeUnsupportedError(saveMode: Any, pathExists: Boolean): Throwable = {
    val errorSubClass = if (pathExists) "EXISTENT_PATH" else "NON_EXISTENT_PATH"
    new SparkIllegalArgumentException(
      errorClass = s"UNSUPPORTED_SAVE_MODE.$errorSubClass",
      messageParameters = Map("saveMode" -> toSQLValue(saveMode, StringType)))
  }

  def cannotClearOutputDirectoryError(staticPrefixPath: Path): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2056",
      messageParameters = Map("staticPrefixPath" -> staticPrefixPath.toString()),
      cause = null)
  }

  def cannotClearPartitionDirectoryError(path: Path): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2057",
      messageParameters = Map("path" -> path.toString()),
      cause = null)
  }

  def failedToCastValueToDataTypeForPartitionColumnError(
      value: String, dataType: DataType, columnName: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2058",
      messageParameters = Map(
        "value" -> value,
        "dataType" -> dataType.toString(),
        "columnName" -> columnName))
  }

  def endOfStreamError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2059",
      messageParameters = Map.empty,
      cause = null)
  }

  def fallbackV1RelationReportsInconsistentSchemaError(
      v2Schema: StructType, v1Schema: StructType): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2060",
      messageParameters = Map("v2Schema" -> v2Schema.toString(), "v1Schema" -> v1Schema.toString()))
  }

  def noRecordsFromEmptyDataReaderError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2061",
      messageParameters = Map.empty,
      cause = null)
  }

  def fileNotFoundError(e: FileNotFoundException): SparkFileNotFoundException = {
    new SparkFileNotFoundException(
      errorClass = "_LEGACY_ERROR_TEMP_2062",
      messageParameters = Map("message" -> e.getMessage))
  }

  def unsupportedSchemaColumnConvertError(
      filePath: String,
      column: String,
      logicalType: String,
      physicalType: String,
      e: Exception): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2063",
      messageParameters = Map(
        "filePath" -> filePath,
        "column" -> column,
        "logicalType" -> logicalType,
        "physicalType" -> physicalType),
      cause = e)
  }

  def cannotReadFilesError(
      e: Throwable,
      path: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2064",
      messageParameters = Map("path" -> path),
      cause = e)
  }

  def cannotCreateColumnarReaderError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2065",
      messageParameters = Map.empty,
      cause = null)
  }

  def invalidNamespaceNameError(namespace: Array[String]): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2066",
      messageParameters = Map("namespace" -> namespace.quoted))
  }

  def unsupportedPartitionTransformError(
      transform: Transform): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2067",
      messageParameters = Map("transform" -> transform.toString()))
  }

  def missingDatabaseLocationError(): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2068",
      messageParameters = Map.empty)
  }

  def cannotRemoveReservedPropertyError(property: String): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2069",
      messageParameters = Map("property" -> property))
  }

  def writingJobFailedError(cause: Throwable): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2070",
      messageParameters = Map.empty,
      cause = cause)
  }

  def commitDeniedError(
      partId: Int, taskId: Long, attemptId: Int, stageId: Int, stageAttempt: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2071",
      messageParameters = Map(
        "partId" -> partId.toString(),
        "taskId" -> taskId.toString(),
        "attemptId" -> attemptId.toString(),
        "stageId" -> stageId.toString(),
        "stageAttempt" -> stageAttempt.toString()),
      cause = null)
  }

  def unsupportedTableWritesError(ident: Identifier): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_2072",
      messageParameters = Map("idnt" -> ident.quoted),
      cause = null)
  }

  def cannotCreateJDBCTableWithPartitionsError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2073",
      messageParameters = Map.empty)
  }

  def unsupportedUserSpecifiedSchemaError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2074",
      messageParameters = Map.empty)
  }

  def writeUnsupportedForBinaryFileDataSourceError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2075",
      messageParameters = Map.empty)
  }

  def fileLengthExceedsMaxLengthError(status: FileStatus, maxLength: Int): Throwable = {
    new SparkException(
      s"The length of ${status.getPath} is ${status.getLen}, " +
        s"which exceeds the max length allowed: ${maxLength}.")
  }

  def unsupportedFieldNameError(fieldName: String): Throwable = {
    new RuntimeException(s"Unsupported field name: ${fieldName}")
  }

  def cannotSpecifyBothJdbcTableNameAndQueryError(
      jdbcTableName: String, jdbcQueryString: String): Throwable = {
    new IllegalArgumentException(
      s"Both '$jdbcTableName' and '$jdbcQueryString' can not be specified at the same time.")
  }

  def missingJdbcTableNameAndQueryError(
      jdbcTableName: String, jdbcQueryString: String): Throwable = {
    new IllegalArgumentException(
      s"Option '$jdbcTableName' or '$jdbcQueryString' is required."
    )
  }

  def emptyOptionError(optionName: String): Throwable = {
    new IllegalArgumentException(s"Option `$optionName` can not be empty.")
  }

  def invalidJdbcTxnIsolationLevelError(jdbcTxnIsolationLevel: String, value: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value `$value` for parameter `$jdbcTxnIsolationLevel`. This can be " +
        "`NONE`, `READ_UNCOMMITTED`, `READ_COMMITTED`, `REPEATABLE_READ` or `SERIALIZABLE`.")
  }

  def cannotGetJdbcTypeError(dt: DataType): Throwable = {
    new IllegalArgumentException(s"Can't get JDBC type for ${dt.catalogString}")
  }

  def unrecognizedSqlTypeError(sqlType: Int): Throwable = {
    new SparkSQLException(
      errorClass = "UNRECOGNIZED_SQL_TYPE",
      messageParameters = Map("typeName" -> sqlType.toString))
  }

  def unsupportedJdbcTypeError(content: String): Throwable = {
    new SQLException(s"Unsupported type $content")
  }

  def unsupportedArrayElementTypeBasedOnBinaryError(dt: DataType): Throwable = {
    new IllegalArgumentException(s"Unsupported array element " +
      s"type ${dt.catalogString} based on binary")
  }

  def nestedArraysUnsupportedError(): Throwable = {
    new IllegalArgumentException("Nested arrays unsupported")
  }

  def cannotTranslateNonNullValueForFieldError(pos: Int): Throwable = {
    new IllegalArgumentException(s"Can't translate non-null value for field $pos")
  }

  def invalidJdbcNumPartitionsError(n: Int, jdbcNumPartitions: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value `$n` for parameter `$jdbcNumPartitions` in table writing " +
        "via JDBC. The minimum value is 1.")
  }

  def transactionUnsupportedByJdbcServerError(): Throwable = {
    new SparkSQLFeatureNotSupportedException(
      errorClass = "UNSUPPORTED_FEATURE.JDBC_TRANSACTION",
      messageParameters = Map.empty[String, String])
  }

  def dataTypeUnsupportedYetError(dataType: DataType): Throwable = {
    new UnsupportedOperationException(s"$dataType is not supported yet.")
  }

  def unsupportedOperationForDataTypeError(dataType: DataType): Throwable = {
    new UnsupportedOperationException(s"DataType: ${dataType.catalogString}")
  }

  def inputFilterNotFullyConvertibleError(owner: String): Throwable = {
    new SparkException(s"The input filter of $owner should be fully convertible.")
  }

  def cannotReadFooterForFileError(file: Path, e: IOException): Throwable = {
    new SparkException(s"Could not read footer for file: $file", e)
  }

  def cannotReadFooterForFileError(file: FileStatus, e: RuntimeException): Throwable = {
    new IOException(s"Could not read footer for file: $file", e)
  }

  def foundDuplicateFieldInCaseInsensitiveModeError(
      requiredFieldName: String, matchedOrcFields: String): Throwable = {
    new RuntimeException(
      s"""
         |Found duplicate field(s) "$requiredFieldName": $matchedOrcFields
         |in case-insensitive mode
       """.stripMargin.replaceAll("\n", " "))
  }

  def foundDuplicateFieldInFieldIdLookupModeError(
      requiredId: Int, matchedFields: String): Throwable = {
    new RuntimeException(
      s"""
         |Found duplicate field(s) "$requiredId": $matchedFields
         |in id mapping mode
       """.stripMargin.replaceAll("\n", " "))
  }

  def failedToMergeIncompatibleSchemasError(
      left: StructType, right: StructType, e: Throwable): Throwable = {
    new SparkException(s"Failed to merge incompatible schemas $left and $right", e)
  }

  def ddlUnsupportedTemporarilyError(ddl: String): Throwable = {
    new UnsupportedOperationException(s"$ddl is not supported temporarily.")
  }

  def executeBroadcastTimeoutError(timeout: Long, ex: Option[TimeoutException]): Throwable = {
    new SparkException(
      s"""
         |Could not execute broadcast in $timeout secs. You can increase the timeout
         |for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or disable broadcast join
         |by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1
       """.stripMargin.replaceAll("\n", " "), ex.orNull)
  }

  def cannotCompareCostWithTargetCostError(cost: String): Throwable = {
    new IllegalArgumentException(s"Could not compare cost with $cost")
  }

  def unsupportedDataTypeError(dt: String): Throwable = {
    new UnsupportedOperationException(s"Unsupported data type: ${dt}")
  }

  def notSupportTypeError(dataType: DataType): Throwable = {
    new Exception(s"not support type: $dataType")
  }

  def notSupportNonPrimitiveTypeError(): Throwable = {
    new RuntimeException("Not support non-primitive type now")
  }

  def unsupportedTypeError(dataType: DataType): Throwable = {
    new Exception(s"Unsupported type: ${dataType.catalogString}")
  }

  def useDictionaryEncodingWhenDictionaryOverflowError(): Throwable = {
    new IllegalStateException(
      "Dictionary encoding should not be used because of dictionary overflow.")
  }

  def endOfIteratorError(): Throwable = {
    new NoSuchElementException("End of the iterator")
  }

  def cannotAllocateMemoryToGrowBytesToBytesMapError(): Throwable = {
    new IOException("Could not allocate memory to grow BytesToBytesMap")
  }

  def cannotAcquireMemoryToBuildLongHashedRelationError(size: Long, got: Long): Throwable = {
    new SparkException(s"Can't acquire $size bytes memory to build hash relation, " +
      s"got $got bytes")
  }

  def cannotAcquireMemoryToBuildUnsafeHashedRelationError(): Throwable = {
    new SparkOutOfMemoryError("There is not enough memory to build hash map")
  }

  def rowLargerThan256MUnsupportedError(): Throwable = {
    new UnsupportedOperationException("Does not support row that is larger than 256M")
  }

  def cannotBuildHashedRelationWithUniqueKeysExceededError(): Throwable = {
    new UnsupportedOperationException(
      "Cannot build HashedRelation with more than 1/3 billions unique keys")
  }

  def cannotBuildHashedRelationLargerThan8GError(): Throwable = {
    new UnsupportedOperationException(
      "Can not build a HashedRelation that is larger than 8G")
  }

  def failedToPushRowIntoRowQueueError(rowQueue: String): Throwable = {
    new SparkException(s"failed to push a row into $rowQueue")
  }

  def unexpectedWindowFunctionFrameError(frame: String): Throwable = {
    new RuntimeException(s"Unexpected window function frame $frame.")
  }

  def cannotParseStatisticAsPercentileError(
      stats: String, e: NumberFormatException): Throwable = {
    new IllegalArgumentException(s"Unable to parse $stats as a percentile", e)
  }

  def statisticNotRecognizedError(stats: String): Throwable = {
    new IllegalArgumentException(s"$stats is not a recognised statistic")
  }

  def unknownColumnError(unknownColumn: String): Throwable = {
    new IllegalArgumentException(s"Unknown column: $unknownColumn")
  }

  def unexpectedAccumulableUpdateValueError(o: Any): Throwable = {
    new IllegalArgumentException(s"Unexpected: $o")
  }

  def unscaledValueTooLargeForPrecisionError(): Throwable = {
    new ArithmeticException("Unscaled value too large for precision. " +
      s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error.")
  }

  def decimalPrecisionExceedsMaxPrecisionError(precision: Int, maxPrecision: Int): Throwable = {
    new ArithmeticException(
      s"Decimal precision $precision exceeds max precision $maxPrecision")
  }

  def outOfDecimalTypeRangeError(str: UTF8String): Throwable = {
    new ArithmeticException(s"out of decimal type range: $str")
  }

  def unsupportedArrayTypeError(clazz: Class[_]): Throwable = {
    new RuntimeException(s"Do not support array of type $clazz.")
  }

  def unsupportedJavaTypeError(clazz: Class[_]): Throwable = {
    new RuntimeException(s"Do not support type $clazz.")
  }

  def failedParsingStructTypeError(raw: String): Throwable = {
    new RuntimeException(s"Failed parsing ${StructType.simpleString}: $raw")
  }

  def failedMergingFieldsError(leftName: String, rightName: String, e: Throwable): Throwable = {
    new SparkException(s"Failed to merge fields '$leftName' and '$rightName'. ${e.getMessage}")
  }

  def cannotMergeDecimalTypesWithIncompatibleScaleError(
      leftScale: Int, rightScale: Int): Throwable = {
    new SparkException("Failed to merge decimal types with incompatible " +
      s"scale $leftScale and $rightScale")
  }

  def cannotMergeIncompatibleDataTypesError(left: DataType, right: DataType): Throwable = {
    new SparkException(s"Failed to merge incompatible data types ${left.catalogString}" +
      s" and ${right.catalogString}")
  }

  def exceedMapSizeLimitError(size: Int): Throwable = {
    new RuntimeException(s"Unsuccessful attempt to build maps with $size elements " +
      s"due to exceeding the map size limit ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.")
  }

  def duplicateMapKeyFoundError(key: Any): Throwable = {
    new RuntimeException(s"Duplicate map key $key was found, please check the input " +
      "data. If you want to remove the duplicated keys, you can set " +
      s"${SQLConf.MAP_KEY_DEDUP_POLICY.key} to ${SQLConf.MapKeyDedupPolicy.LAST_WIN} so that " +
      "the key inserted at last takes precedence.")
  }

  def mapDataKeyArrayLengthDiffersFromValueArrayLengthError(): Throwable = {
    new RuntimeException("The key array and value array of MapData must have the same length.")
  }

  def fieldDiffersFromDerivedLocalDateError(
      field: ChronoField, actual: Int, expected: Int, candidate: LocalDate): Throwable = {
    new DateTimeException(s"Conflict found: Field $field $actual differs from" +
      s" $field $expected derived from $candidate")
  }

  def failToParseDateTimeInNewParserError(s: String, e: Throwable): Throwable = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER",
      messageParameters = Map(
        "datetime" -> toSQLValue(s, StringType),
        "config" -> toSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key)),
      e)
  }

  def failToRecognizePatternAfterUpgradeError(pattern: String, e: Throwable): Throwable = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION",
      messageParameters = Map(
        "pattern" -> toSQLValue(pattern, StringType),
        "config" -> toSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key)),
      e)
  }

  def failToRecognizePatternError(pattern: String, e: Throwable): Throwable = {
    new RuntimeException(s"Fail to recognize '$pattern' pattern in the" +
      " DateTimeFormatter. You can form a valid datetime pattern" +
      " with the guide from https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html",
      e)
  }

  def registeringStreamingQueryListenerError(e: Exception): Throwable = {
    new SparkException("Exception when registering StreamingQueryListener", e)
  }

  def concurrentQueryInstanceError(): Throwable = {
    new SparkConcurrentModificationException(
      errorClass = "CONCURRENT_QUERY",
      messageParameters = Map.empty[String, String])
  }

  def cannotParseJsonArraysAsStructsError(): Throwable = {
    new RuntimeException("Parsing JSON arrays as structs is forbidden.")
  }

  def cannotParseStringAsDataTypeError(parser: JsonParser, token: JsonToken, dataType: DataType)
  : Throwable = {
    new RuntimeException(
      s"Cannot parse field name ${parser.getCurrentName}, " +
        s"field value ${parser.getText}, " +
        s"[$token] as target spark data type [$dataType].")
  }

  def cannotParseStringAsDataTypeError(pattern: String, value: String, dataType: DataType)
  : Throwable = {
    new RuntimeException(
      s"Cannot parse field value ${toSQLValue(value, StringType)} " +
        s"for pattern ${toSQLValue(pattern, StringType)} " +
        s"as target spark data type [$dataType].")
  }

  def failToParseEmptyStringForDataTypeError(dataType: DataType): Throwable = {
    new RuntimeException(
      s"Failed to parse an empty string for data type ${dataType.catalogString}")
  }

  def failToParseValueForDataTypeError(parser: JsonParser, token: JsonToken, dataType: DataType)
  : Throwable = {
    new RuntimeException(
      s"Failed to parse field name ${parser.getCurrentName}, " +
        s"field value ${parser.getText}, " +
        s"[$token] to target spark data type [$dataType].")
  }

  def rootConverterReturnNullError(): Throwable = {
    new RuntimeException("Root converter returned null")
  }

  def cannotHaveCircularReferencesInBeanClassError(clazz: Class[_]): Throwable = {
    new UnsupportedOperationException(
      "Cannot have circular references in bean class, but got the circular reference " +
        s"of class $clazz")
  }

  def cannotHaveCircularReferencesInClassError(t: String): Throwable = {
    new UnsupportedOperationException(
      s"cannot have circular references in class, but got the circular reference of class $t")
  }

  def cannotUseInvalidJavaIdentifierAsFieldNameError(
      fieldName: String, walkedTypePath: WalkedTypePath): Throwable = {
    new UnsupportedOperationException(s"`$fieldName` is not a valid identifier of " +
      s"Java and cannot be used as field name\n$walkedTypePath")
  }

  def cannotFindEncoderForTypeError(
      tpe: String, walkedTypePath: WalkedTypePath): Throwable = {
    new UnsupportedOperationException(s"No Encoder found for $tpe\n$walkedTypePath")
  }

  def attributesForTypeUnsupportedError(schema: Schema): Throwable = {
    new UnsupportedOperationException(s"Attributes for type $schema is not supported")
  }

  def schemaForTypeUnsupportedError(tpe: String): Throwable = {
    new UnsupportedOperationException(s"Schema for type $tpe is not supported")
  }

  def cannotFindConstructorForTypeError(tpe: String): Throwable = {
    new UnsupportedOperationException(
      s"""
         |Unable to find constructor for $tpe.
         |This could happen if $tpe is an interface, or a trait without companion object
         |constructor.
       """.stripMargin.replaceAll("\n", " "))
  }

  def paramExceedOneCharError(paramName: String): Throwable = {
    new RuntimeException(s"$paramName cannot be more than one character")
  }

  def paramIsNotIntegerError(paramName: String, value: String): Throwable = {
    new RuntimeException(s"$paramName should be an integer. Found ${toSQLValue(value, StringType)}")
  }

  def paramIsNotBooleanValueError(paramName: String): Throwable = {
    new Exception(s"$paramName flag can be true or false")
  }

  def foundNullValueForNotNullableFieldError(name: String): Throwable = {
    new RuntimeException(s"null value found but field $name is not nullable.")
  }

  def malformedCSVRecordError(): Throwable = {
    new RuntimeException("Malformed CSV record")
  }

  def elementsOfTupleExceedLimitError(): Throwable = {
    new UnsupportedOperationException("Due to Scala's limited support of tuple, " +
      "tuple with more than 22 elements are not supported.")
  }

  def expressionDecodingError(e: Exception, expressions: Seq[Expression]): Throwable = {
    new RuntimeException(s"Error while decoding: $e\n" +
      s"${expressions.map(_.simpleString(SQLConf.get.maxToStringFields)).mkString("\n")}", e)
  }

  def expressionEncodingError(e: Exception, expressions: Seq[Expression]): Throwable = {
    new RuntimeException(s"Error while encoding: $e\n" +
      s"${expressions.map(_.simpleString(SQLConf.get.maxToStringFields)).mkString("\n")}", e)
  }

  def classHasUnexpectedSerializerError(clsName: String, objSerializer: Expression): Throwable = {
    new RuntimeException(s"class $clsName has unexpected serializer: $objSerializer")
  }

  def cannotGetOuterPointerForInnerClassError(innerCls: Class[_]): Throwable = {
    new RuntimeException(s"Failed to get outer pointer for ${innerCls.getName}")
  }

  def userDefinedTypeNotAnnotatedAndRegisteredError(udt: UserDefinedType[_]): Throwable = {
    new SparkException(s"${udt.userClass.getName} is not annotated with " +
      "SQLUserDefinedType nor registered with UDTRegistration.}")
  }

  def unsupportedOperandTypeForSizeFunctionError(dataType: DataType): Throwable = {
    new UnsupportedOperationException(
      s"The size function doesn't support the operand type ${dataType.getClass.getCanonicalName}")
  }

  def unexpectedValueForStartInFunctionError(prettyName: String): RuntimeException = {
    new RuntimeException(
      s"Unexpected value for start in function $prettyName: SQL array indices start at 1.")
  }

  def unexpectedValueForLengthInFunctionError(prettyName: String): RuntimeException = {
    new RuntimeException(s"Unexpected value for length in function $prettyName: " +
      "length must be greater than or equal to 0.")
  }

  def elementAtByIndexZeroError(context: SQLQueryContext): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "ELEMENT_AT_BY_INDEX_ZERO",
      cause = null,
      messageParameters = Map.empty,
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def concatArraysWithElementsExceedLimitError(numberOfElements: Long): Throwable = {
    new RuntimeException(
      s"""
         |Unsuccessful try to concat arrays with $numberOfElements
         |elements due to exceeding the array size limit
         |${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.
       """.stripMargin.replaceAll("\n", " "))
  }

  def flattenArraysWithElementsExceedLimitError(numberOfElements: Long): Throwable = {
    new RuntimeException(
      s"""
         |Unsuccessful try to flatten an array of arrays with $numberOfElements
         |elements due to exceeding the array size limit
         |${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.
       """.stripMargin.replaceAll("\n", " "))
  }

  def createArrayWithElementsExceedLimitError(count: Any): RuntimeException = {
    new RuntimeException(
      s"""
         |Unsuccessful try to create array with $count elements
         |due to exceeding the array size limit
         |${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.
       """.stripMargin.replaceAll("\n", " "))
  }

  def unionArrayWithElementsExceedLimitError(length: Int): Throwable = {
    new RuntimeException(
      s"""
         |Unsuccessful try to union arrays with $length
         |elements due to exceeding the array size limit
         |${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.
       """.stripMargin.replaceAll("\n", " "))
  }

  def initialTypeNotTargetDataTypeError(dataType: DataType, target: String): Throwable = {
    new UnsupportedOperationException(s"Initial type ${dataType.catalogString} must be a $target")
  }

  def initialTypeNotTargetDataTypesError(dataType: DataType): Throwable = {
    new UnsupportedOperationException(
      s"Initial type ${dataType.catalogString} must be " +
        s"an ${ArrayType.simpleString}, a ${StructType.simpleString} or a ${MapType.simpleString}")
  }

  def malformedRecordsDetectedInSchemaInferenceError(e: Throwable): Throwable = {
    new SparkException("Malformed records are detected in schema inference. " +
      s"Parse Mode: ${FailFastMode.name}.", e)
  }

  def malformedJSONError(): Throwable = {
    new SparkException("Malformed JSON")
  }

  def malformedRecordsDetectedInSchemaInferenceError(dataType: DataType): Throwable = {
    new SparkException(
      s"""
         |Malformed records are detected in schema inference.
         |Parse Mode: ${FailFastMode.name}. Reasons: Failed to infer a common schema.
         |Struct types are expected, but `${dataType.catalogString}` was found.
       """.stripMargin.replaceAll("\n", " "))
  }

  def decorrelateInnerQueryThroughPlanUnsupportedError(plan: LogicalPlan): Throwable = {
    new UnsupportedOperationException(
      s"Decorrelate inner query through ${plan.nodeName} is not supported.")
  }

  def methodCalledInAnalyzerNotAllowedError(): Throwable = {
    new RuntimeException("This method should not be called in the analyzer")
  }

  def cannotSafelyMergeSerdePropertiesError(
      props1: Map[String, String],
      props2: Map[String, String],
      conflictKeys: Set[String]): Throwable = {
    new UnsupportedOperationException(
      s"""
         |Cannot safely merge SERDEPROPERTIES:
         |${props1.map { case (k, v) => s"$k=$v" }.mkString("{", ",", "}")}
         |${props2.map { case (k, v) => s"$k=$v" }.mkString("{", ",", "}")}
         |The conflict keys: ${conflictKeys.mkString(", ")}
         |""".stripMargin)
  }

  def pairUnsupportedAtFunctionError(
      r1: ValueInterval, r2: ValueInterval, function: String): Throwable = {
    new UnsupportedOperationException(s"Not supported pair: $r1, $r2 at $function()")
  }

  def onceStrategyIdempotenceIsBrokenForBatchError[TreeType <: TreeNode[_]](
      batchName: String, plan: TreeType, reOptimized: TreeType): Throwable = {
    new RuntimeException(
      s"""
         |Once strategy's idempotence is broken for batch $batchName
         |${sideBySide(plan.treeString, reOptimized.treeString).mkString("\n")}
       """.stripMargin)
  }

  def structuralIntegrityOfInputPlanIsBrokenInClassError(className: String): Throwable = {
    new RuntimeException("The structural integrity of the input plan is broken in " +
      s"$className.")
  }

  def structuralIntegrityIsBrokenAfterApplyingRuleError(
      ruleName: String, batchName: String): Throwable = {
    new RuntimeException(s"After applying rule $ruleName in batch $batchName, " +
      "the structural integrity of the plan is broken.")
  }

  def ruleIdNotFoundForRuleError(ruleName: String): Throwable = {
    new NoSuchElementException(s"Rule id not found for $ruleName")
  }

  def cannotCreateArrayWithElementsExceedLimitError(
      numElements: Long, additionalErrorMessage: String): Throwable = {
    new RuntimeException(
      s"""
         |Cannot create array with $numElements
         |elements of data due to exceeding the limit
         |${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH} elements for ArrayData.
         |$additionalErrorMessage
       """.stripMargin.replaceAll("\n", " "))
  }

  def malformedRecordsDetectedInRecordParsingError(e: BadRecordException): Throwable = {
    new SparkException("Malformed records are detected in record parsing. " +
      s"Parse Mode: ${FailFastMode.name}. To process malformed records as null " +
      "result, try setting the option 'mode' as 'PERMISSIVE'.", e)
  }

  def remoteOperationsUnsupportedError(): Throwable = {
    new RuntimeException("Remote operations not supported")
  }

  def invalidKerberosConfigForHiveServer2Error(): Throwable = {
    new IOException(
      "HiveServer2 Kerberos principal or keytab is not correctly configured")
  }

  def parentSparkUIToAttachTabNotFoundError(): Throwable = {
    new SparkException("Parent SparkUI to attach this tab to not found!")
  }

  def inferSchemaUnsupportedForHiveError(): Throwable = {
    new UnsupportedOperationException("inferSchema is not supported for hive data source.")
  }

  def requestedPartitionsMismatchTablePartitionsError(
      table: CatalogTable, partition: Map[String, Option[String]]): Throwable = {
    new SparkException(
      s"""
         |Requested partitioning does not match the ${table.identifier.table} table:
         |Requested partitions: ${partition.keys.mkString(",")}
         |Table partitions: ${table.partitionColumnNames.mkString(",")}
       """.stripMargin)
  }

  def dynamicPartitionKeyNotAmongWrittenPartitionPathsError(key: String): Throwable = {
    new SparkException(
      s"Dynamic partition key ${toSQLValue(key, StringType)} is not among written partition paths.")
  }

  def cannotRemovePartitionDirError(partitionPath: Path): Throwable = {
    new RuntimeException(s"Cannot remove partition directory '$partitionPath'")
  }

  def cannotCreateStagingDirError(message: String, e: IOException): Throwable = {
    new RuntimeException(s"Cannot create staging directory: $message", e)
  }

  def serDeInterfaceNotFoundError(e: NoClassDefFoundError): Throwable = {
    new ClassNotFoundException("The SerDe interface removed since Hive 2.3(HIVE-15167)." +
      " Please migrate your custom SerDes to Hive 2.3. See HIVE-15167 for more details.", e)
  }

  def convertHiveTableToCatalogTableError(
      e: SparkException, dbName: String, tableName: String): Throwable = {
    new SparkException(s"${e.getMessage}, db: $dbName, table: $tableName", e)
  }

  def cannotRecognizeHiveTypeError(
      e: ParseException, fieldType: String, fieldName: String): Throwable = {
    new SparkException(
      s"Cannot recognize hive type string: $fieldType, column: $fieldName", e)
  }

  def getTablesByTypeUnsupportedByHiveVersionError(): Throwable = {
    new UnsupportedOperationException("Hive 2.2 and lower versions don't support " +
      "getTablesByType. Please use Hive 2.3 or higher version.")
  }

  def dropTableWithPurgeUnsupportedError(): Throwable = {
    new UnsupportedOperationException("DROP TABLE ... PURGE")
  }

  def alterTableWithDropPartitionAndPurgeUnsupportedError(): Throwable = {
    new UnsupportedOperationException("ALTER TABLE ... DROP PARTITION ... PURGE")
  }

  def invalidPartitionFilterError(): Throwable = {
    new UnsupportedOperationException(
      """Partition filter cannot have both `"` and `'` characters""")
  }

  def getPartitionMetadataByFilterError(e: InvocationTargetException): Throwable = {
    new RuntimeException(
      s"""
         |Caught Hive MetaException attempting to get partition metadata by filter
         |from Hive. You can set the Spark configuration setting
         |${SQLConf.HIVE_METASTORE_PARTITION_PRUNING_FALLBACK_ON_EXCEPTION.key} to true to work
         |around this problem, however this will result in degraded performance. Please
         |report a bug: https://issues.apache.org/jira/browse/SPARK
       """.stripMargin.replaceAll("\n", " "), e)
  }

  def unsupportedHiveMetastoreVersionError(version: String, key: String): Throwable = {
    new UnsupportedOperationException(s"Unsupported Hive Metastore version ($version). " +
      s"Please set $key with a valid version.")
  }

  def loadHiveClientCausesNoClassDefFoundError(
      cnf: NoClassDefFoundError,
      execJars: Seq[URL],
      key: String,
      e: InvocationTargetException): Throwable = {
    new ClassNotFoundException(
      s"""
         |$cnf when creating Hive client using classpath: ${execJars.mkString(", ")}\n
         |Please make sure that jars for your version of hive and hadoop are included in the
         |paths passed to $key.
       """.stripMargin.replaceAll("\n", " "), e)
  }

  def cannotFetchTablesOfDatabaseError(dbName: String, e: Exception): Throwable = {
    new SparkException(s"Unable to fetch tables of db $dbName", e)
  }

  def illegalLocationClauseForViewPartitionError(): Throwable = {
    new SparkException("LOCATION clause illegal for view partition")
  }

  def renamePathAsExistsPathError(srcPath: Path, dstPath: Path): Throwable = {
    new SparkFileAlreadyExistsException(
      errorClass = "FAILED_RENAME_PATH",
      messageParameters = Map(
        "sourcePath" -> srcPath.toString,
        "targetPath" -> dstPath.toString))
  }

  def renameAsExistsPathError(dstPath: Path): Throwable = {
    new FileAlreadyExistsException(s"Failed to rename as $dstPath already exists")
  }

  def renameSrcPathNotFoundError(srcPath: Path): Throwable = {
    new SparkFileNotFoundException(
      errorClass = "RENAME_SRC_PATH_NOT_FOUND",
      messageParameters = Map("sourcePath" -> srcPath.toString))
  }

  def failedRenameTempFileError(srcPath: Path, dstPath: Path): Throwable = {
    new IOException(s"Failed to rename temp file $srcPath to $dstPath as rename returned false")
  }

  def legacyMetadataPathExistsError(metadataPath: Path, legacyMetadataPath: Path): Throwable = {
    new SparkException(
      s"""
         |Error: we detected a possible problem with the location of your "_spark_metadata"
         |directory and you likely need to move it before restarting this query.
         |
         |Earlier version of Spark incorrectly escaped paths when writing out the
         |"_spark_metadata" directory for structured streaming. While this was corrected in
         |Spark 3.0, it appears that your query was started using an earlier version that
         |incorrectly handled the "_spark_metadata" path.
         |
         |Correct "_spark_metadata" Directory: $metadataPath
         |Incorrect "_spark_metadata" Directory: $legacyMetadataPath
         |
         |Please move the data from the incorrect directory to the correct one, delete the
         |incorrect directory, and then restart this query. If you believe you are receiving
         |this message in error, you can disable it with the SQL conf
         |${SQLConf.STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED.key}.
       """.stripMargin)
  }

  def partitionColumnNotFoundInSchemaError(col: String, schema: StructType): Throwable = {
    new RuntimeException(s"Partition column $col not found in schema $schema")
  }

  def stateNotDefinedOrAlreadyRemovedError(): Throwable = {
    new NoSuchElementException("State is either not defined or has already been removed")
  }

  def cannotSetTimeoutDurationError(): Throwable = {
    new UnsupportedOperationException(
      "Cannot set timeout duration without enabling processing time timeout in " +
        "[map|flatMap]GroupsWithState")
  }

  def cannotGetEventTimeWatermarkError(): Throwable = {
    new UnsupportedOperationException(
      "Cannot get event time watermark timestamp without setting watermark before " +
        "[map|flatMap]GroupsWithState")
  }

  def cannotSetTimeoutTimestampError(): Throwable = {
    new UnsupportedOperationException(
      "Cannot set timeout timestamp without enabling event time timeout in " +
        "[map|flatMapGroupsWithState")
  }

  def batchMetadataFileNotFoundError(batchMetadataFile: Path): Throwable = {
    new FileNotFoundException(s"Unable to find batch $batchMetadataFile")
  }

  def multiStreamingQueriesUsingPathConcurrentlyError(
      path: String, e: FileAlreadyExistsException): Throwable = {
    new ConcurrentModificationException(
      s"Multiple streaming queries are concurrently using $path", e)
  }

  def addFilesWithAbsolutePathUnsupportedError(commitProtocol: String): Throwable = {
    new UnsupportedOperationException(
      s"$commitProtocol does not support adding files with an absolute path")
  }

  def microBatchUnsupportedByDataSourceError(
      srcName: String,
      disabledSources: String,
      table: Table): Throwable = {
    new UnsupportedOperationException(s"""
         |Data source $srcName does not support microbatch processing.
         |
         |Either the data source is disabled at
         |SQLConf.get.DISABLED_V2_STREAMING_MICROBATCH_READERS.key (The disabled sources
         |are [$disabledSources]) or the table $table does not have MICRO_BATCH_READ
         |capability. Meanwhile, the fallback, data source v1, is not available."
       """.stripMargin)
  }

  def cannotExecuteStreamingRelationExecError(): Throwable = {
    new UnsupportedOperationException("StreamingRelationExec cannot be executed")
  }

  def invalidStreamingOutputModeError(outputMode: Option[OutputMode]): Throwable = {
    new UnsupportedOperationException(s"Invalid output mode: $outputMode")
  }

  def invalidCatalogNameError(name: String): Throwable = {
    new SparkException(s"Invalid catalog name: $name")
  }

  def catalogPluginClassNotFoundError(name: String): Throwable = {
    new CatalogNotFoundException(
      s"Catalog '$name' plugin class not found: spark.sql.catalog.$name is not defined")
  }

  def catalogPluginClassNotImplementedError(name: String, pluginClassName: String): Throwable = {
    new SparkException(
      s"Plugin class for catalog '$name' does not implement CatalogPlugin: $pluginClassName")
  }

  def catalogPluginClassNotFoundForCatalogError(
      name: String,
      pluginClassName: String,
      e: Exception): Throwable = {
    new SparkException(s"Cannot find catalog plugin class for catalog '$name': $pluginClassName", e)
  }

  def catalogFailToFindPublicNoArgConstructorError(
      name: String,
      pluginClassName: String,
      e: Exception): Throwable = {
    new SparkException(
      s"Failed to find public no-arg constructor for catalog '$name': $pluginClassName)", e)
  }

  def catalogFailToCallPublicNoArgConstructorError(
      name: String,
      pluginClassName: String,
      e: Exception): Throwable = {
    new SparkException(
      s"Failed to call public no-arg constructor for catalog '$name': $pluginClassName)", e)
  }

  def cannotInstantiateAbstractCatalogPluginClassError(
      name: String,
      pluginClassName: String,
      e: Exception): Throwable = {
    new SparkException("Cannot instantiate abstract catalog plugin class for " +
      s"catalog '$name': $pluginClassName", e.getCause)
  }

  def failedToInstantiateConstructorForCatalogError(
      name: String,
      pluginClassName: String,
      e: Exception): Throwable = {
    new SparkException("Failed during instantiating constructor for catalog " +
      s"'$name': $pluginClassName", e.getCause)
  }

  def noSuchElementExceptionError(): Throwable = {
    new NoSuchElementException
  }

  def noSuchElementExceptionError(key: String): Throwable = {
    new NoSuchElementException(key)
  }

  def cannotMutateReadOnlySQLConfError(): Throwable = {
    new UnsupportedOperationException("Cannot mutate ReadOnlySQLConf.")
  }

  def cannotCloneOrCopyReadOnlySQLConfError(): Throwable = {
    new UnsupportedOperationException("Cannot clone/copy ReadOnlySQLConf.")
  }

  def cannotGetSQLConfInSchedulerEventLoopThreadError(): Throwable = {
    new RuntimeException("Cannot get SQLConf inside scheduler event loop thread.")
  }

  def unsupportedOperationExceptionError(): Throwable = {
    new UnsupportedOperationException
  }

  def nullLiteralsCannotBeCastedError(name: String): Throwable = {
    new UnsupportedOperationException(s"null literals can't be casted to $name")
  }

  def notUserDefinedTypeError(name: String, userClass: String): Throwable = {
    new SparkException(s"$name is not an UserDefinedType. Please make sure registering " +
        s"an UserDefinedType for ${userClass}")
  }

  def cannotLoadUserDefinedTypeError(name: String, userClass: String): Throwable = {
    new SparkException(s"Can not load in UserDefinedType ${name} for user class ${userClass}.")
  }

  def notPublicClassError(name: String): Throwable = {
    new UnsupportedOperationException(
      s"$name is not a public class. Only public classes are supported.")
  }

  def primitiveTypesNotSupportedError(): Throwable = {
    new UnsupportedOperationException("Primitive types are not supported.")
  }

  def fieldIndexOnRowWithoutSchemaError(): Throwable = {
    new UnsupportedOperationException("fieldIndex on a Row without schema is undefined.")
  }

  def valueIsNullError(index: Int): Throwable = {
    new NullPointerException(s"Value at index ${toSQLValue(index, IntegerType)} is null")
  }

  def onlySupportDataSourcesProvidingFileFormatError(providingClass: String): Throwable = {
    new SparkException(s"Only Data Sources providing FileFormat are supported: $providingClass")
  }

  def failToSetOriginalPermissionBackError(
      permission: FsPermission,
      path: Path,
      e: Throwable): Throwable = {
    new SparkSecurityException(
      errorClass = "RESET_PERMISSION_TO_ORIGINAL",
      messageParameters = Map(
        "permission" -> permission.toString,
        "path" -> path.toString,
        "message" -> e.getMessage))
  }

  def failToSetOriginalACLBackError(aclEntries: String, path: Path, e: Throwable): Throwable = {
    new SecurityException(s"Failed to set original ACL $aclEntries back to " +
      s"the created path: $path. Exception: ${e.getMessage}")
  }

  def multiFailuresInStageMaterializationError(error: Throwable): Throwable = {
    new SparkException("Multiple failures in stage materialization.", error)
  }

  def unrecognizedCompressionSchemaTypeIDError(typeId: Int): Throwable = {
    new UnsupportedOperationException(s"Unrecognized compression scheme type ID: $typeId")
  }

  def getParentLoggerNotImplementedError(className: String): Throwable = {
    new SQLFeatureNotSupportedException(s"$className.getParentLogger is not yet implemented.")
  }

  def cannotCreateParquetConverterForTypeError(t: DecimalType, parquetType: String): Throwable = {
    new RuntimeException(
      s"""
         |Unable to create Parquet converter for ${t.typeName}
         |whose Parquet type is $parquetType without decimal metadata. Please read this
         |column/field as Spark BINARY type.
       """.stripMargin.replaceAll("\n", " "))
  }

  def cannotCreateParquetConverterForDecimalTypeError(
      t: DecimalType, parquetType: String): Throwable = {
    new RuntimeException(
      s"""
         |Unable to create Parquet converter for decimal type ${t.json} whose Parquet type is
         |$parquetType.  Parquet DECIMAL type can only be backed by INT32, INT64,
         |FIXED_LEN_BYTE_ARRAY, or BINARY.
       """.stripMargin.replaceAll("\n", " "))
  }

  def cannotCreateParquetConverterForDataTypeError(
      t: DataType, parquetType: String): Throwable = {
    new RuntimeException(s"Unable to create Parquet converter for data type ${t.json} " +
      s"whose Parquet type is $parquetType")
  }

  def cannotAddMultiPartitionsOnNonatomicPartitionTableError(tableName: String): Throwable = {
    new UnsupportedOperationException(
      s"Nonatomic partition table $tableName can not add multiple partitions.")
  }

  def userSpecifiedSchemaUnsupportedByDataSourceError(provider: TableProvider): Throwable = {
    new UnsupportedOperationException(
      s"${provider.getClass.getSimpleName} source does not support user-specified schema.")
  }

  def cannotDropMultiPartitionsOnNonatomicPartitionTableError(tableName: String): Throwable = {
    new UnsupportedOperationException(
      s"Nonatomic partition table $tableName can not drop multiple partitions.")
  }

  def truncateMultiPartitionUnsupportedError(tableName: String): Throwable = {
    new UnsupportedOperationException(
      s"The table $tableName does not support truncation of multiple partition.")
  }

  def overwriteTableByUnsupportedExpressionError(table: Table): Throwable = {
    new SparkException(s"Table does not support overwrite by expression: $table")
  }

  def dynamicPartitionOverwriteUnsupportedByTableError(table: Table): Throwable = {
    new SparkException(s"Table does not support dynamic partition overwrite: $table")
  }

  def failedMergingSchemaError(schema: StructType, e: SparkException): Throwable = {
    new SparkException(s"Failed merging schema:\n${schema.treeString}", e)
  }

  def cannotBroadcastTableOverMaxTableRowsError(
      maxBroadcastTableRows: Long, numRows: Long): Throwable = {
    new SparkException(
      s"Cannot broadcast the table over $maxBroadcastTableRows rows: $numRows rows")
  }

  def cannotBroadcastTableOverMaxTableBytesError(
      maxBroadcastTableBytes: Long, dataSize: Long): Throwable = {
    new SparkException("Cannot broadcast the table that is larger than" +
      s" ${maxBroadcastTableBytes >> 30}GB: ${dataSize >> 30} GB")
  }

  def notEnoughMemoryToBuildAndBroadcastTableError(
      oe: OutOfMemoryError, tables: Seq[TableIdentifier]): Throwable = {
    val analyzeTblMsg = if (tables.nonEmpty) {
      " or analyze these tables through: " +
        s"${tables.map(t => s"ANALYZE TABLE $t COMPUTE STATISTICS;").mkString(" ")}."
    } else {
      "."
    }
    new OutOfMemoryError("Not enough memory to build and broadcast the table to all " +
      "worker nodes. As a workaround, you can either disable broadcast by setting " +
      s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
      s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value$analyzeTblMsg")
      .initCause(oe.getCause)
  }

  def executeCodePathUnsupportedError(execName: String): Throwable = {
    new UnsupportedOperationException(s"$execName does not support the execute() code path.")
  }

  def cannotMergeClassWithOtherClassError(className: String, otherClass: String): Throwable = {
    new UnsupportedOperationException(
      s"Cannot merge $className with $otherClass")
  }

  def continuousProcessingUnsupportedByDataSourceError(sourceName: String): Throwable = {
    new UnsupportedOperationException(
      s"Data source $sourceName does not support continuous processing.")
  }

  def failedToReadDataError(failureReason: Throwable): Throwable = {
    new SparkException("Data read failed", failureReason)
  }

  def failedToGenerateEpochMarkerError(failureReason: Throwable): Throwable = {
    new SparkException("Epoch marker generation failed", failureReason)
  }

  def foreachWriterAbortedDueToTaskFailureError(): Throwable = {
    new SparkException("Foreach writer has been aborted due to a task failure")
  }

  def integerOverflowError(message: String): Throwable = {
    new ArithmeticException(s"Integer overflow. $message")
  }

  def failedToReadDeltaFileError(fileToRead: Path, clazz: String, keySize: Int): Throwable = {
    new IOException(
      s"Error reading delta file $fileToRead of $clazz: key size cannot be $keySize")
  }

  def failedToReadSnapshotFileError(fileToRead: Path, clazz: String, message: String): Throwable = {
    new IOException(s"Error reading snapshot file $fileToRead of $clazz: $message")
  }

  def cannotPurgeAsBreakInternalStateError(): Throwable = {
    new UnsupportedOperationException("Cannot purge as it might break internal state.")
  }

  def cleanUpSourceFilesUnsupportedError(): Throwable = {
    new UnsupportedOperationException("Clean up source files is not supported when" +
      " reading from the output directory of FileStreamSink.")
  }

  def latestOffsetNotCalledError(): Throwable = {
    new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method")
  }

  def legacyCheckpointDirectoryExistsError(
      checkpointPath: Path, legacyCheckpointDir: String): Throwable = {
    new SparkException(
      s"""
         |Error: we detected a possible problem with the location of your checkpoint and you
         |likely need to move it before restarting this query.
         |
         |Earlier version of Spark incorrectly escaped paths when writing out checkpoints for
         |structured streaming. While this was corrected in Spark 3.0, it appears that your
         |query was started using an earlier version that incorrectly handled the checkpoint
         |path.
         |
         |Correct Checkpoint Directory: $checkpointPath
         |Incorrect Checkpoint Directory: $legacyCheckpointDir
         |
         |Please move the data from the incorrect directory to the correct one, delete the
         |incorrect directory, and then restart this query. If you believe you are receiving
         |this message in error, you can disable it with the SQL conf
         |${SQLConf.STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED.key}.
       """.stripMargin)
  }

  def subprocessExitedError(
      exitCode: Int, stderrBuffer: CircularBuffer, cause: Throwable): Throwable = {
    new SparkException(s"Subprocess exited with status $exitCode. " +
      s"Error: ${stderrBuffer.toString}", cause)
  }

  def outputDataTypeUnsupportedByNodeWithoutSerdeError(
      nodeName: String, dt: DataType): Throwable = {
    new SparkException(s"$nodeName without serde does not support " +
      s"${dt.getClass.getSimpleName} as output data type")
  }

  def invalidStartIndexError(numRows: Int, startIndex: Int): Throwable = {
    new ArrayIndexOutOfBoundsException(
      "Invalid `startIndex` provided for generating iterator over the array. " +
        s"Total elements: $numRows, requested `startIndex`: $startIndex")
  }

  def concurrentModificationOnExternalAppendOnlyUnsafeRowArrayError(
      className: String): Throwable = {
    new ConcurrentModificationException(
      s"The backing $className has been modified since the creation of this Iterator")
  }

  def doExecuteBroadcastNotImplementedError(nodeName: String): Throwable = {
    new UnsupportedOperationException(s"$nodeName does not implement doExecuteBroadcast")
  }

  def defaultDatabaseNotExistsError(defaultDatabase: String): Throwable = {
    new SparkException(
      errorClass = "DEFAULT_DATABASE_NOT_EXISTS",
      messageParameters = Map("defaultDatabase" -> defaultDatabase),
      cause = null
    )
  }

  def databaseNameConflictWithSystemPreservedDatabaseError(globalTempDB: String): Throwable = {
    new SparkException(
      s"""
         |$globalTempDB is a system preserved database, please rename your existing database
         |to resolve the name conflict, or set a different value for
         |${GLOBAL_TEMP_DATABASE.key}, and launch your Spark application again.
       """.stripMargin.split("\n").mkString(" "))
  }

  def commentOnTableUnsupportedError(): Throwable = {
    new SQLFeatureNotSupportedException("comment on table is not supported")
  }

  def unsupportedUpdateColumnNullabilityError(): Throwable = {
    new SQLFeatureNotSupportedException("UpdateColumnNullability is not supported")
  }

  def renameColumnUnsupportedForOlderMySQLError(): Throwable = {
    new SQLFeatureNotSupportedException(
      "Rename column is only supported for MySQL version 8.0 and above.")
  }

  def failedToExecuteQueryError(e: Throwable): QueryExecutionException = {
    val message = "Hit an error when executing a query" +
      (if (e.getMessage == null) "" else s": ${e.getMessage}")
    new QueryExecutionException(message, e)
  }

  def nestedFieldUnsupportedError(colName: String): Throwable = {
    new UnsupportedOperationException(s"Nested field $colName is not supported.")
  }

  def transformationsAndActionsNotInvokedByDriverError(): Throwable = {
    new SparkException(
      """
        |Dataset transformations and actions can only be invoked by the driver, not inside of
        |other Dataset transformations; for example, dataset1.map(x => dataset2.values.count()
        |* x) is invalid because the values transformation and count action cannot be
        |performed inside of the dataset1.map transformation. For more information,
        |see SPARK-28702.
      """.stripMargin.split("\n").mkString(" "))
  }

  def repeatedPivotsUnsupportedError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_FEATURE.REPEATED_PIVOT",
      messageParameters = Map.empty[String, String])
  }

  def pivotNotAfterGroupByUnsupportedError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_FEATURE.PIVOT_AFTER_GROUP_BY",
      messageParameters = Map.empty[String, String])
  }

  private val aesFuncName = toSQLId("aes_encrypt") + "/" + toSQLId("aes_decrypt")

  def invalidAesKeyLengthError(actualLength: Int): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "INVALID_PARAMETER_VALUE",
      messageParameters = Map(
        "parameter" -> "key",
        "functionName" -> aesFuncName,
        "expected" -> ("expects a binary value with 16, 24 or 32 bytes, " +
          s"but got ${actualLength.toString} bytes.")))
  }

  def aesModeUnsupportedError(mode: String, padding: String): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "UNSUPPORTED_FEATURE.AES_MODE",
      messageParameters = Map(
        "mode" -> mode,
        "padding" -> padding,
        "functionName" -> aesFuncName))
  }

  def aesCryptoError(detailMessage: String): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "INVALID_PARAMETER_VALUE",
      messageParameters = Map(
        "parameter" -> "expr, key",
        "functionName" -> aesFuncName,
        "expected" -> s"Detail message: $detailMessage"))
  }

  def hiveTableWithAnsiIntervalsError(tableName: String): Throwable = {
    new UnsupportedOperationException(s"Hive table $tableName with ANSI intervals is not supported")
  }

  def cannotConvertOrcTimestampToTimestampNTZError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_FEATURE.ORC_TYPE_CAST",
      messageParameters = Map(
        "orcType" -> toSQLType(TimestampType),
        "toType" -> toSQLType(TimestampNTZType)))
  }

  def cannotConvertOrcTimestampNTZToTimestampLTZError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_FEATURE.ORC_TYPE_CAST",
      messageParameters = Map(
        "orcType" -> toSQLType(TimestampNTZType),
        "toType" -> toSQLType(TimestampType)))
  }

  def writePartitionExceedConfigSizeWhenDynamicPartitionError(
      numWrittenParts: Int,
      maxDynamicPartitions: Int,
      maxDynamicPartitionsKey: String): Throwable = {
    new SparkException(
      s"Number of dynamic partitions created is $numWrittenParts" +
        s", which is more than $maxDynamicPartitions" +
        s". To solve this try to set $maxDynamicPartitionsKey" +
        s" to at least $numWrittenParts.")
  }

  def invalidNumberFormatError(valueType: String, input: String, format: String): Throwable = {
    new IllegalArgumentException(
      s"The input $valueType '$input' does not match the given number format: '$format'")
  }

  def multipleBucketTransformsError(): Throwable = {
    new UnsupportedOperationException("Multiple bucket transforms are not supported.")
  }

  def unsupportedCreateNamespaceCommentError(): Throwable = {
    new SQLFeatureNotSupportedException("Create namespace comment is not supported")
  }

  def unsupportedRemoveNamespaceCommentError(): Throwable = {
    new SQLFeatureNotSupportedException("Remove namespace comment is not supported")
  }

  def unsupportedDropNamespaceRestrictError(): Throwable = {
    new SQLFeatureNotSupportedException("Drop namespace restrict is not supported")
  }

  def timestampAddOverflowError(micros: Long, amount: Int, unit: String): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "DATETIME_OVERFLOW",
      messageParameters = Map(
        "operation" -> (s"add ${toSQLValue(amount, IntegerType)} $unit to " +
          s"${toSQLValue(DateTimeUtils.microsToInstant(micros), TimestampType)}")),
      context = Array.empty,
      summary = "")
  }

  def invalidBucketFile(path: String): Throwable = {
    new SparkException(
      errorClass = "INVALID_BUCKET_FILE",
      messageParameters = Map("path" -> path),
      cause = null)
  }

  def multipleRowSubqueryError(context: SQLQueryContext): Throwable = {
    new SparkException(
      errorClass = "MULTI_VALUE_SUBQUERY_ERROR",
      messageParameters = Map.empty,
      cause = null,
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def nullComparisonResultError(): Throwable = {
    new SparkException(
      errorClass = "NULL_COMPARISON_RESULT",
      messageParameters = Map.empty,
      cause = null)
  }

  def invalidPatternError(funcName: String, pattern: String): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "INVALID_PARAMETER_VALUE",
      messageParameters = Map(
        "parameter" -> "regexp",
        "functionName" -> toSQLId(funcName),
        "expected" -> pattern))
  }

  def tooManyArrayElementsError(
      numElements: Int,
      elementSize: Int): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "TOO_MANY_ARRAY_ELEMENTS",
      messageParameters = Map(
        "numElements" -> numElements.toString,
        "size" -> elementSize.toString))
  }
}

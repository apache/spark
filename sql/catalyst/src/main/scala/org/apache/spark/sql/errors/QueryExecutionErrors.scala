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
import java.text.{ParseException => JavaParseException}
import java.time.{DateTimeException, LocalDate}
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoField
import java.util.ConcurrentModificationException
import java.util.concurrent.TimeoutException

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileStatus, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.{Partition, SparkArithmeticException, SparkArrayIndexOutOfBoundsException, SparkClassNotFoundException, SparkConcurrentModificationException, SparkDateTimeException, SparkException, SparkFileAlreadyExistsException, SparkFileNotFoundException, SparkIllegalArgumentException, SparkIllegalStateException, SparkIndexOutOfBoundsException, SparkNoSuchElementException, SparkNoSuchMethodException, SparkNumberFormatException, SparkRuntimeException, SparkSecurityException, SparkSQLException, SparkSQLFeatureNotSupportedException, SparkUnsupportedOperationException, SparkUpgradeException}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.catalyst.analysis.UnresolvedGenerator
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ValueInterval
import org.apache.spark.sql.catalyst.trees.TreeNode
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
object QueryExecutionErrors extends QueryErrorsBase {

  def internalMissingTimezoneIdError(): Throwable = {
    new SparkIllegalStateException(errorClass = "INTERNAL_ERROR",
      messageParameters = Array("Missing timezoneId where it is mandatory."))
  }

  def logicalHintOperatorNotRemovedDuringAnalysisError(): Throwable = {
    new SparkIllegalStateException(errorClass = "INTERNAL_ERROR",
      messageParameters = Array(
        "Internal error: logical hint operator should have been removed during analysis"))
  }

  def cannotEvaluateExpressionError(expression: Expression): Throwable = {
    new SparkUnsupportedOperationException(errorClass = "INTERNAL_ERROR",
      messageParameters = Array(s"Cannot evaluate expression: $expression"))
  }

  def cannotGenerateCodeForExpressionError(expression: Expression): Throwable = {
    new SparkUnsupportedOperationException(errorClass = "INTERNAL_ERROR",
      messageParameters = Array(s"Cannot generate code for expression: $expression"))
  }

  def cannotTerminateGeneratorError(generator: UnresolvedGenerator): Throwable = {
    new SparkUnsupportedOperationException(errorClass = "INTERNAL_ERROR",
      messageParameters = Array(s"Cannot terminate expression: $generator"))
  }

  def castingCauseOverflowError(t: Any, from: DataType, to: DataType): ArithmeticException = {
    new SparkArithmeticException(errorClass = "CAST_CAUSES_OVERFLOW",
      messageParameters = Array(
        toSQLValue(t, from), toSQLType(to), toSQLConf(SQLConf.ANSI_ENABLED.key)))
  }

  def cannotChangeDecimalPrecisionError(
      value: Decimal,
      decimalPrecision: Int,
      decimalScale: Int,
      context: String): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "CANNOT_CHANGE_DECIMAL_PRECISION",
      messageParameters = Array(
        value.toDebugString,
        decimalPrecision.toString,
        decimalScale.toString,
        toSQLConf(SQLConf.ANSI_ENABLED.key),
        context))
  }

  def invalidInputSyntaxForNumericError(
      e: NumberFormatException,
      errorContext: String): NumberFormatException = {
    new NumberFormatException(s"${e.getMessage}. To return NULL instead, use 'try_cast'. " +
      s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error." + errorContext)
  }

  def invalidInputSyntaxForNumericError(
      to: DataType,
      s: UTF8String,
      errorContext: String): NumberFormatException = {
    new SparkNumberFormatException(errorClass = "INVALID_SYNTAX_FOR_CAST",
      messageParameters = Array(toSQLType(to), toSQLValue(s, StringType),
        SQLConf.ANSI_ENABLED.key, errorContext))
  }

  def cannotCastFromNullTypeError(to: DataType): Throwable = {
    new SparkException(errorClass = "CANNOT_CAST_DATATYPE",
      messageParameters = Array(NullType.typeName, to.typeName), null)
  }

  def cannotCastError(from: DataType, to: DataType): Throwable = {
    new SparkException(errorClass = "CANNOT_CAST_DATATYPE",
      messageParameters = Array(from.typeName, to.typeName), null)
  }

  def cannotParseDecimalError(): Throwable = {
    new SparkIllegalStateException(errorClass = "CANNOT_PARSE_DECIMAL",
      messageParameters = Array.empty)
  }

  def dataTypeUnsupportedError(dataType: String, failure: String): Throwable = {
    new SparkIllegalArgumentException(errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Array(dataType + failure))
  }

  def failedExecuteUserDefinedFunctionError(funcCls: String, inputTypes: String,
      outputType: String, e: Throwable): Throwable = {
    new SparkException(errorClass = "FAILED_EXECUTE_UDF",
      messageParameters = Array(funcCls, inputTypes, outputType), e)
  }

  def divideByZeroError(context: String): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "DIVIDE_BY_ZERO",
      messageParameters = Array(toSQLConf(SQLConf.ANSI_ENABLED.key), context))
  }

  def invalidArrayIndexError(index: Int, numElements: Int): ArrayIndexOutOfBoundsException = {
    invalidArrayIndexErrorInternal(index, numElements, SQLConf.ANSI_ENABLED.key)
  }

  def invalidInputIndexError(index: Int, numElements: Int): ArrayIndexOutOfBoundsException = {
    invalidArrayIndexErrorInternal(index, numElements, SQLConf.ANSI_ENABLED.key)
  }

  private def invalidArrayIndexErrorInternal(
      index: Int,
      numElements: Int,
      key: String): ArrayIndexOutOfBoundsException = {
    new SparkArrayIndexOutOfBoundsException(
      errorClass = "INVALID_ARRAY_INDEX",
      messageParameters = Array(
        toSQLValue(index, IntegerType), toSQLValue(numElements, IntegerType), toSQLConf(key)))
  }

  def invalidElementAtIndexError(
       index: Int,
       numElements: Int): ArrayIndexOutOfBoundsException = {
    new SparkArrayIndexOutOfBoundsException(
      errorClass = "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
      messageParameters =
        Array(
          toSQLValue(index, IntegerType),
          toSQLValue(numElements, IntegerType),
          toSQLConf(SQLConf.ANSI_ENABLED.key)))
  }

  def mapKeyNotExistError(key: Any, dataType: DataType, context: String): NoSuchElementException = {
    new SparkNoSuchElementException(errorClass = "MAP_KEY_DOES_NOT_EXIST",
      messageParameters = Array(toSQLValue(key, dataType), SQLConf.ANSI_ENABLED.key, context))
  }

  def invalidFractionOfSecondError(): DateTimeException = {
    new SparkDateTimeException(
      errorClass = "INVALID_FRACTION_OF_SECOND",
      Array(toSQLConf(SQLConf.ANSI_ENABLED.key)))
  }

  def ansiDateTimeParseError(e: DateTimeParseException): DateTimeParseException = {
    val newMessage = s"${e.getMessage}. " +
      s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error."
    new DateTimeParseException(newMessage, e.getParsedString, e.getErrorIndex, e.getCause)
  }

  def ansiDateTimeError(e: DateTimeException): DateTimeException = {
    val newMessage = s"${e.getMessage}. " +
      s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error."
    new DateTimeException(newMessage, e.getCause)
  }

  def ansiParseError(e: JavaParseException): JavaParseException = {
    val newMessage = s"${e.getMessage}. " +
      s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error."
    new JavaParseException(newMessage, e.getErrorOffset)
  }

  def ansiIllegalArgumentError(message: String): IllegalArgumentException = {
    val newMessage = s"$message. If necessary set ${SQLConf.ANSI_ENABLED.key} " +
      s"to false to bypass this error."
    new IllegalArgumentException(newMessage)
  }

  def ansiIllegalArgumentError(e: IllegalArgumentException): IllegalArgumentException = {
    ansiIllegalArgumentError(e.getMessage)
  }

  def overflowInSumOfDecimalError(context: String): ArithmeticException = {
    arithmeticOverflowError("Overflow in sum of decimals", errorContext = context)
  }

  def overflowInIntegralDivideError(context: String): ArithmeticException = {
    arithmeticOverflowError("Overflow in integral divide", "try_divide", context)
  }

  def mapSizeExceedArraySizeWhenZipMapError(size: Int): RuntimeException = {
    new RuntimeException(s"Unsuccessful try to zip maps with $size " +
      "unique keys due to exceeding the array size limit " +
      s"${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.")
  }

  def literalTypeUnsupportedError(v: Any): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("LITERAL_TYPE", s"${v.toString}", s"${v.getClass.toString}"))
  }

  def pivotColumnUnsupportedError(v: Any, dataType: DataType): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("PIVOT_TYPE",
        s"${v.toString}", s"${toSQLType(dataType)}"))
  }

  def noDefaultForDataTypeError(dataType: DataType): RuntimeException = {
    new RuntimeException(s"no default for type $dataType")
  }

  def orderedOperationUnsupportedByDataTypeError(dataType: DataType): Throwable = {
    new IllegalArgumentException(s"Type $dataType does not support ordered operations")
  }

  def regexGroupIndexLessThanZeroError(): Throwable = {
    new IllegalArgumentException("The specified group index cannot be less than zero")
  }

  def regexGroupIndexExceedGroupCountError(
      groupCount: Int, groupIndex: Int): Throwable = {
    new IllegalArgumentException(
      s"Regex group count is $groupCount, but the specified group index is $groupIndex")
  }

  def invalidUrlError(url: UTF8String, e: URISyntaxException): Throwable = {
    new IllegalArgumentException(s"Find an invalid url string ${url.toString}. " +
      s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error.", e)
  }

  def dataTypeOperationUnsupportedError(): Throwable = {
    new UnsupportedOperationException("dataType")
  }

  def mergeUnsupportedByWindowFunctionError(): Throwable = {
    new UnsupportedOperationException("Window Functions do not support merging.")
  }

  def dataTypeUnexpectedError(dataType: DataType): Throwable = {
    new UnsupportedOperationException(s"Unexpected data type ${dataType.catalogString}")
  }

  def typeUnsupportedError(dataType: DataType): Throwable = {
    new IllegalArgumentException(s"Unexpected type $dataType")
  }

  def negativeValueUnexpectedError(frequencyExpression : Expression): Throwable = {
    new SparkException(s"Negative values found in ${frequencyExpression.sql}")
  }

  def addNewFunctionMismatchedWithFunctionError(funcName: String): Throwable = {
    new IllegalArgumentException(s"$funcName is not matched at addNewFunction")
  }

  def cannotGenerateCodeForUncomparableTypeError(
      codeType: String, dataType: DataType): Throwable = {
    new IllegalArgumentException(
      s"cannot generate $codeType code for un-comparable type: ${dataType.catalogString}")
  }

  def cannotGenerateCodeForUnsupportedTypeError(dataType: DataType): Throwable = {
    new IllegalArgumentException(s"cannot generate code for unsupported type: $dataType")
  }

  def cannotInterpolateClassIntoCodeBlockError(arg: Any): Throwable = {
    new IllegalArgumentException(
      s"Can not interpolate ${arg.getClass.getName} into code block.")
  }

  def customCollectionClsNotResolvedError(): Throwable = {
    new UnsupportedOperationException("not resolved")
  }

  def classUnsupportedByMapObjectsError(cls: Class[_]): RuntimeException = {
    new RuntimeException(s"class `${cls.getName}` is not supported by `MapObjects` as " +
      "resulting collection.")
  }

  def nullAsMapKeyNotAllowedError(): RuntimeException = {
    new RuntimeException("Cannot use null as map key!")
  }

  def methodNotDeclaredError(name: String): Throwable = {
    new SparkNoSuchMethodException(errorClass = "INTERNAL_ERROR",
      messageParameters = Array(
        s"""A method named "$name" is not declared in any enclosing class nor any supertype"""))
  }

  def constructorNotFoundError(cls: String): Throwable = {
    new RuntimeException(s"Couldn't find a valid constructor on $cls")
  }

  def primaryConstructorNotFoundError(cls: Class[_]): Throwable = {
    new RuntimeException(s"Couldn't find a primary constructor on $cls")
  }

  def unsupportedNaturalJoinTypeError(joinType: JoinType): Throwable = {
    new RuntimeException("Unsupported natural join type " + joinType)
  }

  def notExpectedUnresolvedEncoderError(attr: AttributeReference): Throwable = {
    new RuntimeException(s"Unresolved encoder expected, but $attr was found.")
  }

  def unsupportedEncoderError(): Throwable = {
    new RuntimeException("Only expression encoders are supported for now.")
  }

  def notOverrideExpectedMethodsError(className: String, m1: String, m2: String): Throwable = {
    new RuntimeException(s"$className must override either $m1 or $m2")
  }

  def failToConvertValueToJsonError(value: AnyRef, cls: Class[_], dataType: DataType): Throwable = {
    new RuntimeException(s"Failed to convert value $value (class of $cls) " +
      s"with the type of $dataType to JSON.")
  }

  def unexpectedOperatorInCorrelatedSubquery(op: LogicalPlan, pos: String = ""): Throwable = {
    new RuntimeException(s"Unexpected operator $op in correlated subquery" + pos)
  }

  def unreachableError(err: String = ""): Throwable = {
    new RuntimeException("This line should be unreachable" + err)
  }

  def unsupportedRoundingMode(roundMode: BigDecimal.RoundingMode.Value): Throwable = {
    new RuntimeException(s"Not supported rounding mode: $roundMode")
  }

  def resolveCannotHandleNestedSchema(plan: LogicalPlan): Throwable = {
    new RuntimeException(s"Can not handle nested schema yet...  plan $plan")
  }

  def inputExternalRowCannotBeNullError(): RuntimeException = {
    new RuntimeException("The input external row cannot be null.")
  }

  def fieldCannotBeNullMsg(index: Int, fieldName: String): String = {
    s"The ${index}th field '$fieldName' of input row cannot be null."
  }

  def fieldCannotBeNullError(index: Int, fieldName: String): RuntimeException = {
    new RuntimeException(fieldCannotBeNullMsg(index, fieldName))
  }

  def unableToCreateDatabaseAsFailedToCreateDirectoryError(
      dbDefinition: CatalogDatabase, e: IOException): Throwable = {
    new SparkException(s"Unable to create database ${dbDefinition.name} as failed " +
      s"to create its directory ${dbDefinition.locationUri}", e)
  }

  def unableToDropDatabaseAsFailedToDeleteDirectoryError(
      dbDefinition: CatalogDatabase, e: IOException): Throwable = {
    new SparkException(s"Unable to drop database ${dbDefinition.name} as failed " +
      s"to delete its directory ${dbDefinition.locationUri}", e)
  }

  def unableToCreateTableAsFailedToCreateDirectoryError(
      table: String, defaultTableLocation: Path, e: IOException): Throwable = {
    new SparkException(s"Unable to create table $table as failed " +
      s"to create its directory $defaultTableLocation", e)
  }

  def unableToDeletePartitionPathError(partitionPath: Path, e: IOException): Throwable = {
    new SparkException(s"Unable to delete partition path $partitionPath", e)
  }

  def unableToDropTableAsFailedToDeleteDirectoryError(
      table: String, dir: Path, e: IOException): Throwable = {
    new SparkException(s"Unable to drop table $table as failed " +
      s"to delete its directory $dir", e)
  }

  def unableToRenameTableAsFailedToRenameDirectoryError(
      oldName: String, newName: String, oldDir: Path, e: IOException): Throwable = {
    new SparkException(s"Unable to rename table $oldName to $newName as failed " +
      s"to rename its directory $oldDir", e)
  }

  def unableToCreatePartitionPathError(partitionPath: Path, e: IOException): Throwable = {
    new SparkException(s"Unable to create partition path $partitionPath", e)
  }

  def unableToRenamePartitionPathError(oldPartPath: Path, e: IOException): Throwable = {
    new SparkException(s"Unable to rename partition path $oldPartPath", e)
  }

  def methodNotImplementedError(methodName: String): Throwable = {
    new UnsupportedOperationException(s"$methodName is not implemented")
  }

  def arithmeticOverflowError(e: ArithmeticException): ArithmeticException = {
    new ArithmeticException(s"${e.getMessage}. If necessary set ${SQLConf.ANSI_ENABLED.key} " +
      s"to false to bypass this error.")
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      errorContext: String = ""): ArithmeticException = {
    val alternative = if (hint.nonEmpty) s" To return NULL instead, use '$hint'." else ""
    new SparkArithmeticException("ARITHMETIC_OVERFLOW",
      Array(message, alternative, SQLConf.ANSI_ENABLED.key, errorContext))
  }

  def unaryMinusCauseOverflowError(originValue: Int): ArithmeticException = {
    arithmeticOverflowError(s"- ${toSQLValue(originValue, IntegerType)} caused overflow")
  }

  def binaryArithmeticCauseOverflowError(
      eval1: Short, symbol: String, eval2: Short): ArithmeticException = {
    arithmeticOverflowError(
      s"${toSQLValue(eval1, ShortType)} $symbol ${toSQLValue(eval2, ShortType)} caused overflow")
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
    new SparkException(s"Unsupported table change: ${e.getMessage}", e)
  }

  def notADatasourceRDDPartitionError(split: Partition): Throwable = {
    new SparkException(s"[BUG] Not a DataSourceRDDPartition: $split")
  }

  def dataPathNotSpecifiedError(): Throwable = {
    new IllegalArgumentException("'path' is not specified")
  }

  def createStreamingSourceNotSpecifySchemaError(): Throwable = {
    new IllegalArgumentException(
      s"""
         |Schema must be specified when creating a streaming source DataFrame. If some
         |files already exist in the directory, then depending on the file format you
         |may be able to create a static DataFrame on that directory with
         |'spark.read.load(directory)' and infer schema from it.
       """.stripMargin)
  }

  def streamedOperatorUnsupportedByDataSourceError(
      className: String, operator: String): Throwable = {
    new UnsupportedOperationException(
      s"Data source $className does not support streamed $operator")
  }

  def multiplePathsSpecifiedError(allPaths: Seq[String]): Throwable = {
    new IllegalArgumentException("Expected exactly one path to be specified, but " +
      s"got: ${allPaths.mkString(", ")}")
  }

  def failedToFindDataSourceError(provider: String, error: Throwable): Throwable = {
    new ClassNotFoundException(
      s"""
         |Failed to find data source: $provider. Please find packages at
         |https://spark.apache.org/third-party-projects.html
       """.stripMargin, error)
  }

  def removedClassInSpark2Error(className: String, e: Throwable): Throwable = {
    new ClassNotFoundException(s"$className was removed in Spark 2.0. " +
      "Please check if your library is compatible with Spark 2.0", e)
  }

  def incompatibleDataSourceRegisterError(e: Throwable): Throwable = {
    new SparkClassNotFoundException("INCOMPATIBLE_DATASOURCE_REGISTER", Array(e.getMessage), e)
  }

  def sparkUpgradeInReadingDatesError(
      format: String, config: String, option: String): SparkUpgradeException = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION",
      messageParameters = Array(
        "READ_ANCIENT_DATETIME",
        format,
        toSQLConf(config),
        option,
        toSQLConf(config),
        option),
      cause = null
    )
  }

  def sparkUpgradeInWritingDatesError(format: String, config: String): SparkUpgradeException = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION",
      messageParameters = Array(
        "WRITE_ANCIENT_DATETIME",
        format,
        toSQLConf(config),
        toSQLConf(config)),
      cause = null
    )
  }

  def buildReaderUnsupportedForFileFormatError(format: String): Throwable = {
    new UnsupportedOperationException(s"buildReader is not supported for $format")
  }

  def jobAbortedError(cause: Throwable): Throwable = {
    new SparkException("Job aborted.", cause)
  }

  def taskFailedWhileWritingRowsError(cause: Throwable): Throwable = {
    new SparkException("Task failed while writing rows.", cause)
  }

  def readCurrentFileNotFoundError(e: FileNotFoundException): Throwable = {
    new FileNotFoundException(
      s"""
         |${e.getMessage}\n
         |It is possible the underlying files have been updated. You can explicitly invalidate
         |the cache in Spark by running 'REFRESH TABLE tableName' command in SQL or by
         |recreating the Dataset/DataFrame involved.
       """.stripMargin)
  }

  def saveModeUnsupportedError(saveMode: Any, pathExists: Boolean): Throwable = {
    pathExists match {
      case true => new SparkIllegalArgumentException(errorClass = "UNSUPPORTED_SAVE_MODE",
        messageParameters = Array("EXISTENT_PATH", toSQLValue(saveMode, StringType)))
      case _ => new SparkIllegalArgumentException(errorClass = "UNSUPPORTED_SAVE_MODE",
        messageParameters = Array("NON_EXISTENT_PATH", toSQLValue(saveMode, StringType)))
    }
  }

  def cannotClearOutputDirectoryError(staticPrefixPath: Path): Throwable = {
    new IOException(s"Unable to clear output directory $staticPrefixPath prior to writing to it")
  }

  def cannotClearPartitionDirectoryError(path: Path): Throwable = {
    new IOException(s"Unable to clear partition directory $path prior to writing to it")
  }

  def failedToCastValueToDataTypeForPartitionColumnError(
      value: String, dataType: DataType, columnName: String): Throwable = {
    new RuntimeException(s"Failed to cast value `$value` to " +
      s"`$dataType` for partition column `$columnName`")
  }

  def endOfStreamError(): Throwable = {
    new NoSuchElementException("End of stream")
  }

  def fallbackV1RelationReportsInconsistentSchemaError(
      v2Schema: StructType, v1Schema: StructType): Throwable = {
    new IllegalArgumentException(
      "The fallback v1 relation reports inconsistent schema:\n" +
        "Schema of v2 scan:     " + v2Schema + "\n" +
        "Schema of v1 relation: " + v1Schema)
  }

  def noRecordsFromEmptyDataReaderError(): Throwable = {
    new IOException("No records should be returned from EmptyDataReader")
  }

  def fileNotFoundError(e: FileNotFoundException): Throwable = {
    new FileNotFoundException(
      e.getMessage + "\n" +
        "It is possible the underlying files have been updated. " +
        "You can explicitly invalidate the cache in Spark by " +
        "recreating the Dataset/DataFrame involved.")
  }

  def unsupportedSchemaColumnConvertError(
      filePath: String,
      column: String,
      logicalType: String,
      physicalType: String,
      e: Exception): Throwable = {
    val message = "Parquet column cannot be converted in " +
      s"file $filePath. Column: $column, " +
      s"Expected: $logicalType, Found: $physicalType"
    new QueryExecutionException(message, e)
  }

  def cannotReadFilesError(
      e: Throwable,
      path: String): Throwable = {
    val message = s"Encountered error while reading file $path. Details: "
    new QueryExecutionException(message, e)
  }

  def cannotCreateColumnarReaderError(): Throwable = {
    new UnsupportedOperationException("Cannot create columnar reader.")
  }

  def invalidNamespaceNameError(namespace: Array[String]): Throwable = {
    new IllegalArgumentException(s"Invalid namespace name: ${namespace.quoted}")
  }

  def unsupportedPartitionTransformError(transform: Transform): Throwable = {
    new UnsupportedOperationException(
      s"Unsupported partition transform: $transform")
  }

  def missingDatabaseLocationError(): Throwable = {
    new IllegalArgumentException("Missing database location")
  }

  def cannotRemoveReservedPropertyError(property: String): Throwable = {
    new UnsupportedOperationException(s"Cannot remove reserved property: $property")
  }

  def writingJobFailedError(cause: Throwable): Throwable = {
    new SparkException("Writing job failed.", cause)
  }

  def writingJobAbortedError(e: Throwable): Throwable = {
    new SparkException(
      errorClass = "WRITING_JOB_ABORTED",
      messageParameters = Array.empty,
      cause = e)
  }

  def commitDeniedError(
      partId: Int, taskId: Long, attemptId: Int, stageId: Int, stageAttempt: Int): Throwable = {
    val message = s"Commit denied for partition $partId (task $taskId, attempt $attemptId, " +
      s"stage $stageId.$stageAttempt)"
    new CommitDeniedException(message, stageId, partId, attemptId)
  }

  def unsupportedTableWritesError(ident: Identifier): Throwable = {
    new SparkException(
      s"Table implementation does not support writes: ${ident.quoted}")
  }

  def cannotCreateJDBCTableWithPartitionsError(): Throwable = {
    new UnsupportedOperationException("Cannot create JDBC table with partition")
  }

  def unsupportedUserSpecifiedSchemaError(): Throwable = {
    new UnsupportedOperationException("user-specified schema")
  }

  def writeUnsupportedForBinaryFileDataSourceError(): Throwable = {
    new UnsupportedOperationException("Write is not supported for binary file data source")
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
    new SparkSQLException(errorClass = "UNRECOGNIZED_SQL_TYPE", Array(sqlType.toString))
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
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("JDBC_TRANSACTION"))
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
       """.stripMargin.replaceAll("\n", " "), ex.getOrElse(null))
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

  def cannotMergeDecimalTypesWithIncompatiblePrecisionAndScaleError(
      leftPrecision: Int, rightPrecision: Int, leftScale: Int, rightScale: Int): Throwable = {
    new SparkException("Failed to merge decimal types with incompatible " +
      s"precision $leftPrecision and $rightPrecision & scale $leftScale and $rightScale")
  }

  def cannotMergeDecimalTypesWithIncompatiblePrecisionError(
      leftPrecision: Int, rightPrecision: Int): Throwable = {
    new SparkException("Failed to merge decimal types with incompatible " +
      s"precision $leftPrecision and $rightPrecision")
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
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION",
      messageParameters = Array(
        "PARSE_DATETIME_BY_NEW_PARSER",
        toSQLValue(s, StringType),
        toSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key)),
      e)
  }

  def failToFormatDateTimeInNewFormatterError(
      resultCandidate: String, e: Throwable): Throwable = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION",
      messageParameters = Array(
        "PARSE_DATETIME_BY_NEW_PARSER",
        toSQLValue(resultCandidate, StringType),
        toSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key)),
      e)
  }

  def failToRecognizePatternAfterUpgradeError(pattern: String, e: Throwable): Throwable = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION",
      messageParameters = Array(
        "DATETIME_PATTERN_RECOGNITION",
        toSQLValue(pattern, StringType),
        toSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key)),
      e)
  }

  def failToRecognizePatternError(pattern: String, e: Throwable): Throwable = {
    new RuntimeException(s"Fail to recognize '$pattern' pattern in the" +
      " DateTimeFormatter. You can form a valid datetime pattern" +
      " with the guide from https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html",
      e)
  }

  def cannotCastToDateTimeError(
      value: Any, from: DataType, to: DataType, errorContext: String): Throwable = {
    val valueString = toSQLValue(value, from)
    new SparkDateTimeException("INVALID_SYNTAX_FOR_CAST",
      Array(toSQLType(to), valueString, SQLConf.ANSI_ENABLED.key, errorContext))
  }

  def registeringStreamingQueryListenerError(e: Exception): Throwable = {
    new SparkException("Exception when registering StreamingQueryListener", e)
  }

  def concurrentQueryInstanceError(): Throwable = {
    new SparkConcurrentModificationException("CONCURRENT_QUERY", Array.empty)
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

  def invalidInputSyntaxForBooleanError(
      s: UTF8String,
      errorContext: String): UnsupportedOperationException = {
    new UnsupportedOperationException(s"invalid input syntax for type boolean: $s. " +
      s"To return NULL instead, use 'try_cast'. If necessary set ${SQLConf.ANSI_ENABLED.key} " +
      "to false to bypass this error." + errorContext)
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

  def sqlArrayIndexNotStartAtOneError(): ArrayIndexOutOfBoundsException = {
    new ArrayIndexOutOfBoundsException("SQL array indices start at 1")
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

  def cannotConvertColumnToJSONError(name: String, dataType: DataType): Throwable = {
    new UnsupportedOperationException(
      s"Unable to convert column $name of type ${dataType.catalogString} to JSON.")
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

  def indexOutOfBoundsOfArrayDataError(idx: Int): Throwable = {
    new SparkIndexOutOfBoundsException(
      errorClass = "INDEX_OUT_OF_BOUNDS", Array(toSQLValue(idx, IntegerType)))
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
    new SparkFileAlreadyExistsException(errorClass = "FAILED_RENAME_PATH",
      Array(srcPath.toString, dstPath.toString))
  }

  def renameAsExistsPathError(dstPath: Path): Throwable = {
    new FileAlreadyExistsException(s"Failed to rename as $dstPath already exists")
  }

  def renameSrcPathNotFoundError(srcPath: Path): Throwable = {
    new SparkFileNotFoundException(errorClass = "RENAME_SRC_PATH_NOT_FOUND",
      Array(srcPath.toString))
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

  def microBatchUnsupportedByDataSourceError(srcName: String): Throwable = {
    new UnsupportedOperationException(
      s"Data source $srcName does not support microbatch processing.")
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
      pluginClassName: String): Throwable = {
    new SparkException(s"Cannot find catalog plugin class for catalog '$name': $pluginClassName")
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
    new SparkSecurityException(errorClass = "FAILED_SET_ORIGINAL_PERMISSION_BACK",
      Array(permission.toString, path.toString, e.getMessage))
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

  def notEnoughMemoryToBuildAndBroadcastTableError(oe: OutOfMemoryError): Throwable = {
    new OutOfMemoryError("Not enough memory to build and broadcast the table to all " +
      "worker nodes. As a workaround, you can either disable broadcast by setting " +
      s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
      s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
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
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("REPEATED_PIVOT"))
  }

  def pivotNotAfterGroupByUnsupportedError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("PIVOT_AFTER_GROUP_BY"))
  }

  private val aesFuncName = toSQLId("aes_encrypt") + "/" + toSQLId("aes_decrypt")

  def invalidAesKeyLengthError(actualLength: Int): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "INVALID_PARAMETER_VALUE",
      messageParameters = Array(
        "key",
        s"the $aesFuncName function",
        s"expects a binary value with 16, 24 or 32 bytes, but got ${actualLength.toString} bytes."))
  }

  def aesModeUnsupportedError(mode: String, padding: String): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("AES_MODE", mode, padding, aesFuncName))
  }

  def aesCryptoError(detailMessage: String): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "INVALID_PARAMETER_VALUE",
      messageParameters = Array(
        "expr, key",
        s"the $aesFuncName function",
        s"Detail message: $detailMessage"))
  }

  def hiveTableWithAnsiIntervalsError(tableName: String): Throwable = {
    new UnsupportedOperationException(s"Hive table $tableName with ANSI intervals is not supported")
  }

  def cannotConvertOrcTimestampToTimestampNTZError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("ORC_TYPE_CAST",
        toSQLType(TimestampType),
        toSQLType(TimestampNTZType)))
  }

  def cannotConvertOrcTimestampNTZToTimestampLTZError(): Throwable = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_FEATURE",
      messageParameters = Array("ORC_TYPE_CAST",
        toSQLType(TimestampNTZType),
        toSQLType(TimestampType)))
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
      messageParameters = Array(
        s"add ${toSQLValue(amount, IntegerType)} $unit to " +
        s"${toSQLValue(DateTimeUtils.microsToInstant(micros), TimestampType)}"))
  }
}

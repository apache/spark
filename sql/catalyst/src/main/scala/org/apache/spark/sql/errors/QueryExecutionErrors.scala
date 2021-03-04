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
import java.net.URISyntaxException
import java.time.DateTimeException

import org.apache.hadoop.fs.Path
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.{Partition, SparkException}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.sql.catalyst.analysis.UnresolvedGenerator
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.expressions.{Expression, UnevaluableAggregate}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.types.{DataType, Decimal, StructType}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * Object for grouping error messages from (most) exceptions thrown during query execution.
 * This does not include exceptions thrown during the eager execution of commands, which are
 * grouped into [[QueryCompilationErrors]].
 */
object QueryExecutionErrors {

  def columnChangeUnsupportedError(): Throwable = {
    new UnsupportedOperationException("Please add an implementation for a column change here")
  }

  def logicalHintOperatorNotRemovedDuringAnalysisError(): Throwable = {
    new IllegalStateException(
      "Internal error: logical hint operator should have been removed during analysis")
  }

  def cannotEvaluateExpressionError(expression: Expression): Throwable = {
    new UnsupportedOperationException(s"Cannot evaluate expression: $expression")
  }

  def cannotGenerateCodeForExpressionError(expression: Expression): Throwable = {
    new UnsupportedOperationException(s"Cannot generate code for expression: $expression")
  }

  def cannotTerminateGeneratorError(generator: UnresolvedGenerator): Throwable = {
    new UnsupportedOperationException(s"Cannot terminate expression: $generator")
  }

  def castingCauseOverflowError(t: Any, targetType: String): ArithmeticException = {
    new ArithmeticException(s"Casting $t to $targetType causes overflow")
  }

  def cannotChangeDecimalPrecisionError(
      value: Decimal, decimalPrecision: Int, decimalScale: Int): ArithmeticException = {
    new ArithmeticException(s"${value.toDebugString} cannot be represented as " +
      s"Decimal($decimalPrecision, $decimalScale).")
  }

  def invalidInputSyntaxForNumericError(s: UTF8String): NumberFormatException = {
    new NumberFormatException(s"invalid input syntax for type numeric: $s")
  }

  def cannotCastFromNullTypeError(to: DataType): Throwable = {
    new SparkException(s"should not directly cast from NullType to $to.")
  }

  def cannotCastError(from: DataType, to: DataType): Throwable = {
    new SparkException(s"Cannot cast $from to $to.")
  }

  def cannotParseDecimalError(): Throwable = {
    new IllegalArgumentException("Cannot parse any decimal")
  }

  def simpleStringWithNodeIdUnsupportedError(nodeName: String): Throwable = {
    new UnsupportedOperationException(s"$nodeName does not implement simpleStringWithNodeId")
  }

  def evaluateUnevaluableAggregateUnsupportedError(
      methodName: String, unEvaluable: UnevaluableAggregate): Throwable = {
    new UnsupportedOperationException(s"Cannot evaluate $methodName: $unEvaluable")
  }

  def dataTypeUnsupportedError(dt: DataType): Throwable = {
    new SparkException(s"Unsupported data type $dt")
  }

  def dataTypeUnsupportedError(dataType: String, failure: String): Throwable = {
    new IllegalArgumentException(s"Unsupported dataType: $dataType, $failure")
  }

  def failedExecuteUserDefinedFunctionError(funcCls: String, inputTypes: String,
      outputType: String, e: Throwable): Throwable = {
    new SparkException(
      s"Failed to execute user defined function ($funcCls: ($inputTypes) => $outputType)", e)
  }

  def divideByZeroError(): ArithmeticException = {
    new ArithmeticException("divide by zero")
  }

  def invalidArrayIndexError(index: Int, numElements: Int): ArrayIndexOutOfBoundsException = {
    new ArrayIndexOutOfBoundsException(s"Invalid index: $index, numElements: $numElements")
  }

  def mapKeyNotExistError(key: Any): NoSuchElementException = {
    new NoSuchElementException(s"Key $key does not exist.")
  }

  def rowFromCSVParserNotExpectedError(): Throwable = {
    new IllegalArgumentException("Expected one row from CSV parser.")
  }

  def inputTypeUnsupportedError(dataType: DataType): Throwable = {
    new IllegalArgumentException(s"Unsupported input type ${dataType.catalogString}")
  }

  def invalidFractionOfSecondError(): DateTimeException = {
    new DateTimeException("The fraction of sec must be zero. Valid range is [0, 60].")
  }

  def overflowInSumOfDecimalError(): ArithmeticException = {
    new ArithmeticException("Overflow in sum of decimals.")
  }

  def mapSizeExceedArraySizeWhenZipMapError(size: Int): RuntimeException = {
    new RuntimeException(s"Unsuccessful try to zip maps with $size " +
      "unique keys due to exceeding the array size limit " +
      s"${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.")
  }

  def copyNullFieldNotAllowedError(): Throwable = {
    new IllegalStateException("Do not attempt to copy a null field")
  }

  def literalTypeUnsupportedError(v: Any): RuntimeException = {
    new RuntimeException(s"Unsupported literal type ${v.getClass} $v")
  }

  def noDefaultForDataTypeError(dataType: DataType): RuntimeException = {
    new RuntimeException(s"no default for type $dataType")
  }

  def doGenCodeOfAliasShouldNotBeCalledError(): Throwable = {
    new IllegalStateException("Alias.doGenCode should not be called.")
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
    new IllegalArgumentException(s"Find an invaild url string ${url.toString}", e)
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
    new NoSuchMethodException(s"""A method named "$name" is not declared """ +
      "in any enclosing class nor any supertype")
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

  def tableStatsNotSpecifiedError(): Throwable = {
    new IllegalStateException("table stats must be specified.")
  }

  def unaryMinusCauseOverflowError(originValue: Short): ArithmeticException = {
    new ArithmeticException(s"- $originValue caused overflow.")
  }

  def binaryArithmeticCauseOverflowError(
      eval1: Short, symbol: String, eval2: Short): ArithmeticException = {
    new ArithmeticException(s"$eval1 $symbol $eval2 caused overflow.")
  }

  def failedSplitSubExpressionMsg(length: Int): String = {
    "Failed to split subexpression code into small functions because " +
      s"the parameter length of at least one split function went over the JVM limit: $length"
  }

  def failedSplitSubExpressionError(length: Int): Throwable = {
    new IllegalStateException(failedSplitSubExpressionMsg(length))
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

  def cannotDropNonemptyNamespaceError(namespace: Seq[String]): Throwable = {
    new SparkException(
      s"Cannot drop a non-empty namespace: ${namespace.quoted}. " +
        "Use CASCADE option to drop a non-empty namespace.")
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

  def cannotReadParquetFilesError(e: Exception): Throwable = {
    val message = "Encounter error while reading parquet files. " +
      "One possible cause: Parquet column cannot be converted in the " +
      "corresponding files. Details: "
    throw new QueryExecutionException(message, e)
  }

  def cannotBuildColumnarReaderError(): Throwable = {
    new UnsupportedOperationException("Cannot create columnar reader.")
  }

  def invalidNamespaceNameError(namespace: Array[String]): Throwable = {
    new IllegalArgumentException(s"Invalid namespace name: ${namespace.quoted}")
  }

  def unsupportedPartitionTransformError(transform: Transform): Throwable = {
    new UnsupportedOperationException(
      s"SessionCatalog does not support partition transform: $transform")
  }

  def missingDatabaseLocationError(): Throwable = {
    new IllegalArgumentException("Missing database location")
  }

  def cannotRemoveReservedPropertyError(property: String): Throwable = {
    new UnsupportedOperationException(s"Cannot remove reserved property: $property")
  }

  def namespaceIsNotEmptyError(namespace: Array[String]): Throwable = {
    new IllegalStateException(s"Namespace ${namespace.quoted} is not empty")
  }

  def writingJobFailedError(cause: Throwable): Throwable = {
    new SparkException("Writing job failed.", cause)
  }

  def writingJobAbortedError(e: Throwable): Throwable = {
    new SparkException("Writing job aborted.", e)
  }

  def commitDeniedError(
      partId: Int, taskId: Long, attemptId: Int, stageId: Int, stageAttempt: Int): Throwable = {
    val message = s"Commit denied for partition $partId (task $taskId, attempt $attemptId, " +
      s"stage $stageId.$stageAttempt)"
    new CommitDeniedException(message, stageId, partId, attemptId)
  }

  def unsupportedWritesError(ident: Identifier): Throwable = {
    new SparkException(
      s"Table implementation does not support writes: ${ident.quoted}")
  }

  def cannotCreateJDBCTableWithPartitions(): Throwable = {
    new UnsupportedOperationException("Cannot create JDBC table with partition")
  }
}

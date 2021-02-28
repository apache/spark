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

import java.io.IOException
import java.net.URISyntaxException
import java.time.DateTimeException

import org.apache.hadoop.fs.Path
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.UnresolvedGenerator
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
import org.apache.spark.sql.catalyst.expressions.{Expression, UnevaluableAggregate}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

/**
 * Object for grouping all error messages of the query runtime.
 * Currently it includes all SparkExceptions and RuntimeExceptions(e.g.
 * UnsupportedOperationException, IllegalStateException).
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
}

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

import com.fasterxml.jackson.core.JsonToken

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

abstract class PartialValueException(val cause: Throwable) extends Exception(cause) {
  def partialResult: Serializable
  override def getStackTrace(): Array[StackTraceElement] = cause.getStackTrace()
  override def fillInStackTrace(): Throwable = this
}

/**
 * Exception thrown when the underlying parser returns a partial result of parsing an object/row.
 * @param partialResult the partial result of parsing a bad record.
 * @param cause the actual exception about why the parser cannot return full result.
 */
case class PartialResultException(
    override val partialResult: InternalRow,
    override val cause: Throwable) extends PartialValueException(cause)

/**
 * Exception thrown when the underlying parser returns a partial array result.
 * @param partialResult the partial array result.
 * @param cause the actual exception about why the parser cannot return full result.
 */
case class PartialArrayDataResultException(
    override val partialResult: ArrayData,
    override val cause: Throwable) extends PartialValueException(cause)

/**
 * Exception thrown when the underlying parser returns a partial map result.
 * @param partialResult the partial map result.
 * @param cause the actual exception about why the parser cannot return full result.
 */
case class PartialMapDataResultException(
    override val partialResult: MapData,
    override val cause: Throwable) extends PartialValueException(cause)

/**
 * Exception thrown when the underlying parser returns partial result list of parsing.
 * @param partialResults the partial result list of parsing bad records.
 * @param cause the actual exception about why the parser cannot return full result.
 */
case class PartialResultArrayException(
     partialResults: Array[InternalRow],
     cause: Throwable)
  extends Exception(cause)

/**
 * Exception thrown when the underlying parser met a bad record and can't parse it.
 * The stacktrace is not collected for better performance, and thus, this exception should
 * not be used in a user-facing context.
 * @param record a function to return the record that cause the parser to fail
 * @param partialResults a function that returns an row array, which is the partial results of
 *                      parsing this bad record.
 * @param cause the actual exception about why the record is bad and can't be parsed. It's better
 *                      to use `LazyBadRecordCauseWrapper` here to delay heavy cause construction
 *                      until it's needed.
 */
case class BadRecordException(
    @transient record: () => UTF8String,
    @transient partialResults: () => Array[InternalRow] = () => Array.empty[InternalRow],
    cause: Throwable) extends Exception(cause) {
  override def getStackTrace(): Array[StackTraceElement] = new Array[StackTraceElement](0)
  override def fillInStackTrace(): Throwable = this
}

/**
 * Exception to use as `BadRecordException` cause to delay heavy user-facing exception construction.
 * Does not contain stacktrace and used only for control flow
 */
case class LazyBadRecordCauseWrapper(cause: () => Throwable) extends Exception() {
  override def getStackTrace(): Array[StackTraceElement] = new Array[StackTraceElement](0)
  override def fillInStackTrace(): Throwable = this
}

/**
 * Exception thrown when the underlying parser parses a JSON array as a struct.
 */
case class JsonArraysAsStructsException() extends RuntimeException()

/**
 * Exception thrown when the underlying parser can not parses a String as a datatype.
 */
case class StringAsDataTypeException(
    fieldName: String,
    fieldValue: String,
    dataType: DataType) extends RuntimeException()

/**
 * No-stacktrace equivalent of `QueryExecutionErrors.cannotParseJSONFieldError`.
 * Used for code control flow in the parser without overhead of creating a full exception.
 */
case class CannotParseJSONFieldException(
    fieldName: String,
    fieldValue: String,
    jsonType: JsonToken,
    dataType: DataType) extends RuntimeException() {
  override def getStackTrace(): Array[StackTraceElement] = new Array[StackTraceElement](0)
  override def fillInStackTrace(): Throwable = this
}

/**
 * No-stacktrace equivalent of `QueryExecutionErrors.emptyJsonFieldValueError`.
 * Used for code control flow in the parser without overhead of creating a full exception.
 */
case class EmptyJsonFieldValueException(dataType: DataType) extends RuntimeException() {
  override def getStackTrace(): Array[StackTraceElement] = new Array[StackTraceElement](0)
  override def fillInStackTrace(): Throwable = this
}

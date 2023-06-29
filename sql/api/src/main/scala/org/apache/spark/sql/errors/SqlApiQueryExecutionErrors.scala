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

import org.apache.spark.{SparkArithmeticException, SparkException, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Object for grouping error messages from (most) exceptions thrown during query execution.
 * This does not include exceptions thrown during the eager execution of commands, which are
 * grouped into [[QueryCompilationErrors]].
 */
private[sql] object SqlApiQueryExecutionErrors {
  def unsupportedOperationExceptionError(): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "_LEGACY_ERROR_TEMP_2225",
      messageParameters = Map.empty)
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
}

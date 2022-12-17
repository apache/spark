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

package org.apache.spark.python.errors

import org.apache.spark._

/**
 * Object for grouping error messages from exceptions thrown by PySpark.
 */
private[python] object PySparkErrors {

  def columnInListError(funcName: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.COLUMN_IN_LIST",
      messageParameters = Map("funcName" -> funcName))
  }

  def higherOrderFunctionShouldReturnColumnError(
      funcName: String, returnType: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.HIGHER_ORDER_FUNCTION_SHOULD_RETURN_COLUMN",
      messageParameters = Map("funcName" -> funcName, "returnType" -> returnType))
  }

  def notColumnError(argName: String, argType: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.NOT_A_COLUMN",
      messageParameters = Map("argName" -> argName, "argType" -> argType))
  }

  def notStringError(argName: String, argType: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.NOT_A_STRING",
      messageParameters = Map("argName" -> argName, "argType" -> argType))
  }

  def notColumnOrIntegerError(argName: String, argType: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.NOT_COLUMN_OR_INTEGER",
      messageParameters = Map("argName" -> argName, "argType" -> argType))
  }

  def notColumnOrIntegerOrStringError(argName: String, argType: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.NOT_COLUMN_OR_INTEGER_OR_STRING",
      messageParameters = Map("argName" -> argName, "argType" -> argType))
  }

  def notColumnOrStringError(argName: String, argType: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.NOT_COLUMN_OR_STRING",
      messageParameters = Map("argName" -> argName, "argType" -> argType))
  }

  def unsupportedParamTypeForHigherOrderFunctionError(funcName: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION",
      messageParameters = Map("funcName" -> funcName))
  }

  def invalidNumberOfColumnsError(funcName: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.WRONG_NUM_COLUMNS",
      messageParameters = Map("funcName" -> funcName))
  }

  def invalidHigherOrderFunctionArgumentNumberError(
      funcName: String, numArgs: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "PYSPARK.WRONG_NUM_ARGS_FOR_HIGHER_ORDER_FUNCTION",
      messageParameters = Map("funcName" -> funcName, "numArgs" -> numArgs))
  }
}

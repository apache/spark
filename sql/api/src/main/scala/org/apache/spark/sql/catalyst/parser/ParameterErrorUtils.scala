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
package org.apache.spark.sql.catalyst.parser

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.SparkException

/**
 * Utility object for parameter-related error handling.
 * Centralizes common error messages and patterns used across parameter substitution.
 */
object ParameterErrorUtils {

  /**
   * Throws an internal error for parameter markers found in data type contexts.
   * This indicates that parameter substitution should have occurred before parsing.
   *
   * @param ctx The parser context containing the parameter marker
   * @throws SparkException.internalError Always throws this exception
   */
  def parameterMarkerInDataTypeError(ctx: ParserRuleContext): Nothing = {
    throw SparkException.internalError(
      s"Parameter marker '${ctx.getText}' found in data type context. " +
      "Parameter substitution should have occurred before reaching this point.")
  }

  /**
   * Throws an internal error for unexpected parameter markers in contexts where
   * they should have been substituted.
   *
   * @param parameterText The text of the parameter marker
   * @param context Description of the context where the parameter was found
   * @throws SparkException.internalError Always throws this exception
   */
  def unexpectedParameterMarkerError(parameterText: String, context: String): Nothing = {
    throw SparkException.internalError(
      s"Parameter marker '$parameterText' found in $context. " +
      "Parameter substitution should have occurred before reaching this point.")
  }
}
